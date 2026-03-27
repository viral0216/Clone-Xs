"""FinOps queries using Databricks system tables only.

Two-tier cache: local JSON file (survives restarts) → execute_sql_cached (in-memory).
All queries use 600s TTL. No ANALYZE TABLE, no DESCRIBE DETAIL, no SDK API calls.

System tables used:
- system.billing.usage + system.billing.list_prices → actual $ costs
- system.compute.clusters, .warehouses, .warehouse_events, .node_timeline
- system.query.history
- system.storage.predictive_optimization_operations_history
- {catalog}.information_schema.tables → table sizes
"""

import json
import logging
import time
from datetime import datetime, timedelta
from pathlib import Path

from databricks.sdk import WorkspaceClient

from src.client import execute_sql, execute_sql_cached

logger = logging.getLogger(__name__)

_CACHE_TTL = 600  # 10 minutes
_CACHE_DIR = Path(__file__).parent.parent / "config" / "cache"
_CACHE_DIR.mkdir(parents=True, exist_ok=True)


# ── Local file cache ─────────────────────────────────────────────────

def _cache_get(key: str, ttl: int = _CACHE_TTL):
    """Read from local JSON cache if fresh."""
    path = _CACHE_DIR / f"{key}.json"
    try:
        if path.exists():
            data = json.loads(path.read_text())
            if time.time() - data.get("_ts", 0) < ttl:
                logger.debug(f"Cache HIT: {key}")
                return data.get("result")
    except Exception:
        pass
    return None


def _cache_set(key: str, result):
    """Write to local JSON cache."""
    path = _CACHE_DIR / f"{key}.json"
    try:
        path.write_text(json.dumps({"_ts": time.time(), "result": result}, default=str))
    except Exception as e:
        logger.debug(f"Cache write failed for {key}: {e}")


def _safe_float(v) -> float:
    try:
        return float(v) if v is not None else 0.0
    except (ValueError, TypeError):
        return 0.0


def _safe_int(v) -> int:
    try:
        return int(float(v)) if v is not None else 0
    except (ValueError, TypeError):
        return 0


# ── Billing + Cost ───────────────────────────────────────────────────

def query_billing_cost(
    client: WorkspaceClient, warehouse_id: str, days: int = 30,
) -> dict:
    """Query billing.usage JOIN list_prices for actual dollar costs.

    Returns daily_trend, total_cost, total_dbus, by_sku, by_product, by_warehouse, by_user.
    """
    cache_key = f"finops_billing_{days}d"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")

    sql = f"""
        SELECT
            DATE(u.usage_date) AS date,
            u.sku_name AS sku,
            u.billing_origin_product AS product,
            u.usage_metadata.warehouse_id AS warehouse_id,
            u.identity_metadata.run_as AS run_as,
            SUM(u.usage_quantity) AS total_dbus,
            SUM(u.usage_quantity * COALESCE(p.pricing.effective_list.default, 0)) AS list_cost
        FROM system.billing.usage u
        LEFT JOIN system.billing.list_prices p
          ON u.sku_name = p.sku_name
          AND u.usage_unit = p.usage_unit
          AND p.price_start_time <= u.usage_date
          AND (p.price_end_time IS NULL OR p.price_end_time > u.usage_date)
        WHERE u.usage_date >= '{cutoff}'
        GROUP BY 1, 2, 3, 4, 5
        ORDER BY date DESC
    """

    try:
        rows = execute_sql_cached(client, warehouse_id, sql, ttl=_CACHE_TTL)
    except Exception as e:
        logger.warning(f"Billing cost query failed: {e}")
        return {"error": str(e), "daily_trend": [], "total_cost": 0, "total_dbus": 0,
                "by_sku": [], "by_product": [], "by_warehouse": [], "by_user": []}

    # Aggregate rows into breakdowns
    daily = {}
    sku_map = {}
    product_map = {}
    warehouse_map = {}
    user_map = {}
    total_cost = 0.0
    total_dbus = 0.0

    for r in rows:
        date = str(r.get("date", ""))[:10]
        dbus = _safe_float(r.get("total_dbus"))
        cost = _safe_float(r.get("list_cost"))
        sku = r.get("sku") or "Unknown"
        product = r.get("product") or "Unknown"
        wh = r.get("warehouse_id") or ""
        user = r.get("run_as") or "Unknown"

        total_cost += cost
        total_dbus += dbus

        if date:
            d = daily.setdefault(date, {"date": date, "cost": 0, "dbus": 0})
            d["cost"] += cost
            d["dbus"] += dbus

        s = sku_map.setdefault(sku, {"sku": sku, "cost": 0, "dbus": 0})
        s["cost"] += cost
        s["dbus"] += dbus

        p = product_map.setdefault(product, {"product": product, "cost": 0, "dbus": 0})
        p["cost"] += cost
        p["dbus"] += dbus

        if wh:
            w = warehouse_map.setdefault(wh, {"warehouse_id": wh, "cost": 0, "dbus": 0})
            w["cost"] += cost
            w["dbus"] += dbus

        u = user_map.setdefault(user, {"user": user, "cost": 0, "dbus": 0})
        u["cost"] += cost
        u["dbus"] += dbus

    # Round values
    for m in [daily, sku_map, product_map, warehouse_map, user_map]:
        for v in (m.values() if isinstance(m, dict) else []):
            if isinstance(v, dict):
                v["cost"] = round(v.get("cost", 0), 2)
                v["dbus"] = round(v.get("dbus", 0), 2)

    result = {
        "daily_trend": sorted(daily.values(), key=lambda d: d["date"]),
        "total_cost": round(total_cost, 2),
        "total_dbus": round(total_dbus, 2),
        "avg_daily_cost": round(total_cost / max(len(daily), 1), 2),
        "by_sku": sorted(sku_map.values(), key=lambda x: x["cost"], reverse=True),
        "by_product": sorted(product_map.values(), key=lambda x: x["cost"], reverse=True),
        "by_warehouse": sorted(warehouse_map.values(), key=lambda x: x["cost"], reverse=True),
        "by_user": sorted(user_map.values(), key=lambda x: x["cost"], reverse=True)[:20],
        "days": days,
        "currency": "USD",
    }

    _cache_set(cache_key, result)
    return result


# ── Warehouses ───────────────────────────────────────────────────────

def query_warehouses(client: WorkspaceClient, warehouse_id: str) -> dict:
    """Query system.compute.warehouses for latest state of each warehouse."""
    cache_key = "finops_warehouses"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    sql = """
        SELECT w.*
        FROM system.compute.warehouses w
        INNER JOIN (
            SELECT warehouse_id, MAX(change_time) AS max_time
            FROM system.compute.warehouses
            GROUP BY warehouse_id
        ) latest ON w.warehouse_id = latest.warehouse_id AND w.change_time = latest.max_time
        WHERE w.delete_time IS NULL
        ORDER BY w.warehouse_name
    """

    try:
        rows = execute_sql_cached(client, warehouse_id, sql, ttl=_CACHE_TTL)
    except Exception as e:
        logger.warning(f"Warehouses query failed: {e}")
        return {"warehouses": [], "summary": {}, "warnings": [], "error": str(e)}

    warehouses = []
    warnings = []
    running = 0
    stopped = 0

    for r in rows:
        wh = {
            "warehouse_id": r.get("warehouse_id", ""),
            "name": r.get("warehouse_name", ""),
            "type": r.get("warehouse_type", ""),
            "size": r.get("warehouse_size", ""),
            "min_clusters": _safe_int(r.get("min_clusters")),
            "max_clusters": _safe_int(r.get("max_clusters")),
            "auto_stop_minutes": _safe_int(r.get("auto_stop_minutes")),
            "tags": r.get("tags") or {},
            "change_time": str(r.get("change_time", "")),
        }
        warehouses.append(wh)

        if wh["auto_stop_minutes"] == 0:
            warnings.append({
                "warehouse_id": wh["warehouse_id"],
                "name": wh["name"],
                "severity": "warning",
                "message": f"Warehouse '{wh['name']}' has auto-stop disabled — may incur idle costs",
            })
        if wh["auto_stop_minutes"] > 120:
            warnings.append({
                "warehouse_id": wh["warehouse_id"],
                "name": wh["name"],
                "severity": "info",
                "message": f"Warehouse '{wh['name']}' auto-stop is {wh['auto_stop_minutes']}m — consider reducing to save costs",
            })

    result = {
        "warehouses": warehouses,
        "summary": {"total": len(warehouses), "running": running, "stopped": stopped},
        "warnings": warnings,
    }

    _cache_set(cache_key, result)
    return result


# ── Warehouse Events ─────────────────────────────────────────────────

def query_warehouse_events(
    client: WorkspaceClient, warehouse_id: str, days: int = 7,
) -> list[dict]:
    """Query system.compute.warehouse_events for start/stop/scale events."""
    cache_key = f"finops_wh_events_{days}d"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")

    sql = f"""
        SELECT
            warehouse_id,
            event_type,
            event_time,
            cluster_count
        FROM system.compute.warehouse_events
        WHERE event_time >= '{cutoff}'
        ORDER BY event_time DESC
        LIMIT 500
    """

    try:
        rows = execute_sql_cached(client, warehouse_id, sql, ttl=_CACHE_TTL)
    except Exception as e:
        logger.warning(f"Warehouse events query failed: {e}")
        return []

    result = [{
        "warehouse_id": r.get("warehouse_id", ""),
        "event_type": r.get("event_type", ""),
        "event_time": str(r.get("event_time", "")),
        "cluster_count": _safe_int(r.get("cluster_count")),
    } for r in rows]

    _cache_set(cache_key, result)
    return result


# ── Clusters ─────────────────────────────────────────────────────────

def query_clusters(client: WorkspaceClient, warehouse_id: str) -> dict:
    """Query system.compute.clusters for latest state of each cluster."""
    cache_key = "finops_clusters"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    sql = """
        SELECT c.*
        FROM system.compute.clusters c
        INNER JOIN (
            SELECT cluster_id, MAX(change_time) AS max_time
            FROM system.compute.clusters
            GROUP BY cluster_id
        ) latest ON c.cluster_id = latest.cluster_id AND c.change_time = latest.max_time
        WHERE c.delete_time IS NULL
        ORDER BY c.cluster_name
    """

    try:
        rows = execute_sql_cached(client, warehouse_id, sql, ttl=_CACHE_TTL)
    except Exception as e:
        logger.warning(f"Clusters query failed: {e}")
        return {"clusters": [], "summary": {}, "error": str(e)}

    clusters = []
    for r in rows:
        clusters.append({
            "cluster_id": r.get("cluster_id", ""),
            "cluster_name": r.get("cluster_name", ""),
            "owned_by": r.get("owned_by", ""),
            "driver_node_type": r.get("driver_node_type", ""),
            "worker_node_type": r.get("worker_node_type", ""),
            "worker_count": _safe_int(r.get("worker_count")),
            "min_autoscale_workers": _safe_int(r.get("min_autoscale_workers")),
            "max_autoscale_workers": _safe_int(r.get("max_autoscale_workers")),
            "auto_termination_minutes": _safe_int(r.get("auto_termination_minutes")),
            "dbr_version": r.get("dbr_version", ""),
            "cluster_source": r.get("cluster_source", ""),
            "change_time": str(r.get("change_time", "")),
        })

    result = {
        "clusters": clusters,
        "summary": {"total": len(clusters)},
    }

    _cache_set(cache_key, result)
    return result


# ── Node Utilization ─────────────────────────────────────────────────

def query_node_utilization(
    client: WorkspaceClient, warehouse_id: str, days: int = 7,
) -> list[dict]:
    """Query system.compute.node_timeline for CPU/memory utilization.

    Max 90 days due to system table retention.
    """
    cache_key = f"finops_node_util_{days}d"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    days = min(days, 90)
    cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")

    sql = f"""
        SELECT
            cluster_id,
            DATE(start_time) AS date,
            ROUND(AVG(cpu_user_percent + cpu_system_percent), 1) AS avg_cpu_pct,
            ROUND(AVG(mem_used_percent), 1) AS avg_mem_pct,
            ROUND(MAX(cpu_user_percent + cpu_system_percent), 1) AS max_cpu_pct,
            ROUND(MAX(mem_used_percent), 1) AS max_mem_pct,
            COUNT(*) AS sample_count
        FROM system.compute.node_timeline
        WHERE start_time >= '{cutoff}'
        GROUP BY cluster_id, DATE(start_time)
        ORDER BY date DESC
    """

    try:
        rows = execute_sql_cached(client, warehouse_id, sql, ttl=_CACHE_TTL)
    except Exception as e:
        logger.warning(f"Node utilization query failed: {e}")
        return []

    result = [{
        "cluster_id": r.get("cluster_id", ""),
        "date": str(r.get("date", ""))[:10],
        "avg_cpu_pct": _safe_float(r.get("avg_cpu_pct")),
        "avg_mem_pct": _safe_float(r.get("avg_mem_pct")),
        "max_cpu_pct": _safe_float(r.get("max_cpu_pct")),
        "max_mem_pct": _safe_float(r.get("max_mem_pct")),
        "sample_count": _safe_int(r.get("sample_count")),
    } for r in rows]

    _cache_set(cache_key, result)
    return result


# ── Query Stats ──────────────────────────────────────────────────────

def query_query_stats(
    client: WorkspaceClient, warehouse_id: str, days: int = 30,
) -> dict:
    """Query system.query.history for performance stats.

    Uses dynamic column detection (schema varies across workspaces).
    """
    cache_key = f"finops_query_stats_{days}d"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")

    # Probe columns
    try:
        probe = execute_sql(client, warehouse_id, "SELECT * FROM system.query.history LIMIT 1")
        cols = set(probe[0].keys()) if probe else set()
    except Exception as e:
        logger.warning(f"system.query.history not available: {e}")
        return {"summary": {}, "by_warehouse": [], "by_user": [], "slowest": [], "error": str(e)}

    def pick(preferred, default="NULL"):
        for c in preferred:
            if c in cols:
                return c
        return default

    col_id = pick(["statement_id", "query_id"])
    col_text = pick(["statement_text", "query_text"])
    col_user = pick(["executed_by", "user_name"])
    col_wh = pick(["warehouse_id", "compute_id"])
    col_status = pick(["status", "execution_status"])
    col_dur = pick(["total_duration_ms", "total_time_ms", "duration_ms"])
    col_read = pick(["read_bytes", "read_io_bytes"])
    col_start = pick(["start_time", "query_start_time"])

    # Summary
    summary_sql = f"""
        SELECT
            COUNT(*) AS total_queries,
            ROUND(AVG({col_dur})) AS avg_duration_ms,
            ROUND(PERCENTILE({col_dur}, 0.95)) AS p95_duration_ms,
            SUM({col_read}) AS total_read_bytes
        FROM system.query.history
        WHERE {col_start} >= '{cutoff}'
    """

    # By warehouse
    wh_sql = f"""
        SELECT
            {col_wh} AS warehouse_id,
            COUNT(*) AS query_count,
            ROUND(AVG({col_dur})) AS avg_duration_ms,
            ROUND(PERCENTILE({col_dur}, 0.95)) AS p95_duration_ms,
            SUM({col_read}) AS total_read_bytes
        FROM system.query.history
        WHERE {col_start} >= '{cutoff}'
        GROUP BY {col_wh}
        ORDER BY query_count DESC
    """

    # By user
    user_sql = f"""
        SELECT
            {col_user} AS user_name,
            COUNT(*) AS query_count,
            ROUND(AVG({col_dur})) AS avg_duration_ms,
            SUM({col_read}) AS total_read_bytes
        FROM system.query.history
        WHERE {col_start} >= '{cutoff}'
        GROUP BY {col_user}
        ORDER BY query_count DESC
        LIMIT 20
    """

    # Slowest queries
    slow_sql = f"""
        SELECT
            {col_id} AS query_id,
            SUBSTRING({col_text}, 1, 200) AS query_text,
            {col_user} AS user_name,
            {col_wh} AS warehouse_id,
            {col_dur} AS total_duration_ms,
            {col_read} AS read_bytes,
            {col_start} AS start_time
        FROM system.query.history
        WHERE {col_start} >= '{cutoff}'
        ORDER BY {col_dur} DESC
        LIMIT 50
    """

    try:
        summary = execute_sql_cached(client, warehouse_id, summary_sql, ttl=_CACHE_TTL)
        by_warehouse = execute_sql_cached(client, warehouse_id, wh_sql, ttl=_CACHE_TTL)
        by_user = execute_sql_cached(client, warehouse_id, user_sql, ttl=_CACHE_TTL)
        slowest = execute_sql_cached(client, warehouse_id, slow_sql, ttl=_CACHE_TTL)
    except Exception as e:
        logger.warning(f"Query stats queries failed: {e}")
        return {"summary": {}, "by_warehouse": [], "by_user": [], "slowest": [], "error": str(e)}

    result = {
        "summary": summary[0] if summary else {},
        "by_warehouse": by_warehouse or [],
        "by_user": by_user or [],
        "slowest": slowest or [],
    }

    _cache_set(cache_key, result)
    return result


# ── Storage (information_schema) ─────────────────────────────────────

def query_storage(
    client: WorkspaceClient, warehouse_id: str, catalog: str,
) -> dict:
    """Query {catalog}.information_schema.tables for table sizes.

    Single SQL query — no per-table DESCRIBE DETAIL or ANALYZE TABLE.
    """
    cache_key = f"finops_storage_{catalog}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    sql = f"""
        SELECT
            table_schema,
            table_name,
            table_type,
            COALESCE(CAST(data_size_bytes AS BIGINT), 0) AS total_bytes,
            last_altered
        FROM `{catalog}`.information_schema.tables
        WHERE table_type IN ('MANAGED', 'EXTERNAL')
          AND table_schema NOT IN ('information_schema')
        ORDER BY total_bytes DESC
    """

    try:
        rows = execute_sql_cached(client, warehouse_id, sql, ttl=_CACHE_TTL)
        if not rows or not isinstance(rows, list):
            rows = []
    except Exception as e:
        logger.warning(f"Storage info_schema query failed: {e}")
        return {"catalog": catalog, "tables": [], "schema_summaries": [],
                "total_bytes": 0, "num_tables": 0, "error": str(e)}

    tables = []
    schema_map = {}
    total_bytes = 0

    for r in rows:
        size = _safe_int(r.get("total_bytes"))
        table = {
            "schema": r.get("table_schema", ""),
            "table": r.get("table_name", ""),
            "table_name": f"{r.get('table_schema', '')}.{r.get('table_name', '')}",
            "total_bytes": size,
            "last_altered": str(r.get("last_altered", "")),
        }
        tables.append(table)
        total_bytes += size

        sm = schema_map.setdefault(r.get("table_schema", ""), {"total_bytes": 0, "num_tables": 0})
        sm["total_bytes"] += size
        sm["num_tables"] += 1

    schema_summaries = [
        {"schema": k, "total_bytes": v["total_bytes"], "num_tables": v["num_tables"]}
        for k, v in sorted(schema_map.items(), key=lambda x: x[1]["total_bytes"], reverse=True)
    ]

    result = {
        "catalog": catalog,
        "num_schemas": len(schema_map),
        "num_tables": len(tables),
        "total_bytes": total_bytes,
        "schema_summaries": schema_summaries,
        "tables": tables,
        "top_tables": tables[:20],
    }

    _cache_set(cache_key, result)
    return result


# ── Recommendations ──────────────────────────────────────────────────

def query_recommendations(
    client: WorkspaceClient, warehouse_id: str, catalog: str = "",
) -> dict:
    """Combined recommendations from predictive optimization + warehouse warnings."""
    cache_key = f"finops_recommendations_{catalog or 'all'}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    recommendations = []
    optimization_ops = []

    # 1. Predictive optimization
    catalog_filter = f"AND catalog_name = '{catalog}'" if catalog else ""
    opt_sql = f"""
        SELECT
            CONCAT(catalog_name, '.', schema_name, '.', table_name) AS table_fqn,
            operation_type AS recommendation_type,
            operation_status,
            usage_unit,
            start_time AS last_checked
        FROM system.storage.predictive_optimization_operations_history
        WHERE start_time >= CURRENT_DATE() - INTERVAL 7 DAYS
          {catalog_filter}
        ORDER BY start_time DESC
    """
    try:
        optimization_ops = execute_sql_cached(client, warehouse_id, opt_sql, ttl=_CACHE_TTL) or []
    except Exception as e:
        logger.debug(f"Predictive optimization query failed: {e}")

    # 2. Warehouse warnings
    wh_data = query_warehouses(client, warehouse_id)
    recommendations.extend(wh_data.get("warnings", []))

    # 3. Node utilization warnings (underutilized clusters)
    try:
        util = query_node_utilization(client, warehouse_id, days=7)
        # Group by cluster, find those with low avg CPU
        cluster_avg = {}
        for r in util:
            cid = r["cluster_id"]
            ca = cluster_avg.setdefault(cid, {"samples": 0, "cpu_sum": 0, "mem_sum": 0})
            ca["samples"] += 1
            ca["cpu_sum"] += r["avg_cpu_pct"]
            ca["mem_sum"] += r["avg_mem_pct"]

        for cid, ca in cluster_avg.items():
            avg_cpu = ca["cpu_sum"] / max(ca["samples"], 1)
            avg_mem = ca["mem_sum"] / max(ca["samples"], 1)
            if avg_cpu < 15 and avg_mem < 25:
                recommendations.append({
                    "cluster_id": cid,
                    "severity": "warning",
                    "message": f"Cluster {cid[:12]}... is underutilized (avg CPU {avg_cpu:.0f}%, mem {avg_mem:.0f}%) — consider downsizing",
                })
    except Exception:
        pass

    result = {
        "recommendations": recommendations,
        "optimization_ops": optimization_ops,
        "total_recommendations": len(recommendations),
        "total_optimization_ops": len(optimization_ops),
    }

    _cache_set(cache_key, result)
    return result


# ── Cost per Query ───────────────────────────────────────────────────

def query_cost_per_query(
    client: WorkspaceClient, warehouse_id: str, days: int = 30,
) -> dict:
    """Attribute cost to individual queries, excluding idle warehouse time.

    Only hours with actual query execution get attributed to queries.
    Idle hours (warehouse running, no queries) are reported separately.
    """
    cache_key = f"finops_query_costs_{days}d"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")

    # 1. Query cost attribution (only active hours via INNER JOIN)
    sql = f"""
        WITH hourly_warehouse_cost AS (
            SELECT
                DATE_TRUNC('HOUR', u.usage_start_time) AS hour,
                u.usage_metadata.warehouse_id AS warehouse_id,
                SUM(u.usage_quantity * COALESCE(p.pricing.effective_list.default, 0)) AS hour_cost,
                SUM(u.usage_quantity) AS hour_dbus
            FROM system.billing.usage u
            LEFT JOIN system.billing.list_prices p
              ON u.sku_name = p.sku_name AND u.usage_unit = p.usage_unit
              AND p.price_start_time <= u.usage_date
              AND (p.price_end_time IS NULL OR p.price_end_time > u.usage_date)
            WHERE u.usage_date >= '{cutoff}'
              AND u.usage_metadata.warehouse_id IS NOT NULL
            GROUP BY 1, 2
        ),
        query_durations AS (
            SELECT
                statement_id,
                SUBSTRING(statement_text, 1, 100) AS query_text,
                executed_by,
                compute.warehouse_id AS warehouse_id,
                start_time,
                total_duration_ms,
                execution_duration_ms,
                execution_status AS status,
                statement_type,
                read_bytes,
                produced_rows,
                DATE_TRUNC('HOUR', start_time) AS hour,
                -- Use execution_duration preferentially; fall back to total_duration; cap at 1 hour
                -- Minimum 1ms so every query gets some attribution (no zero-division)
                GREATEST(LEAST(COALESCE(execution_duration_ms, total_duration_ms, 1), 3600000), 1) AS effective_duration_ms
            FROM system.query.history
            WHERE start_time >= '{cutoff}'
              AND compute.warehouse_id IS NOT NULL
        ),
        hourly_total_exec AS (
            SELECT hour, warehouse_id, SUM(effective_duration_ms) AS total_exec_ms
            FROM query_durations
            GROUP BY 1, 2
        ),
        costed AS (
            SELECT
                q.statement_id,
                q.query_text,
                q.executed_by,
                q.warehouse_id,
                q.start_time,
                q.total_duration_ms,
                q.execution_duration_ms,
                q.status,
                q.statement_type,
                q.read_bytes,
                q.produced_rows,
                ROUND(COALESCE(wc.hour_cost, 0) * (q.effective_duration_ms / NULLIF(hte.total_exec_ms, 0)), 4) AS estimated_cost,
                ROUND(COALESCE(wc.hour_dbus, 0) * (q.effective_duration_ms / NULLIF(hte.total_exec_ms, 0)), 4) AS estimated_dbus
            FROM query_durations q
            LEFT JOIN hourly_warehouse_cost wc ON q.hour = wc.hour AND q.warehouse_id = wc.warehouse_id
            LEFT JOIN hourly_total_exec hte ON q.hour = hte.hour AND q.warehouse_id = hte.warehouse_id
        )
        SELECT * FROM costed
        ORDER BY estimated_cost DESC NULLS LAST
    """

    # 2. Idle cost summary (hours with warehouse cost but no queries)
    idle_sql = f"""
        WITH hourly_warehouse_cost AS (
            SELECT
                DATE_TRUNC('HOUR', u.usage_start_time) AS hour,
                u.usage_metadata.warehouse_id AS warehouse_id,
                SUM(u.usage_quantity * COALESCE(p.pricing.effective_list.default, 0)) AS hour_cost
            FROM system.billing.usage u
            LEFT JOIN system.billing.list_prices p
              ON u.sku_name = p.sku_name AND u.usage_unit = p.usage_unit
              AND p.price_start_time <= u.usage_date
              AND (p.price_end_time IS NULL OR p.price_end_time > u.usage_date)
            WHERE u.usage_date >= '{cutoff}'
              AND u.usage_metadata.warehouse_id IS NOT NULL
            GROUP BY 1, 2
        ),
        active_hours AS (
            SELECT DISTINCT DATE_TRUNC('HOUR', start_time) AS hour, compute.warehouse_id AS warehouse_id
            FROM system.query.history
            WHERE start_time >= '{cutoff}'
              AND compute.warehouse_id IS NOT NULL
        )
        SELECT
            SUM(wc.hour_cost) AS total_warehouse_cost,
            SUM(CASE WHEN ah.hour IS NOT NULL THEN wc.hour_cost ELSE 0 END) AS active_cost,
            SUM(CASE WHEN ah.hour IS NULL THEN wc.hour_cost ELSE 0 END) AS idle_cost,
            COUNT(*) AS total_hours,
            COUNT(ah.hour) AS active_hours,
            COUNT(*) - COUNT(ah.hour) AS idle_hours
        FROM hourly_warehouse_cost wc
        LEFT JOIN active_hours ah ON wc.hour = ah.hour AND wc.warehouse_id = ah.warehouse_id
    """

    try:
        rows = execute_sql_cached(client, warehouse_id, sql, ttl=_CACHE_TTL)
        if not rows or not isinstance(rows, list):
            rows = []
    except Exception as e:
        logger.warning(f"Query cost attribution failed: {e}")
        return {"queries": [], "summary": {}, "by_user": [], "by_statement_type": [], "idle": {}, "error": str(e)}

    # Get idle cost breakdown
    idle = {"idle_cost": 0, "active_cost": 0, "total_warehouse_cost": 0, "idle_hours": 0, "active_hours": 0, "total_hours": 0}
    try:
        idle_rows = execute_sql_cached(client, warehouse_id, idle_sql, ttl=_CACHE_TTL)
        if idle_rows and isinstance(idle_rows, list) and idle_rows[0]:
            r = idle_rows[0]
            idle = {
                "idle_cost": round(_safe_float(r.get("idle_cost")), 2),
                "active_cost": round(_safe_float(r.get("active_cost")), 2),
                "total_warehouse_cost": round(_safe_float(r.get("total_warehouse_cost")), 2),
                "idle_hours": _safe_int(r.get("idle_hours")),
                "active_hours": _safe_int(r.get("active_hours")),
                "total_hours": _safe_int(r.get("total_hours")),
            }
    except Exception as e:
        logger.debug(f"Idle cost query failed: {e}")

    # Build aggregations
    total_cost = 0.0
    user_map = {}
    stmt_map = {}
    queries = []

    for r in rows:
        cost = _safe_float(r.get("estimated_cost"))
        dbus = _safe_float(r.get("estimated_dbus"))
        user = r.get("executed_by") or "Unknown"
        stmt_type = r.get("statement_type") or "OTHER"
        total_cost += cost

        queries.append({
            "statement_id": r.get("statement_id", ""),
            "query_text": r.get("query_text", ""),
            "executed_by": user,
            "warehouse_id": r.get("warehouse_id", ""),
            "start_time": str(r.get("start_time", "")),
            "total_duration_ms": _safe_int(r.get("total_duration_ms")),
            "execution_duration_ms": _safe_int(r.get("execution_duration_ms")),
            "status": r.get("status", ""),
            "statement_type": stmt_type,
            "read_bytes": _safe_int(r.get("read_bytes")),
            "produced_rows": _safe_int(r.get("produced_rows")),
            "estimated_cost": round(cost, 4),
            "estimated_dbus": round(dbus, 4),
        })

        u = user_map.setdefault(user, {"user": user, "cost": 0, "count": 0})
        u["cost"] += cost
        u["count"] += 1

        s = stmt_map.setdefault(stmt_type, {"statement_type": stmt_type, "cost": 0, "count": 0})
        s["cost"] += cost
        s["count"] += 1

    for m in [user_map, stmt_map]:
        for v in m.values():
            v["cost"] = round(v["cost"], 2)

    result = {
        "queries": queries,
        "summary": {
            "total_cost": round(total_cost, 2),
            "total_queries": len(queries),
            "avg_cost_per_query": round(total_cost / max(len(queries), 1), 4),
        },
        "idle": idle,
        "by_user": sorted(user_map.values(), key=lambda x: x["cost"], reverse=True),
        "by_statement_type": sorted(stmt_map.values(), key=lambda x: x["cost"], reverse=True),
        "days": days,
    }

    _cache_set(cache_key, result)
    return result


# ── Cost per Job ─────────────────────────────────────────────────────

def query_cost_per_job(
    client: WorkspaceClient, warehouse_id: str, days: int = 30,
) -> dict:
    """Attribute cost to jobs using billing.usage where job_id IS NOT NULL."""
    cache_key = f"finops_job_costs_{days}d"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")

    sql = f"""
        SELECT
            u.usage_metadata.job_id AS job_id,
            FIRST(u.usage_metadata.job_name, TRUE) AS job_name,
            u.identity_metadata.run_as AS run_as,
            u.billing_origin_product AS product,
            COUNT(DISTINCT u.usage_date) AS active_days,
            SUM(u.usage_quantity) AS total_dbus,
            SUM(u.usage_quantity * COALESCE(p.pricing.effective_list.default, 0)) AS total_cost,
            MIN(u.usage_date) AS first_run,
            MAX(u.usage_date) AS last_run
        FROM system.billing.usage u
        LEFT JOIN system.billing.list_prices p
          ON u.sku_name = p.sku_name AND u.usage_unit = p.usage_unit
          AND p.price_start_time <= u.usage_date
          AND (p.price_end_time IS NULL OR p.price_end_time > u.usage_date)
        WHERE u.usage_date >= '{cutoff}'
          AND u.usage_metadata.job_id IS NOT NULL
        GROUP BY 1, 3, 4
        ORDER BY total_cost DESC
        LIMIT 200
    """

    try:
        rows = execute_sql_cached(client, warehouse_id, sql, ttl=_CACHE_TTL)
        if not rows or not isinstance(rows, list):
            rows = []
    except Exception as e:
        logger.warning(f"Job cost attribution failed: {e}")
        return {"jobs": [], "summary": {}, "by_user": [], "by_product": [], "error": str(e)}

    total_cost = 0.0
    user_map = {}
    product_map = {}
    jobs = []

    for r in rows:
        cost = _safe_float(r.get("total_cost"))
        dbus = _safe_float(r.get("total_dbus"))
        user = r.get("run_as") or "Unknown"
        product = r.get("product") or "Unknown"
        total_cost += cost

        jobs.append({
            "job_id": r.get("job_id", ""),
            "job_name": r.get("job_name") or r.get("job_id", ""),
            "run_as": user,
            "product": product,
            "active_days": _safe_int(r.get("active_days")),
            "total_dbus": round(dbus, 2),
            "total_cost": round(cost, 2),
            "first_run": str(r.get("first_run", "")),
            "last_run": str(r.get("last_run", "")),
        })

        u = user_map.setdefault(user, {"user": user, "cost": 0, "count": 0})
        u["cost"] += cost
        u["count"] += 1

        p = product_map.setdefault(product, {"product": product, "cost": 0, "count": 0})
        p["cost"] += cost
        p["count"] += 1

    for m in [user_map, product_map]:
        for v in m.values():
            v["cost"] = round(v["cost"], 2)

    result = {
        "jobs": jobs,
        "summary": {
            "total_cost": round(total_cost, 2),
            "total_jobs": len(jobs),
            "avg_cost_per_job": round(total_cost / max(len(jobs), 1), 2),
        },
        "by_user": sorted(user_map.values(), key=lambda x: x["cost"], reverse=True),
        "by_product": sorted(product_map.values(), key=lambda x: x["cost"], reverse=True),
        "days": days,
    }

    _cache_set(cache_key, result)
    return result


# ── System Table Access Check ────────────────────────────────────────

def check_system_tables(client: WorkspaceClient, warehouse_id: str) -> dict:
    """Probe which system tables are accessible."""
    cache_key = "finops_system_status"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    tables = {
        "billing_usage": "system.billing.usage",
        "list_prices": "system.billing.list_prices",
        "compute_clusters": "system.compute.clusters",
        "compute_warehouses": "system.compute.warehouses",
        "warehouse_events": "system.compute.warehouse_events",
        "node_timeline": "system.compute.node_timeline",
        "query_history": "system.query.history",
        "predictive_optimization": "system.storage.predictive_optimization_operations_history",
    }

    result = {}
    for key, table in tables.items():
        try:
            execute_sql(client, warehouse_id, f"SELECT 1 FROM {table} LIMIT 1", max_retries=1)
            result[key] = True
        except Exception:
            result[key] = False

    _cache_set(cache_key, result)
    return result
