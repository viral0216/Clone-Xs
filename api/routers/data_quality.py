"""Data Quality observability endpoints — freshness, anomalies, volume, suites, incidents, health score."""

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from api.dependencies import get_db_client, get_app_config

router = APIRouter()


# ── Request / Response Models ────────────────────────────────────────────────

class RecordMetricRequest(BaseModel):
    table_fqn: str
    column_name: str = ""
    metric_name: str
    value: float


class CreateSuiteRequest(BaseModel):
    name: str
    description: str = ""
    checks: list[dict] = Field(default=[])


class VolumeSnapshotRequest(BaseModel):
    catalog: str
    schema_name: str = ""


# ── Freshness ────────────────────────────────────────────────────────────────

@router.get("/freshness/{catalog}", summary="Check table freshness for a catalog")
async def freshness_check(
    catalog: str,
    schema: Optional[str] = Query(default=None, description="Optional schema filter"),
    max_stale_hours: int = Query(default=24, ge=1),
    client=Depends(get_db_client),
):
    """Check freshness of all tables in a catalog. Flags tables not updated within max_stale_hours."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")

    from src.data_freshness import check_freshness
    return check_freshness(
        client, catalog, schema=schema,
        max_stale_hours=max_stale_hours, warehouse_id=wid, config=config,
    )


@router.get(
    "/freshness/{catalog}/{schema}/{table}/history",
    summary="Get freshness history for a table",
)
async def freshness_history(
    catalog: str,
    schema: str,
    table: str,
    limit: int = Query(default=100, ge=1, le=1000),
    client=Depends(get_db_client),
):
    """Get historical freshness snapshots for a specific table."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    table_fqn = f"{catalog}.{schema}.{table}"

    from src.data_freshness import get_freshness_history
    return get_freshness_history(table_fqn, client=client, warehouse_id=wid, config=config, limit=limit)


# ── Anomalies ────────────────────────────────────────────────────────────────

@router.get("/anomalies", summary="List recent anomalies")
async def list_anomalies(
    limit: int = Query(default=50, ge=1, le=500),
    severity: Optional[str] = Query(default=None, description="Filter: 'warning' or 'critical'"),
    client=Depends(get_db_client),
):
    """Get recent anomalies detected in DQ metrics."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")

    from src.anomaly_detection import get_anomalies
    anomalies = get_anomalies(client=client, warehouse_id=wid, config=config, limit=limit, severity=severity)
    return {"anomalies": anomalies, "total": len(anomalies)}


@router.get("/anomalies/metrics/{table_fqn:path}", summary="Get metric history with baselines")
async def metric_history(
    table_fqn: str,
    metric_name: str = Query(..., description="Metric name to query"),
    limit: int = Query(default=100, ge=1, le=1000),
    client=Depends(get_db_client),
):
    """Get historical values for a metric with baseline bands (mean, warning, critical)."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")

    from src.anomaly_detection import get_metric_history
    return get_metric_history(table_fqn, metric_name, client=client, warehouse_id=wid, config=config, limit=limit)


@router.get("/metrics/recent", summary="List recent metric measurements")
async def recent_metrics(
    limit: int = Query(default=50, ge=1, le=500),
    client=Depends(get_db_client),
):
    """Get recent metric measurements (all, not just anomalies) for the dashboard."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")

    from src.anomaly_detection import _get_schema, _query_sql
    import logging
    schema = _get_schema(config)
    try:
        rows = _query_sql(
            f"""SELECT id, table_fqn, column_name, metric_name, value,
                       measured_at, baseline_mean, baseline_stddev, z_score,
                       is_anomaly, severity
                FROM {schema}.metric_baselines
                ORDER BY measured_at DESC""",
            limit=limit, client=client, warehouse_id=wid,
        )
        return {"metrics": rows, "total": len(rows)}
    except Exception as e:
        err_str = str(e)
        logging.getLogger(__name__).warning(f"Could not load recent metrics: {err_str}")
        hint = None
        if "TABLE_OR_VIEW_NOT_FOUND" in err_str:
            hint = "Table not found. Go to Settings → Audit & Logs → Initialize Tables to create required tables."
        return {"metrics": [], "total": 0, "hint": hint}


@router.post("/anomalies/record", summary="Record a metric measurement")
async def record_metric(req: RecordMetricRequest, client=Depends(get_db_client)):
    """Record a DQ metric measurement and auto-detect anomalies.

    Computes z-score against the rolling baseline. Returns the stored record
    including anomaly classification (normal / warning / critical).
    """
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")

    from src.anomaly_detection import record_metric as _record
    return _record(
        table_fqn=req.table_fqn,
        column_name=req.column_name,
        metric_name=req.metric_name,
        value=req.value,
        client=client,
        warehouse_id=wid,
        config=config,
    )


# ── Volume ───────────────────────────────────────────────────────────────────

@router.get("/volume/{catalog}", summary="Get row counts for all tables in a catalog")
async def volume_overview(
    catalog: str,
    schema: Optional[str] = Query(default=None),
    client=Depends(get_db_client),
):
    """Query row counts for all tables in a catalog using information_schema or table stats."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")

    from src.data_freshness import _query_sql, _esc

    schema_filter = ""
    if schema:
        schema_filter = f"AND table_schema = '{_esc(schema)}'"

    # Step 1: List tables with size metadata (fast — no data scan)
    list_query = f"""
        SELECT table_catalog, table_schema, table_name
        FROM `{_esc(catalog)}`.information_schema.tables
        WHERE table_type = 'MANAGED'
          AND table_schema NOT IN ('information_schema', 'default')
          {schema_filter}
        ORDER BY table_schema, table_name
    """
    try:
        tables = _query_sql(list_query, limit=5000, client=client, warehouse_id=wid)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not list tables: {e}")

    # Step 2: Get row counts + size in parallel using COUNT(*) and DESCRIBE DETAIL
    import asyncio
    from concurrent.futures import ThreadPoolExecutor

    def _get_table_stats(tbl):
        table_fqn = f"{tbl['table_catalog']}.{tbl['table_schema']}.{tbl['table_name']}"
        row_count = None
        size_bytes = None
        last_modified = None
        try:
            rows = _query_sql(
                f"SELECT COUNT(*) AS cnt FROM {table_fqn}", limit=1,
                client=client, warehouse_id=wid,
            )
            if rows:
                row_count = int(rows[0]["cnt"])
        except Exception:
            pass
        try:
            detail = _query_sql(
                f"DESCRIBE DETAIL {table_fqn}", limit=1,
                client=client, warehouse_id=wid,
            )
            if detail:
                size_bytes = int(detail[0].get("sizeInBytes", 0)) if detail[0].get("sizeInBytes") is not None else None
                last_modified = detail[0].get("lastModified")
        except Exception:
            pass
        return table_fqn, row_count, size_bytes, last_modified

    # Fetch stats for all tables in parallel
    max_workers = min(len(tables), config.get("max_parallel_queries", 10))
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor(max_workers=max(max_workers, 1)) as pool:
        futures = [loop.run_in_executor(pool, _get_table_stats, tbl) for tbl in tables]
        stats = await asyncio.gather(*futures)

    # Step 3: Fetch previous snapshot row counts from metric_baselines
    prev_lookup: dict[str, float] = {}
    try:
        from src.anomaly_detection import _get_schema
        audit_schema = _get_schema(config)
        prev_sql = f"""
            SELECT table_fqn, value AS previous_rows
            FROM (
                SELECT table_fqn, value,
                       ROW_NUMBER() OVER (PARTITION BY table_fqn ORDER BY measured_at DESC) AS rn
                FROM {audit_schema}.metric_baselines
                WHERE metric_name = 'row_count'
                  AND column_name = '*'
                  AND table_fqn LIKE '{_esc(catalog)}.%'
            )
            WHERE rn = 1
        """
        prev_rows = _query_sql(prev_sql, limit=5000, client=client, warehouse_id=wid)
        for r in prev_rows:
            try:
                prev_lookup[r["table_fqn"]] = float(r["previous_rows"])
            except (ValueError, TypeError, KeyError):
                pass
    except Exception:
        pass  # No snapshot history yet — previous stays None

    # Step 4: Build results with change detection
    results = []
    for fqn, current, size_bytes, last_modified in stats:
        prev = prev_lookup.get(fqn)
        change_pct = None
        if current is not None and prev is not None and prev > 0:
            change_pct = round((current - prev) / prev * 100, 2)
        results.append({
            "table_name": fqn,
            "current_rows": current,
            "previous_rows": int(prev) if prev is not None else None,
            "change_pct": change_pct,
            "size_bytes": size_bytes,
            "last_modified": last_modified,
        })

    return {
        "catalog": catalog,
        "schema_filter": schema,
        "total_tables": len(results),
        "tables": results,
    }


@router.post("/volume/snapshot", summary="Take a volume snapshot")
async def volume_snapshot(req: VolumeSnapshotRequest, client=Depends(get_db_client)):
    """Take a volume snapshot (row counts) and record as metrics for anomaly detection."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")

    from src.data_freshness import _query_sql, _esc
    from src.anomaly_detection import record_metric as _record

    schema_filter = ""
    if req.schema_name:
        schema_filter = f"AND table_schema = '{_esc(req.schema_name)}'"

    query = f"""
        SELECT table_catalog, table_schema, table_name
        FROM `{_esc(req.catalog)}`.information_schema.tables
        WHERE table_type = 'MANAGED'
          AND table_schema NOT IN ('information_schema', 'default')
          {schema_filter}
        ORDER BY table_schema, table_name
    """
    try:
        tables = _query_sql(query, limit=5000, client=client, warehouse_id=wid)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not list tables: {e}")

    import asyncio
    from concurrent.futures import ThreadPoolExecutor

    def _count_and_record(tbl):
        table_fqn = f"{tbl['table_catalog']}.{tbl['table_schema']}.{tbl['table_name']}"
        try:
            count_rows = _query_sql(
                f"SELECT COUNT(*) AS row_count FROM {table_fqn}",
                limit=1, client=client, warehouse_id=wid,
            )
            row_count = int(count_rows[0]["row_count"]) if count_rows else 0
            _record(
                table_fqn=table_fqn, column_name="*",
                metric_name="row_count", value=float(row_count),
                client=client, warehouse_id=wid, config=config,
            )
            return True
        except Exception:
            return False

    max_workers = min(len(tables), config.get("max_parallel_queries", 10))
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [loop.run_in_executor(pool, _count_and_record, tbl) for tbl in tables]
        results = await asyncio.gather(*futures)

    recorded = sum(1 for r in results if r)
    errors = sum(1 for r in results if not r)

    return {
        "catalog": req.catalog,
        "schema_name": req.schema_name,
        "tables_recorded": recorded,
        "errors": errors,
    }


@router.get("/volume/{catalog}/history", summary="Get volume snapshot history")
async def volume_history(
    catalog: str,
    days: int = Query(default=30),
    client=Depends(get_db_client),
):
    """Get historical row count snapshots for trend charts."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")

    from src.data_freshness import _query_sql, _esc
    from src.anomaly_detection import _get_schema

    audit_schema = _get_schema(config)

    # Aggregate total rows per snapshot timestamp across all tables in catalog
    trend_sql = f"""
        SELECT
            DATE(measured_at) AS snapshot_date,
            COUNT(DISTINCT table_fqn) AS table_count,
            SUM(value) AS total_rows
        FROM {audit_schema}.metric_baselines
        WHERE metric_name = 'row_count'
          AND column_name = '*'
          AND table_fqn LIKE '{_esc(catalog)}.%'
          AND measured_at >= DATEADD(DAY, -{days}, CURRENT_TIMESTAMP())
        GROUP BY DATE(measured_at)
        ORDER BY snapshot_date
    """
    try:
        trend = _query_sql(trend_sql, limit=1000, client=client, warehouse_id=wid)
    except Exception:
        trend = []

    # Per-table history (top 20 largest tables for sparklines)
    table_history_sql = f"""
        SELECT table_fqn, DATE(measured_at) AS snapshot_date, value AS row_count
        FROM {audit_schema}.metric_baselines
        WHERE metric_name = 'row_count'
          AND column_name = '*'
          AND table_fqn LIKE '{_esc(catalog)}.%'
          AND measured_at >= DATEADD(DAY, -{days}, CURRENT_TIMESTAMP())
        ORDER BY table_fqn, measured_at
    """
    try:
        table_hist = _query_sql(table_history_sql, limit=10000, client=client, warehouse_id=wid)
    except Exception:
        table_hist = []

    # Group by table
    per_table: dict[str, list] = {}
    for r in table_hist:
        fqn = r.get("table_fqn", "")
        if fqn not in per_table:
            per_table[fqn] = []
        per_table[fqn].append({
            "date": str(r.get("snapshot_date", "")),
            "rows": float(r.get("row_count", 0)),
        })

    return {
        "catalog": catalog,
        "days": days,
        "trend": [
            {
                "date": str(r.get("snapshot_date", "")),
                "table_count": int(r.get("table_count", 0)),
                "total_rows": float(r.get("total_rows", 0)),
            }
            for r in trend
        ],
        "per_table": per_table,
    }


# ── Suites ───────────────────────────────────────────────────────────────────

@router.get("/suites", summary="List expectation suites")
async def list_suites():
    """List all expectation suites."""
    from src.expectation_suites import list_suites as _list
    return {"suites": _list()}


@router.post("/suites", summary="Create an expectation suite")
async def create_suite(req: CreateSuiteRequest):
    """Create a new expectation suite with grouped DQ checks."""
    from src.expectation_suites import create_suite as _create
    return _create(name=req.name, description=req.description, checks=req.checks)


@router.get("/suites/{suite_id}", summary="Get expectation suite details")
async def get_suite(suite_id: str):
    """Get a single expectation suite by ID."""
    from src.expectation_suites import get_suite as _get
    suite = _get(suite_id)
    if not suite:
        raise HTTPException(status_code=404, detail=f"Suite {suite_id} not found")
    return suite


@router.delete("/suites/{suite_id}", summary="Delete an expectation suite")
async def delete_suite(suite_id: str):
    """Delete an expectation suite by ID."""
    from src.expectation_suites import delete_suite as _delete
    if not _delete(suite_id):
        raise HTTPException(status_code=404, detail=f"Suite {suite_id} not found")
    return {"status": "deleted", "suite_id": suite_id}


@router.post("/suites/{suite_id}/run", summary="Execute all checks in a suite")
async def run_suite(suite_id: str, client=Depends(get_db_client)):
    """Execute all checks in an expectation suite and return combined pass/fail results."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")

    from src.expectation_suites import run_suite as _run
    result = _run(suite_id, client=client, warehouse_id=wid, config=config)
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return result


# ── Incidents ────────────────────────────────────────────────────────────────

@router.get("/incidents", summary="Unified incident feed")
async def incidents(
    limit: int = Query(default=50, ge=1, le=500),
    client=Depends(get_db_client),
):
    """Unified incident feed combining failed DQ rules, stale tables, anomalies,
    and reconciliation mismatches into a single prioritized list.
    """
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")

    incidents_list = []

    # 1. Anomalies
    try:
        from src.anomaly_detection import get_anomalies
        anomalies = get_anomalies(client=client, warehouse_id=wid, config=config, limit=limit)
        for a in anomalies:
            incidents_list.append({
                "type": "anomaly",
                "severity": a.get("severity", "warning"),
                "title": f"Anomaly in {a.get('metric_name', 'unknown')} for {a.get('table_fqn', 'unknown')}",
                "description": f"z-score={a.get('z_score', 0):.2f}, value={a.get('value', 0)}",
                "table_fqn": a.get("table_fqn"),
                "timestamp": a.get("measured_at"),
                "details": a,
            })
    except Exception as e:
        logger_msg = f"Could not load anomaly incidents: {e}"
        import logging
        logging.getLogger(__name__).debug(logger_msg)

    # 2. Reconciliation mismatches (from recent runs)
    try:
        from src.reconciliation_store import get_reconciliation_history
        runs = get_reconciliation_history(client=client, warehouse_id=wid, config=config, limit=10)
        for run in runs:
            mismatched = int(run.get("mismatched", 0))
            errs = int(run.get("errors", 0))
            if mismatched > 0 or errs > 0:
                incidents_list.append({
                    "type": "reconciliation_mismatch",
                    "severity": "critical" if mismatched > 5 else "warning",
                    "title": f"Reconciliation mismatch: {run.get('source_catalog', '')} -> {run.get('destination_catalog', '')}",
                    "description": f"{mismatched} mismatched, {errs} errors out of {run.get('total_tables', 0)} tables",
                    "table_fqn": None,
                    "timestamp": run.get("executed_at"),
                    "details": run,
                })
    except Exception as e:
        import logging
        logging.getLogger(__name__).debug(f"Could not load reconciliation incidents: {e}")

    # Sort by timestamp descending (most recent first)
    incidents_list.sort(key=lambda x: x.get("timestamp") or "", reverse=True)
    incidents_list = incidents_list[:limit]

    return {
        "total": len(incidents_list),
        "incidents": incidents_list,
    }


# ── Anomaly Detection Settings ────────────────────────────────────────────────

@router.get("/anomaly-settings", summary="Get anomaly detection thresholds")
async def get_anomaly_settings():
    """Get current anomaly detection configuration."""
    config = await get_app_config()
    ad = config.get("anomaly_detection", {})
    sources = ad.get("system_table_sources", {})
    return {
        "baseline_window": int(ad.get("baseline_window", 30)),
        "warning_threshold": float(ad.get("warning_threshold", 2.0)),
        "critical_threshold": float(ad.get("critical_threshold", 3.0)),
        "max_parallel_queries": int(config.get("max_parallel_queries", 10)),
        "system_table_sources": {
            "billing": bool(sources.get("billing", False)),
            "compute": bool(sources.get("compute", False)),
            "query_history": bool(sources.get("query_history", False)),
            "storage": bool(sources.get("storage", False)),
        },
    }


@router.put("/anomaly-settings", summary="Update anomaly detection thresholds")
async def update_anomaly_settings(req: dict):
    """Update anomaly detection thresholds in the config file."""
    from src.config import load_config
    import yaml

    config_path = "config/clone_config.yaml"
    try:
        with open(config_path, "r") as f:
            raw = yaml.safe_load(f) or {}
    except FileNotFoundError:
        raw = {}

    if "anomaly_detection" not in raw:
        raw["anomaly_detection"] = {}

    if "baseline_window" in req:
        raw["anomaly_detection"]["baseline_window"] = int(req["baseline_window"])
    if "warning_threshold" in req:
        raw["anomaly_detection"]["warning_threshold"] = float(req["warning_threshold"])
    if "critical_threshold" in req:
        raw["anomaly_detection"]["critical_threshold"] = float(req["critical_threshold"])
    if "system_table_sources" in req:
        raw["anomaly_detection"]["system_table_sources"] = {
            "billing": bool(req["system_table_sources"].get("billing", False)),
            "compute": bool(req["system_table_sources"].get("compute", False)),
            "query_history": bool(req["system_table_sources"].get("query_history", False)),
            "storage": bool(req["system_table_sources"].get("storage", False)),
        }
    if "max_parallel_queries" in req:
        raw["max_parallel_queries"] = int(req["max_parallel_queries"])

    with open(config_path, "w") as f:
        yaml.dump(raw, f, default_flow_style=False, sort_keys=False)

    return {**raw.get("anomaly_detection", {}), "max_parallel_queries": raw.get("max_parallel_queries", 10)}


@router.get("/anomalies/system-tables", summary="Detect anomalies from Databricks system tables")
async def system_table_anomalies(
    days: int = Query(default=7, ge=1, le=90),
    client=Depends(get_db_client),
):
    """Scan Databricks system tables for anomalies — billing spikes, slow queries,
    cluster failures, and storage growth."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    ad = config.get("anomaly_detection", {})
    sources = ad.get("system_table_sources", {})

    from src.data_freshness import _query_sql

    anomalies = []

    # ── 1. Billing anomalies (cost spikes) ──
    if sources.get("billing", False):
        try:
            rows = _query_sql(f"""
                WITH daily AS (
                    SELECT DATE(usage_date) AS d,
                           SUM(usage_quantity) AS dbu
                    FROM system.billing.usage
                    WHERE usage_date >= DATEADD(DAY, -{days}, CURRENT_DATE())
                    GROUP BY DATE(usage_date)
                ),
                stats AS (
                    SELECT AVG(dbu) AS mean_dbu, STDDEV(dbu) AS std_dbu FROM daily
                )
                SELECT d, dbu, mean_dbu, std_dbu,
                       CASE WHEN std_dbu > 0 THEN ABS(dbu - mean_dbu) / std_dbu ELSE 0 END AS z_score
                FROM daily CROSS JOIN stats
                WHERE std_dbu > 0 AND ABS(dbu - mean_dbu) / std_dbu > {ad.get('warning_threshold', 2.0)}
                ORDER BY z_score DESC
            """, limit=20, client=client, warehouse_id=wid)
            for r in rows:
                z = float(r.get("z_score", 0))
                anomalies.append({
                    "source": "billing",
                    "severity": "critical" if z > ad.get("critical_threshold", 3.0) else "warning",
                    "title": f"DBU spike on {r.get('d', '')}",
                    "description": f"{float(r.get('dbu', 0)):,.0f} DBUs (avg {float(r.get('mean_dbu', 0)):,.0f}, z={z:.1f})",
                    "timestamp": str(r.get("d", "")),
                    "z_score": z,
                    "metric": "dbu_usage",
                    "value": float(r.get("dbu", 0)),
                })
        except Exception:
            pass

    # ── 2. Compute anomalies (cluster failures, long runtimes) ──
    if sources.get("compute", False):
        try:
            rows = _query_sql(f"""
                SELECT cluster_id, cluster_name, state, state_message,
                       change_time, driver_node_type, worker_count
                FROM system.compute.clusters
                WHERE change_time >= DATEADD(DAY, -{days}, CURRENT_TIMESTAMP())
                  AND state IN ('ERROR', 'TERMINATED')
                  AND state_message IS NOT NULL
                  AND state_message != ''
                ORDER BY change_time DESC
                LIMIT 20
            """, limit=20, client=client, warehouse_id=wid)
            for r in rows:
                anomalies.append({
                    "source": "compute",
                    "severity": "critical" if r.get("state") == "ERROR" else "warning",
                    "title": f"Cluster {r.get('cluster_name', r.get('cluster_id', 'unknown'))} — {r.get('state', '')}",
                    "description": str(r.get("state_message", ""))[:200],
                    "timestamp": str(r.get("change_time", "")),
                    "z_score": 0,
                    "metric": "cluster_state",
                    "value": 0,
                })
        except Exception:
            pass

    # ── 3. Query anomalies (slow/failed queries) ──
    if sources.get("query_history", False):
        try:
            # Failed queries
            rows = _query_sql(f"""
                SELECT statement_id, executed_by, status, error_message,
                       execution_start_time_ms, duration_ms
                FROM system.query.history
                WHERE start_time >= DATEADD(DAY, -{days}, CURRENT_TIMESTAMP())
                  AND status = 'FAILED'
                ORDER BY start_time DESC
                LIMIT 10
            """, limit=10, client=client, warehouse_id=wid)
            for r in rows:
                anomalies.append({
                    "source": "query_history",
                    "severity": "warning",
                    "title": f"Failed query by {r.get('executed_by', 'unknown')}",
                    "description": str(r.get("error_message", ""))[:200],
                    "timestamp": str(r.get("execution_start_time_ms", "")),
                    "z_score": 0,
                    "metric": "query_failure",
                    "value": 0,
                })
        except Exception:
            pass
        try:
            # Slow queries (z-score on duration)
            rows = _query_sql(f"""
                WITH stats AS (
                    SELECT AVG(duration_ms) AS mean_dur, STDDEV(duration_ms) AS std_dur
                    FROM system.query.history
                    WHERE start_time >= DATEADD(DAY, -{days}, CURRENT_TIMESTAMP())
                      AND status = 'FINISHED'
                      AND duration_ms > 0
                )
                SELECT h.statement_id, h.executed_by, h.duration_ms, h.start_time,
                       s.mean_dur, s.std_dur,
                       (h.duration_ms - s.mean_dur) / s.std_dur AS z_score
                FROM system.query.history h CROSS JOIN stats s
                WHERE h.start_time >= DATEADD(DAY, -{days}, CURRENT_TIMESTAMP())
                  AND h.status = 'FINISHED'
                  AND s.std_dur > 0
                  AND (h.duration_ms - s.mean_dur) / s.std_dur > {ad.get('warning_threshold', 2.0)}
                ORDER BY z_score DESC
                LIMIT 10
            """, limit=10, client=client, warehouse_id=wid)
            for r in rows:
                z = float(r.get("z_score", 0))
                dur_s = float(r.get("duration_ms", 0)) / 1000
                anomalies.append({
                    "source": "query_history",
                    "severity": "critical" if z > ad.get("critical_threshold", 3.0) else "warning",
                    "title": f"Slow query by {r.get('executed_by', 'unknown')} — {dur_s:.1f}s",
                    "description": f"Duration {dur_s:.1f}s (avg {float(r.get('mean_dur', 0))/1000:.1f}s, z={z:.1f})",
                    "timestamp": str(r.get("start_time", "")),
                    "z_score": z,
                    "metric": "query_duration",
                    "value": dur_s,
                })
        except Exception:
            pass

    # ── 4. Storage anomalies (table size changes) ──
    if sources.get("storage", False):
        try:
            rows = _query_sql(f"""
                WITH daily AS (
                    SELECT catalog_name, schema_name, table_name,
                           DATE(last_altered) AS d,
                           data_size_bytes
                    FROM system.information_schema.tables
                    WHERE table_type = 'MANAGED'
                      AND last_altered >= DATEADD(DAY, -{days}, CURRENT_TIMESTAMP())
                ),
                totals AS (
                    SELECT d, SUM(data_size_bytes) AS total_bytes
                    FROM daily GROUP BY d
                ),
                stats AS (
                    SELECT AVG(total_bytes) AS mean_bytes, STDDEV(total_bytes) AS std_bytes FROM totals
                )
                SELECT d, total_bytes, mean_bytes, std_bytes,
                       CASE WHEN std_bytes > 0 THEN ABS(total_bytes - mean_bytes) / std_bytes ELSE 0 END AS z_score
                FROM totals CROSS JOIN stats
                WHERE std_bytes > 0 AND ABS(total_bytes - mean_bytes) / std_bytes > {ad.get('warning_threshold', 2.0)}
                ORDER BY z_score DESC
            """, limit=20, client=client, warehouse_id=wid)
            for r in rows:
                z = float(r.get("z_score", 0))
                gb = float(r.get("total_bytes", 0)) / 1_073_741_824
                anomalies.append({
                    "source": "storage",
                    "severity": "critical" if z > ad.get("critical_threshold", 3.0) else "warning",
                    "title": f"Storage anomaly on {r.get('d', '')}",
                    "description": f"Total storage {gb:.1f} GB (z={z:.1f})",
                    "timestamp": str(r.get("d", "")),
                    "z_score": z,
                    "metric": "storage_bytes",
                    "value": float(r.get("total_bytes", 0)),
                })
        except Exception:
            pass

    anomalies.sort(key=lambda x: x.get("z_score", 0), reverse=True)
    return {
        "days": days,
        "sources_enabled": {k: v for k, v in sources.items() if v},
        "total": len(anomalies),
        "anomalies": anomalies,
    }


# ── Health Score ─────────────────────────────────────────────────────────────

@router.get("/health-score/{catalog}", summary="Compute aggregate DQ health score")
async def health_score(
    catalog: str,
    schema: Optional[str] = Query(default=None),
    max_stale_hours: int = Query(default=24, ge=1),
    client=Depends(get_db_client),
):
    """Compute an aggregate data quality health score for a catalog.

    The score (0-100) is derived from:
    - Freshness: % of tables updated within max_stale_hours
    - Anomalies: penalty for recent critical/warning anomalies
    - Reconciliation: latest match rate from reconciliation runs

    Returns the composite score plus breakdowns per dimension.
    """
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")

    scores = {}

    # 1. Freshness score (0-100)
    try:
        from src.data_freshness import check_freshness
        freshness = check_freshness(
            client, catalog, schema=schema,
            max_stale_hours=max_stale_hours, warehouse_id=wid, config=config,
        )
        total = freshness.get("total_tables", 0) if isinstance(freshness, dict) else 0
        fresh = freshness.get("fresh", 0) if isinstance(freshness, dict) else 0
        scores["freshness"] = round((fresh / total) * 100, 1) if total > 0 else 100.0
    except Exception:
        scores["freshness"] = None

    # 2. Anomaly score (0-100) — penalize for recent anomalies
    try:
        from src.anomaly_detection import get_anomalies
        anomalies = get_anomalies(client=client, warehouse_id=wid, config=config, limit=100)
        critical = sum(1 for a in anomalies if a.get("severity") == "critical")
        warning = sum(1 for a in anomalies if a.get("severity") == "warning")
        # Each critical costs 10 points, each warning costs 3 points
        penalty = min(100, critical * 10 + warning * 3)
        scores["anomaly"] = max(0, 100 - penalty)
    except Exception:
        scores["anomaly"] = None

    # 3. Reconciliation score (0-100) — from latest run
    try:
        from src.reconciliation_store import get_reconciliation_history
        runs = get_reconciliation_history(
            client=client, warehouse_id=wid, config=config, limit=1,
            source_catalog=catalog,
        )
        if runs:
            latest = runs[0]
            total_tables = int(latest.get("total_tables", 0))
            matched = int(latest.get("matched", 0))
            scores["reconciliation"] = round((matched / total_tables) * 100, 1) if total_tables > 0 else 100.0
        else:
            scores["reconciliation"] = None
    except Exception:
        scores["reconciliation"] = None

    # Composite score: weighted average of available dimensions
    weights = {"freshness": 0.4, "anomaly": 0.3, "reconciliation": 0.3}
    available = {k: v for k, v in scores.items() if v is not None}

    if available:
        total_weight = sum(weights[k] for k in available)
        composite = sum(v * weights[k] / total_weight for k, v in available.items())
    else:
        composite = None

    # Determine overall status
    if composite is None:
        status = "unknown"
    elif composite >= 90:
        status = "healthy"
    elif composite >= 70:
        status = "warning"
    else:
        status = "critical"

    return {
        "catalog": catalog,
        "schema_filter": schema,
        "composite_score": round(composite, 1) if composite is not None else None,
        "status": status,
        "dimensions": scores,
        "weights": weights,
    }


# ── Dashboard summary endpoints ──────────────────────────────────────────────

@router.get("/freshness/summary", summary="Freshness summary across all catalogs")
async def freshness_summary(client=Depends(get_db_client)):
    """Return aggregate fresh/stale/unknown counts for the dashboard."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")

    try:
        from src.data_freshness import check_freshness
        # Use the configured source_catalog or discover catalogs
        source = config.get("source_catalog", "")
        if not source:
            from src.client import execute_sql
            cats = execute_sql(client, wid, "SHOW CATALOGS")
            skip_cats = {"system", "hive_metastore", "__databricks_internal", "samples"}
            catalogs = [c.get("catalog", c.get("catalog_name", "")) for c in (cats or [])
                        if c.get("catalog", c.get("catalog_name", "")) not in skip_cats
                        and not c.get("catalog", c.get("catalog_name", "")).startswith("__")]
        else:
            catalogs = [source]

        fresh = 0
        stale = 0
        unknown = 0

        for cat in catalogs[:5]:  # cap to avoid long queries
            try:
                result = check_freshness(client, cat, warehouse_id=wid, config=config)
                if isinstance(result, dict):
                    fresh += result.get("fresh", 0)
                    stale += result.get("stale", 0)
                    unknown += result.get("unknown", 0)
            except Exception:
                pass

        return {"fresh": fresh, "stale": stale, "unknown": unknown}
    except Exception:
        return {"fresh": 0, "stale": 0, "unknown": 0}


@router.get("/health/trend", summary="Health score trend over time")
async def health_trend(
    days: int = Query(default=7, ge=1, le=90),
    client=Depends(get_db_client),
):
    """Return daily health scores for the trend chart. Uses DQ run results from the governance tables."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")

    try:
        from src.client import execute_sql
        audit = config.get("audit_trail", {})
        catalog = audit.get("catalog", "clone_audit")
        schema = f"{catalog}.governance"

        # Query DQX run results grouped by date
        rows = execute_sql(client, wid, f"""
            SELECT DATE(executed_at) AS run_date,
                   AVG(pass_rate) AS avg_pass_rate,
                   COUNT(*) AS run_count
            FROM {schema}.dqx_run_results
            WHERE executed_at >= CURRENT_DATE() - INTERVAL {days} DAY
            GROUP BY DATE(executed_at)
            ORDER BY run_date
        """)

        trend = []
        for r in (rows or []):
            score = round(float(r.get("avg_pass_rate", 0)), 1)
            trend.append({
                "date": str(r.get("run_date", "")),
                "score": score,
                "runs": int(r.get("run_count", 0)),
            })
        return trend
    except Exception:
        return []


# ── SLA Compliance Trend ─────────────────────────────────────────────────────

@router.get("/sla/compliance-trend", summary="SLA compliance percentage over time")
async def sla_compliance_trend(
    days: int = Query(default=30, ge=1, le=365),
    client=Depends(get_db_client),
):
    """Get daily SLA compliance trend — pass rate per day over the specified period."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.sla_monitor import get_sla_compliance_trend
    return get_sla_compliance_trend(client, wid, config, days)


# ── Monitoring Configuration ─────────────────────────────────────────────────

class MonitoringConfigRequest(BaseModel):
    table_fqn: str
    metrics: list[str] = Field(default=["row_count", "null_rate", "distinct_count"])
    frequency: str = "daily"
    auto_baseline: bool = True
    baseline_days: int = 7
    enabled: bool = True


class BulkMonitorRequest(BaseModel):
    table_fqns: list[str]
    metrics: list[str] = Field(default=["row_count", "null_rate", "distinct_count"])
    frequency: str = "daily"


@router.get("/monitoring/configs", summary="List all monitoring configurations")
async def list_monitoring_configs():
    """List all table monitoring configurations."""
    from src.monitoring_config import list_monitoring_configs as _list
    configs = _list()
    return {"configs": configs, "total": len(configs)}


@router.post("/monitoring/configs", summary="Create or update a monitoring configuration")
async def create_monitoring_config(req: MonitoringConfigRequest):
    """Create or update monitoring config for a table."""
    from src.monitoring_config import create_monitoring_config as _create
    return _create(
        table_fqn=req.table_fqn, metrics=req.metrics,
        frequency=req.frequency, auto_baseline=req.auto_baseline,
        baseline_days=req.baseline_days, enabled=req.enabled,
    )


@router.put("/monitoring/configs/{config_id}", summary="Update monitoring configuration")
async def update_monitoring_config(config_id: str, req: MonitoringConfigRequest):
    """Update an existing monitoring configuration."""
    from src.monitoring_config import update_monitoring_config as _update
    result = _update(
        config_id, metrics=req.metrics, frequency=req.frequency,
        auto_baseline=req.auto_baseline, baseline_days=req.baseline_days,
        enabled=req.enabled,
    )
    if not result:
        raise HTTPException(status_code=404, detail=f"Config {config_id} not found")
    return result


@router.delete("/monitoring/configs/{config_id}", summary="Delete monitoring configuration")
async def delete_monitoring_config(config_id: str):
    """Delete a monitoring configuration."""
    from src.monitoring_config import delete_monitoring_config as _delete
    if not _delete(config_id):
        raise HTTPException(status_code=404, detail=f"Config {config_id} not found")
    return {"status": "deleted", "config_id": config_id}


@router.post("/monitoring/configs/{config_id}/toggle", summary="Toggle monitoring on/off")
async def toggle_monitoring_config(config_id: str):
    """Toggle enabled/disabled for a monitoring configuration."""
    from src.monitoring_config import toggle_monitoring_config as _toggle
    result = _toggle(config_id)
    if not result:
        raise HTTPException(status_code=404, detail=f"Config {config_id} not found")
    return result


@router.post("/monitoring/bulk-add", summary="Add multiple tables for monitoring")
async def bulk_add_monitoring(req: BulkMonitorRequest):
    """Add multiple tables for monitoring at once."""
    from src.monitoring_config import add_tables_bulk
    results = add_tables_bulk(req.table_fqns, req.metrics, req.frequency)
    return {"added": len(results), "configs": results}


@router.get("/monitoring/discover/{catalog}", summary="Discover tables available for monitoring")
async def discover_tables(
    catalog: str,
    schema: Optional[str] = Query(default=None),
    client=Depends(get_db_client),
):
    """Discover tables in a catalog/schema for monitoring setup."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.monitoring_config import discover_tables as _discover
    tables = _discover(client, wid, catalog, schema)
    return {"catalog": catalog, "schema": schema, "tables": tables, "total": len(tables)}


@router.post("/monitoring/run", summary="Run monitoring for all enabled configs")
async def run_monitoring(client=Depends(get_db_client)):
    """Execute monitoring for all enabled configurations — collect metrics and detect anomalies."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.monitoring_config import run_monitoring as _run
    return _run(client=client, warehouse_id=wid, config=config)
