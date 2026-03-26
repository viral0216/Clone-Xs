"""Query Databricks system tables for billing, optimization, and job insights.

Uses SDK/REST API where available, falls back to system table SQL only
where no API exists (billing, storage, predictive optimization).
"""

import logging
from datetime import datetime, timedelta

from databricks.sdk import WorkspaceClient

from src.client import execute_sql

logger = logging.getLogger(__name__)


def query_billing_usage(
    client: WorkspaceClient, warehouse_id: str, catalog: str = "", days: int = 30
) -> list[dict]:
    """Query system.billing.usage for compute costs.

    Uses only guaranteed columns (usage_date, sku_name, usage_quantity, usage_unit).
    The list_cost column is not available on all workspace tiers.
    """
    cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")

    sql = f"""
        SELECT
            DATE(usage_date) AS date,
            sku_name AS sku,
            SUM(usage_quantity) AS usage_quantity,
            usage_unit
        FROM system.billing.usage
        WHERE usage_date >= '{cutoff}'
        GROUP BY DATE(usage_date), sku_name, usage_unit
        ORDER BY date DESC, sku_name
    """
    try:
        results = execute_sql(client, warehouse_id, sql)
        logger.info(f"Retrieved {len(results)} billing records for last {days} days")
        return results
    except Exception as e:
        logger.debug(f"system.billing.usage not available: {e}")
        return []


def query_predictive_optimization(
    client: WorkspaceClient, warehouse_id: str, catalog: str = ""
) -> list[dict]:
    """Query system.storage.predictive_optimization_operations_history for recommendations.

    No REST API available — system table SQL is the only option.
    """
    catalog_filter = ""
    if catalog:
        catalog_filter = f"AND catalog_name = '{catalog}'"

    sql = f"""
        SELECT
            CONCAT(catalog_name, '.', schema_name, '.', table_name) AS table_fqn,
            operation_type AS recommendation_type,
            operation_status,
            operation_metrics,
            usage_date AS last_checked
        FROM system.storage.predictive_optimization_operations_history
        WHERE usage_date >= CURRENT_DATE() - INTERVAL 7 DAYS
          {catalog_filter}
        ORDER BY usage_date DESC
    """
    try:
        results = execute_sql(client, warehouse_id, sql)
        logger.info(f"Retrieved {len(results)} optimization recommendations")
        return results
    except Exception as e:
        logger.debug(f"system.storage.predictive_optimization_operations_history not available: {e}")
        return []


def query_job_run_timeline(
    client: WorkspaceClient, warehouse_id: str, days: int = 30, job_name_filter: str = ""
) -> list[dict]:
    """Get job run history via the Jobs REST API (SDK wrapper).

    Uses client.jobs.list_runs() instead of system.lakeflow.job_run_timeline SQL.
    """
    cutoff = datetime.utcnow() - timedelta(days=days)
    cutoff_ms = int(cutoff.timestamp() * 1000)

    results = []
    try:
        runs = client.jobs.list_runs(
            start_time_from=cutoff_ms,
            expand_tasks=False,
        )
        for run in runs:
            job_name = run.run_name or ""
            if job_name_filter and job_name_filter.lower() not in job_name.lower():
                continue

            start_ms = run.start_time or 0
            end_ms = run.end_time or 0
            duration_s = (end_ms - start_ms) / 1000 if start_ms and end_ms else None

            status = "UNKNOWN"
            if run.state and run.state.result_state:
                status = str(run.state.result_state.value)
            elif run.state and run.state.life_cycle_state:
                status = str(run.state.life_cycle_state.value)

            results.append({
                "job_id": run.job_id,
                "run_id": run.run_id,
                "job_name": job_name,
                "status": status,
                "start_time": datetime.fromtimestamp(start_ms / 1000).isoformat() if start_ms else None,
                "end_time": datetime.fromtimestamp(end_ms / 1000).isoformat() if end_ms else None,
                "duration_seconds": round(duration_s) if duration_s else None,
                "triggered_by": str(run.trigger) if run.trigger else None,
                "creator_user_name": run.creator_user_name,
            })

            if len(results) >= 500:
                break

        logger.info(f"Retrieved {len(results)} job runs via Jobs API for last {days} days")
    except Exception as e:
        logger.debug(f"Jobs API not available: {e}")
    return results


def query_table_lineage(
    client: WorkspaceClient, warehouse_id: str, catalog: str, days: int = 30
) -> list[dict]:
    """Get table lineage via the Lineage Tracking REST API.

    Uses the /api/2.0/lineage-tracking/table-lineage endpoint instead of
    system.access.table_lineage SQL.
    """
    results = []

    # Get all tables in the catalog to query lineage for
    try:
        schemas = [
            s.name for s in client.schemas.list(catalog_name=catalog)
            if s.name not in ("information_schema", "default")
        ]
    except Exception as e:
        logger.debug(f"Could not list schemas for lineage in {catalog}: {e}")
        return []

    for schema_name in schemas:
        try:
            tables = client.tables.list(catalog_name=catalog, schema_name=schema_name)
            for table in tables:
                if len(results) >= 500:
                    break
                try:
                    # Call lineage REST API via SDK api_client
                    response = client.api_client.do(
                        "GET",
                        "/api/2.0/lineage-tracking/table-lineage",
                        query={
                            "table_name": table.full_name,
                        },
                    )
                    # Parse upstream lineage
                    for upstream in (response.get("upstreams") or []):
                        table_info = upstream.get("tableInfo", {})
                        results.append({
                            "source_table": table_info.get("name", ""),
                            "target_table": table.full_name,
                            "source_type": upstream.get("entityType", ""),
                            "target_type": "TABLE",
                            "event_time": None,
                        })
                    # Parse downstream lineage
                    for downstream in (response.get("downstreams") or []):
                        table_info = downstream.get("tableInfo", {})
                        results.append({
                            "source_table": table.full_name,
                            "target_table": table_info.get("name", ""),
                            "source_type": "TABLE",
                            "target_type": downstream.get("entityType", ""),
                            "event_time": None,
                        })
                except Exception:
                    continue
        except Exception:
            continue

    logger.info(f"Retrieved {len(results)} lineage records via REST API")
    return results


def query_storage_usage(
    client: WorkspaceClient, warehouse_id: str, catalog: str = "", days: int = 30
) -> list[dict]:
    """Query system.storage.tables for storage consumption trends.

    No REST API available — system table SQL is the only option.
    """
    cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
    catalog_filter = ""
    if catalog:
        catalog_filter = f"AND catalog_name = '{catalog}'"

    # Use only columns guaranteed to exist. Some workspaces may not have
    # delta_table_size_in_bytes or active_files_count.
    sql = f"""
        SELECT *
        FROM system.storage.tables
        WHERE 1=1
          {catalog_filter}
        ORDER BY 1
        LIMIT 200
    """
    try:
        results = execute_sql(client, warehouse_id, sql)
        logger.info(f"Retrieved {len(results)} storage records")
        return results
    except Exception as e:
        logger.debug(f"system.storage.tables not available: {e}")
        return []


# ---------------------------------------------------------------------------
# SDK-only data sources (no SQL)
# ---------------------------------------------------------------------------

def query_warehouse_health(client: WorkspaceClient) -> dict:
    """Get SQL warehouse health and configuration via SDK.

    Lists all warehouses with state, size, auto-stop config, and flags
    warehouses with auto-stop disabled or set too high.
    """
    warehouses = []
    warnings = []
    try:
        for wh in client.warehouses.list():
            auto_stop = getattr(wh, "auto_stop_mins", None) or 0
            info = {
                "id": wh.id,
                "name": wh.name,
                "state": str(wh.state) if wh.state else None,
                "size": getattr(wh, "cluster_size", None),
                "auto_stop_mins": auto_stop,
                "num_clusters": getattr(wh, "num_clusters", 1),
                "spot_instance_policy": str(getattr(wh, "spot_instance_policy", "")) or None,
                "warehouse_type": str(getattr(wh, "warehouse_type", "")) or None,
                "creator_name": getattr(wh, "creator_name", None),
            }
            warehouses.append(info)

            if auto_stop == 0:
                warnings.append({"warehouse": wh.name, "issue": "Auto-stop disabled", "severity": "high"})
            elif auto_stop > 120:
                warnings.append({"warehouse": wh.name, "issue": f"Auto-stop set to {auto_stop} mins (>2h)", "severity": "medium"})

        running = sum(1 for w in warehouses if w["state"] and "RUNNING" in str(w["state"]).upper())
        stopped = sum(1 for w in warehouses if w["state"] and "STOPPED" in str(w["state"]).upper())

        logger.info(f"Retrieved {len(warehouses)} warehouses ({running} running)")
    except Exception as e:
        logger.debug(f"Warehouses API not available: {e}")
        return {"warehouses": [], "summary": {}, "warnings": [], "error": str(e)}

    return {
        "warehouses": warehouses,
        "summary": {"total": len(warehouses), "running": running, "stopped": stopped},
        "warnings": warnings,
    }


def query_cluster_health(client: WorkspaceClient, max_events: int = 100) -> dict:
    """Get cluster health and recent events via SDK.

    Lists all clusters with state, autoscale config, and runtime.
    Fetches recent events for running clusters only.
    """
    clusters = []
    recent_events = []
    try:
        for c in client.clusters.list():
            autoscale = None
            if c.autoscale:
                autoscale = {"min_workers": c.autoscale.min_workers, "max_workers": c.autoscale.max_workers}

            clusters.append({
                "cluster_id": c.cluster_id,
                "cluster_name": c.cluster_name,
                "state": str(c.state) if c.state else None,
                "node_type_id": c.node_type_id,
                "autoscale": autoscale,
                "num_workers": c.num_workers,
                "spark_version": c.spark_version,
                "creator_user_name": c.creator_user_name,
            })

        running = sum(1 for c in clusters if c["state"] and "RUNNING" in str(c["state"]).upper())
        terminated = sum(1 for c in clusters if c["state"] and "TERMINATED" in str(c["state"]).upper())
        pending = sum(1 for c in clusters if c["state"] and "PENDING" in str(c["state"]).upper())

        # Fetch events for running clusters only
        event_count = 0
        for c in clusters:
            if event_count >= max_events:
                break
            if c["state"] and "RUNNING" in str(c["state"]).upper():
                try:
                    events_resp = client.clusters.events(cluster_id=c["cluster_id"], limit=20)
                    for ev in (events_resp.events or []):
                        recent_events.append({
                            "cluster_name": c["cluster_name"],
                            "type": str(ev.type) if ev.type else None,
                            "timestamp": str(ev.timestamp) if ev.timestamp else None,
                            "details": str(ev.details)[:200] if ev.details else None,
                        })
                        event_count += 1
                        if event_count >= max_events:
                            break
                except Exception:
                    continue

        logger.info(f"Retrieved {len(clusters)} clusters ({running} running), {len(recent_events)} events")
    except Exception as e:
        logger.debug(f"Clusters API not available: {e}")
        return {"clusters": [], "summary": {}, "recent_events": [], "error": str(e)}

    return {
        "clusters": clusters,
        "summary": {"total": len(clusters), "running": running, "terminated": terminated, "pending": pending},
        "recent_events": recent_events,
    }


def query_dlt_pipeline_health(client: WorkspaceClient, max_events_per_pipeline: int = 20) -> dict:
    """Get DLT pipeline health and recent events via SDK."""
    pipelines = []
    events = []
    try:
        for p in client.pipelines.list_pipelines():
            pipelines.append({
                "pipeline_id": p.pipeline_id,
                "name": p.name,
                "state": str(p.state) if p.state else None,
                "creator_user_name": getattr(p, "creator_user_name", None),
            })

            if len(pipelines) >= 200:
                break

        running = sum(1 for p in pipelines if p["state"] and "RUNNING" in str(p["state"]).upper())
        failed = sum(1 for p in pipelines if p["state"] and "FAILED" in str(p["state"]).upper())
        idle = sum(1 for p in pipelines if p["state"] and "IDLE" in str(p["state"]).upper())

        # Fetch events for failed/running pipelines
        for p in pipelines:
            if p["state"] and any(s in str(p["state"]).upper() for s in ("RUNNING", "FAILED")):
                try:
                    for ev in client.pipelines.list_pipeline_events(
                        pipeline_id=p["pipeline_id"], max_results=max_events_per_pipeline
                    ):
                        events.append({
                            "pipeline_name": p["name"],
                            "event_type": ev.event_type,
                            "level": str(ev.level) if ev.level else None,
                            "message": str(ev.message)[:200] if ev.message else None,
                            "timestamp": str(ev.timestamp) if ev.timestamp else None,
                        })
                except Exception:
                    continue

        logger.info(f"Retrieved {len(pipelines)} pipelines, {len(events)} events")
    except Exception as e:
        logger.debug(f"Pipelines API not available: {e}")
        return {"pipelines": [], "summary": {}, "events": [], "error": str(e)}

    return {
        "pipelines": pipelines,
        "summary": {"total": len(pipelines), "running": running, "failed": failed, "idle": idle},
        "events": events,
    }


def query_query_performance(client: WorkspaceClient, days: int = 30, max_results: int = 200) -> dict:
    """Get query execution performance via SDK query history API."""
    queries = []
    try:
        from databricks.sdk.service.sql import QueryFilter, TimeRange

        end_ms = int(datetime.utcnow().timestamp() * 1000)
        start_ms = int((datetime.utcnow() - timedelta(days=days)).timestamp() * 1000)

        for q in client.query_history.list(
            filter_by=QueryFilter(
                query_start_time_range=TimeRange(start_time_ms=start_ms, end_time_ms=end_ms),
            ),
        ):
            queries.append({
                "query_id": q.query_id,
                "query_text": (q.query_text or "")[:200],
                "status": str(q.status) if q.status else None,
                "duration_ms": q.duration,
                "rows_produced": q.rows_produced,
                "user_name": q.user_name,
                "warehouse_id": q.warehouse_id,
                "executed_as_user_name": getattr(q, "executed_as_user_name", None),
            })
            if len(queries) >= max_results:
                break

        # Compute stats
        durations = [q["duration_ms"] for q in queries if q["duration_ms"]]
        avg_duration = round(sum(durations) / len(durations)) if durations else 0
        sorted_dur = sorted(durations)
        p95_duration = sorted_dur[int(len(sorted_dur) * 0.95)] if sorted_dur else 0
        failure_rate = sum(1 for q in queries if q["status"] and "FAILED" in str(q["status"]).upper()) / max(len(queries), 1)

        # Top 10 slowest
        slowest = sorted(queries, key=lambda x: x.get("duration_ms") or 0, reverse=True)[:10]

        # By warehouse
        by_warehouse = {}
        for q in queries:
            wh = q.get("warehouse_id") or "unknown"
            if wh not in by_warehouse:
                by_warehouse[wh] = {"warehouse_id": wh, "query_count": 0, "avg_duration_ms": 0, "total_duration_ms": 0}
            by_warehouse[wh]["query_count"] += 1
            by_warehouse[wh]["total_duration_ms"] += q.get("duration_ms") or 0
        for wh in by_warehouse.values():
            wh["avg_duration_ms"] = round(wh["total_duration_ms"] / max(wh["query_count"], 1))
        by_warehouse_list = sorted(by_warehouse.values(), key=lambda x: x["query_count"], reverse=True)

        logger.info(f"Retrieved {len(queries)} queries from history")
    except Exception as e:
        logger.debug(f"Query History API not available: {e}")
        return {"queries": [], "summary": {}, "slowest": [], "by_warehouse": [], "error": str(e)}

    return {
        "queries": queries[:50],  # Only return top 50 to UI
        "summary": {
            "total_queries": len(queries),
            "avg_duration_ms": avg_duration,
            "p95_duration_ms": p95_duration,
            "failure_rate": round(failure_rate, 4),
        },
        "slowest": slowest,
        "by_warehouse": by_warehouse_list,
    }


def query_metastore_summary(client: WorkspaceClient) -> dict:
    """Get current metastore info and catalog/schema/table counts via SDK."""
    try:
        metastore = client.metastores.current()
        info = {
            "metastore_id": metastore.metastore_id,
            "name": metastore.name,
            "region": getattr(metastore, "region", None),
            "storage_root": getattr(metastore, "storage_root", None),
            "owner": metastore.owner,
            "cloud": getattr(metastore, "cloud", None),
        }
    except Exception as e:
        logger.debug(f"Metastore API not available: {e}")
        return {"metastore": {}, "counts": {}, "error": str(e)}

    # Count catalogs, schemas, tables
    catalog_count = 0
    schema_count = 0
    table_count = 0
    try:
        for cat in client.catalogs.list():
            catalog_count += 1
            try:
                for sch in client.schemas.list(catalog_name=cat.name):
                    if sch.name not in ("information_schema", "default"):
                        schema_count += 1
            except Exception:
                continue
    except Exception:
        pass

    logger.info(f"Metastore: {info.get('name')}, {catalog_count} catalogs, {schema_count} schemas")
    return {
        "metastore": info,
        "counts": {"catalogs": catalog_count, "schemas": schema_count},
    }


def query_sql_alerts(client: WorkspaceClient) -> list[dict]:
    """Get SQL alerts status via SDK."""
    alerts = []
    try:
        # Try alerts_v2 first
        for a in client.alerts_v2.list_alerts():
            alerts.append({
                "id": a.id,
                "display_name": getattr(a, "display_name", None) or getattr(a, "name", None),
                "state": str(a.state) if a.state else None,
                "owner_user_name": getattr(a, "owner_user_name", None),
                "lifecycle_state": str(getattr(a, "lifecycle_state", None)) or None,
                "query_id": getattr(a, "query_id", None),
            })
        logger.info(f"Retrieved {len(alerts)} SQL alerts via alerts_v2")
    except Exception:
        # Fallback to legacy alerts API
        try:
            for a in client.alerts.list():
                alerts.append({
                    "id": a.id,
                    "display_name": a.name,
                    "state": str(a.state) if a.state else None,
                    "owner_user_name": getattr(a, "user", {}).get("name") if hasattr(a, "user") else None,
                })
            logger.info(f"Retrieved {len(alerts)} SQL alerts via legacy API")
        except Exception as e:
            logger.debug(f"Alerts API not available: {e}")
    return alerts


def query_table_usage_summary(
    client: WorkspaceClient, warehouse_id: str, catalog: str, days: int = 90,
) -> list[dict]:
    """Get table access patterns by delegating to usage_analysis module."""
    try:
        from src.usage_analysis import query_table_access_patterns
        return query_table_access_patterns(client, warehouse_id, catalog, days)
    except Exception as e:
        logger.debug(f"Table usage analysis not available: {e}")
        return []


def get_system_insights_summary(
    client: WorkspaceClient, warehouse_id: str, catalog: str = "", days: int = 30,
    job_name_filter: str = "",
) -> dict:
    """Get a unified summary from all available sources.

    Uses SDK/REST API where available, falls back to system table SQL otherwise.
    Each section fails independently.
    """
    # Each source is optional — empty results are normal (not errors).
    # Only surface actual failures that the user should know about.
    billing = query_billing_usage(client, warehouse_id, catalog, days)
    optimization = query_predictive_optimization(client, warehouse_id, catalog)
    job_runs = query_job_run_timeline(client, warehouse_id, days, job_name_filter)
    lineage = query_table_lineage(client, warehouse_id, catalog, days)
    storage = query_storage_usage(client, warehouse_id, catalog, days)

    # Compute summary stats
    total_dbus = sum(float(r.get("usage_quantity", 0) or 0) for r in billing)
    total_jobs = len(job_runs)
    failed_jobs = sum(1 for r in job_runs if str(r.get("status", "")).upper() in ("FAILED", "ERROR"))
    total_storage_bytes = sum(int(r.get("storage_in_bytes", 0) or 0) for r in storage)

    return {
        "summary": {
            "total_dbus": round(total_dbus, 2),
            "total_jobs": total_jobs,
            "failed_jobs": failed_jobs,
            "optimization_recommendations": len(optimization),
            "lineage_relationships": len(lineage),
            "total_storage_gb": round(total_storage_bytes / (1024**3), 2),
        },
        "billing": billing,
        "optimization": optimization,
        "job_runs": job_runs,
        "lineage": lineage,
        "storage": storage,
        "available_sources": [
            s for s, d in [
                ("billing", billing),
                ("optimization", optimization),
                ("job_runs", job_runs),
                ("lineage", lineage),
                ("storage", storage),
            ] if d
        ],
    }
