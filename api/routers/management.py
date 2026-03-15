"""Management endpoints: rollback, preflight, PII scan, sync."""

from fastapi import APIRouter, Depends, Request

from api.dependencies import get_db_client, get_app_config, get_job_manager
from api.models.management import PIIScanRequest, PreflightRequest, RollbackRequest, SyncRequest
from api.queue.job_manager import JobManager

router = APIRouter()


@router.post("/preflight")
async def run_preflight(req: PreflightRequest, client=Depends(get_db_client)):
    """Run pre-flight checks before cloning."""
    from src.preflight import run_preflight
    config = await get_app_config()
    wid = req.warehouse_id or config["sql_warehouse_id"]
    result = run_preflight(
        client, wid, req.source_catalog, req.destination_catalog,
        check_write=req.check_write,
    )
    return result


@router.get("/rollback/logs")
async def list_rollback_logs():
    """List available rollback logs."""
    from src.rollback import list_rollback_logs
    return list_rollback_logs()


@router.post("/rollback")
async def rollback(req: RollbackRequest, client=Depends(get_db_client)):
    """Rollback a previous clone operation."""
    from src.rollback import rollback
    config = await get_app_config()
    wid = req.warehouse_id or config["sql_warehouse_id"]
    result = rollback(client, wid, req.log_file, drop_catalog=req.drop_catalog)
    return result


@router.post("/pii-scan")
async def pii_scan(req: PIIScanRequest, client=Depends(get_db_client)):
    """Scan catalog for PII columns."""
    from src.pii_detection import scan_catalog_for_pii
    config = await get_app_config()
    wid = req.warehouse_id or config["sql_warehouse_id"]
    result = scan_catalog_for_pii(
        client, wid, req.source_catalog, req.exclude_schemas,
        sample_data=req.sample_data, max_workers=req.max_workers,
    )
    return result


@router.post("/sync")
async def sync_catalogs_bg(
    req: SyncRequest,
    client=Depends(get_db_client),
    app_config=Depends(get_app_config),
    jm: JobManager = Depends(get_job_manager),
):
    """Submit sync as a background job."""
    config = dict(app_config)
    config["source_catalog"] = req.source_catalog
    config["destination_catalog"] = req.destination_catalog
    config["sql_warehouse_id"] = req.warehouse_id or config.get("sql_warehouse_id", "")
    config["exclude_schemas"] = req.exclude_schemas
    config["dry_run"] = req.dry_run
    config["drop_extra"] = req.drop_extra
    job_id = await jm.submit_job("sync", config, client)
    return {"job_id": job_id, "status": "queued", "message": "Sync job submitted"}


@router.get("/catalogs")
async def list_catalogs(client=Depends(get_db_client)):
    """List all Unity Catalog catalogs."""
    try:
        catalogs = [c.name for c in client.catalogs.list() if c.name]
        return catalogs
    except Exception as e:
        return []


@router.get("/catalogs/{catalog}/schemas")
async def list_schemas(catalog: str, client=Depends(get_db_client)):
    """List schemas in a catalog."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    try:
        from src.client import execute_sql
        rows = execute_sql(client, wid, f"SELECT schema_name FROM {catalog}.information_schema.schemata WHERE schema_name NOT IN ('information_schema', 'default') ORDER BY schema_name")
        return [r["schema_name"] for r in rows]
    except Exception:
        return []


@router.get("/catalogs/{catalog}/{schema}/tables")
async def list_tables(catalog: str, schema: str, client=Depends(get_db_client)):
    """List tables in a schema."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    try:
        from src.client import execute_sql
        rows = execute_sql(client, wid, f"SELECT table_name FROM {catalog}.information_schema.tables WHERE table_schema = '{schema}' ORDER BY table_name")
        return [r["table_name"] for r in rows]
    except Exception:
        return []


@router.get("/audit")
async def get_audit_log(client=Depends(get_db_client)):
    """Get clone audit trail entries from Unity Catalog Delta table."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    # Try Delta-based run logs first, fall back to audit trail
    try:
        from src.run_logs import query_run_logs
        logs = query_run_logs(client, wid, config, limit=50)
        if logs:
            return logs
    except Exception:
        pass
    # Fallback: try audit_trail
    try:
        from src.audit_trail import query_audit_history
        return query_audit_history(client, wid, config, limit=50)
    except Exception:
        return []


@router.post("/audit/init")
async def init_audit_tables(req: dict, client=Depends(get_db_client)):
    """Initialize audit and run log Delta tables in Unity Catalog."""
    from fastapi import HTTPException
    config = await get_app_config()
    wid = req.get("warehouse_id") or config.get("sql_warehouse_id", "")
    if not wid:
        # Try to find a running warehouse
        try:
            from src.auth import list_warehouses
            warehouses = list_warehouses(client)
            running = [w for w in warehouses if w.get("state") == "RUNNING"]
            if running:
                wid = running[0]["id"]
            elif warehouses:
                wid = warehouses[0]["id"]
        except Exception:
            pass
    if not wid:
        raise HTTPException(status_code=400, detail="No SQL warehouse available. Select a warehouse in Settings first.")

    audit_config = {
        "audit_trail": {
            "catalog": req.get("catalog", "clone_audit"),
            "schema": req.get("schema", "logs"),
        }
    }
    tables_created = []
    try:
        from src.run_logs import ensure_run_logs_table
        fqn = ensure_run_logs_table(client, wid, audit_config)
        tables_created.append(fqn)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create run_logs table: {e}")
    try:
        from src.audit_trail import ensure_audit_table
        fqn = ensure_audit_table(client, wid, audit_config)
        tables_created.append(fqn)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create audit table: {e}")
    # Create metrics table
    try:
        from src.client import execute_sql
        catalog = req.get("catalog", "clone_audit")
        metrics_schema = f"{catalog}.metrics"
        metrics_fqn = f"{metrics_schema}.clone_metrics"
        try:
            execute_sql(client, wid, f"CREATE SCHEMA IF NOT EXISTS {metrics_schema}")
        except Exception:
            pass
        execute_sql(client, wid, f"""
        CREATE TABLE IF NOT EXISTS {metrics_fqn} (
            operation_id STRING,
            source_catalog STRING,
            destination_catalog STRING,
            clone_type STRING,
            started_at TIMESTAMP,
            completed_at TIMESTAMP,
            duration_seconds DOUBLE,
            total_tables INT,
            successful INT,
            failed INT,
            failure_rate DOUBLE,
            throughput_tables_per_min DOUBLE,
            avg_table_clone_seconds DOUBLE,
            total_row_count BIGINT,
            total_size_bytes BIGINT,
            metrics_json STRING,
            recorded_at TIMESTAMP
        )
        USING DELTA
        COMMENT 'Clone operation metrics'
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true'
        )
        """)
        tables_created.append(metrics_fqn)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create metrics table: {e}")
    # Describe the tables we just created
    schemas = {}
    for fqn in tables_created:
        try:
            from src.client import execute_sql
            cols = execute_sql(client, wid, f"DESCRIBE TABLE {fqn}")
            schemas[fqn] = cols
        except Exception:
            schemas[fqn] = []

    return {"status": "ok", "tables_created": tables_created, "schemas": schemas}


@router.post("/audit/describe")
async def describe_audit_tables(req: dict, client=Depends(get_db_client)):
    """Describe the schema of audit tables."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    if not wid:
        try:
            from src.auth import list_warehouses
            warehouses = list_warehouses(client)
            running = [w for w in warehouses if w.get("state") == "RUNNING"]
            wid = running[0]["id"] if running else (warehouses[0]["id"] if warehouses else "")
        except Exception:
            pass
    if not wid:
        return {"schemas": {}}

    catalog = req.get("catalog", "clone_audit")
    schema = req.get("schema", "logs")
    tables = [
        f"{catalog}.{schema}.run_logs",
        f"{catalog}.{schema}.clone_operations",
        f"{catalog}.metrics.clone_metrics",
    ]
    schemas = {}
    from src.client import execute_sql
    for fqn in tables:
        try:
            cols = execute_sql(client, wid, f"DESCRIBE TABLE {fqn}")
            # Filter out partition/metadata rows
            cols = [c for c in cols if c.get("col_name") and not c["col_name"].startswith("#")]
            schemas[fqn] = cols
        except Exception:
            pass  # Table may not exist yet
    return {"schemas": schemas}


@router.get("/audit/{job_id}/logs")
async def get_job_run_log(job_id: str, client=Depends(get_db_client)):
    """Get full run log detail (including log lines) for a specific job from Delta."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    try:
        from src.run_logs import get_run_log_detail
        detail = get_run_log_detail(client, wid, job_id, config)
        return detail or {"error": "Log not found"}
    except Exception as e:
        return {"error": str(e)}


@router.post("/compliance")
async def generate_compliance_report(req: dict, client=Depends(get_db_client)):
    """Generate a compliance report."""
    from src.compliance import generate_report
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    try:
        return generate_report(client, wid, req.get("catalog", ""), req.get("report_type", "data_governance"))
    except Exception as e:
        return {"error": str(e), "sections": [], "score": 0}


@router.get("/templates")
async def list_templates():
    """List available clone templates."""
    from src.clone_templates import TEMPLATES
    return [{"name": k, **v} for k, v in TEMPLATES.items()]


@router.get("/schedule")
async def list_schedules():
    """List scheduled clone jobs."""
    try:
        from src.scheduler import list_schedules
        return list_schedules()
    except Exception:
        return []


@router.post("/schedule")
async def create_schedule(req: dict):
    """Create a scheduled clone job."""
    try:
        from src.scheduler import create_schedule
        return create_schedule(req)
    except Exception as e:
        return {"error": str(e)}


@router.post("/multi-clone")
async def multi_clone(req: dict, client=Depends(get_db_client), jm: JobManager = Depends(get_job_manager)):
    """Clone to multiple destinations."""
    results = []
    for dest in req.get("destinations", []):
        config = {
            "source_catalog": req.get("source_catalog"),
            "destination_catalog": dest.get("catalog"),
            "clone_type": req.get("clone_type", "DEEP"),
            "sql_warehouse_id": "",
        }
        job_id = await jm.submit_job("clone", config, client)
        results.append({"destination": dest.get("catalog"), "job_id": job_id, "status": "queued"})
    return results


@router.post("/lineage")
async def query_lineage(req: dict, client=Depends(get_db_client)):
    """Query lineage for a catalog or table."""
    try:
        from src.lineage_tracker import get_lineage
        return get_lineage(req.get("catalog", ""), req.get("table"))
    except Exception as e:
        return {"entries": [], "error": str(e)}


@router.post("/impact")
async def analyze_impact(req: dict, client=Depends(get_db_client)):
    """Analyze downstream impact of changes."""
    try:
        from src.impact_analysis import analyze_impact
        config = await get_app_config()
        wid = config.get("sql_warehouse_id", "")
        return analyze_impact(client, wid, req.get("catalog", ""), req.get("schema"), req.get("table"))
    except Exception as e:
        return {"affected": [], "risk_level": "unknown", "error": str(e)}


@router.post("/preview")
async def preview_data(req: dict, client=Depends(get_db_client)):
    """Preview source vs destination data."""
    try:
        from src.preview import preview_table
        config = await get_app_config()
        wid = config.get("sql_warehouse_id", "")
        return preview_table(
            client, wid,
            req.get("source_catalog", ""), req.get("dest_catalog", ""),
            req.get("schema", ""), req.get("table", ""),
            limit=req.get("limit", 50),
        )
    except Exception as e:
        return {"source_rows": [], "dest_rows": [], "error": str(e)}


@router.post("/warehouse/start")
async def start_warehouse(req: dict, client=Depends(get_db_client)):
    """Start a SQL warehouse."""
    try:
        client.warehouses.start(req["warehouse_id"])
        return {"status": "starting", "warehouse_id": req["warehouse_id"]}
    except Exception as e:
        return {"error": str(e)}


@router.post("/warehouse/stop")
async def stop_warehouse(req: dict, client=Depends(get_db_client)):
    """Stop a SQL warehouse."""
    try:
        client.warehouses.stop(req["warehouse_id"])
        return {"status": "stopping", "warehouse_id": req["warehouse_id"]}
    except Exception as e:
        return {"error": str(e)}


@router.get("/rbac/policies")
async def list_rbac_policies():
    """List RBAC policies."""
    try:
        from src.rbac import list_policies
        return list_policies()
    except Exception:
        return []


@router.post("/rbac/policies")
async def create_rbac_policy(req: dict):
    """Create an RBAC policy."""
    try:
        from src.rbac import create_policy
        return create_policy(req)
    except Exception as e:
        return {"error": str(e)}


@router.get("/plugins")
async def list_plugins():
    """List available plugins."""
    try:
        from src.plugin_registry import list_plugins
        return list_plugins()
    except Exception:
        return []


@router.post("/plugins/toggle")
async def toggle_plugin(req: dict):
    """Enable or disable a plugin."""
    try:
        from src.plugin_registry import toggle_plugin
        return toggle_plugin(req.get("name", ""), req.get("enabled", True))
    except Exception as e:
        return {"error": str(e)}


@router.get("/monitor/metrics")
async def get_metrics():
    """Get clone operation metrics."""
    try:
        from src.metrics import get_metrics_summary
        return get_metrics_summary()
    except Exception:
        return {"total_clones": 0, "success_rate": 0, "avg_duration": 0, "tables_per_hour": 0, "by_status": {}}
