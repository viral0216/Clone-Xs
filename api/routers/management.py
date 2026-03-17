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
async def list_rollback_logs(client=Depends(get_db_client)):
    """List available rollback logs from Delta table, falling back to local files."""
    from src.rollback import query_rollback_logs_delta, list_rollback_logs as list_local
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    # Try Delta first
    if wid:
        try:
            delta_logs = query_rollback_logs_delta(client, wid, config)
            if delta_logs:
                return delta_logs
        except Exception:
            pass
    # Fallback to local JSON files
    return list_local()


@router.post("/rollback")
async def rollback(req: RollbackRequest, client=Depends(get_db_client)):
    """Rollback a previous clone operation."""
    from src.rollback import rollback
    config = await get_app_config()
    wid = req.warehouse_id or config["sql_warehouse_id"]
    result = rollback(client, wid, req.log_file, drop_catalog=req.drop_catalog, config=config)
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


@router.get("/catalogs/{catalog}/info")
async def get_catalog_info(catalog: str, client=Depends(get_db_client)):
    """Get catalog details including storage location via DESCRIBE CATALOG EXTENDED."""
    from src.client import execute_sql
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    result = {"name": catalog, "storage_root": "", "owner": "", "comment": ""}

    # Try DESCRIBE CATALOG EXTENDED — most reliable for storage location
    try:
        rows = execute_sql(client, wid, f"DESCRIBE CATALOG EXTENDED `{catalog}`")
        for row in rows:
            key = (row.get("info_name") or row.get("col_name") or "").lower().strip()
            val = row.get("info_value") or row.get("data_type") or ""
            if key in ("location", "storage_root", "storage root", "managed location"):
                result["storage_root"] = val
            elif key == "owner":
                result["owner"] = val
            elif key == "comment":
                result["comment"] = val
        if result["storage_root"]:
            return result
    except Exception:
        pass

    # Fallback to SDK
    try:
        info = client.catalogs.get(catalog)
        result["storage_root"] = getattr(info, "storage_root", None) or getattr(info, "storage_location", None) or ""
        result["owner"] = getattr(info, "owner", "") or ""
        result["comment"] = getattr(info, "comment", "") or ""
    except Exception:
        pass

    return result


@router.get("/catalogs/{catalog}/schemas")
async def list_schemas(catalog: str, client=Depends(get_db_client)):
    """List schemas in a catalog using the SDK (no SQL warehouse needed)."""
    from src.client import list_schemas_sdk
    try:
        schemas = list_schemas_sdk(client, catalog, exclude=["information_schema", "default"])
        if schemas:
            return sorted(schemas)
    except Exception:
        pass
    # Fallback to SQL if SDK fails
    try:
        from src.client import execute_sql
        config = await get_app_config()
        wid = config.get("sql_warehouse_id", "")
        rows = execute_sql(client, wid, f"SELECT schema_name FROM {catalog}.information_schema.schemata WHERE schema_name NOT IN ('information_schema', 'default') ORDER BY schema_name")
        return [r["schema_name"] for r in rows]
    except Exception:
        return []


@router.get("/catalogs/{catalog}/{schema}/tables")
async def list_tables(catalog: str, schema: str, client=Depends(get_db_client)):
    """List tables in a schema using the SDK (no SQL warehouse needed)."""
    from src.client import list_tables_sdk
    try:
        tables = list_tables_sdk(client, catalog, schema)
        if tables:
            return sorted([t["table_name"] for t in tables])
    except Exception:
        pass
    # Fallback to SQL if SDK fails
    try:
        from src.client import execute_sql
        config = await get_app_config()
        wid = config.get("sql_warehouse_id", "")
        rows = execute_sql(client, wid, f"SELECT table_name FROM {catalog}.information_schema.tables WHERE table_schema = '{schema}' ORDER BY table_name")
        return [r["table_name"] for r in rows]
    except Exception:
        return []


@router.get("/uc-objects")
async def list_uc_objects(client=Depends(get_db_client)):
    """List all Unity Catalog workspace-level objects using the SDK (no SQL warehouse needed)."""
    result = {}

    # External Locations
    try:
        result["external_locations"] = [
            {"name": e.name, "url": getattr(e, "url", ""), "credential_name": getattr(e, "credential_name", ""),
             "owner": getattr(e, "owner", ""), "comment": getattr(e, "comment", ""),
             "read_only": getattr(e, "read_only", False)}
            for e in client.external_locations.list() if e.name
        ]
    except Exception:
        result["external_locations"] = []

    # Storage Credentials
    try:
        result["storage_credentials"] = [
            {"name": c.name, "owner": getattr(c, "owner", ""), "comment": getattr(c, "comment", ""),
             "read_only": getattr(c, "read_only", False),
             "used_for_managed_storage": getattr(c, "used_for_managed_storage", False)}
            for c in client.storage_credentials.list() if c.name
        ]
    except Exception:
        result["storage_credentials"] = []

    # Connections (for Lakehouse Federation)
    try:
        result["connections"] = [
            {"name": c.name, "connection_type": str(getattr(c, "connection_type", "")),
             "owner": getattr(c, "owner", ""), "comment": getattr(c, "comment", "")}
            for c in client.connections.list() if c.name
        ]
    except Exception:
        result["connections"] = []

    # Registered Models (ML)
    try:
        models = []
        for m in client.registered_models.list():
            if m.name:
                models.append({
                    "name": m.name, "full_name": getattr(m, "full_name", ""),
                    "owner": getattr(m, "owner", ""), "comment": getattr(m, "comment", ""),
                    "catalog_name": getattr(m, "catalog_name", ""), "schema_name": getattr(m, "schema_name", ""),
                })
            if len(models) >= 500:
                break
        result["registered_models"] = models
    except Exception:
        result["registered_models"] = []

    # Metastores
    try:
        current = client.metastores.current()
        result["metastore"] = {
            "name": getattr(current, "name", ""),
            "metastore_id": getattr(current, "metastore_id", ""),
            "owner": getattr(current, "owner", ""),
            "cloud": getattr(current, "cloud", ""),
            "region": getattr(current, "region", ""),
            "storage_root": getattr(current, "storage_root", ""),
            "default_data_access_config_id": getattr(current, "default_data_access_config_id", ""),
        }
    except Exception:
        result["metastore"] = None

    # Shares (Delta Sharing)
    try:
        result["shares"] = [
            {"name": s.name, "owner": getattr(s, "owner", ""), "comment": getattr(s, "comment", "")}
            for s in client.shares.list() if s.name
        ]
    except Exception:
        result["shares"] = []

    # Recipients (Delta Sharing)
    try:
        result["recipients"] = [
            {"name": r.name, "owner": getattr(r, "owner", ""), "comment": getattr(r, "comment", ""),
             "authentication_type": str(getattr(r, "authentication_type", ""))}
            for r in client.recipients.list() if r.name
        ]
    except Exception:
        result["recipients"] = []

    return result


@router.get("/catalogs/{catalog}/{schema}/{table}/info")
async def get_table_info(catalog: str, schema: str, table: str, client=Depends(get_db_client)):
    """Get detailed table metadata using the SDK (no SQL warehouse needed)."""
    from src.client import get_table_info_sdk
    full_name = f"{catalog}.{schema}.{table}"
    info = get_table_info_sdk(client, full_name)
    if info:
        return info
    return {"error": f"Table {full_name} not found", "name": table, "full_name": full_name}


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
            user_name STRING,
            status STRING,
            job_type STRING,
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
        # Add new columns only if they don't already exist
        try:
            existing = {r["col_name"].lower() for r in execute_sql(client, wid, f"DESCRIBE TABLE {metrics_fqn}") if r.get("col_name")}
            for col_name, col_type in [("user_name", "STRING"), ("status", "STRING"), ("job_type", "STRING")]:
                if col_name.lower() not in existing:
                    try:
                        execute_sql(client, wid, f"ALTER TABLE {metrics_fqn} ADD COLUMN {col_name} {col_type}")
                    except Exception:
                        pass
        except Exception:
            pass
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create metrics table: {e}")
    # Create rollback logs table
    try:
        from src.rollback import ensure_rollback_table
        rollback_fqn = ensure_rollback_table(client, wid, audit_config)
        tables_created.append(rollback_fqn)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create rollback table: {e}")
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
        f"{catalog}.{schema}.rollback_logs",
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
    return [{"key": k, **v} for k, v in TEMPLATES.items()]


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
async def query_lineage_endpoint(req: dict, client=Depends(get_db_client)):
    """Query lineage for a catalog or table.

    Merges data from up to 4 sources, supports multi-hop tracing,
    notebook/job attribution, time range filtering, and column lineage.
    """
    from src.client import execute_sql as _exec

    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    catalog = req.get("catalog", "")
    table = req.get("table")  # may be "schema.table"
    include_columns = req.get("include_columns", False)
    depth = min(req.get("depth", 1), 5)  # multi-hop depth, max 5
    date_from = req.get("date_from")  # ISO date string
    date_to = req.get("date_to")  # ISO date string

    table_entries = []
    column_entries = []
    sources_used = []
    target_fqn = f"{catalog}.{table}" if table else None

    # Time range WHERE clause fragment
    def _time_filter(col: str) -> str:
        parts = []
        if date_from:
            parts.append(f"{col} >= '{date_from}'")
        if date_to:
            parts.append(f"{col} <= '{date_to}T23:59:59'")
        return (" AND " + " AND ".join(parts)) if parts else ""

    # ── Source 1: system.access.table_lineage (with multi-hop + attribution) ──
    try:
        time_flt = _time_filter("event_time")
        if target_fqn:
            # Multi-hop: iteratively discover upstream/downstream
            visited_up = set()
            visited_down = set()
            frontier_up = {target_fqn}
            frontier_down = {target_fqn}

            for hop in range(depth):
                # Upstream hop
                if frontier_up:
                    placeholders = ",".join(f"'{t}'" for t in frontier_up)
                    up_sql = f"""
                        SELECT source_table_full_name, target_table_full_name,
                               source_type, target_type, entity_type,
                               entity_id, event_time
                        FROM system.access.table_lineage
                        WHERE target_table_full_name IN ({placeholders})
                          AND source_table_full_name IS NOT NULL
                          {time_flt}
                        ORDER BY event_time DESC LIMIT 200
                    """
                    rows = _exec(client, wid, up_sql)
                    next_up = set()
                    for r in rows:
                        src = r.get("source_table_full_name", "")
                        dst = r.get("target_table_full_name", "")
                        edge = (src, dst)
                        if edge not in visited_up:
                            visited_up.add(edge)
                            table_entries.append({
                                "source": src,
                                "destination": dst,
                                "clone_type": r.get("source_type", "READ"),
                                "timestamp": str(r.get("event_time", "")),
                                "direction": "upstream",
                                "data_source": "system_table",
                                "hop": hop + 1,
                                "entity_type": r.get("entity_type", ""),
                                "entity_id": r.get("entity_id", ""),
                            })
                            next_up.add(src)
                    frontier_up = next_up - {t for s, t in visited_up}

                # Downstream hop
                if frontier_down:
                    placeholders = ",".join(f"'{t}'" for t in frontier_down)
                    down_sql = f"""
                        SELECT source_table_full_name, target_table_full_name,
                               source_type, target_type, entity_type,
                               entity_id, event_time
                        FROM system.access.table_lineage
                        WHERE source_table_full_name IN ({placeholders})
                          AND target_table_full_name IS NOT NULL
                          {time_flt}
                        ORDER BY event_time DESC LIMIT 200
                    """
                    rows = _exec(client, wid, down_sql)
                    next_down = set()
                    for r in rows:
                        src = r.get("source_table_full_name", "")
                        dst = r.get("target_table_full_name", "")
                        edge = (src, dst)
                        if edge not in visited_down:
                            visited_down.add(edge)
                            table_entries.append({
                                "source": src,
                                "destination": dst,
                                "clone_type": r.get("target_type", "WRITE"),
                                "timestamp": str(r.get("event_time", "")),
                                "direction": "downstream",
                                "data_source": "system_table",
                                "hop": hop + 1,
                                "entity_type": r.get("entity_type", ""),
                                "entity_id": r.get("entity_id", ""),
                            })
                            next_down.add(dst)
                    frontier_down = next_down - {s for s, t in visited_down}
        else:
            # Catalog-level
            cat_sql = f"""
                SELECT source_table_full_name, target_table_full_name,
                       source_type, target_type, entity_type, entity_id, event_time
                FROM system.access.table_lineage
                WHERE (source_table_full_name LIKE '{catalog}.%'
                   OR target_table_full_name LIKE '{catalog}.%')
                   {time_flt}
                ORDER BY event_time DESC LIMIT 300
            """
            rows = _exec(client, wid, cat_sql)
            for r in rows:
                src = r.get("source_table_full_name", "")
                dst = r.get("target_table_full_name", "")
                direction = "downstream" if src.startswith(f"{catalog}.") else "upstream"
                table_entries.append({
                    "source": src, "destination": dst,
                    "clone_type": r.get("source_type", ""),
                    "timestamp": str(r.get("event_time", "")),
                    "direction": direction,
                    "data_source": "system_table",
                    "hop": 1,
                    "entity_type": r.get("entity_type", ""),
                    "entity_id": r.get("entity_id", ""),
                })
        if table_entries:
            sources_used.append("system.access.table_lineage")
    except Exception:
        pass

    # ── Source 2: system.access.column_lineage ──
    if include_columns and target_fqn:
        try:
            time_flt = _time_filter("event_time")
            col_sql = f"""
                SELECT source_table_full_name, source_column_name,
                       target_table_full_name, target_column_name,
                       event_time
                FROM system.access.column_lineage
                WHERE (target_table_full_name = '{target_fqn}'
                   OR source_table_full_name = '{target_fqn}')
                   {time_flt}
                ORDER BY event_time DESC LIMIT 300
            """
            col_rows = _exec(client, wid, col_sql)
            for r in col_rows:
                column_entries.append({
                    "source_table": r.get("source_table_full_name", ""),
                    "source_column": r.get("source_column_name", ""),
                    "target_table": r.get("target_table_full_name", ""),
                    "target_column": r.get("target_column_name", ""),
                    "timestamp": str(r.get("event_time", "")),
                })
            if column_entries:
                sources_used.append("system.access.column_lineage")
        except Exception:
            pass

    # ── Source 3: Clone-Xs lineage tracker ──
    try:
        from src.lineage_tracker import query_lineage
        fqn = f"{catalog}.{table}" if table else None
        rows = query_lineage(client, wid, table_fqn=fqn, limit=100)
        if not table and catalog:
            rows = [r for r in rows if catalog in r.get("source_fqn", "") or catalog in r.get("dest_fqn", "")]
        for r in rows:
            ts = str(r.get("cloned_at", ""))
            if date_from and ts < date_from:
                continue
            if date_to and ts > date_to + "T23:59:59":
                continue
            table_entries.append({
                "source": r.get("source_fqn", ""),
                "destination": r.get("dest_fqn", ""),
                "clone_type": r.get("clone_type", "CLONE"),
                "timestamp": ts,
                "direction": "downstream",
                "data_source": "clone_xs",
                "hop": 1,
                "entity_type": "CLONE_XS",
                "entity_id": r.get("operation_id", ""),
            })
        if rows:
            sources_used.append("clone_xs_lineage")
    except Exception:
        pass

    # ── Source 4: run_logs / audit_trail fallback ──
    if not table_entries:
        try:
            from src.run_logs import query_run_logs
            logs = query_run_logs(client, wid, config=config, limit=100)
            if catalog:
                logs = [r for r in logs if catalog in (r.get("source_catalog") or "") or catalog in (r.get("destination_catalog") or "")]
            for r in logs:
                if r.get("source_catalog") and r.get("destination_catalog"):
                    ts = str(r.get("started_at", ""))
                    if date_from and ts < date_from:
                        continue
                    if date_to and ts > date_to + "T23:59:59":
                        continue
                    table_entries.append({
                        "source": r.get("source_catalog", ""),
                        "destination": r.get("destination_catalog", ""),
                        "clone_type": r.get("clone_type", "CLONE"),
                        "timestamp": ts,
                        "direction": "downstream",
                        "data_source": "run_logs",
                        "hop": 1,
                        "entity_type": r.get("job_type", ""),
                        "entity_id": r.get("job_id", ""),
                    })
            if table_entries:
                sources_used.append("run_logs")
        except Exception:
            pass

    if not table_entries:
        try:
            from src.audit_trail import query_audit_history
            rows = query_audit_history(client, wid, config, limit=100, source_catalog=catalog or None)
            for r in rows:
                if r.get("source_catalog") and r.get("destination_catalog"):
                    ts = str(r.get("started_at", ""))
                    if date_from and ts < date_from:
                        continue
                    if date_to and ts > date_to + "T23:59:59":
                        continue
                    table_entries.append({
                        "source": r.get("source_catalog", ""),
                        "destination": r.get("destination_catalog", ""),
                        "clone_type": r.get("clone_type", "CLONE"),
                        "timestamp": ts,
                        "direction": "downstream",
                        "data_source": "audit_trail",
                        "hop": 1,
                        "entity_type": r.get("operation_type", ""),
                        "entity_id": r.get("operation_id", ""),
                    })
            if table_entries:
                sources_used.append("audit_trail")
        except Exception:
            pass

    # Deduplicate by source+destination (keep first = most recent)
    seen = set()
    unique_entries = []
    for e in table_entries:
        key = (e["source"], e["destination"])
        if key not in seen:
            seen.add(key)
            unique_entries.append(e)

    # Compute graph stats
    all_nodes = set()
    in_degree = {}
    out_degree = {}
    for e in unique_entries:
        s, d = e["source"], e["destination"]
        all_nodes.add(s)
        all_nodes.add(d)
        out_degree[s] = out_degree.get(s, 0) + 1
        in_degree[d] = in_degree.get(d, 0) + 1

    orphans = [n for n in all_nodes if in_degree.get(n, 0) == 0 and out_degree.get(n, 0) > 0]
    sinks = [n for n in all_nodes if out_degree.get(n, 0) == 0 and in_degree.get(n, 0) > 0]

    # Most connected = highest total degree
    total_degree = {}
    for n in all_nodes:
        total_degree[n] = in_degree.get(n, 0) + out_degree.get(n, 0)
    most_connected = sorted(total_degree.items(), key=lambda x: -x[1])[:10]

    # Build graph nodes/edges for visualization
    node_set = set()
    graph_nodes = []
    for e in unique_entries:
        for n in (e["source"], e["destination"]):
            if n and n not in node_set:
                node_set.add(n)
                graph_nodes.append({
                    "id": n,
                    "label": n.split(".")[-1] if "." in n else n,
                    "full_name": n,
                    "in_degree": in_degree.get(n, 0),
                    "out_degree": out_degree.get(n, 0),
                    "is_target": n == target_fqn,
                })
    graph_edges = [
        {"source": e["source"], "target": e["destination"], "type": e.get("clone_type", ""),
         "hop": e.get("hop", 1), "direction": e.get("direction", "")}
        for e in unique_entries
    ]

    return {
        "entries": unique_entries,
        "column_lineage": column_entries,
        "sources": sources_used,
        "total": len(unique_entries),
        "graph": {"nodes": graph_nodes, "edges": graph_edges},
        "stats": {
            "total_nodes": len(all_nodes),
            "total_edges": len(unique_entries),
            "orphans": orphans[:20],
            "sinks": sinks[:20],
            "most_connected": [{"name": n, "degree": d} for n, d in most_connected],
        },
    }


@router.post("/impact")
async def analyze_impact_endpoint(req: dict, client=Depends(get_db_client)):
    """Analyze downstream impact of changes."""
    try:
        from src.impact_analysis import analyze_impact
        config = await get_app_config()
        wid = config.get("sql_warehouse_id", "")

        # Build config dict that analyze_impact expects as 4th arg
        impact_config = dict(config)
        impact_config["schema"] = req.get("schema")
        impact_config["table"] = req.get("table")

        result = analyze_impact(client, wid, req.get("catalog", ""), impact_config)

        # Map field names to what the UI expects
        return {
            "affected_views": result.get("dependent_views", []),
            "affected_functions": result.get("dependent_functions", []),
            "downstream_tables": [],
            "risk_level": result.get("risk_level", "unknown"),
            "total_dependent_objects": result.get("total_dependent_objects", 0),
        }
    except Exception as e:
        return {"affected_views": [], "affected_functions": [], "downstream_tables": [], "risk_level": "unknown", "error": str(e)}


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
async def get_metrics(client=Depends(get_db_client)):
    """Get clone operation metrics from Delta tables."""
    try:
        from src.metrics import get_metrics_summary
        config = await get_app_config()
        wid = config.get("sql_warehouse_id", "")
        return get_metrics_summary(client, wid, config)
    except Exception:
        from src.metrics import _empty_summary
        return _empty_summary()


@router.get("/notifications")
async def get_notifications(client=Depends(get_db_client)):
    """Get recent notifications from run logs (completions, failures, TTL warnings)."""
    try:
        from src.client import execute_sql
        from src.run_logs import get_run_logs_fqn
        from src.audit_trail import get_audit_table_fqn
        config = await get_app_config()
        wid = config.get("sql_warehouse_id", "")

        run_logs_fqn = get_run_logs_fqn(config)
        audit_fqn = get_audit_table_fqn(config)
        metrics_fqn = config.get("metrics_table", "clone_audit.metrics.clone_metrics")

        # Normalize column names via SQL aliases for each table
        queries = [
            # run_logs: job_id, job_type
            f"""SELECT job_id, job_type, source_catalog, destination_catalog,
                       status, completed_at, duration_seconds, error_message
                FROM {run_logs_fqn}
                WHERE completed_at IS NOT NULL
                ORDER BY completed_at DESC LIMIT 20""",
            # clone_operations: operation_id → job_id, operation_type → job_type
            f"""SELECT operation_id AS job_id, operation_type AS job_type,
                       source_catalog, destination_catalog,
                       status, completed_at, duration_seconds, error_message
                FROM {audit_fqn}
                WHERE completed_at IS NOT NULL
                ORDER BY completed_at DESC LIMIT 20""",
            # clone_metrics: operation_id → job_id, derive status from failed count
            f"""SELECT operation_id AS job_id, 'clone' AS job_type,
                       source_catalog, destination_catalog,
                       CASE WHEN failed > 0 THEN 'completed_with_errors'
                            ELSE 'success' END AS status,
                       completed_at, duration_seconds,
                       CAST(NULL AS STRING) AS error_message
                FROM {metrics_fqn}
                WHERE completed_at IS NOT NULL
                ORDER BY completed_at DESC LIMIT 20""",
        ]

        rows = []
        for sql in queries:
            try:
                rows = execute_sql(client, wid, sql)
                if rows:
                    break
            except Exception:
                continue

        items = []
        for r in rows:
            status = r.get("status", "")
            src = r.get("source_catalog", "")
            dest = r.get("destination_catalog", "")
            job_type = r.get("job_type") or "clone"

            if status in ("completed", "success"):
                msg = f"{job_type.capitalize()} completed: {src} → {dest}"
                ntype = "success"
            elif status == "failed":
                err = r.get("error_message", "")
                msg = f"{job_type.capitalize()} failed: {src} → {dest}"
                if err:
                    msg += f" — {err[:80]}"
                ntype = "error"
            else:
                msg = f"{job_type.capitalize()} {status}: {src} → {dest}"
                ntype = "info"

            items.append({
                "type": ntype,
                "message": msg,
                "timestamp": str(r.get("completed_at", "")),
                "status": status,
                "job_id": r.get("job_id") or "",
            })

        return {"unread_count": len(items), "items": items}
    except Exception:
        return {"unread_count": 0, "items": []}


@router.get("/catalog-health")
async def get_catalog_health(client=Depends(get_db_client)):
    """Get aggregate catalog health score based on recent operations."""
    try:
        from src.client import execute_sql
        from src.run_logs import get_run_logs_fqn
        from src.audit_trail import get_audit_table_fqn
        config = await get_app_config()
        wid = config.get("sql_warehouse_id", "")

        # Get recent operation stats per source catalog
        run_logs_fqn = get_run_logs_fqn(config)
        audit_fqn = get_audit_table_fqn(config)
        metrics_fqn = config.get("metrics_table", "clone_audit.metrics.clone_metrics")

        catalogs = {}

        # Queries with normalized status column
        health_queries = [
            # run_logs & clone_operations have status column
            f"""SELECT source_catalog, status, COUNT(*) as cnt,
                       MAX(completed_at) as last_operation
                FROM {run_logs_fqn}
                WHERE source_catalog IS NOT NULL AND source_catalog != ''
                GROUP BY source_catalog, status ORDER BY source_catalog""",
            f"""SELECT source_catalog, status, COUNT(*) as cnt,
                       MAX(completed_at) as last_operation
                FROM {audit_fqn}
                WHERE source_catalog IS NOT NULL AND source_catalog != ''
                GROUP BY source_catalog, status ORDER BY source_catalog""",
            # clone_metrics: derive status from failed column
            f"""SELECT source_catalog,
                       CASE WHEN failed > 0 THEN 'completed_with_errors'
                            ELSE 'success' END AS status,
                       COUNT(*) as cnt,
                       MAX(completed_at) as last_operation
                FROM {metrics_fqn}
                WHERE source_catalog IS NOT NULL AND source_catalog != ''
                GROUP BY source_catalog, CASE WHEN failed > 0 THEN 'completed_with_errors' ELSE 'success' END
                ORDER BY source_catalog""",
        ]

        rows = []
        for sql in health_queries:
            try:
                rows = execute_sql(client, wid, sql)
                if rows:
                    break
            except Exception:
                continue
        for r in rows:
            cat = r.get("source_catalog", "")
            if not cat:
                continue
            if cat not in catalogs:
                catalogs[cat] = {"catalog": cat, "total": 0, "succeeded": 0, "failed": 0, "last_operation": None, "score": 100}
            cnt = int(r.get("cnt", 0))
            catalogs[cat]["total"] += cnt
            if r.get("status") in ("completed", "success"):
                catalogs[cat]["succeeded"] += cnt
            elif r.get("status") == "failed":
                catalogs[cat]["failed"] += cnt
            lo = r.get("last_operation")
            if lo:
                catalogs[cat]["last_operation"] = str(lo)

        # Query for object counts — try clone_operations first, then clone_metrics
        # clone_operations: tables_cloned, tables_failed, total_size_bytes
        # clone_metrics: total_tables, failed, total_size_bytes
        obj_queries = [
            f"""SELECT source_catalog,
                       SUM(tables_cloned) as tables,
                       SUM(tables_failed) as tables_failed,
                       SUM(total_size_bytes) as total_bytes
                FROM {audit_fqn}
                WHERE source_catalog IS NOT NULL AND source_catalog != ''
                GROUP BY source_catalog""",
            f"""SELECT source_catalog,
                       SUM(total_tables) as tables,
                       SUM(failed) as tables_failed,
                       SUM(total_size_bytes) as total_bytes
                FROM {metrics_fqn}
                WHERE source_catalog IS NOT NULL AND source_catalog != ''
                GROUP BY source_catalog""",
        ]
        for obj_sql in obj_queries:
            try:
                agg_rows = execute_sql(client, wid, obj_sql)
                if agg_rows:
                    for r in agg_rows:
                        cat = r.get("source_catalog", "")
                        if cat in catalogs:
                            catalogs[cat]["tables_cloned"] = int(r.get("tables", 0) or 0)
                            catalogs[cat]["tables_failed"] = int(r.get("tables_failed", 0) or 0)
                            catalogs[cat]["total_bytes"] = int(r.get("total_bytes", 0) or 0)
                    break
            except Exception:
                continue

        # Compute health scores
        for cat in catalogs.values():
            score = 100
            total = cat.get("total", 0)
            failed = cat.get("failed", 0)
            if total > 0 and failed > 0:
                failure_rate = failed / total
                if failure_rate > 0.5:
                    score -= 30
                elif failure_rate > 0.2:
                    score -= 20
                elif failure_rate > 0:
                    score -= 10
            tf = cat.get("tables_failed", 0)
            if tf and tf > 5:
                score -= 15
            elif tf and tf > 0:
                score -= 5
            if not cat.get("last_operation"):
                score -= 10
            cat["score"] = max(0, min(100, score))

        result = sorted(catalogs.values(), key=lambda c: c["score"])
        return {"catalogs": result}
    except Exception:
        return {"catalogs": []}
