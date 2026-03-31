"""Reconciliation endpoints — row-level and column-level with SQL/Spark toggle."""

import os

from typing import Optional

from fastapi import APIRouter, Depends, Query, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field

from api.dependencies import get_db_client, get_app_config, get_credentials, get_job_manager

router = APIRouter()


# ── Request Models ────────────────────────────────────────────────────────────

class ReconciliationValidateRequest(BaseModel):
    source_catalog: str
    destination_catalog: str
    schema_name: str = ""
    table_name: str = ""
    exclude_schemas: list[str] = Field(default=["information_schema", "default"])
    use_checksum: bool = False
    max_workers: int = 4
    use_spark: bool = False


class ReconciliationCompareRequest(BaseModel):
    source_catalog: str
    destination_catalog: str
    schema_name: str = ""
    table_name: str = ""
    exclude_schemas: list[str] = Field(default=["information_schema", "default"])
    use_checksum: bool = False
    max_workers: int = 4
    use_spark: bool = False


class ReconciliationProfileRequest(BaseModel):
    source_catalog: str
    schema_name: str = ""
    exclude_schemas: list[str] = Field(default=["information_schema", "default"])
    use_spark: bool = False


class ReconciliationPreviewRequest(BaseModel):
    source_catalog: str
    destination_catalog: str
    schema_name: str
    table_name: str


class DeepReconciliationRequest(BaseModel):
    source_catalog: str
    destination_catalog: str
    schema_name: str = ""
    table_name: str = ""
    key_columns: list[str] = Field(default=[])
    include_columns: list[str] = Field(default=[])
    ignore_columns: list[str] = Field(default=[])
    sample_diffs: int = 10
    use_checksum: bool = False
    max_workers: int = 4
    ignore_nulls: bool = False
    ignore_case: bool = False
    ignore_whitespace: bool = False
    decimal_precision: int = 0


class BatchReconciliationRequest(BaseModel):
    source_catalog: str
    destination_catalog: str
    tables: list[dict] = Field(default=[], description="List of {schema_name, table_name} pairs")
    use_checksum: bool = False
    max_workers: int = 4
    use_spark: bool = False


class BatchCompareRequest(BaseModel):
    source_catalog: str
    destination_catalog: str
    tables: list[dict] = Field(default=[], description="List of {schema_name, table_name} pairs")
    use_checksum: bool = False
    max_workers: int = 4
    use_spark: bool = False


class BatchDeepReconciliationRequest(BaseModel):
    source_catalog: str
    destination_catalog: str
    tables: list[dict] = Field(default=[], description="List of {schema_name, table_name} pairs")
    key_columns: list[str] = Field(default=[])
    include_columns: list[str] = Field(default=[])
    ignore_columns: list[str] = Field(default=[])
    sample_diffs: int = 10
    use_checksum: bool = False
    max_workers: int = 4
    ignore_nulls: bool = False
    ignore_case: bool = False
    ignore_whitespace: bool = False
    decimal_precision: int = 0


# ── Spark Status & Configure ─────────────────────────────────────────────────

@router.get("/spark-status", summary="Check Spark session status")
async def spark_status(client=Depends(get_db_client), creds: tuple = Depends(get_credentials)):
    """Check Spark session availability for reconciliation."""
    try:
        from src.spark_session import ensure_configured, get_spark_status
        # Push auth context into env so Spark session picks it up
        host, token, _ = creds
        if host:
            os.environ["DATABRICKS_HOST"] = host
        if token:
            os.environ["DATABRICKS_TOKEN"] = token
        ensure_configured()
        return get_spark_status()
    except Exception as e:
        return {"available": False, "error": str(e), "cluster_id": "", "serverless": False, "session_active": False}


@router.post("/spark-configure", summary="Configure Spark session")
async def spark_configure(req: dict, creds: tuple = Depends(get_credentials)):
    """Configure Spark session (cluster_id or serverless mode) for reconciliation."""
    from src.spark_session import configure_spark, get_spark_status
    host, token, _ = creds
    host = host or os.environ.get("DATABRICKS_HOST", "")
    token = token or os.environ.get("DATABRICKS_TOKEN", "")
    configure_spark(
        cluster_id=req.get("cluster_id", ""),
        serverless=req.get("serverless", False),
        host=host,
        token=token,
    )
    return get_spark_status()


# ── Row-Level Validation ─────────────────────────────────────────────────────

@router.post("/validate", summary="Row-level reconciliation (SQL or Spark)")
async def validate(req: ReconciliationValidateRequest, client=Depends(get_db_client)):
    """Compare row counts and checksums between source and destination catalogs.

    When use_spark=true, uses Spark DataFrames via Databricks Connect.
    Otherwise falls back to SQL warehouse execution.
    Results are stored in Delta tables for audit trail.
    """
    import time
    import logging as _log
    _logger = _log.getLogger(__name__)
    start = time.time()
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    execution_mode = "spark" if req.use_spark else "sql"

    _logger.info(f"[Reconciliation] source={req.source_catalog} dest={req.destination_catalog} "
                 f"schema={req.schema_name!r} table={req.table_name!r} spark={req.use_spark}")

    # Single table reconciliation
    if req.schema_name and req.table_name:
        if req.use_spark:
            from src.reconciliation_spark import validate_table_spark
            result = validate_table_spark(
                req.source_catalog, req.destination_catalog,
                req.schema_name, req.table_name, req.use_checksum,
            )
            details = [result]
        else:
            from src.validation import validate_table
            result = validate_table(
                client, wid, req.source_catalog, req.destination_catalog,
                req.schema_name, req.table_name, req.use_checksum,
            )
            details = [result]
        matched = sum(1 for r in details if r["match"])
        response = {
            "total_tables": len(details), "matched": matched,
            "mismatched": len(details) - matched - sum(1 for r in details if r.get("error")),
            "errors": sum(1 for r in details if r.get("error")),
            "details": details,
        }
    else:
        # Schema or full catalog reconciliation
        schemas_filter = [req.schema_name] if req.schema_name else None
        if req.use_spark:
            from src.reconciliation_spark import validate_catalog_spark
            response = validate_catalog_spark(
                source_catalog=req.source_catalog,
                dest_catalog=req.destination_catalog,
                exclude_schemas=req.exclude_schemas,
                include_schemas=schemas_filter,
                use_checksum=req.use_checksum,
                max_workers=req.max_workers,
            )
        else:
            from src.validation import validate_catalog
            response = validate_catalog(
                client, wid,
                source_catalog=req.source_catalog,
                dest_catalog=req.destination_catalog,
                exclude_schemas=req.exclude_schemas,
                max_workers=req.max_workers,
                use_checksum=req.use_checksum,
            )

    duration = time.time() - start

    # Store results in Delta tables (non-blocking — don't fail the response)
    try:
        from src.reconciliation_store import store_reconciliation_result
        run_id = store_reconciliation_result(
            client, wid, config, response,
            run_type="row-level",
            source_catalog=req.source_catalog,
            destination_catalog=req.destination_catalog,
            schema_name=req.schema_name,
            table_name=req.table_name,
            execution_mode=execution_mode,
            use_checksum=req.use_checksum,
            max_workers=req.max_workers,
            duration_seconds=duration,
        )
        response["run_id"] = run_id
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"Could not store reconciliation results: {e}")

    response["duration_seconds"] = round(duration, 2)
    return response


# ── Column-Level Comparison ──────────────────────────────────────────────────

@router.post("/compare", summary="Column-level reconciliation (SQL or Spark)")
async def compare(req: ReconciliationCompareRequest, client=Depends(get_db_client)):
    """Compare column schemas between source and destination catalogs.

    When use_spark=true, uses Spark schema inspection.
    Otherwise falls back to SQL warehouse execution.
    Results are stored in Delta tables for audit trail.
    """
    import time
    start = time.time()
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    execution_mode = "spark" if req.use_spark else "sql"

    if req.use_spark:
        from src.reconciliation_spark import compare_catalogs_spark
        response = compare_catalogs_spark(
            source_catalog=req.source_catalog,
            dest_catalog=req.destination_catalog,
            exclude_schemas=req.exclude_schemas,
            max_workers=req.max_workers,
            use_checksum=req.use_checksum,
        )
    else:
        from src.compare import compare_catalogs_deep
        response = compare_catalogs_deep(
            client, wid,
            source_catalog=req.source_catalog,
            dest_catalog=req.destination_catalog,
            exclude_schemas=req.exclude_schemas,
            max_workers=req.max_workers,
            use_checksum=req.use_checksum,
        )

    duration = time.time() - start

    # Store results in Delta tables
    try:
        from src.reconciliation_store import store_reconciliation_result
        # Adapt compare results to the common storage shape
        adapted = {
            "total_tables": response.get("total_tables", 0),
            "matched": response.get("tables_ok", 0),
            "mismatched": response.get("tables_with_issues", 0),
            "errors": 0,
            "details": [
                {
                    "schema": d.get("schema", ""),
                    "table": d.get("table", ""),
                    "source_count": None,
                    "dest_count": None,
                    "match": not d.get("issues"),
                    "checksum_match": None,
                    "error": "; ".join(d.get("issues", [])) if d.get("issues") else None,
                }
                for d in response.get("details", [])
            ],
        }
        run_id = store_reconciliation_result(
            client, wid, config, adapted,
            run_type="column-level",
            source_catalog=req.source_catalog,
            destination_catalog=req.destination_catalog,
            schema_name=req.schema_name,
            table_name=req.table_name,
            execution_mode=execution_mode,
            use_checksum=req.use_checksum,
            max_workers=req.max_workers,
            duration_seconds=duration,
        )
        response["run_id"] = run_id
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"Could not store comparison results: {e}")

    response["duration_seconds"] = round(duration, 2)
    return response


# ── Column Profiling ─────────────────────────────────────────────────────────

@router.post("/profile", summary="Column profiling (SQL or Spark)")
async def profile(req: ReconciliationProfileRequest, client=Depends(get_db_client)):
    """Profile column statistics for a catalog.

    When use_spark=true, uses Spark DataFrame operations.
    Otherwise falls back to SQL warehouse execution.
    """
    if req.use_spark:
        from src.reconciliation_spark import profile_catalog_spark
        return profile_catalog_spark(
            catalog=req.source_catalog,
            exclude_schemas=req.exclude_schemas,
        )
    else:
        from src.profiling import profile_catalog
        config = await get_app_config()
        wid = config.get("sql_warehouse_id", "")
        return profile_catalog(
            client, wid,
            catalog=req.source_catalog,
            exclude_schemas=req.exclude_schemas,
        )


# ── Preview ──────────────────────────────────────────────────────────────────

@router.post("/preview", summary="Preview table pair before deep reconciliation")
async def preview(req: ReconciliationPreviewRequest, client=Depends(get_db_client)):
    """Get metadata, column match status, and sample rows for a table pair.

    Uses Spark Connect for data access. The SDK client is only used for
    key column detection (optional — falls back gracefully).
    """
    from src.reconciliation_deep import get_table_preview
    try:
        return get_table_preview(
            client, req.source_catalog, req.destination_catalog,
            req.schema_name, req.table_name,
        )
    except Exception as e:
        # If SDK client fails, try without it (Spark-only mode)
        return get_table_preview(
            None, req.source_catalog, req.destination_catalog,
            req.schema_name, req.table_name,
        )


# ── Deep Reconciliation ─────────────────────────────────────────────────────

@router.post("/deep-validate", summary="Deep row-level reconciliation via Spark")
async def deep_validate(req: DeepReconciliationRequest, client=Depends(get_db_client)):
    """Full row-level reconciliation using PySpark DataFrames.

    Classifies every row as matched, missing, extra, or modified.
    For modified rows, shows column-level diffs. Requires Spark.
    Results are stored in Delta tables.
    """
    import time
    start = time.time()
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")

    from src.reconciliation_deep import deep_reconcile_catalog
    comparison_options = {
        "ignore_nulls": req.ignore_nulls,
        "ignore_case": req.ignore_case,
        "ignore_whitespace": req.ignore_whitespace,
        "decimal_precision": req.decimal_precision,
    }
    response = deep_reconcile_catalog(
        source_catalog=req.source_catalog,
        dest_catalog=req.destination_catalog,
        schema_name=req.schema_name,
        table_name=req.table_name,
        key_columns=req.key_columns or None,
        include_columns=req.include_columns or None,
        ignore_columns=req.ignore_columns or None,
        sample_diffs=req.sample_diffs,
        use_checksum=req.use_checksum,
        max_workers=req.max_workers,
        comparison_options=comparison_options,
    )

    duration = time.time() - start

    # Store results in Delta
    try:
        from src.reconciliation_store import store_reconciliation_result
        adapted = {
            "total_tables": response.get("total_tables", 0),
            "matched": response.get("matched_rows", 0),
            "mismatched": response.get("missing_in_dest", 0) + response.get("extra_in_dest", 0) + response.get("modified_rows", 0),
            "errors": response.get("errors", 0),
            "details": [
                {
                    "schema": d.get("schema", ""),
                    "table": d.get("table", ""),
                    "source_count": d.get("source_count"),
                    "dest_count": d.get("dest_count"),
                    "match": d.get("missing_in_dest", 0) == 0 and d.get("extra_in_dest", 0) == 0 and d.get("modified_rows", 0) == 0,
                    "checksum_match": None,
                    "error": d.get("error"),
                }
                for d in response.get("details", [])
            ],
        }
        run_id = store_reconciliation_result(
            client, wid, config, adapted,
            run_type="deep",
            source_catalog=req.source_catalog,
            destination_catalog=req.destination_catalog,
            schema_name=req.schema_name,
            table_name=req.table_name,
            execution_mode="spark-deep",
            use_checksum=req.use_checksum,
            max_workers=req.max_workers,
            duration_seconds=duration,
        )
        response["run_id"] = run_id
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"Could not store deep reconciliation results: {e}")

    response["duration_seconds"] = round(duration, 2)
    return response


# ── History & Compare Runs ────────────────────────────────────────────────────

@router.get("/history", summary="Reconciliation run history")
async def reconciliation_history(
    limit: int = Query(default=20, ge=1, le=200),
    run_type: Optional[str] = Query(default=None),
    source_catalog: Optional[str] = Query(default=None),
    client=Depends(get_db_client),
):
    """Query past reconciliation runs from Delta tables.

    Supports optional filters on run_type (row-level, column-level, deep)
    and source_catalog. Returns runs ordered by executed_at DESC.
    """
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")

    from src.reconciliation_store import get_reconciliation_history
    runs = get_reconciliation_history(
        client=client,
        warehouse_id=wid,
        config=config,
        limit=limit,
        run_type=run_type,
        source_catalog=source_catalog,
    )
    return {"runs": runs, "total": len(runs)}


class CompareRunsRequest(BaseModel):
    run_id_a: str
    run_id_b: str


@router.post("/compare-runs", summary="Compare two reconciliation runs side-by-side")
async def compare_runs(req: CompareRunsRequest, client=Depends(get_db_client)):
    """Fetch two reconciliation runs by ID and return them side-by-side for comparison."""
    from fastapi import HTTPException
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")

    from src.reconciliation_store import _query_sql, _get_schema, _esc
    schema = _get_schema(config)

    results = {}
    for label, run_id in [("run_a", req.run_id_a), ("run_b", req.run_id_b)]:
        query = f"""
            SELECT run_id, run_type, source_catalog, destination_catalog,
                   schema_name, table_name, execution_mode, total_tables,
                   matched, mismatched, errors, checksum_enabled, max_workers,
                   duration_seconds, executed_at, executed_by
            FROM {schema}.reconciliation_runs
            WHERE run_id = '{_esc(run_id)}'
        """
        rows = _query_sql(query, limit=1, client=client, warehouse_id=wid)
        if not rows:
            raise HTTPException(status_code=404, detail=f"Run {run_id} not found")
        results[label] = rows[0]

        # Also fetch details for each run
        details_query = f"""
            SELECT schema_name, table_name, source_count, dest_count,
                   delta_count, match, checksum_match, error, executed_at
            FROM {schema}.reconciliation_details
            WHERE run_id = '{_esc(run_id)}'
        """
        results[f"{label}_details"] = _query_sql(
            details_query, limit=1000, client=client, warehouse_id=wid,
        )

    return results


# ── SQL Execution (Spark or Warehouse) ───────────────────────────────────────

@router.post("/execute-sql", summary="Execute SQL via Spark or SQL Warehouse")
async def execute_sql_endpoint(req: dict, client=Depends(get_db_client)):
    """Execute arbitrary SQL using either Spark Connect (serverless) or SQL warehouse.

    Request body:
      - sql: SQL query string (required)
      - use_spark: bool (default false) — if true, runs via Spark Connect
      - warehouse_id: optional warehouse ID override (for SQL mode)
    """
    from fastapi import HTTPException
    sql_query = req.get("sql", "").strip()
    use_spark = req.get("use_spark", False)

    if not sql_query:
        raise HTTPException(status_code=400, detail="sql is required")

    if use_spark:
        try:
            from src.spark_session import get_spark
            spark = get_spark()
            if spark is None:
                raise HTTPException(status_code=400, detail="Spark session not available. Connect first.")
            rows = [row.asDict() for row in spark.sql(sql_query).limit(10000).collect()]
            for row in rows:
                for k, v in row.items():
                    if v is not None and not isinstance(v, (str, int, float, bool)):
                        row[k] = str(v)
            return rows
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
    else:
        config = await get_app_config()
        wid = req.get("warehouse_id") or config.get("sql_warehouse_id", "")
        if not wid:
            raise HTTPException(status_code=400, detail="No SQL warehouse configured")
        try:
            from src.client import execute_sql
            rows = execute_sql(client, wid, sql_query)
            return rows if rows else []
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))


# ── Alert Rules ──────────────────────────────────────────────────────────────

@router.get("/alerts/rules", summary="List alert rules")
async def list_alerts(client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.reconciliation_alerts import list_alert_rules
    return list_alert_rules(client, wid, config)


@router.post("/alerts/rules", summary="Create alert rule")
async def create_alert(req: dict, client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.reconciliation_alerts import create_alert_rule
    return create_alert_rule(
        client, wid, config,
        name=req.get("name", ""),
        metric=req.get("metric", "match_rate"),
        operator=req.get("operator", "<"),
        threshold=float(req.get("threshold", 95)),
        severity=req.get("severity", "warning"),
        source_catalog=req.get("source_catalog", ""),
        destination_catalog=req.get("destination_catalog", ""),
        notify_channels=req.get("notify_channels", ["email"]),
    )


@router.delete("/alerts/rules/{rule_id}", summary="Delete alert rule")
async def delete_alert(rule_id: str, client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.reconciliation_alerts import delete_alert_rule
    delete_alert_rule(rule_id, client, wid, config)
    return {"status": "deleted", "rule_id": rule_id}


@router.get("/alerts/history", summary="Get alert history")
async def alert_history(limit: int = 50, client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.reconciliation_alerts import get_alert_history
    return get_alert_history(client, wid, config, limit)


# ── Remediation ──────────────────────────────────────────────────────────────

@router.post("/remediate", summary="Generate fix SQL for mismatches")
async def remediate(req: dict):
    """Generate SQL statements to fix reconciliation mismatches."""
    from src.reconciliation_remediate import generate_fix_sql
    return generate_fix_sql(
        source_catalog=req.get("source_catalog", ""),
        dest_catalog=req.get("destination_catalog", ""),
        schema=req.get("schema_name", ""),
        table_name=req.get("table_name", ""),
        key_columns=req.get("key_columns", []),
        fix_type=req.get("fix_type", "all"),
    )


# ── Scheduling ───────────────────────────────────────────────────────────────

@router.get("/schedules", summary="List reconciliation schedules")
async def list_schedules():
    """List all scheduled reconciliation jobs."""
    from src.reconciliation_schedule import list_recon_schedules
    return list_recon_schedules()


@router.post("/schedules", summary="Create reconciliation schedule")
async def create_schedule(req: dict):
    """Create a new scheduled reconciliation job."""
    from src.reconciliation_schedule import create_recon_schedule
    return create_recon_schedule(
        name=req.get("name", ""),
        source_catalog=req.get("source_catalog", ""),
        destination_catalog=req.get("destination_catalog", ""),
        cron=req.get("cron", ""),
        schema_name=req.get("schema_name", ""),
        table_name=req.get("table_name", ""),
        key_columns=req.get("key_columns"),
        comparison_options=req.get("comparison_options"),
    )


@router.delete("/schedules/{schedule_id}")
async def delete_schedule(schedule_id: str):
    """Delete a reconciliation schedule."""
    from src.reconciliation_schedule import delete_recon_schedule
    deleted = delete_recon_schedule(schedule_id)
    if not deleted:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"Schedule {schedule_id} not found")
    return {"status": "deleted", "schedule_id": schedule_id}


@router.post("/schedules/{schedule_id}/pause")
async def pause_schedule(schedule_id: str):
    """Pause a reconciliation schedule."""
    from src.reconciliation_schedule import pause_recon_schedule
    result = pause_recon_schedule(schedule_id)
    if result is None:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"Schedule {schedule_id} not found")
    return result


@router.post("/schedules/{schedule_id}/resume")
async def resume_schedule(schedule_id: str):
    """Resume a paused reconciliation schedule."""
    from src.reconciliation_schedule import resume_recon_schedule
    result = resume_recon_schedule(schedule_id)
    if result is None:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"Schedule {schedule_id} not found")
    return result


# ── Batch Reconciliation (Background Jobs) ───────────────────────────────────

@router.post("/batch-validate", summary="Submit batch row-level reconciliation job")
async def batch_validate(
    req: BatchReconciliationRequest,
    client=Depends(get_db_client),
    app_config=Depends(get_app_config),
    jm=Depends(get_job_manager),
):
    """Submit a batch reconciliation job that runs in the background.

    Returns a job_id immediately. Poll GET /batch-validate/{job_id} for progress
    or connect via WS /reconciliation/ws/{job_id} for live updates.
    """
    if not req.tables:
        from fastapi import HTTPException
        raise HTTPException(status_code=400, detail="No tables specified")

    config = dict(app_config)
    config.update({
        "source_catalog": req.source_catalog,
        "destination_catalog": req.destination_catalog,
        "tables": [t if isinstance(t, dict) else t for t in req.tables],
        "use_checksum": req.use_checksum,
        "max_workers": req.max_workers,
        "use_spark": req.use_spark,
    })
    job_id = await jm.submit_job("reconciliation-batch", config, client)
    return {"job_id": job_id, "status": "queued", "total_tables": len(req.tables)}


@router.get("/batch-validate/{job_id}", summary="Get batch reconciliation job status")
async def get_batch_job(job_id: str, jm=Depends(get_job_manager)):
    """Get status and progress of a batch reconciliation job."""
    job = jm.get_job(job_id)
    if not job:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Job not found")
    return job


@router.delete("/batch-validate/{job_id}", summary="Cancel a batch reconciliation job")
async def cancel_batch_job(job_id: str, jm=Depends(get_job_manager)):
    """Cancel a queued batch reconciliation job."""
    jm.cancel_job(job_id)
    return {"status": "cancelled", "job_id": job_id}


@router.websocket("/ws/{job_id}")
async def recon_progress_ws(websocket: WebSocket, job_id: str, jm=Depends(get_job_manager)):
    """WebSocket endpoint for live batch reconciliation progress."""
    await jm.connection_manager.connect(websocket, job_id)
    try:
        while True:
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_json({"type": "pong"})
    except WebSocketDisconnect:
        jm.connection_manager.disconnect(websocket, job_id)


# ── Batch Column-Level Comparison (Background Jobs) ──────────────────────────

@router.post("/batch-compare", summary="Submit batch column-level comparison job")
async def batch_compare(
    req: BatchCompareRequest,
    client=Depends(get_db_client),
    app_config=Depends(get_app_config),
    jm=Depends(get_job_manager),
):
    """Submit a batch column-level comparison job that runs in the background.

    Returns a job_id immediately. Poll GET /batch-compare/{job_id} for progress
    or connect via WS /reconciliation/ws/{job_id} for live updates.
    """
    if not req.tables:
        from fastapi import HTTPException
        raise HTTPException(status_code=400, detail="No tables specified")

    config = dict(app_config)
    config.update({
        "source_catalog": req.source_catalog,
        "destination_catalog": req.destination_catalog,
        "tables": [t if isinstance(t, dict) else t for t in req.tables],
        "use_checksum": req.use_checksum,
        "max_workers": req.max_workers,
        "use_spark": req.use_spark,
    })
    job_id = await jm.submit_job("reconciliation-batch-compare", config, client)
    return {"job_id": job_id, "status": "queued", "total_tables": len(req.tables)}


@router.get("/batch-compare/{job_id}", summary="Get batch column-level comparison job status")
async def get_batch_compare_job(job_id: str, jm=Depends(get_job_manager)):
    """Get status and progress of a batch column-level comparison job."""
    job = jm.get_job(job_id)
    if not job:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Job not found")
    return job


@router.delete("/batch-compare/{job_id}", summary="Cancel a batch column-level comparison job")
async def cancel_batch_compare_job(job_id: str, jm=Depends(get_job_manager)):
    """Cancel a queued batch column-level comparison job."""
    jm.cancel_job(job_id)
    return {"status": "cancelled", "job_id": job_id}


# ── Batch Deep Reconciliation (Background Jobs) ─────────────────────────────

@router.post("/batch-deep-validate", summary="Submit batch deep reconciliation job")
async def batch_deep_validate(
    req: BatchDeepReconciliationRequest,
    client=Depends(get_db_client),
    app_config=Depends(get_app_config),
    jm=Depends(get_job_manager),
):
    """Submit a batch deep reconciliation job that runs in the background.

    Returns a job_id immediately. Poll GET /batch-deep-validate/{job_id} for progress
    or connect via WS /reconciliation/ws/{job_id} for live updates.
    """
    if not req.tables:
        from fastapi import HTTPException
        raise HTTPException(status_code=400, detail="No tables specified")

    config = dict(app_config)
    config.update({
        "source_catalog": req.source_catalog,
        "destination_catalog": req.destination_catalog,
        "tables": [t if isinstance(t, dict) else t for t in req.tables],
        "key_columns": req.key_columns,
        "include_columns": req.include_columns,
        "ignore_columns": req.ignore_columns,
        "sample_diffs": req.sample_diffs,
        "use_checksum": req.use_checksum,
        "max_workers": req.max_workers,
        "comparison_options": {
            "ignore_nulls": req.ignore_nulls,
            "ignore_case": req.ignore_case,
            "ignore_whitespace": req.ignore_whitespace,
            "decimal_precision": req.decimal_precision,
        },
    })
    job_id = await jm.submit_job("reconciliation-batch-deep", config, client)
    return {"job_id": job_id, "status": "queued", "total_tables": len(req.tables)}


@router.get("/batch-deep-validate/{job_id}", summary="Get batch deep reconciliation job status")
async def get_batch_deep_job(job_id: str, jm=Depends(get_job_manager)):
    """Get status and progress of a batch deep reconciliation job."""
    job = jm.get_job(job_id)
    if not job:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Job not found")
    return job


@router.delete("/batch-deep-validate/{job_id}", summary="Cancel a batch deep reconciliation job")
async def cancel_batch_deep_job(job_id: str, jm=Depends(get_job_manager)):
    """Cancel a queued batch deep reconciliation job."""
    jm.cancel_job(job_id)
    return {"status": "cancelled", "job_id": job_id}


# ── Run Detail Drill-Down ────────────────────────────────────────────────────

@router.get("/history/{run_id}/details", summary="Get per-table details for a reconciliation run")
async def get_run_details_endpoint(run_id: str, client=Depends(get_db_client)):
    """Get per-table details for a specific reconciliation run (for history drill-down)."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.reconciliation_store import get_run_details
    details = get_run_details(client=client, warehouse_id=wid, config=config, run_id=run_id)
    return {"run_id": run_id, "details": details}
