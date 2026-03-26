"""Reconciliation endpoints — row-level and column-level with SQL/Spark toggle."""

import os

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field

from api.dependencies import get_db_client, get_app_config, get_credentials

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
    max_workers: int = 4


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
        )
    else:
        from src.compare import compare_catalogs_deep
        response = compare_catalogs_deep(
            client, wid,
            source_catalog=req.source_catalog,
            dest_catalog=req.destination_catalog,
            exclude_schemas=req.exclude_schemas,
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
    response = deep_reconcile_catalog(
        source_catalog=req.source_catalog,
        dest_catalog=req.destination_catalog,
        schema_name=req.schema_name,
        table_name=req.table_name,
        key_columns=req.key_columns or None,
        include_columns=req.include_columns or None,
        ignore_columns=req.ignore_columns or None,
        sample_diffs=req.sample_diffs,
        max_workers=req.max_workers,
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
            duration_seconds=duration,
        )
        response["run_id"] = run_id
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"Could not store deep reconciliation results: {e}")

    response["duration_seconds"] = round(duration, 2)
    return response


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
