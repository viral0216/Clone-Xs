"""Store reconciliation results in Delta tables for audit trail and trending.

Creates tables in the audit catalog (clone_audit.reconciliation by default):
- reconciliation_runs: One row per reconciliation run with summary
- reconciliation_details: One row per table compared with row counts and status

Supports two storage backends:
1. Spark (serverless) — uses spark.sql() via Databricks Connect (preferred)
2. SQL Warehouse — uses execute_sql() via SDK (fallback)
"""

import logging
import uuid
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


def _get_schema(config: dict) -> str:
    audit = config.get("audit_trail", {})
    catalog = audit.get("catalog", "clone_audit")
    return f"{catalog}.reconciliation"


def _esc(s) -> str:
    if not s:
        return ""
    return str(s).replace("\\", "\\\\").replace("'", "\\'")


_RUNS_DDL = """
    run_id STRING,
    run_type STRING,
    source_catalog STRING,
    destination_catalog STRING,
    schema_name STRING,
    table_name STRING,
    execution_mode STRING,
    total_tables INT,
    matched INT,
    mismatched INT,
    errors INT,
    checksum_enabled BOOLEAN,
    max_workers INT,
    duration_seconds DOUBLE,
    executed_at TIMESTAMP,
    executed_by STRING
"""

_DETAILS_DDL = """
    run_id STRING,
    schema_name STRING,
    table_name STRING,
    source_count BIGINT,
    dest_count BIGINT,
    delta_count BIGINT,
    match BOOLEAN,
    checksum_match BOOLEAN,
    error STRING,
    executed_at TIMESTAMP
"""


def _get_spark():
    """Try to get Spark session; return None if unavailable."""
    try:
        from src.spark_session import get_spark_safe
        return get_spark_safe()
    except Exception:
        return None


def _run_sql(sql: str, client=None, warehouse_id: str = ""):
    """Execute SQL via Spark (preferred) or SQL warehouse (fallback)."""
    spark = _get_spark()
    if spark:
        spark.sql(sql)
        return

    if client and warehouse_id:
        from src.client import execute_sql
        execute_sql(client, warehouse_id, sql)
        return

    raise RuntimeError("No Spark session or SQL warehouse available for storage")


def ensure_reconciliation_tables(client=None, warehouse_id: str = "", config: dict = None):
    """Create reconciliation Delta tables if they don't exist."""
    config = config or {}
    schema = _get_schema(config)

    try:
        _run_sql(f"CREATE SCHEMA IF NOT EXISTS {schema}", client, warehouse_id)
    except Exception:
        pass

    for tbl_name, cols in [("reconciliation_runs", _RUNS_DDL), ("reconciliation_details", _DETAILS_DDL)]:
        try:
            _run_sql(f"""
                CREATE TABLE IF NOT EXISTS {schema}.{tbl_name} ({cols})
                USING DELTA
                COMMENT 'Clone-Xs Reconciliation: {tbl_name}'
                TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
            """, client, warehouse_id)
        except Exception as e:
            logger.warning(f"Could not create {schema}.{tbl_name}: {e}")


def store_reconciliation_result(
    client,
    warehouse_id: str,
    config: dict,
    result: dict,
    run_type: str = "row-level",
    source_catalog: str = "",
    destination_catalog: str = "",
    schema_name: str = "",
    table_name: str = "",
    execution_mode: str = "sql",
    use_checksum: bool = False,
    max_workers: int = 4,
    duration_seconds: float = 0,
):
    """Store a reconciliation run and its details in Delta tables.

    Uses Spark (serverless) if available, falls back to SQL warehouse.
    """
    schema = _get_schema(config)
    run_id = str(uuid.uuid4())[:12]
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    # Ensure tables exist
    try:
        ensure_reconciliation_tables(client, warehouse_id, config)
    except Exception as e:
        logger.warning(f"Could not ensure reconciliation tables: {e}")
        return run_id

    # Insert run summary
    try:
        _run_sql(f"""
            INSERT INTO {schema}.reconciliation_runs VALUES (
                '{run_id}',
                '{_esc(run_type)}',
                '{_esc(source_catalog)}',
                '{_esc(destination_catalog)}',
                '{_esc(schema_name)}',
                '{_esc(table_name)}',
                '{_esc(execution_mode)}',
                {result.get('total_tables', 0)},
                {result.get('matched', 0)},
                {result.get('mismatched', 0)},
                {result.get('errors', 0)},
                {str(use_checksum).lower()},
                {max_workers},
                {duration_seconds:.2f},
                '{now}',
                ''
            )
        """, client, warehouse_id)
    except Exception as e:
        logger.warning(f"Could not store reconciliation run: {e}")

    # Insert details (batch in groups of 50)
    details = result.get("details", [])
    batch_size = 50
    for i in range(0, len(details), batch_size):
        batch = details[i:i + batch_size]
        values = []
        for r in batch:
            src_count = r.get("source_count")
            dst_count = r.get("dest_count")
            delta = (src_count or 0) - (dst_count or 0) if src_count is not None and dst_count is not None else 0
            values.append(f"""(
                '{run_id}',
                '{_esc(r.get("schema", ""))}',
                '{_esc(r.get("table", ""))}',
                {src_count if src_count is not None else 'NULL'},
                {dst_count if dst_count is not None else 'NULL'},
                {delta},
                {str(r.get("match", False)).lower()},
                {str(r.get("checksum_match")).lower() if r.get("checksum_match") is not None else 'NULL'},
                {f"'{_esc(r.get('error', ''))}'" if r.get("error") else 'NULL'},
                '{now}'
            )""")

        if values:
            try:
                _run_sql(f"""
                    INSERT INTO {schema}.reconciliation_details VALUES {', '.join(values)}
                """, client, warehouse_id)
            except Exception as e:
                logger.warning(f"Could not store reconciliation details batch: {e}")

    storage_mode = "Spark" if _get_spark() else "SQL Warehouse"
    logger.info(f"Stored reconciliation run {run_id} via {storage_mode}: {len(details)} table results")
    return run_id


def _query_sql(sql: str, limit: int = 20, client=None, warehouse_id: str = "") -> list[dict]:
    """Execute a SELECT query and return rows as list of dicts.

    Uses Spark (preferred) or SQL warehouse (fallback).
    """
    spark = _get_spark()
    if spark:
        rows = spark.sql(sql).limit(limit).collect()
        result = []
        for row in rows:
            d = row.asDict()
            for k, v in d.items():
                if v is not None and not isinstance(v, (str, int, float, bool)):
                    d[k] = str(v)
            result.append(d)
        return result

    if client and warehouse_id:
        from src.client import execute_sql
        rows = execute_sql(client, warehouse_id, sql)
        return (rows or [])[:limit]

    raise RuntimeError("No Spark session or SQL warehouse available for querying")


def get_reconciliation_history(
    client=None,
    warehouse_id: str = "",
    config: dict = None,
    limit: int = 20,
    run_type: str = None,
    source_catalog: str = None,
) -> list[dict]:
    """Query past reconciliation runs from Delta tables.

    Uses Spark or SQL warehouse. Supports optional filters on run_type
    and source_catalog. Returns list of run dicts ordered by executed_at DESC.
    """
    config = config or {}
    schema = _get_schema(config)

    where_clauses = []
    if run_type:
        where_clauses.append(f"run_type = '{_esc(run_type)}'")
    if source_catalog:
        where_clauses.append(f"source_catalog = '{_esc(source_catalog)}'")

    where_sql = ""
    if where_clauses:
        where_sql = "WHERE " + " AND ".join(where_clauses)

    query = f"""
        SELECT run_id, run_type, source_catalog, destination_catalog,
               schema_name, table_name, execution_mode, total_tables,
               matched, mismatched, errors, checksum_enabled, max_workers,
               duration_seconds, executed_at, executed_by
        FROM {schema}.reconciliation_runs
        {where_sql}
        ORDER BY executed_at DESC
    """

    try:
        return _query_sql(query, limit=limit, client=client, warehouse_id=warehouse_id)
    except Exception as e:
        logger.warning(f"Could not query reconciliation history: {e}")
        return []
