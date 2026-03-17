"""Audit trail — log every operation to a Delta table for compliance."""

import json
import logging
import os
from datetime import datetime

from src.client import execute_sql

logger = logging.getLogger(__name__)

DEFAULT_AUDIT_CATALOG = "clone_audit"
DEFAULT_AUDIT_SCHEMA = "logs"
DEFAULT_AUDIT_TABLE = "clone_operations"


def get_audit_table_fqn(config: dict) -> str:
    """Get fully qualified name for the audit table."""
    audit = config.get("audit_trail", {})
    catalog = audit.get("catalog", DEFAULT_AUDIT_CATALOG)
    schema = audit.get("schema", DEFAULT_AUDIT_SCHEMA)
    table = audit.get("table", DEFAULT_AUDIT_TABLE)
    return f"{catalog}.{schema}.{table}"


def ensure_audit_table(client, warehouse_id: str, config: dict) -> str:
    """Create the audit catalog/schema/table if they don't exist.

    Returns:
        Fully qualified table name.
    """
    audit = config.get("audit_trail", {})
    catalog = audit.get("catalog", DEFAULT_AUDIT_CATALOG)
    schema = audit.get("schema", DEFAULT_AUDIT_SCHEMA)
    table = audit.get("table", DEFAULT_AUDIT_TABLE)
    fqn = f"{catalog}.{schema}.{table}"

    # Create catalog — skip if it already exists or requires managed location
    try:
        execute_sql(client, warehouse_id, f"CREATE CATALOG IF NOT EXISTS {catalog}")
    except Exception as e:
        try:
            execute_sql(client, warehouse_id, f"USE CATALOG {catalog}")
        except Exception:
            raise RuntimeError(
                f"Cannot create or access catalog '{catalog}'. "
                f"Either create it manually in the Databricks UI or use an existing catalog. "
                f"Original error: {e}"
            )
    # Create schema
    execute_sql(client, warehouse_id, f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    # Create table
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {fqn} (
        operation_id STRING,
        operation_type STRING,
        source_catalog STRING,
        destination_catalog STRING,
        clone_type STRING,
        started_at TIMESTAMP,
        completed_at TIMESTAMP,
        duration_seconds DOUBLE,
        status STRING,
        user_name STRING,
        host STRING,
        tables_cloned INT,
        tables_failed INT,
        views_cloned INT,
        functions_cloned INT,
        volumes_cloned INT,
        total_size_bytes BIGINT,
        tables_skipped INT,
        clone_mode STRING,
        trigger STRING,
        destination_existed BOOLEAN,
        config_json STRING,
        summary_json STRING,
        error_message STRING,
        tags MAP<STRING, STRING>
    )
    USING DELTA
    COMMENT 'Audit trail for all Clone-Xs operations'
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'delta.autoOptimize.optimizeWrite' = 'true'
    )
    """
    execute_sql(client, warehouse_id, create_sql)

    # Add new columns only if they don't already exist
    new_columns = [
        ("tables_skipped", "INT"),
        ("clone_mode", "STRING"),
        ("trigger", "STRING"),
        ("destination_existed", "BOOLEAN"),
    ]
    try:
        existing = {r["col_name"].lower() for r in execute_sql(client, warehouse_id, f"DESCRIBE TABLE {fqn}") if r.get("col_name")}
        for col_name, col_type in new_columns:
            if col_name.lower() not in existing:
                try:
                    execute_sql(client, warehouse_id, f"ALTER TABLE {fqn} ADD COLUMN {col_name} {col_type}")
                except Exception:
                    pass
    except Exception:
        pass

    logger.info(f"Audit table ready: {fqn}")
    return fqn


def log_operation_start(
    client,
    warehouse_id: str,
    config: dict,
    operation_id: str,
    operation_type: str = "clone",
) -> None:
    """Log the start of a clone operation."""
    fqn = get_audit_table_fqn(config)
    source = config.get("source_catalog", "")
    dest = config.get("destination_catalog", "")
    clone_type = config.get("clone_type", "DEEP")
    host = os.environ.get("DATABRICKS_HOST", "unknown")
    user = os.environ.get("USER", os.environ.get("USERNAME", "unknown"))
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # Sanitize config for storage (remove tokens)
    safe_config = {k: v for k, v in config.items() if "token" not in k.lower()}
    config_json = json.dumps(safe_config).replace("'", "''")

    # Determine clone_mode from config
    clone_mode = "full"
    if config.get("data_filters") or config.get("table_filters"):
        clone_mode = "filtered"
    elif config.get("load_type", "").upper() == "INCREMENTAL":
        clone_mode = "incremental"

    # Determine trigger source
    trigger = config.get("_trigger", "manual")

    sql = f"""
    INSERT INTO {fqn}
    (operation_id, operation_type, source_catalog, destination_catalog,
     clone_type, started_at, status, user_name, host,
     clone_mode, trigger, config_json)
    VALUES
    ('{operation_id}', '{operation_type}', '{source}', '{dest}',
     '{clone_type}', '{now}', 'running', '{user}', '{host}',
     '{clone_mode}', '{trigger}', '{config_json}')
    """
    try:
        execute_sql(client, warehouse_id, sql)
        logger.info(f"Audit: operation {operation_id} started")
    except Exception as e:
        logger.warning(f"Failed to write audit start log: {e}")


def log_operation_complete(
    client,
    warehouse_id: str,
    config: dict,
    operation_id: str,
    summary: dict,
    started_at: datetime,
    error_message: str | None = None,
) -> None:
    """Log the completion of a clone operation."""
    fqn = get_audit_table_fqn(config)
    now = datetime.utcnow()
    duration = (now - started_at).total_seconds()
    completed_str = now.strftime("%Y-%m-%d %H:%M:%S")

    # Extract counts — support both clone summary format and generic result dicts
    tables_info = summary.get("tables", {})
    if isinstance(tables_info, dict):
        tables_cloned = tables_info.get("cloned", 0) or tables_info.get("success", 0)
        tables_failed = tables_info.get("failed", 0)
    else:
        tables_cloned = summary.get("synced", 0) or summary.get("tables_cloned", 0)
        tables_failed = summary.get("failed", 0) or summary.get("tables_failed", 0)

    views_info = summary.get("views", {})
    views_cloned = views_info.get("cloned", 0) or views_info.get("success", 0) if isinstance(views_info, dict) else 0
    funcs_info = summary.get("functions", {})
    functions_cloned = funcs_info.get("cloned", 0) or funcs_info.get("success", 0) if isinstance(funcs_info, dict) else 0
    vols_info = summary.get("volumes", {})
    volumes_cloned = vols_info.get("cloned", 0) or vols_info.get("success", 0) if isinstance(vols_info, dict) else 0

    # New columns
    tables_skipped = 0
    if isinstance(tables_info, dict):
        tables_skipped = tables_info.get("skipped", 0) or tables_info.get("excluded", 0)
    else:
        tables_skipped = summary.get("tables_skipped", 0) or summary.get("skipped", 0)
    destination_existed = summary.get("destination_existed", False)

    status = "failed" if error_message else ("completed_with_errors" if tables_failed > 0 else "success")
    summary_json = json.dumps(summary).replace("'", "''")
    error_msg = (error_message or "").replace("'", "''")

    sql = f"""
    UPDATE {fqn}
    SET completed_at = '{completed_str}',
        duration_seconds = {duration},
        status = '{status}',
        tables_cloned = {tables_cloned},
        tables_failed = {tables_failed},
        tables_skipped = {tables_skipped},
        views_cloned = {views_cloned},
        functions_cloned = {functions_cloned},
        volumes_cloned = {volumes_cloned},
        destination_existed = {str(destination_existed).lower()},
        summary_json = '{summary_json}',
        error_message = '{error_msg}'
    WHERE operation_id = '{operation_id}'
    """
    try:
        execute_sql(client, warehouse_id, sql)
        logger.info(
            f"Audit: operation {operation_id} completed — "
            f"{status}, {duration:.1f}s"
        )
    except Exception as e:
        logger.warning(f"Failed to write audit completion log: {e}")


def query_audit_history(
    client,
    warehouse_id: str,
    config: dict,
    limit: int = 20,
    source_catalog: str | None = None,
    status: str | None = None,
) -> list[dict]:
    """Query the audit trail for past clone operations.

    Returns:
        List of audit records.
    """
    fqn = get_audit_table_fqn(config)
    where_clauses = []
    if source_catalog:
        where_clauses.append(f"source_catalog = '{source_catalog}'")
    if status:
        where_clauses.append(f"status = '{status}'")

    where = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

    sql = f"""
    SELECT operation_id, operation_type, source_catalog, destination_catalog,
           clone_type, started_at, completed_at, duration_seconds, status,
           user_name, host, tables_cloned, tables_failed, error_message
    FROM {fqn}
    {where}
    ORDER BY started_at DESC
    LIMIT {limit}
    """
    rows = execute_sql(client, warehouse_id, sql)

    logger.info(f"Audit history ({len(rows)} records):")
    logger.info("-" * 100)
    for row in rows:
        dur = f"{float(row.get('duration_seconds') or 0):.0f}s"
        logger.info(
            f"  {row['operation_id'][:8]}... | {row['started_at']} | "
            f"{row['source_catalog']} -> {row['destination_catalog']} | "
            f"{row['status']} | {dur} | "
            f"tables: {row.get('tables_cloned', 0)}/{row.get('tables_failed', 0)} ok/fail"
        )

    return rows
