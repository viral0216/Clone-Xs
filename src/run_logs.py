"""Persist job run logs to a Unity Catalog Delta table.

After each job completes (or fails), the captured log lines are batch-inserted
into a Delta table for permanent, queryable storage. This complements the
audit_trail module (which stores operation summaries) by keeping the full
execution trace.

Default table: clone_audit.logs.run_logs
"""

import json
import logging
import os
from datetime import datetime, timezone

from src.client import execute_sql
from src.table_registry import get_table_fqn

logger = logging.getLogger(__name__)


def get_run_logs_fqn(config: dict | None = None) -> str:
    """Get fully qualified table name for run logs."""
    return get_table_fqn(config or {}, "logs", "run_logs")


def ensure_run_logs_table(client, warehouse_id: str, config: dict | None = None) -> str:
    """Create the run_logs Delta table if it doesn't exist.

    Returns:
        Fully qualified table name.
    """
    fqn = get_run_logs_fqn(config)
    catalog, schema, _ = fqn.split(".")

    from src.catalog_utils import ensure_catalog_and_schema
    ensure_catalog_and_schema(client, warehouse_id, catalog, schema)

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {fqn} (
        job_id STRING,
        job_type STRING,
        source_catalog STRING,
        destination_catalog STRING,
        clone_type STRING,
        status STRING,
        started_at TIMESTAMP,
        completed_at TIMESTAMP,
        duration_seconds DOUBLE,
        log_lines ARRAY<STRING>,
        result_json STRING,
        error_message STRING,
        config_json STRING,
        user_name STRING,
        host STRING,
        tables_cloned INT,
        tables_failed INT,
        total_size_bytes BIGINT,
        recorded_at TIMESTAMP
    )
    USING DELTA
    COMMENT 'Detailed run logs for clone operations'
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
    """
    execute_sql(client, warehouse_id, create_sql)

    # Add new columns only if they don't already exist
    new_columns = [
        ("tables_cloned", "INT"),
        ("tables_failed", "INT"),
        ("total_size_bytes", "BIGINT"),
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

    logger.info(f"Run logs table ready: {fqn}")
    return fqn


def save_run_log(
    client,
    warehouse_id: str,
    job: dict,
    config: dict | None = None,
) -> None:
    """Persist a completed job's run log to the Delta table.

    Args:
        client: Databricks WorkspaceClient.
        warehouse_id: SQL warehouse ID.
        job: Job dict from JobManager (has job_id, status, logs, result, etc.).
        config: Optional config dict for table location override.
    """
    fqn = get_run_logs_fqn(config)

    job_id = job.get("job_id", "")
    job_type = job.get("job_type", "clone")
    source = job.get("source_catalog", "") or ""
    dest = job.get("destination_catalog", "") or ""
    clone_type = job.get("clone_type", "") or ""
    status = job.get("status", "unknown")
    started_at = job.get("started_at", "")
    completed_at = job.get("completed_at", "")
    error_msg = (job.get("error") or "").replace("'", "''")
    host = os.environ.get("DATABRICKS_HOST", "unknown")
    user = os.environ.get("USER", os.environ.get("USERNAME", "unknown"))
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    # Duration
    duration = 0.0
    if started_at and completed_at:
        try:
            t1 = datetime.fromisoformat(started_at)
            t2 = datetime.fromisoformat(completed_at)
            duration = (t2 - t1).total_seconds()
        except Exception:
            pass

    def _sql_escape(s: str) -> str:
        """Escape a string for Databricks SQL by doubling single quotes."""
        return s.replace("'", "''")

    # Serialize log lines as array
    log_lines = job.get("logs", [])
    escaped_lines = [_sql_escape(line) for line in log_lines]
    log_array = "ARRAY(" + ", ".join(f"'{line}'" for line in escaped_lines) + ")"

    # Serialize result
    result_json = ""
    if job.get("result"):
        try:
            result_json = _sql_escape(json.dumps(job["result"], default=str))
        except Exception:
            result_json = "{}"

    # Sanitize config
    config_json = ""
    if config:
        safe = {k: v for k, v in config.items() if "token" not in k.lower() and "secret" not in k.lower()}
        try:
            config_json = _sql_escape(json.dumps(safe, default=str))
        except Exception:
            config_json = "{}"

    # Extract object counts from result
    result = job.get("result") or {}
    tables_info = result.get("tables", {})
    if isinstance(tables_info, dict):
        tables_cloned = tables_info.get("cloned", 0) or tables_info.get("success", 0)
        tables_failed = tables_info.get("failed", 0)
    else:
        tables_cloned = result.get("synced", 0) or result.get("tables_cloned", 0)
        tables_failed = result.get("failed", 0) or result.get("tables_failed", 0)
    total_size_bytes = result.get("total_size_bytes", 0) or 0

    sql = f"""
    INSERT INTO {fqn}
    (job_id, job_type, source_catalog, destination_catalog, clone_type,
     status, started_at, completed_at, duration_seconds,
     log_lines, result_json, error_message, config_json,
     user_name, host, tables_cloned, tables_failed, total_size_bytes,
     recorded_at)
    VALUES
    ('{job_id}', '{job_type}', '{source}', '{dest}', '{clone_type}',
     '{status}',
     {f"'{started_at}'" if started_at else 'NULL'},
     {f"'{completed_at}'" if completed_at else 'NULL'},
     {duration},
     {log_array},
     '{result_json}',
     '{error_msg}',
     '{config_json}',
     '{user}', '{host}', {tables_cloned}, {tables_failed}, {total_size_bytes},
     '{now}')
    """
    try:
        execute_sql(client, warehouse_id, sql)
        logger.info(f"Run log saved to {fqn} for job {job_id}")
    except Exception as e:
        logger.warning(f"Failed to save run log to Delta: {e}")


def query_run_logs(
    client,
    warehouse_id: str,
    config: dict | None = None,
    limit: int = 50,
    job_type: str | None = None,
    status: str | None = None,
) -> list[dict]:
    """Query run logs from the Delta table.

    Returns:
        List of run log records.
    """
    fqn = get_run_logs_fqn(config)
    where_parts = []
    if job_type:
        where_parts.append(f"job_type = '{job_type}'")
    if status:
        where_parts.append(f"status = '{status}'")
    where = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""

    sql = f"""
    SELECT job_id, job_type, source_catalog, destination_catalog, clone_type,
           status, started_at, completed_at, duration_seconds,
           size(log_lines) as log_line_count,
           error_message, user_name, host, recorded_at
    FROM {fqn}
    {where}
    ORDER BY recorded_at DESC
    LIMIT {limit}
    """
    try:
        return execute_sql(client, warehouse_id, sql)
    except Exception as e:
        logger.warning(f"Failed to query run logs: {e}")
        return []


def get_run_log_detail(
    client,
    warehouse_id: str,
    job_id: str,
    config: dict | None = None,
) -> dict | None:
    """Get full run log detail including log lines for a specific job.

    Returns:
        Run log record with log_lines, or None if not found.
    """
    fqn = get_run_logs_fqn(config)
    sql = f"""
    SELECT *
    FROM {fqn}
    WHERE job_id = '{job_id}'
    LIMIT 1
    """
    try:
        rows = execute_sql(client, warehouse_id, sql)
        return rows[0] if rows else None
    except Exception as e:
        logger.warning(f"Failed to get run log detail: {e}")
        return None
