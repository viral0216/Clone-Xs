"""Audit trail — log every operation to a Delta table for compliance."""

import json
import logging
import os
from datetime import datetime, timezone

from src.client import execute_sql
from src.table_registry import get_catalog, get_schema_fqn, get_table_fqn

logger = logging.getLogger(__name__)


def get_audit_table_fqn(config: dict) -> str:
    """Get fully qualified name for the audit table."""
    audit = config.get("audit_trail", {})
    table = audit.get("table", "clone_operations")
    return get_table_fqn(config, "logs", table)


def ensure_audit_table(client, warehouse_id: str, config: dict) -> str:
    """Create the audit catalog/schema/table if they don't exist.

    Returns:
        Fully qualified table name.
    """
    fqn = get_audit_table_fqn(config)
    catalog = get_catalog(config)
    schema_fqn = get_schema_fqn(config, "logs")
    schema = schema_fqn.split(".", 1)[1]

    from src.catalog_utils import ensure_catalog_and_schema

    ensure_catalog_and_schema(client, warehouse_id, catalog, schema)
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

    # Add new columns only if they don't already exist. Older audit tables
    # are upgraded in place via ALTER ADD COLUMN — old rows have NULL for the
    # new columns, which compliance/finance queries can COALESCE to 0.
    new_columns = [
        ("tables_skipped", "INT"),
        ("clone_mode", "STRING"),
        ("trigger", "STRING"),
        ("destination_existed", "BOOLEAN"),
        # Databricks per-CLONE metrics aggregated across all per-table CLONE
        # statements. `bytes_copied` is the cloud-egress finance number.
        ("bytes_copied", "BIGINT"),
        ("files_copied", "BIGINT"),
        ("source_table_size", "BIGINT"),
        ("source_num_of_files", "BIGINT"),
    ]
    try:
        existing = {
            r["col_name"].lower()
            for r in execute_sql(client, warehouse_id, f"DESCRIBE TABLE {fqn}")
            if r.get("col_name")
        }
        for col_name, col_type in new_columns:
            if col_name.lower() not in existing:
                try:
                    execute_sql(
                        client, warehouse_id, f"ALTER TABLE {fqn} ADD COLUMN {col_name} {col_type}"
                    )
                except Exception as e:
                    logger.warning("Failed to add audit column '%s' to %s: %s", col_name, fqn, e)
    except Exception as e:
        logger.warning("Failed to check/add audit columns on %s: %s", fqn, e)

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
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    # Sanitize config for storage (remove tokens)
    safe_config = {k: v for k, v in config.items() if "token" not in k.lower()}
    config_json = json.dumps(safe_config).replace("'", "''")

    # Determine clone_mode from config
    clone_mode = "full"
    if config.get("data_filters") or config.get("table_filters"):
        clone_mode = "filtered"
    elif config.get("load_type", "").upper() == "INCREMENTAL":
        clone_mode = "incremental"

    # Determine trigger source (escape for safe SQL interpolation)
    trigger = config.get("_trigger", "manual").replace("'", "''")
    clone_mode = clone_mode.replace("'", "''")

    sql = f"""
    INSERT INTO {fqn}
    (operation_id, operation_type, source_catalog, destination_catalog,
     clone_type, started_at, status, user_name, host,
     clone_mode, `trigger`, config_json)
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
    now = datetime.now(timezone.utc)
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
    views_cloned = (
        views_info.get("cloned", 0) or views_info.get("success", 0)
        if isinstance(views_info, dict)
        else 0
    )
    funcs_info = summary.get("functions", {})
    functions_cloned = (
        funcs_info.get("cloned", 0) or funcs_info.get("success", 0)
        if isinstance(funcs_info, dict)
        else 0
    )
    vols_info = summary.get("volumes", {})
    volumes_cloned = (
        vols_info.get("cloned", 0) or vols_info.get("success", 0)
        if isinstance(vols_info, dict)
        else 0
    )

    # New columns
    tables_skipped = 0
    if isinstance(tables_info, dict):
        tables_skipped = tables_info.get("skipped", 0) or tables_info.get("excluded", 0)
    else:
        tables_skipped = summary.get("tables_skipped", 0) or summary.get("skipped", 0)
    destination_existed = summary.get("destination_existed", False)

    # Databricks CLONE metrics — `bytes_copied` is the headline number for
    # cloud-egress finance. Both orchestrators emit these at top level via
    # _build_summary / CrossWorkspaceResult.to_dict.
    bytes_copied = int(summary.get("bytes_copied", 0) or 0)
    files_copied = int(summary.get("files_copied", 0) or 0)
    source_table_size = int(summary.get("source_table_size", 0) or 0)
    source_num_of_files = int(summary.get("source_num_of_files", 0) or 0)

    status = (
        "failed" if error_message else ("completed_with_errors" if tables_failed > 0 else "success")
    )
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
        bytes_copied = {bytes_copied},
        files_copied = {files_copied},
        source_table_size = {source_table_size},
        source_num_of_files = {source_num_of_files},
        summary_json = '{summary_json}',
        error_message = '{error_msg}'
    WHERE operation_id = '{operation_id}'
    """
    try:
        execute_sql(client, warehouse_id, sql)
        logger.info(f"Audit: operation {operation_id} completed — {status}, {duration:.1f}s")
    except Exception as e:
        logger.warning(f"Failed to write audit completion log: {e}")


# --- convert-to-delta audit (#13) ---------------------------------------
#
# Distinct from the clone audit table because the schema shape is wrong:
# clone_operations has `destination_catalog`, `tables_cloned`, `views_cloned`
# etc., none of which apply to in-place format conversion. A sibling
# `convert_operations` table keeps semantics clean (no fields that mean
# nothing) and lets reporting queries on each surface stay simple.
#
# One row per (operation_id, fqn) pair — a batch of N targets produces N
# rows linked by operation_id, written incrementally as each table
# finishes. Live observability matters here because each conversion can
# take minutes for large tables; a batch-at-end write would leave
# operators staring at a blank audit table during the run.


def get_convert_audit_table_fqn(config: dict) -> str:
    """Get fully qualified name for the convert-to-delta audit table.

    Lives in the same `logs` schema as `clone_operations` by convention,
    so a single GRANT on the schema covers both audit surfaces. The table
    name can be overridden via ``audit_trail.convert_table`` in config.
    """
    audit = config.get("audit_trail", {})
    table = audit.get("convert_table", "convert_operations")
    return get_table_fqn(config, "logs", table)


def ensure_convert_audit_table(client, warehouse_id: str, config: dict) -> str:
    """Create the convert audit table if it doesn't exist. Idempotent."""
    fqn = get_convert_audit_table_fqn(config)
    catalog = get_catalog(config)
    schema_fqn = get_schema_fqn(config, "logs")
    schema = schema_fqn.split(".", 1)[1]

    from src.catalog_utils import ensure_catalog_and_schema

    ensure_catalog_and_schema(client, warehouse_id, catalog, schema)
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {fqn} (
        operation_id STRING,
        fqn STRING,
        source_format STRING,
        status STRING,
        started_at TIMESTAMP,
        completed_at TIMESTAMP,
        duration_ms BIGINT,
        user_name STRING,
        host STRING,
        dry_run BOOLEAN,
        `trigger` STRING,
        error_message STRING,
        recorded_at TIMESTAMP
    )
    USING DELTA
    COMMENT 'Audit trail for Clone-Xs convert-to-delta operations'
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'delta.autoOptimize.optimizeWrite' = 'true'
    )
    """
    execute_sql(client, warehouse_id, create_sql)
    logger.info(f"Convert audit table ready: {fqn}")
    return fqn


def log_convert_result(
    client,
    warehouse_id: str,
    config: dict,
    *,
    operation_id: str,
    fqn_target: str,
    source_format: str,
    status: str,
    started_at: datetime,
    completed_at: datetime,
    duration_ms: int,
    dry_run: bool,
    trigger: str = "manual",
    error_message: str | None = None,
) -> None:
    """Write one convert audit row. Best-effort — failures don't break
    the conversion (we already converted the table; the audit row is
    secondary and a swallowed warning is the right behaviour).

    Each (operation_id, fqn_target) pair is a row. Status mirrors the
    ConvertResult statuses: converted / failed / skipped.
    """
    audit_fqn = get_convert_audit_table_fqn(config)
    host = os.environ.get("DATABRICKS_HOST", "unknown")
    user = os.environ.get("USER", os.environ.get("USERNAME", "unknown"))
    started_str = started_at.strftime("%Y-%m-%d %H:%M:%S")
    completed_str = completed_at.strftime("%Y-%m-%d %H:%M:%S")
    recorded_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    # Defence in depth on SQL injection — every interpolated string field
    # gets single-quote-doubling. The fqn_target comes from user input via
    # the API model, so it's worth being explicit.
    def esc(s: str) -> str:
        return (s or "").replace("'", "''")

    sql = f"""
    INSERT INTO {audit_fqn}
    (operation_id, fqn, source_format, status, started_at, completed_at,
     duration_ms, user_name, host, dry_run, `trigger`, error_message, recorded_at)
    VALUES
    ('{esc(operation_id)}', '{esc(fqn_target)}', '{esc(source_format)}',
     '{esc(status)}', '{started_str}', '{completed_str}',
     {int(duration_ms)}, '{esc(user)}', '{esc(host)}',
     {str(bool(dry_run)).lower()}, '{esc(trigger)}',
     {f"'{esc(error_message)}'" if error_message else "NULL"},
     '{recorded_str}')
    """
    try:
        execute_sql(client, warehouse_id, sql)
    except Exception as e:
        logger.warning(f"Failed to write convert audit row for {fqn_target}: {e}")


def query_convert_history(
    client,
    warehouse_id: str,
    config: dict,
    *,
    limit: int = 50,
    status: str | None = None,
    fqn_like: str | None = None,
    dry_run: bool | None = None,
    operation_id: str | None = None,
) -> list[dict]:
    """Query the convert-to-delta audit table with optional filters.

    Returns rows ordered by ``recorded_at DESC`` so the UI can render
    "most recent first" without client-side sorting. Filters are
    optional — pass `None` to skip a predicate. Each row keeps the
    column shape `ensure_convert_audit_table` defines, so the response
    is JSON-friendly without further mapping.

    Defensive: if the audit table doesn't exist (operator never ran a
    convert, or audit init failed silently), returns ``[]`` rather
    than raising. The history endpoint should not 500 on a fresh
    workspace where the table simply doesn't exist yet.
    """
    audit_fqn = get_convert_audit_table_fqn(config)

    def esc(s: str) -> str:
        return (s or "").replace("'", "''")

    where: list[str] = []
    if status:
        where.append(f"status = '{esc(status)}'")
    if fqn_like:
        # The operator-facing field is `fqn` (3-part). LIKE so the UI
        # can filter by catalog or `catalog.schema` prefix without
        # needing the full table name.
        where.append(f"fqn LIKE '{esc(fqn_like)}'")
    if dry_run is not None:
        where.append(f"dry_run = {str(bool(dry_run)).lower()}")
    if operation_id:
        where.append(f"operation_id = '{esc(operation_id)}'")

    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    # Cap at 1000 even if the caller asks for more — protects the
    # warehouse and keeps the JSON response under a sensible size.
    capped_limit = max(1, min(int(limit), 1000))

    sql = f"""
    SELECT operation_id, fqn, source_format, status,
           started_at, completed_at, duration_ms,
           user_name, host, dry_run, `trigger`, error_message, recorded_at
    FROM {audit_fqn}
    {where_sql}
    ORDER BY recorded_at DESC
    LIMIT {capped_limit}
    """
    try:
        return execute_sql(client, warehouse_id, sql)
    except Exception as e:
        logger.warning(f"Failed to query convert history from {audit_fqn}: {e}")
        return []


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
