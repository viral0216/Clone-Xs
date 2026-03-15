import logging
from datetime import datetime

from databricks.sdk import WorkspaceClient

from src.client import execute_sql

logger = logging.getLogger(__name__)


def ensure_audit_table(
    client: WorkspaceClient, warehouse_id: str,
    audit_catalog: str, audit_schema: str, audit_table: str = "clone_audit_log",
    dry_run: bool = False,
) -> str:
    """Create the audit log table if it doesn't exist. Returns full table name."""
    full_name = f"`{audit_catalog}`.`{audit_schema}`.`{audit_table}`"

    sql = f"""
        CREATE TABLE IF NOT EXISTS {full_name} (
            clone_id STRING,
            timestamp TIMESTAMP,
            source_catalog STRING,
            destination_catalog STRING,
            clone_type STRING,
            load_type STRING,
            dry_run BOOLEAN,
            schemas_processed INT,
            tables_success INT,
            tables_failed INT,
            tables_skipped INT,
            views_success INT,
            views_failed INT,
            views_skipped INT,
            functions_success INT,
            functions_failed INT,
            functions_skipped INT,
            volumes_success INT,
            volumes_failed INT,
            volumes_skipped INT,
            total_errors INT,
            error_details STRING,
            status STRING,
            duration_seconds DOUBLE
        )
    """
    execute_sql(client, warehouse_id, sql, dry_run=dry_run)
    logger.info(f"{'[DRY RUN] ' if dry_run else ''}Ensured audit table exists: {full_name}")
    return full_name


def write_audit_log(
    client: WorkspaceClient, warehouse_id: str,
    audit_table: str,
    summary: dict,
    config: dict,
    dry_run: bool = False,
) -> None:
    """Write a clone operation record to the audit log table."""
    now = datetime.now().isoformat()
    clone_id = f"clone_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    total_errors = len(summary.get("errors", []))
    error_details = "; ".join(summary.get("errors", []))[:4000]  # Truncate for column
    status = "SUCCESS" if total_errors == 0 and all(
        summary[t]["failed"] == 0 for t in ("tables", "views", "functions", "volumes")
    ) else "FAILED"

    duration = summary.get("duration_seconds", 0) or 0

    # Escape single quotes in error details
    error_details = error_details.replace("'", "''")

    sql = f"""
        INSERT INTO {audit_table} VALUES (
            '{clone_id}',
            TIMESTAMP '{now}',
            '{config['source_catalog']}',
            '{config['destination_catalog']}',
            '{config['clone_type']}',
            '{config['load_type']}',
            {str(config.get('dry_run', False)).lower()},
            {summary['schemas_processed']},
            {summary['tables']['success']},
            {summary['tables']['failed']},
            {summary['tables']['skipped']},
            {summary['views']['success']},
            {summary['views']['failed']},
            {summary['views']['skipped']},
            {summary['functions']['success']},
            {summary['functions']['failed']},
            {summary['functions']['skipped']},
            {summary['volumes']['success']},
            {summary['volumes']['failed']},
            {summary['volumes']['skipped']},
            {total_errors},
            '{error_details}',
            '{status}',
            {duration:.1f}
        )
    """

    try:
        execute_sql(client, warehouse_id, sql, dry_run=dry_run)
        logger.info(f"{'[DRY RUN] ' if dry_run else ''}Audit log written: {clone_id} ({status}, {duration:.0f}s)")
    except Exception as e:
        logger.error(f"Failed to write audit log: {e}")


def get_audit_history(
    client: WorkspaceClient, warehouse_id: str,
    audit_table: str, limit: int = 20,
) -> list[dict]:
    """Get recent audit log entries."""
    sql = f"""
        SELECT clone_id, timestamp, source_catalog, destination_catalog,
               clone_type, status, tables_success, tables_failed,
               total_errors, duration_seconds
        FROM {audit_table}
        ORDER BY timestamp DESC
        LIMIT {limit}
    """
    return execute_sql(client, warehouse_id, sql)
