import logging
import time
import uuid
from datetime import datetime

from databricks.sdk import WorkspaceClient

from src.client import execute_sql
from src.diff import compare_catalogs

logger = logging.getLogger(__name__)


def sync_catalogs(
    client: WorkspaceClient,
    warehouse_id: str,
    source_catalog: str,
    dest_catalog: str,
    exclude_schemas: list[str],
    clone_type: str = "DEEP",
    dry_run: bool = False,
    drop_extra: bool = False,
    **kwargs,
) -> dict:
    """Two-way sync: add missing objects from source, optionally drop extras in dest.

    This ensures the destination catalog matches the source exactly.
    """
    logger.info(f"Syncing catalogs: {source_catalog} -> {dest_catalog}")

    diff = compare_catalogs(client, warehouse_id, source_catalog, dest_catalog, exclude_schemas)

    results = {
        "tables_added": 0,
        "tables_dropped": 0,
        "views_added": 0,
        "views_dropped": 0,
        "schemas_added": 0,
        "schemas_dropped": 0,
        "errors": [],
    }

    # Add missing schemas
    for schema_diff in diff.get("schemas", {}).get("details", []):
        schema = schema_diff.get("schema_name", schema_diff.get("schema", ""))
        if not schema:
            continue

        # Check if schema exists only in source (missing from dest)
        missing_in_dest = schema_diff.get("only_in_source", False)
        extra_in_dest = schema_diff.get("only_in_dest", False)

        if missing_in_dest:
            sql = f"CREATE SCHEMA IF NOT EXISTS `{dest_catalog}`.`{schema}`"
            try:
                execute_sql(client, warehouse_id, sql, dry_run=dry_run)
                results["schemas_added"] += 1
                logger.info(f"{'[DRY RUN] ' if dry_run else ''}Created schema: {dest_catalog}.{schema}")
            except Exception as e:
                results["errors"].append(f"Create schema {schema}: {e}")

        if extra_in_dest and drop_extra:
            sql = f"DROP SCHEMA IF EXISTS `{dest_catalog}`.`{schema}` CASCADE"
            try:
                execute_sql(client, warehouse_id, sql, dry_run=dry_run)
                results["schemas_dropped"] += 1
                logger.info(f"{'[DRY RUN] ' if dry_run else ''}Dropped schema: {dest_catalog}.{schema}")
            except Exception as e:
                results["errors"].append(f"Drop schema {schema}: {e}")

    # Process each schema for missing/extra tables
    for schema_name in _get_common_schemas(client, warehouse_id, source_catalog, dest_catalog, exclude_schemas):
        # Find missing tables (in source but not in dest)
        src_tables = _get_table_set(client, warehouse_id, source_catalog, schema_name)
        dst_tables = _get_table_set(client, warehouse_id, dest_catalog, schema_name)

        missing_tables = src_tables - dst_tables
        extra_tables = dst_tables - src_tables

        # Add missing tables
        for table_name in missing_tables:
            clone_keyword = "DEEP CLONE" if clone_type == "DEEP" else "SHALLOW CLONE"
            sql = (
                f"CREATE TABLE IF NOT EXISTS `{dest_catalog}`.`{schema_name}`.`{table_name}` "
                f"{clone_keyword} `{source_catalog}`.`{schema_name}`.`{table_name}`"
            )
            try:
                execute_sql(client, warehouse_id, sql, dry_run=dry_run)
                results["tables_added"] += 1
                logger.info(f"{'[DRY RUN] ' if dry_run else ''}Added table: {schema_name}.{table_name}")
            except Exception as e:
                results["errors"].append(f"Add table {schema_name}.{table_name}: {e}")

        # Drop extra tables
        if drop_extra:
            for table_name in extra_tables:
                sql = f"DROP TABLE IF EXISTS `{dest_catalog}`.`{schema_name}`.`{table_name}`"
                try:
                    execute_sql(client, warehouse_id, sql, dry_run=dry_run)
                    results["tables_dropped"] += 1
                    logger.info(f"{'[DRY RUN] ' if dry_run else ''}Dropped table: {schema_name}.{table_name}")
                except Exception as e:
                    results["errors"].append(f"Drop table {schema_name}.{table_name}: {e}")

        # Handle views
        src_views = _get_view_set(client, warehouse_id, source_catalog, schema_name)
        dst_views = _get_view_set(client, warehouse_id, dest_catalog, schema_name)

        missing_views = src_views - dst_views
        extra_views = dst_views - src_views

        for view_name in missing_views:
            # Get view definition from source
            view_sql = f"""
                SELECT view_definition
                FROM {source_catalog}.information_schema.views
                WHERE table_schema = '{schema_name}'
                AND table_name = '{view_name}'
            """
            try:
                rows = execute_sql(client, warehouse_id, view_sql)
                if rows:
                    defn = rows[0]["view_definition"]
                    defn = defn.replace(f"`{source_catalog}`.", f"`{dest_catalog}`.")
                    defn = defn.replace(f"{source_catalog}.", f"{dest_catalog}.")
                    sql = f"CREATE OR REPLACE VIEW `{dest_catalog}`.`{schema_name}`.`{view_name}` AS {defn}"
                    execute_sql(client, warehouse_id, sql, dry_run=dry_run)
                    results["views_added"] += 1
                    logger.info(f"{'[DRY RUN] ' if dry_run else ''}Added view: {schema_name}.{view_name}")
            except Exception as e:
                results["errors"].append(f"Add view {schema_name}.{view_name}: {e}")

        if drop_extra:
            for view_name in extra_views:
                sql = f"DROP VIEW IF EXISTS `{dest_catalog}`.`{schema_name}`.`{view_name}`"
                try:
                    execute_sql(client, warehouse_id, sql, dry_run=dry_run)
                    results["views_dropped"] += 1
                    logger.info(f"{'[DRY RUN] ' if dry_run else ''}Dropped view: {schema_name}.{view_name}")
                except Exception as e:
                    results["errors"].append(f"Drop view {schema_name}.{view_name}: {e}")

    # Print summary
    logger.info("=" * 60)
    logger.info(f"SYNC SUMMARY: {source_catalog} -> {dest_catalog}")
    logger.info("=" * 60)
    logger.info(f"  Schemas added:  {results['schemas_added']}")
    logger.info(f"  Schemas dropped: {results['schemas_dropped']}")
    logger.info(f"  Tables added:   {results['tables_added']}")
    logger.info(f"  Tables dropped: {results['tables_dropped']}")
    logger.info(f"  Views added:    {results['views_added']}")
    logger.info(f"  Views dropped:  {results['views_dropped']}")
    if results["errors"]:
        logger.warning(f"  Errors: {len(results['errors'])}")
    logger.info("=" * 60)

    # Save run log + audit trail to Delta (skip if called from API JobManager)
    if not dry_run and not kwargs.get("_api_managed_logs"):
        job_id = str(uuid.uuid4())[:8]
        sync_end = time.time()
        started_dt = datetime.fromtimestamp(sync_end - (results.get("duration_seconds", 0) or 0))
        error_msg = "; ".join(results.get("errors", [])) if results.get("errors") else None

        try:
            from src.run_logs import save_run_log
            job_record = {
                "job_id": job_id,
                "job_type": "sync",
                "source_catalog": source_catalog,
                "destination_catalog": dest_catalog,
                "clone_type": clone_type,
                "status": "failed" if results.get("errors") else "completed",
                "started_at": started_dt.isoformat(),
                "completed_at": datetime.now().isoformat(),
                "result": results,
                "error": error_msg,
                "logs": [],
            }
            save_run_log(client, warehouse_id, job_record)
        except Exception as e:
            logger.debug(f"Could not save sync run log to Delta: {e}")

        try:
            from src.audit_trail import log_operation_start, log_operation_complete
            cfg = {"source_catalog": source_catalog, "destination_catalog": dest_catalog, "clone_type": clone_type}
            log_operation_start(client, warehouse_id, cfg, job_id, operation_type="sync")
            log_operation_complete(client, warehouse_id, cfg, job_id, results, started_dt, error_message=error_msg)
        except Exception as e:
            logger.debug(f"Could not save audit trail to Delta: {e}")

    return results


def _get_common_schemas(
    client: WorkspaceClient, warehouse_id: str,
    source_catalog: str, dest_catalog: str, exclude_schemas: list[str],
) -> list[str]:
    """Get schemas that exist in both catalogs."""
    exclude_clause = ",".join(f"'{s}'" for s in exclude_schemas)

    src_sql = f"SELECT schema_name FROM {source_catalog}.information_schema.schemata WHERE schema_name NOT IN ({exclude_clause})"
    dst_sql = f"SELECT schema_name FROM {dest_catalog}.information_schema.schemata WHERE schema_name NOT IN ({exclude_clause})"

    src_schemas = {r["schema_name"] for r in execute_sql(client, warehouse_id, src_sql)}
    dst_schemas = {r["schema_name"] for r in execute_sql(client, warehouse_id, dst_sql)}

    return list(src_schemas | dst_schemas)


def _get_table_set(
    client: WorkspaceClient, warehouse_id: str, catalog: str, schema: str,
) -> set[str]:
    """Get set of table names in a schema."""
    sql = f"""
        SELECT table_name
        FROM {catalog}.information_schema.tables
        WHERE table_schema = '{schema}'
        AND table_type IN ('MANAGED', 'EXTERNAL')
    """
    try:
        return {r["table_name"] for r in execute_sql(client, warehouse_id, sql)}
    except Exception:
        return set()


def _get_view_set(
    client: WorkspaceClient, warehouse_id: str, catalog: str, schema: str,
) -> set[str]:
    """Get set of view names in a schema."""
    sql = f"""
        SELECT table_name
        FROM {catalog}.information_schema.views
        WHERE table_schema = '{schema}'
    """
    try:
        return {r["table_name"] for r in execute_sql(client, warehouse_id, sql)}
    except Exception:
        return set()
