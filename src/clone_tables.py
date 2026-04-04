import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import SecurableType

from src.client import execute_sql, list_tables_sdk
from src.clone_tags import copy_table_properties, copy_table_tags
from src.constraints import copy_table_comments, copy_table_constraints
from src.log_formatter import (
    bold, bold_green, bold_red, bold_yellow, green, red, yellow, dim,
    OK, FAIL, SKIP, WARN, ARROW,
)
from src.permissions import copy_table_permissions, update_ownership
from src.rollback import record_object, get_table_version, record_table_version
from src.security import copy_table_security

logger = logging.getLogger(__name__)


def get_tables(
    client: WorkspaceClient, warehouse_id: str, catalog: str, schema: str,
    order_by_size: str | None = None,
) -> list[dict]:
    """List all tables in a schema, optionally ordered by size.

    Args:
        order_by_size: "asc" (smallest first), "desc" (largest first), or None.
    """
    all_tables = list_tables_sdk(client, catalog, schema)
    tables = [t for t in all_tables if t["table_type"] in ("MANAGED", "EXTERNAL")]

    if order_by_size and tables:
        # Get sizes for ordering
        sized = []
        for t in tables:
            size = _get_table_size(client, warehouse_id, catalog, schema, t["table_name"])
            sized.append((t, size))
        sized.sort(key=lambda x: x[1], reverse=(order_by_size == "desc"))
        tables = [t for t, _ in sized]

    return tables


def _get_table_size(
    client: WorkspaceClient, warehouse_id: str, catalog: str, schema: str, table_name: str,
) -> int:
    """Get table size in bytes for ordering. Returns 0 on error."""
    sql = f"DESCRIBE DETAIL `{catalog}`.`{schema}`.`{table_name}`"
    try:
        rows = execute_sql(client, warehouse_id, sql)
        if rows:
            return int(rows[0].get("sizeInBytes", 0))
    except Exception:
        pass
    return 0


def get_existing_tables(
    client: WorkspaceClient, warehouse_id: str, catalog: str, schema: str
) -> set[str]:
    """Get set of existing table names in destination schema."""
    rows = list_tables_sdk(client, catalog, schema)
    return {row["table_name"] for row in rows}


def _matches_regex(name: str, include_regex: str | None, exclude_regex: str | None) -> bool:
    """Check if a name matches include/exclude regex patterns."""
    if include_regex and not re.search(include_regex, name):
        return False
    if exclude_regex and re.search(exclude_regex, name):
        return False
    return True


def clone_table(
    client: WorkspaceClient,
    warehouse_id: str,
    source_catalog: str,
    dest_catalog: str,
    schema: str,
    table_name: str,
    clone_type: str,
    dry_run: bool = False,
    as_of_timestamp: str | None = None,
    as_of_version: int | None = None,
    where_clause: str | None = None,
    force_reclone: bool = False,
    schema_only: bool = False,
) -> bool:
    """Clone a single table from source to destination catalog.

    Args:
        as_of_timestamp: Clone from a specific timestamp (Delta time travel).
        as_of_version: Clone from a specific version number (Delta time travel).
        where_clause: Optional WHERE filter. Only applied for DEEP clones.
            Uses CTAS instead of CLONE, which loses Delta history/versioning.
        force_reclone: If True, drop the destination table before cloning to force a fresh clone.
    """
    source = f"`{source_catalog}`.`{schema}`.`{table_name}`"
    dest = f"`{dest_catalog}`.`{schema}`.`{table_name}`"

    # Force re-clone by dropping existing destination table
    if force_reclone:
        try:
            execute_sql(client, warehouse_id, f"DROP TABLE IF EXISTS {dest}", dry_run=dry_run)
            logger.info(f"{'[DRY RUN] ' if dry_run else ''}{WARN} Dropped table for re-clone: {dest}")
        except Exception as e:
            logger.warning(f"{WARN} Failed to drop table {dest} for re-clone: {e}")

    # Schema-only mode: create empty table with same structure (no data)
    if schema_only:
        sql = f"CREATE TABLE IF NOT EXISTS {dest} LIKE {source}"
        try:
            execute_sql(client, warehouse_id, sql, dry_run=dry_run)
            logger.info(f"{'[DRY RUN] ' if dry_run else ''}{OK} Created empty table: {source} {ARROW} {dest} {dim('(schema-only)')}")
            return True
        except Exception as e:
            logger.error(f"{FAIL} Failed to create empty table {dest}: {e}")
            return False

    # If where_clause is provided and clone_type is DEEP, use CTAS
    if where_clause and clone_type == "DEEP":
        logger.warning(
            f"Using filtered clone (CTAS) for {source}. "
            "Filtered clones lose Delta history/versioning."
        )
        sql = f"CREATE TABLE IF NOT EXISTS {dest} AS SELECT * FROM {source} WHERE {where_clause}"
    else:
        if where_clause and clone_type != "DEEP":
            logger.warning(
                f"WHERE clause ignored for {clone_type} clone of {source}. "
                "Filtered clones are only supported with DEEP clone type."
            )

        clone_keyword = "DEEP CLONE" if clone_type == "DEEP" else "SHALLOW CLONE"

        # Add time travel clause if specified
        time_travel = ""
        if as_of_timestamp:
            time_travel = f" TIMESTAMP AS OF '{as_of_timestamp}'"
        elif as_of_version is not None:
            time_travel = f" VERSION AS OF {as_of_version}"

        sql = f"CREATE TABLE IF NOT EXISTS {dest} {clone_keyword} {source}{time_travel}"

    try:
        execute_sql(client, warehouse_id, sql, dry_run=dry_run)
        tt_info = ""
        if not (where_clause and clone_type == "DEEP"):
            time_travel = ""
            if as_of_timestamp:
                time_travel = f" TIMESTAMP AS OF '{as_of_timestamp}'"
            elif as_of_version is not None:
                time_travel = f" VERSION AS OF {as_of_version}"
            tt_info = f", {time_travel.strip()}" if time_travel else ""
        filter_info = f", WHERE {where_clause}" if (where_clause and clone_type == "DEEP") else ""
        logger.info(f"{'[DRY RUN] ' if dry_run else ''}{OK} Cloned table: {source} {ARROW} {dest} {dim(f'({clone_type}{tt_info}{filter_info})')}")
        return True
    except Exception as e:
        if "No pipeline was present" in str(e):
            logger.info(f"{SKIP} Skipping DLT pipeline table {source}: {e}")
            return False
        logger.error(f"{FAIL} Failed to clone table {source}: {e}")
        return False


def _clone_single_table(
    client: WorkspaceClient,
    warehouse_id: str,
    source_catalog: str,
    dest_catalog: str,
    schema: str,
    table_name: str,
    clone_type: str,
    dry_run: bool,
    copy_permissions: bool,
    copy_ownership: bool,
    copy_tags: bool,
    copy_properties: bool,
    copy_security: bool,
    copy_constraints: bool,
    copy_comments: bool,
    rollback_log: str | None,
    as_of_timestamp: str | None = None,
    as_of_version: int | None = None,
    where_clause: str | None = None,
    force_reclone: bool = False,
    schema_only: bool = False,
) -> tuple[str, bool]:
    """Clone a single table with all post-clone operations. Returns (table_name, success)."""
    # Record destination table's pre-clone Delta version for RESTORE rollback
    if rollback_log and not dry_run:
        dest_fqn = f"`{dest_catalog}`.`{schema}`.`{table_name}`"
        try:
            pre_version = get_table_version(client, warehouse_id, dest_fqn)
            record_table_version(rollback_log, dest_fqn, pre_version, existed=pre_version is not None)
        except Exception:
            pass  # Don't block clone if version recording fails

    success = clone_table(
        client, warehouse_id, source_catalog, dest_catalog, schema, table_name,
        clone_type, dry_run=dry_run,
        as_of_timestamp=as_of_timestamp, as_of_version=as_of_version,
        where_clause=where_clause, force_reclone=force_reclone,
        schema_only=schema_only,
    )

    if not success:
        return table_name, False

    if rollback_log and not dry_run:
        record_object(rollback_log, "tables", f"`{dest_catalog}`.`{schema}`.`{table_name}`")

    if copy_permissions and not dry_run:
        copy_table_permissions(client, source_catalog, dest_catalog, schema, table_name)

    if copy_ownership and not dry_run:
        update_ownership(
            client, SecurableType.TABLE,
            f"{source_catalog}.{schema}.{table_name}",
            f"{dest_catalog}.{schema}.{table_name}",
        )

    if copy_tags and not dry_run:
        copy_table_tags(
            client, warehouse_id, source_catalog, dest_catalog, schema, table_name,
            dry_run=dry_run,
        )

    if copy_properties and not dry_run:
        copy_table_properties(
            client, warehouse_id, source_catalog, dest_catalog, schema, table_name,
            dry_run=dry_run,
        )

    if copy_security and not dry_run:
        copy_table_security(
            client, warehouse_id, source_catalog, dest_catalog, schema, table_name,
            dry_run=dry_run,
        )

    if copy_constraints and not dry_run:
        copy_table_constraints(
            client, warehouse_id, source_catalog, dest_catalog, schema, table_name,
            dry_run=dry_run,
        )

    if copy_comments and not dry_run:
        copy_table_comments(
            client, warehouse_id, source_catalog, dest_catalog, schema, table_name,
            dry_run=dry_run,
        )

    return table_name, True


def clone_tables_in_schema(
    client: WorkspaceClient,
    warehouse_id: str,
    source_catalog: str,
    dest_catalog: str,
    schema: str,
    clone_type: str,
    exclude_tables: list[str],
    load_type: str,
    dry_run: bool = False,
    copy_permissions: bool = False,
    copy_ownership: bool = False,
    copy_tags: bool = False,
    copy_properties: bool = False,
    copy_security: bool = False,
    copy_constraints: bool = False,
    copy_comments: bool = False,
    rollback_log: str | None = None,
    parallel_tables: int = 1,
    include_tables_regex: str | None = None,
    exclude_tables_regex: str | None = None,
    resumed_tables: set[str] | None = None,
    order_by_size: str | None = None,
    as_of_timestamp: str | None = None,
    as_of_version: int | None = None,
    where_clauses: dict | None = None,
    force_reclone: bool = False,
    schema_only: bool = False,
) -> dict:
    """Clone all tables in a schema. Returns summary of results.

    Args:
        order_by_size: "asc" (smallest first), "desc" (largest first), or None.
        as_of_timestamp: Clone from a specific timestamp (Delta time travel).
        as_of_version: Clone from a specific version number (Delta time travel).
        where_clauses: Optional dict mapping table names to WHERE clauses.
            Keys can be "schema.table_name" for specific tables or "*" for all tables.
        force_reclone: If True, drop destination tables before cloning to force fresh clones.
    """
    tables = get_tables(client, warehouse_id, source_catalog, schema, order_by_size=order_by_size)
    results = {"success": 0, "failed": 0, "skipped": 0}

    # For incremental loads, check what already exists
    existing = set()
    if load_type == "INCREMENTAL":
        existing = get_existing_tables(client, warehouse_id, dest_catalog, schema)

    # Filter tables to process
    tables_to_clone = []
    for table_row in tables:
        table_name = table_row["table_name"]

        if table_name in exclude_tables:
            logger.info(f"  {SKIP} Skipping excluded table: {dim(f'{schema}.{table_name}')}")
            results["skipped"] += 1
            continue

        if table_name.startswith("event_log_") or table_name.startswith("__materialization_"):
            logger.info(f"  {SKIP} Skipping DLT pipeline table: {dim(table_name)}")
            results["skipped"] += 1
            continue

        if not _matches_regex(table_name, include_tables_regex, exclude_tables_regex):
            logger.info(f"  {SKIP} Skipping table (regex filter): {dim(f'{schema}.{table_name}')}")
            results["skipped"] += 1
            continue

        if load_type == "INCREMENTAL" and table_name in existing:
            logger.info(f"  {SKIP} Skipping existing table (incremental): {dim(f'{schema}.{table_name}')}")
            results["skipped"] += 1
            continue

        if resumed_tables and table_name in resumed_tables:
            logger.info(f"  {SKIP} Skipping already cloned table (resume): {dim(f'{schema}.{table_name}')}")
            results["skipped"] += 1
            continue

        tables_to_clone.append(table_name)

    # Clone tables (parallel or sequential)
    def _resolve_where_clause(table_name: str) -> str | None:
        """Resolve WHERE clause for a given table from where_clauses dict."""
        if not where_clauses:
            return None
        # Check specific table first, then wildcard
        clause = where_clauses.get(f"{schema}.{table_name}")
        if clause is None:
            clause = where_clauses.get("*")
        return clause

    if parallel_tables > 1 and len(tables_to_clone) > 1:
        with ThreadPoolExecutor(max_workers=parallel_tables) as executor:
            futures = {
                executor.submit(
                    _clone_single_table,
                    client, warehouse_id, source_catalog, dest_catalog, schema,
                    tname, clone_type, dry_run,
                    copy_permissions, copy_ownership, copy_tags, copy_properties,
                    copy_security, copy_constraints, copy_comments, rollback_log,
                    as_of_timestamp, as_of_version,
                    _resolve_where_clause(tname), force_reclone, schema_only,
                ): tname
                for tname in tables_to_clone
            }
            for future in as_completed(futures):
                _, success = future.result()
                if success:
                    results["success"] += 1
                else:
                    results["failed"] += 1
    else:
        for tname in tables_to_clone:
            _, success = _clone_single_table(
                client, warehouse_id, source_catalog, dest_catalog, schema,
                tname, clone_type, dry_run,
                copy_permissions, copy_ownership, copy_tags, copy_properties,
                copy_security, copy_constraints, copy_comments, rollback_log,
                as_of_timestamp, as_of_version,
                _resolve_where_clause(tname), force_reclone, schema_only,
            )
            if success:
                results["success"] += 1
            else:
                results["failed"] += 1

    return results
