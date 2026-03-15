import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import SecurableType

from src.client import execute_sql, get_max_parallel_queries
from src.permissions import copy_table_permissions, update_ownership
from src.rollback import record_object

logger = logging.getLogger(__name__)


def get_views(client: WorkspaceClient, warehouse_id: str, catalog: str, schema: str) -> list[dict]:
    """List all views in a schema with their definitions."""
    sql = f"""
        SELECT table_name, view_definition
        FROM {catalog}.information_schema.views
        WHERE table_schema = '{schema}'
    """
    return execute_sql(client, warehouse_id, sql)


def get_existing_views(
    client: WorkspaceClient, warehouse_id: str, catalog: str, schema: str
) -> set[str]:
    """Get set of existing view names in destination schema."""
    sql = f"""
        SELECT table_name
        FROM {catalog}.information_schema.views
        WHERE table_schema = '{schema}'
    """
    rows = execute_sql(client, warehouse_id, sql)
    return {row["table_name"] for row in rows}


def clone_view(
    client: WorkspaceClient,
    warehouse_id: str,
    dest_catalog: str,
    source_catalog: str,
    schema: str,
    view_name: str,
    view_definition: str,
    dry_run: bool = False,
) -> bool:
    """Recreate a view in the destination catalog."""
    dest = f"`{dest_catalog}`.`{schema}`.`{view_name}`"

    # Replace source catalog references with destination catalog in the view definition
    updated_definition = view_definition.replace(
        f"`{source_catalog}`.", f"`{dest_catalog}`."
    ).replace(f"{source_catalog}.", f"{dest_catalog}.")

    sql = f"CREATE OR REPLACE VIEW {dest} AS {updated_definition}"

    try:
        execute_sql(client, warehouse_id, sql, dry_run=dry_run)
        logger.info(f"{'[DRY RUN] ' if dry_run else ''}Created view: {dest}")
        return True
    except Exception as e:
        logger.error(f"Failed to create view {dest}: {e}")
        return False


def _clone_single_view(
    client, warehouse_id, dest_catalog, source_catalog, schema,
    view_name, view_definition, dry_run,
    copy_permissions, copy_ownership, rollback_log,
) -> tuple[str, bool]:
    """Clone a single view with post-clone operations. Returns (name, success)."""
    success = clone_view(
        client, warehouse_id, dest_catalog, source_catalog, schema,
        view_name, view_definition, dry_run=dry_run,
    )
    if success:
        if rollback_log and not dry_run:
            record_object(rollback_log, "views", f"`{dest_catalog}`.`{schema}`.`{view_name}`")
        if copy_permissions and not dry_run:
            copy_table_permissions(client, source_catalog, dest_catalog, schema, view_name)
        if copy_ownership and not dry_run:
            update_ownership(
                client, SecurableType.TABLE,
                f"{source_catalog}.{schema}.{view_name}",
                f"{dest_catalog}.{schema}.{view_name}",
            )
    return view_name, success


def clone_views_in_schema(
    client: WorkspaceClient,
    warehouse_id: str,
    source_catalog: str,
    dest_catalog: str,
    schema: str,
    load_type: str,
    dry_run: bool = False,
    copy_permissions: bool = False,
    copy_ownership: bool = False,
    rollback_log: str | None = None,
    include_regex: str | None = None,
    exclude_regex: str | None = None,
    max_workers: int | None = None,
) -> dict:
    """Clone all views in a schema in parallel. Returns summary of results."""
    max_workers = max_workers or get_max_parallel_queries()
    views = get_views(client, warehouse_id, source_catalog, schema)
    results = {"success": 0, "failed": 0, "skipped": 0}

    existing = set()
    if load_type == "INCREMENTAL":
        existing = get_existing_views(client, warehouse_id, dest_catalog, schema)

    # Filter views
    views_to_clone = []
    for view_row in views:
        view_name = view_row["table_name"]

        if include_regex and not re.search(include_regex, view_name):
            results["skipped"] += 1
            continue
        if exclude_regex and re.search(exclude_regex, view_name):
            results["skipped"] += 1
            continue
        if load_type == "INCREMENTAL" and view_name in existing:
            results["skipped"] += 1
            continue

        views_to_clone.append(view_row)

    # Clone in parallel
    if len(views_to_clone) > 1 and max_workers > 1:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    _clone_single_view,
                    client, warehouse_id, dest_catalog, source_catalog, schema,
                    v["table_name"], v["view_definition"], dry_run,
                    copy_permissions, copy_ownership, rollback_log,
                ): v["table_name"]
                for v in views_to_clone
            }
            for future in as_completed(futures):
                _, success = future.result()
                if success:
                    results["success"] += 1
                else:
                    results["failed"] += 1
    else:
        for v in views_to_clone:
            _, success = _clone_single_view(
                client, warehouse_id, dest_catalog, source_catalog, schema,
                v["table_name"], v["view_definition"], dry_run,
                copy_permissions, copy_ownership, rollback_log,
            )
            if success:
                results["success"] += 1
            else:
                results["failed"] += 1

    return results
