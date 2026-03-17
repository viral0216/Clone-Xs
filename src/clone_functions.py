import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

from databricks.sdk import WorkspaceClient

from src.client import execute_sql, get_max_parallel_queries, list_functions_sdk
from src.permissions import copy_function_permissions
from src.rollback import record_object

logger = logging.getLogger(__name__)


def get_functions(
    client: WorkspaceClient, warehouse_id: str, catalog: str, schema: str
) -> list[dict]:
    """List all user-defined functions in a schema."""
    return list_functions_sdk(client, catalog, schema)


def get_function_details(
    client: WorkspaceClient, warehouse_id: str, catalog: str, schema: str, function_name: str
) -> str:
    """Get the DDL for a function using DESCRIBE FUNCTION EXTENDED.

    Extracts only the CREATE FUNCTION statement, filtering out
    Spark session config lines that aren't valid SQL.
    """
    sql = f"DESCRIBE FUNCTION EXTENDED `{catalog}`.`{schema}`.`{function_name}`"
    try:
        rows = execute_sql(client, warehouse_id, sql)
        # Extract the CREATE FUNCTION statement from the output
        for row in rows:
            for value in row.values():
                val_str = str(value).strip()
                if not val_str:
                    continue
                # Only accept lines that contain a CREATE FUNCTION statement
                val_upper = val_str.upper()
                if "CREATE" in val_upper and "FUNCTION" in val_upper:
                    # Filter out Spark config lines embedded in the DDL
                    # e.g. "spark.databricks.sql.functions.aiFunctions..."
                    if val_str.startswith("spark.") or "spark.databricks" in val_str:
                        continue
                    return val_str
    except Exception as e:
        logger.warning(f"Could not describe function {function_name}: {e}")
    return ""


def get_existing_functions(
    client: WorkspaceClient, warehouse_id: str, catalog: str, schema: str
) -> set[str]:
    """Get set of existing function names in destination schema."""
    rows = list_functions_sdk(client, catalog, schema)
    return {row["function_name"] for row in rows}


def clone_function(
    client: WorkspaceClient,
    warehouse_id: str,
    source_catalog: str,
    dest_catalog: str,
    schema: str,
    function_name: str,
    dry_run: bool = False,
) -> bool:
    """Clone a function from source to destination catalog."""
    try:
        ddl = get_function_details(
            client, warehouse_id, source_catalog, schema, function_name
        )
        if not ddl:
            logger.info(f"No DDL found for function {schema}.{function_name}, skipping")
            return False

        # Validate DDL starts with CREATE — skip if it contains Spark config
        ddl_upper = ddl.strip().upper()
        if not ddl_upper.startswith("CREATE"):
            logger.warning(
                f"Skipping function {schema}.{function_name}: DDL is not a valid CREATE statement"
            )
            return False

        # Replace source catalog with destination catalog
        updated_ddl = ddl.replace(
            f"`{source_catalog}`.", f"`{dest_catalog}`."
        ).replace(f"{source_catalog}.", f"{dest_catalog}.")

        # Use CREATE OR REPLACE to handle existing functions
        updated_ddl = updated_ddl.replace("CREATE FUNCTION", "CREATE OR REPLACE FUNCTION", 1)

        execute_sql(client, warehouse_id, updated_ddl, dry_run=dry_run)
        logger.info(f"{'[DRY RUN] ' if dry_run else ''}Cloned function: {source_catalog}.{schema}.{function_name}")
        return True
    except Exception as e:
        logger.warning(f"Skipped function {schema}.{function_name}: {e}")
        return False


def _clone_single_function(
    client, warehouse_id, source_catalog, dest_catalog, schema,
    func_name, dry_run, copy_permissions, rollback_log,
) -> tuple[str, bool]:
    """Clone a single function with post-clone operations. Returns (name, success)."""
    success = clone_function(
        client, warehouse_id, source_catalog, dest_catalog, schema, func_name,
        dry_run=dry_run,
    )
    if success:
        if rollback_log and not dry_run:
            record_object(rollback_log, "functions", f"`{dest_catalog}`.`{schema}`.`{func_name}`")
        if copy_permissions and not dry_run:
            copy_function_permissions(client, source_catalog, dest_catalog, schema, func_name)
    return func_name, success


def clone_functions_in_schema(
    client: WorkspaceClient,
    warehouse_id: str,
    source_catalog: str,
    dest_catalog: str,
    schema: str,
    load_type: str,
    dry_run: bool = False,
    copy_permissions: bool = False,
    rollback_log: str | None = None,
    include_regex: str | None = None,
    exclude_regex: str | None = None,
    max_workers: int | None = None,
) -> dict:
    """Clone all functions in a schema in parallel. Returns summary of results."""
    max_workers = max_workers or get_max_parallel_queries()
    functions = get_functions(client, warehouse_id, source_catalog, schema)
    results = {"success": 0, "failed": 0, "skipped": 0}

    existing = set()
    if load_type == "INCREMENTAL":
        existing = get_existing_functions(client, warehouse_id, dest_catalog, schema)

    # Filter functions
    funcs_to_clone = []
    for func_row in functions:
        func_name = func_row["function_name"]

        if include_regex and not re.search(include_regex, func_name):
            results["skipped"] += 1
            continue
        if exclude_regex and re.search(exclude_regex, func_name):
            results["skipped"] += 1
            continue
        if load_type == "INCREMENTAL" and func_name in existing:
            results["skipped"] += 1
            continue

        funcs_to_clone.append(func_name)

    # Clone in parallel
    if len(funcs_to_clone) > 1 and max_workers > 1:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    _clone_single_function,
                    client, warehouse_id, source_catalog, dest_catalog, schema,
                    fname, dry_run, copy_permissions, rollback_log,
                ): fname
                for fname in funcs_to_clone
            }
            for future in as_completed(futures):
                _, success = future.result()
                if success:
                    results["success"] += 1
                else:
                    results["failed"] += 1
    else:
        for fname in funcs_to_clone:
            _, success = _clone_single_function(
                client, warehouse_id, source_catalog, dest_catalog, schema,
                fname, dry_run, copy_permissions, rollback_log,
            )
            if success:
                results["success"] += 1
            else:
                results["failed"] += 1

    return results
