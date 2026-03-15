import logging
from concurrent.futures import ThreadPoolExecutor

from databricks.sdk import WorkspaceClient

from src.client import execute_sql
from src.progress import ProgressTracker

logger = logging.getLogger(__name__)


def get_all_objects(
    client: WorkspaceClient, warehouse_id: str, catalog: str, exclude_schemas: list[str]
) -> dict:
    """Get all objects (schemas, tables, views, functions, volumes) in a catalog.

    Runs all 5 metadata queries in parallel for faster results.
    """
    result = {
        "schemas": set(),
        "tables": set(),
        "views": set(),
        "functions": set(),
        "volumes": set(),
    }

    exclude_clause = ",".join(f"'{s}'" for s in exclude_schemas)

    def _fetch_schemas():
        sql = f"SELECT schema_name FROM {catalog}.information_schema.schemata WHERE schema_name NOT IN ({exclude_clause})"
        return [row["schema_name"] for row in execute_sql(client, warehouse_id, sql)]

    def _fetch_tables():
        sql = f"SELECT table_schema, table_name FROM {catalog}.information_schema.tables WHERE table_schema NOT IN ({exclude_clause}) AND table_type IN ('MANAGED', 'EXTERNAL')"
        return [f"{r['table_schema']}.{r['table_name']}" for r in execute_sql(client, warehouse_id, sql)]

    def _fetch_views():
        sql = f"SELECT table_schema, table_name FROM {catalog}.information_schema.views WHERE table_schema NOT IN ({exclude_clause})"
        return [f"{r['table_schema']}.{r['table_name']}" for r in execute_sql(client, warehouse_id, sql)]

    def _fetch_functions():
        try:
            sql = f"SELECT routine_schema, routine_name AS function_name FROM {catalog}.information_schema.routines WHERE routine_schema NOT IN ({exclude_clause}) AND routine_type = 'FUNCTION'"
            return [f"{r['routine_schema']}.{r['function_name']}" for r in execute_sql(client, warehouse_id, sql)]
        except Exception:
            return []

    def _fetch_volumes():
        try:
            sql = f"SELECT volume_schema, volume_name FROM {catalog}.information_schema.volumes WHERE volume_schema NOT IN ({exclude_clause})"
            return [f"{r['volume_schema']}.{r['volume_name']}" for r in execute_sql(client, warehouse_id, sql)]
        except Exception:
            return []

    # Run all 5 queries in parallel
    with ThreadPoolExecutor(max_workers=5) as executor:
        f_schemas = executor.submit(_fetch_schemas)
        f_tables = executor.submit(_fetch_tables)
        f_views = executor.submit(_fetch_views)
        f_functions = executor.submit(_fetch_functions)
        f_volumes = executor.submit(_fetch_volumes)

    result["schemas"] = set(f_schemas.result())
    result["tables"] = set(f_tables.result())
    result["views"] = set(f_views.result())
    result["functions"] = set(f_functions.result())
    result["volumes"] = set(f_volumes.result())

    return result


def compare_catalogs(
    client: WorkspaceClient,
    warehouse_id: str,
    source_catalog: str,
    dest_catalog: str,
    exclude_schemas: list[str],
) -> dict:
    """Compare source and destination catalogs. Returns diff report."""
    logger.info(f"Comparing catalogs: {source_catalog} vs {dest_catalog}")

    # Fetch source and dest metadata in parallel with progress
    progress = ProgressTracker(2, "Scanning")
    progress.start()

    with ThreadPoolExecutor(max_workers=2) as executor:
        f_source = executor.submit(get_all_objects, client, warehouse_id, source_catalog, exclude_schemas)
        f_dest = executor.submit(get_all_objects, client, warehouse_id, dest_catalog, exclude_schemas)

    source_objects = f_source.result()
    progress.update(success=True)
    dest_objects = f_dest.result()
    progress.update(success=True)

    progress.stop()

    diff = {}
    for obj_type in ("schemas", "tables", "views", "functions", "volumes"):
        source_set = source_objects[obj_type]
        dest_set = dest_objects[obj_type]

        diff[obj_type] = {
            "only_in_source": sorted(source_set - dest_set),
            "only_in_dest": sorted(dest_set - source_set),
            "in_both": sorted(source_set & dest_set),
            "source_count": len(source_set),
            "dest_count": len(dest_set),
        }

    return diff


def print_diff(diff: dict, source_catalog: str, dest_catalog: str) -> None:
    """Print a formatted diff report."""
    logger.info("=" * 70)
    logger.info(f"CATALOG DIFF: {source_catalog} vs {dest_catalog}")
    logger.info("=" * 70)

    for obj_type in ("schemas", "tables", "views", "functions", "volumes"):
        d = diff[obj_type]
        logger.info(f"\n  {obj_type.upper()}:")
        logger.info(f"    Source: {d['source_count']}  |  Dest: {d['dest_count']}  |  Common: {len(d['in_both'])}")

        if d["only_in_source"]:
            logger.info(f"    Missing in destination ({len(d['only_in_source'])}):")
            for name in d["only_in_source"][:20]:  # Show max 20
                logger.info(f"      + {name}")
            if len(d["only_in_source"]) > 20:
                logger.info(f"      ... and {len(d['only_in_source']) - 20} more")

        if d["only_in_dest"]:
            logger.info(f"    Extra in destination ({len(d['only_in_dest'])}):")
            for name in d["only_in_dest"][:20]:
                logger.info(f"      - {name}")
            if len(d["only_in_dest"]) > 20:
                logger.info(f"      ... and {len(d['only_in_dest']) - 20} more")

        if not d["only_in_source"] and not d["only_in_dest"]:
            logger.info("    In sync")

    logger.info("=" * 70)

    # Overall status
    total_missing = sum(len(diff[t]["only_in_source"]) for t in diff)
    total_extra = sum(len(diff[t]["only_in_dest"]) for t in diff)
    if total_missing == 0 and total_extra == 0:
        logger.info("Catalogs are fully in sync!")
    else:
        logger.info(f"Differences found: {total_missing} missing in dest, {total_extra} extra in dest")
