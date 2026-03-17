import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from databricks.sdk import WorkspaceClient

from src.client import execute_sql, get_max_parallel_queries, list_schemas_sdk, list_tables_sdk, get_table_info_sdk
from src.progress import ProgressTracker

logger = logging.getLogger(__name__)


def get_table_stats(
    client: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
    schema: str,
    table_name: str,
) -> dict:
    """Get detailed statistics for a single table.

    Runs row count, DESCRIBE DETAIL, and column count in parallel.
    """
    fqn = f"`{catalog}`.`{schema}`.`{table_name}`"
    stats = {
        "schema": schema,
        "table": table_name,
        "row_count": None,
        "size_bytes": None,
        "size_display": None,
        "num_columns": None,
        "num_files": None,
        "last_modified": None,
        "format": None,
        "error": None,
    }

    def _fetch_count():
        try:
            rows = execute_sql(client, warehouse_id, f"SELECT COUNT(*) AS cnt FROM {fqn}")
            return int(rows[0]["cnt"]) if rows else None
        except Exception as e:
            return str(e)

    def _fetch_detail():
        try:
            detail = execute_sql(client, warehouse_id, f"DESCRIBE DETAIL {fqn}")
            return detail[0] if detail else None
        except Exception:
            return None

    def _fetch_columns():
        try:
            table_info = get_table_info_sdk(client, f"{catalog}.{schema}.{table_name}")
            if table_info and table_info.get("columns"):
                return len(table_info["columns"])
            return None
        except Exception:
            return None

    # Run all 3 queries in parallel
    with ThreadPoolExecutor(max_workers=3) as executor:
        f_count = executor.submit(_fetch_count)
        f_detail = executor.submit(_fetch_detail)
        f_cols = executor.submit(_fetch_columns)

    count_result = f_count.result()
    if isinstance(count_result, str):
        stats["error"] = count_result
    else:
        stats["row_count"] = count_result

    detail = f_detail.result()
    if detail:
        size = int(detail.get("sizeInBytes", 0))
        stats["size_bytes"] = size
        stats["size_display"] = _format_bytes(size)
        stats["num_files"] = detail.get("numFiles")
        stats["last_modified"] = detail.get("lastModified")
        stats["format"] = detail.get("format")

    stats["num_columns"] = f_cols.result()

    return stats


def _process_schema_stats(
    client: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
    schema: str,
    max_workers: int | None = None,
) -> tuple[str, list[dict]]:
    """Get stats for all tables in a schema in parallel."""
    max_workers = max_workers or get_max_parallel_queries()
    all_tables = list_tables_sdk(client, catalog, schema)
    tables = [t for t in all_tables if t["table_type"] in ("MANAGED", "EXTERNAL")]

    if not tables:
        return schema, []

    schema_tables = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                get_table_stats, client, warehouse_id, catalog, schema, t["table_name"]
            ): t["table_name"]
            for t in tables
        }
        for future in as_completed(futures):
            schema_tables.append(future.result())

    return schema, schema_tables


def catalog_stats(
    client: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
    exclude_schemas: list[str],
    include_schemas: list[str] | None = None,
    max_workers: int | None = None,
) -> dict:
    """Get aggregate statistics for an entire catalog.

    Runs all schema and table queries in parallel.
    """
    max_workers = max_workers or get_max_parallel_queries()
    logger.info(f"Gathering statistics for catalog: {catalog}")

    # Get schemas
    if include_schemas:
        schemas = [s for s in include_schemas if s not in exclude_schemas]
    else:
        schemas = list_schemas_sdk(client, catalog, exclude=exclude_schemas)

    # Process all schemas in parallel with progress bar
    all_stats = []
    schema_summaries = []
    progress = ProgressTracker(len(schemas), "Stats")
    progress.start()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                _process_schema_stats, client, warehouse_id, catalog, schema, max_workers
            ): schema
            for schema in schemas
        }
        for future in as_completed(futures):
            schema_name, schema_tables = future.result()
            all_stats.extend(schema_tables)

            schema_size = sum(t["size_bytes"] or 0 for t in schema_tables)
            schema_rows = sum(t["row_count"] or 0 for t in schema_tables)
            schema_summaries.append({
                "schema": schema_name,
                "num_tables": len(schema_tables),
                "total_size_bytes": schema_size,
                "total_size_display": _format_bytes(schema_size),
                "total_rows": schema_rows,
            })
            progress.update(success=True)

    progress.stop()

    # Sort schema summaries by name
    schema_summaries.sort(key=lambda s: s["schema"])

    total_size = sum(t["size_bytes"] or 0 for t in all_stats)
    total_rows = sum(t["row_count"] or 0 for t in all_stats)

    summary = {
        "catalog": catalog,
        "num_schemas": len(schemas),
        "num_tables": len(all_stats),
        "total_size_bytes": total_size,
        "total_size_display": _format_bytes(total_size),
        "total_rows": total_rows,
        "schema_summaries": schema_summaries,
        "tables": all_stats,
        "top_tables_by_size": sorted(
            [t for t in all_stats if t["size_bytes"]],
            key=lambda t: t["size_bytes"],
            reverse=True,
        )[:10],
        "top_tables_by_rows": sorted(
            [t for t in all_stats if t["row_count"]],
            key=lambda t: t["row_count"],
            reverse=True,
        )[:10],
    }

    # Print summary
    logger.info("=" * 70)
    logger.info(f"CATALOG STATISTICS: {catalog}")
    logger.info("=" * 70)
    logger.info(f"  Schemas:     {summary['num_schemas']}")
    logger.info(f"  Tables:      {summary['num_tables']}")
    logger.info(f"  Total size:  {summary['total_size_display']}")
    logger.info(f"  Total rows:  {summary['total_rows']:,}")

    logger.info("\n  Per-schema breakdown:")
    for ss in schema_summaries:
        logger.info(
            f"    {ss['schema']:30s}  {ss['num_tables']:4d} tables  "
            f"{ss['total_size_display']:>10s}  {ss['total_rows']:>12,} rows"
        )

    if summary["top_tables_by_size"]:
        logger.info("\n  Top 10 tables by size:")
        for t in summary["top_tables_by_size"]:
            logger.info(f"    {t['schema']}.{t['table']:40s}  {t['size_display']:>10s}")

    if summary["top_tables_by_rows"]:
        logger.info("\n  Top 10 tables by row count:")
        for t in summary["top_tables_by_rows"]:
            logger.info(f"    {t['schema']}.{t['table']:40s}  {t['row_count']:>12,} rows")

    logger.info("=" * 70)

    return summary


def _format_bytes(size: int) -> str:
    """Format bytes as human-readable string."""
    if size < 1024:
        return f"{size} B"
    elif size < 1024 ** 2:
        return f"{size / 1024:.1f} KB"
    elif size < 1024 ** 3:
        return f"{size / 1024 ** 2:.1f} MB"
    elif size < 1024 ** 4:
        return f"{size / 1024 ** 3:.2f} GB"
    else:
        return f"{size / 1024 ** 4:.2f} TB"
