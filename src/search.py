import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

from databricks.sdk import WorkspaceClient

from src.client import execute_sql, get_max_parallel_queries
from src.progress import ProgressTracker

logger = logging.getLogger(__name__)


def _search_schema(
    client: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
    schema: str,
    compiled: re.Pattern,
    search_columns: bool,
) -> tuple[list[dict], list[dict]]:
    """Search tables and columns in a single schema."""
    tables_found = []
    columns_found = []

    # Search tables
    table_sql = f"""
        SELECT table_name, table_type, comment
        FROM {catalog}.information_schema.tables
        WHERE table_schema = '{schema}'
    """
    tables = execute_sql(client, warehouse_id, table_sql)

    for t in tables:
        if compiled.search(t["table_name"]):
            tables_found.append({
                "schema": schema,
                "table": t["table_name"],
                "type": t.get("table_type", ""),
                "comment": t.get("comment", ""),
            })

    # Search columns
    if search_columns:
        col_sql = f"""
            SELECT table_name, column_name, data_type, comment
            FROM {catalog}.information_schema.columns
            WHERE table_schema = '{schema}'
        """
        columns = execute_sql(client, warehouse_id, col_sql)

        for c in columns:
            if compiled.search(c["column_name"]):
                columns_found.append({
                    "schema": schema,
                    "table": c["table_name"],
                    "column": c["column_name"],
                    "data_type": c.get("data_type", ""),
                    "comment": c.get("comment", ""),
                })

    return tables_found, columns_found


def search_tables(
    client: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
    pattern: str,
    exclude_schemas: list[str],
    include_schemas: list[str] | None = None,
    search_columns: bool = False,
) -> dict:
    """Search for tables and optionally columns matching a pattern.

    Searches all schemas in parallel.

    Args:
        pattern: Regex pattern to match against table/column names.
        search_columns: Also search within column names.

    Returns:
        Dict with matched tables and columns.
    """
    logger.info(f"Searching catalog '{catalog}' for pattern: {pattern}")

    # Get schemas
    if include_schemas:
        schemas = [s for s in include_schemas if s not in exclude_schemas]
    else:
        exclude_clause = ",".join(f"'{s}'" for s in exclude_schemas)
        sql = f"""
            SELECT schema_name
            FROM {catalog}.information_schema.schemata
            WHERE schema_name NOT IN ({exclude_clause})
        """
        rows = execute_sql(client, warehouse_id, sql)
        schemas = [r["schema_name"] for r in rows]

    compiled = re.compile(pattern, re.IGNORECASE)
    matched_tables = []
    matched_columns = []

    # Search all schemas in parallel with progress bar
    progress = ProgressTracker(len(schemas), "Searching")
    progress.start()

    max_workers = get_max_parallel_queries()
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                _search_schema, client, warehouse_id, catalog, schema,
                compiled, search_columns,
            ): schema
            for schema in schemas
        }
        for future in as_completed(futures):
            tables_found, columns_found = future.result()
            matched_tables.extend(tables_found)
            matched_columns.extend(columns_found)
            progress.update(success=True)

    progress.stop()

    result = {
        "pattern": pattern,
        "catalog": catalog,
        "matched_tables": matched_tables,
        "matched_columns": matched_columns,
    }

    # Print results
    logger.info("=" * 60)
    logger.info(f"SEARCH RESULTS: '{pattern}' in {catalog}")
    logger.info("=" * 60)

    if matched_tables:
        logger.info(f"\n  Tables ({len(matched_tables)} matches):")
        for t in matched_tables:
            comment = f" -- {t['comment']}" if t.get("comment") else ""
            logger.info(f"    {t['schema']}.{t['table']} [{t['type']}]{comment}")
    else:
        logger.info("  No matching tables found.")

    if search_columns and matched_columns:
        logger.info(f"\n  Columns ({len(matched_columns)} matches):")
        for c in matched_columns:
            comment = f" -- {c['comment']}" if c.get("comment") else ""
            logger.info(
                f"    {c['schema']}.{c['table']}.{c['column']} "
                f"({c['data_type']}){comment}"
            )
    elif search_columns:
        logger.info("  No matching columns found.")

    logger.info("=" * 60)

    return result
