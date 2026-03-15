import logging

from databricks.sdk import WorkspaceClient

from src.client import execute_sql

logger = logging.getLogger(__name__)


def sample_table(
    client: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
    schema: str,
    table_name: str,
    limit: int = 5,
) -> list[dict]:
    """Get a sample of rows from a table."""
    sql = f"SELECT * FROM `{catalog}`.`{schema}`.`{table_name}` LIMIT {limit}"
    try:
        return execute_sql(client, warehouse_id, sql)
    except Exception as e:
        logger.error(f"Failed to sample {catalog}.{schema}.{table_name}: {e}")
        return []


def compare_samples(
    client: WorkspaceClient,
    warehouse_id: str,
    source_catalog: str,
    dest_catalog: str,
    schema: str,
    table_name: str,
    limit: int = 5,
    order_by: str | None = None,
) -> dict:
    """Compare sample rows between source and destination tables.

    If order_by is provided, rows are ordered for deterministic comparison.
    """
    order_clause = f"ORDER BY `{order_by}`" if order_by else ""

    src_sql = f"SELECT * FROM `{source_catalog}`.`{schema}`.`{table_name}` {order_clause} LIMIT {limit}"
    dst_sql = f"SELECT * FROM `{dest_catalog}`.`{schema}`.`{table_name}` {order_clause} LIMIT {limit}"

    try:
        source_rows = execute_sql(client, warehouse_id, src_sql)
        dest_rows = execute_sql(client, warehouse_id, dst_sql)
    except Exception as e:
        return {
            "schema": schema,
            "table": table_name,
            "error": str(e),
            "match": False,
        }

    # Compare row by row
    differences = []
    max_rows = max(len(source_rows), len(dest_rows))
    for i in range(max_rows):
        src_row = source_rows[i] if i < len(source_rows) else None
        dst_row = dest_rows[i] if i < len(dest_rows) else None

        if src_row != dst_row:
            differences.append({
                "row_index": i,
                "source": src_row,
                "dest": dst_row,
            })

    return {
        "schema": schema,
        "table": table_name,
        "source_rows": len(source_rows),
        "dest_rows": len(dest_rows),
        "differences": len(differences),
        "match": len(differences) == 0,
        "sample_diffs": differences[:3],  # Show first 3 differences
    }


def preview_table(
    client: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
    schema: str,
    table_name: str,
    limit: int = 10,
) -> None:
    """Print a formatted preview of a table's data."""
    rows = sample_table(client, warehouse_id, catalog, schema, table_name, limit)
    if not rows:
        logger.info(f"No data in {catalog}.{schema}.{table_name}")
        return

    columns = list(rows[0].keys())

    # Calculate column widths
    widths = {col: len(col) for col in columns}
    for row in rows:
        for col in columns:
            val_len = len(str(row.get(col, "")))
            widths[col] = min(max(widths[col], val_len), 40)  # Cap at 40 chars

    # Print header
    header = " | ".join(col.ljust(widths[col])[:widths[col]] for col in columns)
    separator = "-+-".join("-" * widths[col] for col in columns)

    logger.info(f"\nPreview: {catalog}.{schema}.{table_name} ({len(rows)} rows)")
    logger.info(header)
    logger.info(separator)

    for row in rows:
        line = " | ".join(
            str(row.get(col, "")).ljust(widths[col])[:widths[col]]
            for col in columns
        )
        logger.info(line)
