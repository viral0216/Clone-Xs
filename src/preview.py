"""Data sampling preview — side-by-side source vs destination comparison."""

import logging

from src.client import execute_sql

logger = logging.getLogger(__name__)


def preview_comparison(
    client, warehouse_id: str,
    source_catalog: str, dest_catalog: str,
    schema: str, table_name: str,
    limit: int = 10, order_by: str | None = None,
) -> dict:
    """Fetch sample rows from source and dest, return structured comparison."""
    order_clause = f"ORDER BY `{order_by}`" if order_by else ""

    source_sql = f"SELECT * FROM `{source_catalog}`.`{schema}`.`{table_name}` {order_clause} LIMIT {limit}"
    dest_sql = f"SELECT * FROM `{dest_catalog}`.`{schema}`.`{table_name}` {order_clause} LIMIT {limit}"

    try:
        source_rows = execute_sql(client, warehouse_id, source_sql)
    except Exception as e:
        source_rows = []
        logger.warning(f"Could not sample source {source_catalog}.{schema}.{table_name}: {e}")

    try:
        dest_rows = execute_sql(client, warehouse_id, dest_sql)
    except Exception as e:
        dest_rows = []
        logger.warning(f"Could not sample dest {dest_catalog}.{schema}.{table_name}: {e}")

    # Compare rows
    differences = []
    max_rows = max(len(source_rows), len(dest_rows))
    for i in range(max_rows):
        src_row = source_rows[i] if i < len(source_rows) else None
        dst_row = dest_rows[i] if i < len(dest_rows) else None
        if src_row != dst_row:
            differences.append({
                "row_index": i,
                "source": src_row,
                "destination": dst_row,
            })

    return {
        "schema": schema,
        "table": table_name,
        "source_rows": len(source_rows),
        "dest_rows": len(dest_rows),
        "match": len(differences) == 0,
        "differences": differences[:10],  # limit to first 10 diffs
        "source_data": source_rows,
        "dest_data": dest_rows,
    }


def format_side_by_side(comparison: dict) -> str:
    """Format source vs dest rows side-by-side with ANSI highlights for differences."""
    RED = "\033[31m"
    GREEN = "\033[32m"
    RESET = "\033[0m"

    lines = []
    schema = comparison["schema"]
    table = comparison["table"]
    lines.append(f"\n{'=' * 70}")
    lines.append(f"PREVIEW: {schema}.{table}")
    lines.append(f"{'=' * 70}")
    lines.append(f"  Source rows: {comparison['source_rows']}")
    lines.append(f"  Dest rows:   {comparison['dest_rows']}")

    if comparison["match"]:
        lines.append(f"  {GREEN}Data matches{RESET}")
    else:
        lines.append(f"  {RED}Differences found: {len(comparison['differences'])}{RESET}")

    # Show source data as a table
    source_data = comparison.get("source_data", [])
    dest_data = comparison.get("dest_data", [])

    if source_data:
        columns = list(source_data[0].keys())
        # Calculate column widths
        col_widths = {}
        for col in columns:
            max_width = len(col)
            for row in source_data + dest_data:
                val = str(row.get(col, ""))
                max_width = max(max_width, min(len(val), 30))
            col_widths[col] = max_width

        # Header
        header = " | ".join(col.ljust(col_widths[col])[:col_widths[col]] for col in columns)
        lines.append(f"\n  Source ({comparison['source_rows']} rows):")
        lines.append(f"  {header}")
        lines.append(f"  {'-' * len(header)}")

        for row in source_data[:10]:
            row_str = " | ".join(
                str(row.get(col, "")).ljust(col_widths[col])[:col_widths[col]]
                for col in columns
            )
            lines.append(f"  {row_str}")

        lines.append(f"\n  Destination ({comparison['dest_rows']} rows):")
        lines.append(f"  {header}")
        lines.append(f"  {'-' * len(header)}")

        for i, row in enumerate(dest_data[:10]):
            row_str_parts = []
            for col in columns:
                val = str(row.get(col, ""))
                src_val = str(source_data[i].get(col, "")) if i < len(source_data) else ""
                padded = val.ljust(col_widths[col])[:col_widths[col]]
                if val != src_val:
                    row_str_parts.append(f"{RED}{padded}{RESET}")
                else:
                    row_str_parts.append(padded)
            lines.append(f"  {' | '.join(row_str_parts)}")

    # Show differences
    if comparison["differences"]:
        lines.append("\n  Differences:")
        for diff in comparison["differences"][:5]:
            lines.append(f"    Row {diff['row_index']}:")
            if diff["source"] and diff["destination"]:
                for key in diff["source"]:
                    sv = diff["source"].get(key)
                    dv = diff["destination"].get(key)
                    if sv != dv:
                        lines.append(f"      {key}: {RED}{sv}{RESET} -> {GREEN}{dv}{RESET}")

    lines.append(f"{'=' * 70}")
    return "\n".join(lines)


def preview_catalog(
    client, warehouse_id: str,
    source_catalog: str, dest_catalog: str,
    exclude_schemas: list[str],
    limit: int = 5, max_tables: int = 20,
    order_by: str | None = None,
) -> list[dict]:
    """Preview all tables across catalogs."""
    exclude_clause = ",".join(f"'{s}'" for s in exclude_schemas)
    sql = f"""
        SELECT table_schema, table_name
        FROM {dest_catalog}.information_schema.tables
        WHERE table_type IN ('MANAGED', 'EXTERNAL')
          AND table_schema NOT IN ({exclude_clause})
        LIMIT {max_tables}
    """
    tables = execute_sql(client, warehouse_id, sql)

    results = []
    for row in tables:
        comparison = preview_comparison(
            client, warehouse_id, source_catalog, dest_catalog,
            row["table_schema"], row["table_name"],
            limit=limit, order_by=order_by,
        )
        results.append(comparison)

    return results
