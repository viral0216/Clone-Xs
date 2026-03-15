import csv
import json
import logging
import os
from datetime import datetime

from databricks.sdk import WorkspaceClient

from src.client import execute_sql

logger = logging.getLogger(__name__)


def export_catalog_metadata(
    client: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
    exclude_schemas: list[str],
    output_format: str = "csv",
    output_path: str | None = None,
    include_schemas: list[str] | None = None,
) -> str:
    """Export catalog metadata (tables, columns) to CSV or JSON.

    Args:
        output_format: "csv" or "json".
        output_path: Output file path. Auto-generated if not specified.

    Returns:
        Path to the exported file.
    """
    logger.info(f"Exporting catalog metadata: {catalog} (format={output_format})")

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

    tables_data = []
    columns_data = []

    for schema in schemas:
        # Tables
        table_sql = f"""
            SELECT table_name, table_type, comment
            FROM {catalog}.information_schema.tables
            WHERE table_schema = '{schema}'
        """
        tables = execute_sql(client, warehouse_id, table_sql)

        for t in tables:
            table_entry = {
                "catalog": catalog,
                "schema": schema,
                "table": t["table_name"],
                "type": t.get("table_type", ""),
                "comment": t.get("comment", ""),
            }

            # Get size if possible
            try:
                detail = execute_sql(
                    client, warehouse_id,
                    f"DESCRIBE DETAIL `{catalog}`.`{schema}`.`{t['table_name']}`",
                )
                if detail:
                    table_entry["size_bytes"] = detail[0].get("sizeInBytes", "")
                    table_entry["num_files"] = detail[0].get("numFiles", "")
                    table_entry["format"] = detail[0].get("format", "")
            except Exception:
                table_entry["size_bytes"] = ""
                table_entry["num_files"] = ""
                table_entry["format"] = ""

            tables_data.append(table_entry)

        # Columns
        col_sql = f"""
            SELECT table_name, column_name, data_type, is_nullable,
                   column_default, ordinal_position, comment
            FROM {catalog}.information_schema.columns
            WHERE table_schema = '{schema}'
            ORDER BY table_name, ordinal_position
        """
        columns = execute_sql(client, warehouse_id, col_sql)

        for c in columns:
            columns_data.append({
                "catalog": catalog,
                "schema": schema,
                "table": c["table_name"],
                "column": c["column_name"],
                "data_type": c.get("data_type", ""),
                "nullable": c.get("is_nullable", ""),
                "default": c.get("column_default", ""),
                "position": c.get("ordinal_position", ""),
                "comment": c.get("comment", ""),
            })

    # Generate output path
    if not output_path:
        os.makedirs("exports", exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        ext = output_format
        output_path = f"exports/{catalog}_{timestamp}.{ext}"

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    if output_format == "csv":
        _write_csv(output_path, tables_data, columns_data)
    else:
        _write_json(output_path, catalog, tables_data, columns_data)

    logger.info("=" * 60)
    logger.info(f"EXPORT COMPLETE: {catalog}")
    logger.info("=" * 60)
    logger.info(f"  Tables:  {len(tables_data)}")
    logger.info(f"  Columns: {len(columns_data)}")
    logger.info(f"  Format:  {output_format}")
    logger.info(f"  Output:  {output_path}")
    if output_format == "csv":
        cols_path = output_path.replace(".csv", "_columns.csv")
        logger.info(f"  Columns: {cols_path}")
    logger.info("=" * 60)

    return output_path


def _write_csv(output_path: str, tables: list[dict], columns: list[dict]) -> None:
    """Write tables and columns to CSV files."""
    # Tables CSV
    if tables:
        fieldnames = list(tables[0].keys())
        with open(output_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(tables)

    # Columns CSV
    cols_path = output_path.replace(".csv", "_columns.csv")
    if columns:
        fieldnames = list(columns[0].keys())
        with open(cols_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(columns)


def _write_json(
    output_path: str, catalog: str, tables: list[dict], columns: list[dict],
) -> None:
    """Write tables and columns to a JSON file."""
    data = {
        "catalog": catalog,
        "export_time": datetime.now().isoformat(),
        "tables": tables,
        "columns": columns,
    }
    with open(output_path, "w") as f:
        json.dump(data, f, indent=2, default=str)
