import json
import logging
import os
from datetime import datetime

from databricks.sdk import WorkspaceClient

from src.client import execute_sql

logger = logging.getLogger(__name__)

SNAPSHOT_DIR = "snapshots"


def create_snapshot(
    client: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
    exclude_schemas: list[str],
    output_path: str | None = None,
) -> str:
    """Export catalog metadata to a portable JSON manifest.

    Captures schemas, tables (with columns), views, functions, and volumes.
    """
    logger.info(f"Creating snapshot of catalog: {catalog}")

    exclude_clause = ",".join(f"'{s}'" for s in exclude_schemas)
    schema_sql = f"""
        SELECT schema_name, comment
        FROM {catalog}.information_schema.schemata
        WHERE schema_name NOT IN ({exclude_clause})
    """
    schemas = execute_sql(client, warehouse_id, schema_sql)

    snapshot = {
        "catalog": catalog,
        "snapshot_time": datetime.now().isoformat(),
        "schemas": [],
    }

    for schema_row in schemas:
        schema_name = schema_row["schema_name"]
        logger.info(f"  Snapshotting schema: {schema_name}")

        schema_data = {
            "name": schema_name,
            "comment": schema_row.get("comment"),
            "tables": [],
            "views": [],
            "functions": [],
            "volumes": [],
        }

        # Tables
        table_sql = f"""
            SELECT table_name, table_type, comment
            FROM {catalog}.information_schema.tables
            WHERE table_schema = '{schema_name}'
            AND table_type IN ('MANAGED', 'EXTERNAL')
        """
        tables = execute_sql(client, warehouse_id, table_sql)

        for t in tables:
            table_name = t["table_name"]

            # Get columns
            col_sql = f"""
                SELECT column_name, data_type, is_nullable, column_default,
                       ordinal_position, comment
                FROM {catalog}.information_schema.columns
                WHERE table_schema = '{schema_name}'
                AND table_name = '{table_name}'
                ORDER BY ordinal_position
            """
            columns = execute_sql(client, warehouse_id, col_sql)

            schema_data["tables"].append({
                "name": table_name,
                "type": t.get("table_type"),
                "comment": t.get("comment"),
                "columns": columns,
            })

        # Views
        view_sql = f"""
            SELECT table_name, view_definition
            FROM {catalog}.information_schema.views
            WHERE table_schema = '{schema_name}'
        """
        views = execute_sql(client, warehouse_id, view_sql)
        for v in views:
            schema_data["views"].append({
                "name": v["table_name"],
                "definition": v.get("view_definition"),
            })

        # Functions
        try:
            func_sql = f"""
                SELECT function_name, data_type, routine_definition
                FROM {catalog}.information_schema.routines
                WHERE routine_schema = '{schema_name}'
                AND routine_type = 'FUNCTION'
            """
            functions = execute_sql(client, warehouse_id, func_sql)
            for fn in functions:
                schema_data["functions"].append({
                    "name": fn["function_name"],
                    "return_type": fn.get("data_type"),
                    "definition": fn.get("routine_definition"),
                })
        except Exception:
            logger.debug(f"  Could not query routines for {schema_name}")
            pass  # Routines info schema may not be accessible for all schemas

        # Volumes
        try:
            vol_sql = f"""
                SELECT volume_name, volume_type, comment
                FROM {catalog}.information_schema.volumes
                WHERE volume_schema = '{schema_name}'
            """
            volumes = execute_sql(client, warehouse_id, vol_sql)
            for vol in volumes:
                schema_data["volumes"].append({
                    "name": vol["volume_name"],
                    "type": vol.get("volume_type"),
                    "comment": vol.get("comment"),
                })
        except Exception:
            pass  # Volumes info schema may not be available

        snapshot["schemas"].append(schema_data)

    # Calculate totals
    total_tables = sum(len(s["tables"]) for s in snapshot["schemas"])
    total_views = sum(len(s["views"]) for s in snapshot["schemas"])
    total_functions = sum(len(s["functions"]) for s in snapshot["schemas"])
    total_volumes = sum(len(s["volumes"]) for s in snapshot["schemas"])

    snapshot["totals"] = {
        "schemas": len(snapshot["schemas"]),
        "tables": total_tables,
        "views": total_views,
        "functions": total_functions,
        "volumes": total_volumes,
    }

    # Write snapshot
    if not output_path:
        os.makedirs(SNAPSHOT_DIR, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = os.path.join(SNAPSHOT_DIR, f"snapshot_{catalog}_{timestamp}.json")

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(snapshot, f, indent=2, default=str)

    logger.info("=" * 60)
    logger.info(f"SNAPSHOT: {catalog}")
    logger.info("=" * 60)
    logger.info(f"  Schemas:   {snapshot['totals']['schemas']}")
    logger.info(f"  Tables:    {total_tables}")
    logger.info(f"  Views:     {total_views}")
    logger.info(f"  Functions: {total_functions}")
    logger.info(f"  Volumes:   {total_volumes}")
    logger.info(f"  Output:    {output_path}")
    logger.info("=" * 60)

    return output_path


def compare_snapshots(snapshot_path_a: str, snapshot_path_b: str) -> dict:
    """Compare two snapshots to find differences."""
    with open(snapshot_path_a) as f:
        snap_a = json.load(f)
    with open(snapshot_path_b) as f:
        snap_b = json.load(f)

    schemas_a = {s["name"]: s for s in snap_a.get("schemas", [])}
    schemas_b = {s["name"]: s for s in snap_b.get("schemas", [])}

    diff = {
        "snapshot_a": snap_a.get("snapshot_time"),
        "snapshot_b": snap_b.get("snapshot_time"),
        "schemas_added": list(set(schemas_b) - set(schemas_a)),
        "schemas_removed": list(set(schemas_a) - set(schemas_b)),
        "table_changes": [],
    }

    for schema_name in set(schemas_a) & set(schemas_b):
        tables_a = {t["name"] for t in schemas_a[schema_name].get("tables", [])}
        tables_b = {t["name"] for t in schemas_b[schema_name].get("tables", [])}

        added = tables_b - tables_a
        removed = tables_a - tables_b

        if added or removed:
            diff["table_changes"].append({
                "schema": schema_name,
                "tables_added": list(added),
                "tables_removed": list(removed),
            })

    return diff
