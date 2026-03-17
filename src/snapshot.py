import json
import logging
import os
from datetime import datetime

from databricks.sdk import WorkspaceClient

from src.client import (
    execute_sql,
    list_schemas_sdk,
    list_tables_sdk,
    list_views_sdk,
    list_functions_sdk,
    list_volumes_sdk,
    get_table_info_sdk,
)

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

    schema_names = list_schemas_sdk(client, catalog, exclude=exclude_schemas)

    snapshot = {
        "catalog": catalog,
        "snapshot_time": datetime.now().isoformat(),
        "schemas": [],
    }

    for schema_name in schema_names:
        logger.info(f"  Snapshotting schema: {schema_name}")

        schema_data = {
            "name": schema_name,
            "comment": None,
            "tables": [],
            "views": [],
            "functions": [],
            "volumes": [],
        }

        # Tables
        all_tables = list_tables_sdk(client, catalog, schema_name)
        tables = [t for t in all_tables if t["table_type"] in ("MANAGED", "EXTERNAL")]

        for t in tables:
            table_name = t["table_name"]

            # Get columns via SDK
            table_info = get_table_info_sdk(client, f"{catalog}.{schema_name}.{table_name}")
            columns = []
            if table_info and table_info.get("columns"):
                columns = [
                    {
                        "column_name": c["column_name"],
                        "data_type": c["data_type"],
                        "is_nullable": str(c.get("nullable", True)).upper(),
                        "column_default": None,
                        "ordinal_position": idx + 1,
                        "comment": c.get("comment", ""),
                    }
                    for idx, c in enumerate(table_info["columns"])
                ]

            schema_data["tables"].append({
                "name": table_name,
                "type": t.get("table_type"),
                "comment": table_info.get("comment") if table_info else None,
                "columns": columns,
            })

        # Views
        views = list_views_sdk(client, catalog, schema_name)
        for v in views:
            schema_data["views"].append({
                "name": v["table_name"],
                "definition": v.get("view_definition"),
            })

        # Functions
        try:
            functions = list_functions_sdk(client, catalog, schema_name)
            for fn in functions:
                schema_data["functions"].append({
                    "name": fn["function_name"],
                    "return_type": fn.get("data_type"),
                    "definition": None,
                })
        except Exception:
            logger.debug(f"  Could not query routines for {schema_name}")

        # Volumes
        try:
            volumes = list_volumes_sdk(client, catalog, schema_name)
            for vol in volumes:
                schema_data["volumes"].append({
                    "name": vol["volume_name"],
                    "type": vol.get("volume_type"),
                    "comment": None,
                })
        except Exception:
            pass  # Volumes SDK may not be available

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
