import json
import logging
import os
from datetime import datetime

from databricks.sdk import WorkspaceClient

from src.client import execute_sql

logger = logging.getLogger(__name__)

LINEAGE_DIR = "lineage"


def record_lineage(
    source_catalog: str,
    dest_catalog: str,
    schema: str,
    object_name: str,
    object_type: str,
    clone_type: str,
    metadata: dict | None = None,
) -> None:
    """Record a lineage entry for a cloned object."""
    entry = {
        "source": f"{source_catalog}.{schema}.{object_name}",
        "destination": f"{dest_catalog}.{schema}.{object_name}",
        "object_type": object_type,
        "clone_type": clone_type,
        "timestamp": datetime.now().isoformat(),
    }
    if metadata:
        entry["metadata"] = metadata

    lineage_file = _get_lineage_file(dest_catalog)
    lineage = _load_lineage(lineage_file)
    lineage["entries"].append(entry)
    _save_lineage(lineage_file, lineage)


def record_lineage_to_uc(
    client: WorkspaceClient,
    warehouse_id: str,
    lineage_catalog: str,
    lineage_schema: str,
    source_catalog: str,
    dest_catalog: str,
    schema: str,
    object_name: str,
    object_type: str,
    clone_type: str,
    dry_run: bool = False,
) -> None:
    """Record lineage in a Unity Catalog table."""
    table = f"`{lineage_catalog}`.`{lineage_schema}`.`clone_lineage`"

    # Ensure lineage table exists
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table} (
            source_object STRING,
            destination_object STRING,
            object_type STRING,
            clone_type STRING,
            clone_timestamp TIMESTAMP
        )
    """
    execute_sql(client, warehouse_id, create_sql, dry_run=dry_run)

    from src.client import utc_now
    now = utc_now()
    insert_sql = f"""
        INSERT INTO {table} VALUES (
            '{source_catalog}.{schema}.{object_name}',
            '{dest_catalog}.{schema}.{object_name}',
            '{object_type}',
            '{clone_type}',
            TIMESTAMP '{now}'
        )
    """
    try:
        execute_sql(client, warehouse_id, insert_sql, dry_run=dry_run)
    except Exception as e:
        logger.warning(f"Failed to record lineage for {schema}.{object_name}: {e}")


def record_lineage_batch(
    client: WorkspaceClient,
    warehouse_id: str,
    lineage_catalog: str,
    lineage_schema: str,
    entries: list[dict],
    dry_run: bool = False,
    config: dict | None = None,
) -> None:
    """Batch-insert lineage records.

    Each entry: {source, dest, schema, object_name, object_type, clone_type}
    """
    table = f"`{lineage_catalog}`.`{lineage_schema}`.`clone_lineage`"
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table} (
            source_object STRING,
            destination_object STRING,
            object_type STRING,
            clone_type STRING,
            clone_timestamp TIMESTAMP
        )
    """
    execute_sql(client, warehouse_id, create_sql, dry_run=dry_run)

    from src.client import utc_now
    now = utc_now()
    value_rows = []
    for e in entries:
        src_fqn = f"{e['source']}.{e['schema']}.{e['object_name']}"
        dst_fqn = f"{e['dest']}.{e['schema']}.{e['object_name']}"
        value_rows.append(
            f"('{src_fqn}', '{dst_fqn}', '{e['object_type']}', "
            f"'{e['clone_type']}', TIMESTAMP '{now}')"
        )

    from src.table_registry import get_batch_insert_size
    batch_size = get_batch_insert_size(config or {})
    for i in range(0, len(value_rows), batch_size):
        batch = value_rows[i:i + batch_size]
        try:
            execute_sql(client, warehouse_id,
                        f"INSERT INTO {table} VALUES {', '.join(batch)}",
                        dry_run=dry_run)
        except Exception as e:
            logger.warning(f"Failed to batch-insert lineage: {e}")


def get_lineage_for_object(
    client: WorkspaceClient,
    warehouse_id: str,
    lineage_catalog: str,
    lineage_schema: str,
    object_path: str,
) -> list[dict]:
    """Get lineage history for a specific object."""
    table = f"`{lineage_catalog}`.`{lineage_schema}`.`clone_lineage`"
    sql = f"""
        SELECT * FROM {table}
        WHERE source_object = '{object_path}'
        OR destination_object = '{object_path}'
        ORDER BY clone_timestamp DESC
    """
    try:
        return execute_sql(client, warehouse_id, sql)
    except Exception:
        return []


def export_lineage(dest_catalog: str) -> dict:
    """Export all lineage entries for a destination catalog."""
    lineage_file = _get_lineage_file(dest_catalog)
    return _load_lineage(lineage_file)


def _get_lineage_file(dest_catalog: str) -> str:
    return os.path.join(LINEAGE_DIR, f"lineage_{dest_catalog}.json")


def _load_lineage(path: str) -> dict:
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {"entries": [], "created": datetime.now().isoformat()}


def _save_lineage(path: str, data: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
