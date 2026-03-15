import json
import logging
import os
from datetime import datetime

from databricks.sdk import WorkspaceClient

from src.client import execute_sql

logger = logging.getLogger(__name__)

SYNC_STATE_DIR = "sync_state"


def get_table_history(
    client: WorkspaceClient, warehouse_id: str, catalog: str, schema: str, table_name: str
) -> list[dict]:
    """Get Delta table history."""
    sql = f"DESCRIBE HISTORY `{catalog}`.`{schema}`.`{table_name}` LIMIT 50"
    try:
        return execute_sql(client, warehouse_id, sql)
    except Exception as e:
        logger.debug(f"Could not get history for {schema}.{table_name}: {e}")
        return []


def get_last_sync_version(
    source_catalog: str, dest_catalog: str, schema: str, table_name: str
) -> int | None:
    """Get the last synced version from the sync state file."""
    state_file = _get_state_file(source_catalog, dest_catalog)
    if not os.path.exists(state_file):
        return None

    try:
        with open(state_file) as f:
            state = json.load(f)
    except (json.JSONDecodeError, ValueError):
        logger.warning(f"Corrupt sync state file: {state_file} — treating all tables as new")
        return None

    key = f"{schema}.{table_name}"
    entry = state.get("tables", {}).get(key)
    if entry:
        return entry.get("version")
    return None


def save_sync_version(
    source_catalog: str, dest_catalog: str, schema: str, table_name: str, version: int
) -> None:
    """Save the synced version for a table."""
    state_file = _get_state_file(source_catalog, dest_catalog)

    state = {"tables": {}, "last_sync": None}
    if os.path.exists(state_file):
        try:
            with open(state_file) as f:
                state = json.load(f)
        except (json.JSONDecodeError, ValueError):
            logger.warning(f"Corrupt sync state file: {state_file} — resetting")
            state = {"tables": {}, "last_sync": None}

    key = f"{schema}.{table_name}"
    state["tables"][key] = {
        "version": version,
        "synced_at": datetime.now().isoformat(),
    }
    state["last_sync"] = datetime.now().isoformat()

    os.makedirs(os.path.dirname(state_file), exist_ok=True)
    with open(state_file, "w") as f:
        json.dump(state, f, indent=2)


def get_tables_needing_sync(
    client: WorkspaceClient,
    warehouse_id: str,
    source_catalog: str,
    dest_catalog: str,
    schema: str,
) -> list[dict]:
    """Find tables that have changed since last sync."""
    sql = f"""
        SELECT table_name
        FROM {source_catalog}.information_schema.tables
        WHERE table_schema = '{schema}'
        AND table_type IN ('MANAGED', 'EXTERNAL')
    """
    tables = execute_sql(client, warehouse_id, sql)
    needs_sync = []

    for row in tables:
        table_name = row["table_name"]
        last_version = get_last_sync_version(source_catalog, dest_catalog, schema, table_name)

        if last_version is None:
            needs_sync.append({
                "table_name": table_name,
                "reason": "never_synced",
                "last_synced_version": None,
            })
            continue

        history = get_table_history(client, warehouse_id, source_catalog, schema, table_name)
        if history:
            current_version = int(history[0].get("version", 0))
            if current_version > last_version:
                # Count changes since last sync
                changes = [h for h in history if int(h.get("version", 0)) > last_version]
                operations = [h.get("operation", "UNKNOWN") for h in changes]
                needs_sync.append({
                    "table_name": table_name,
                    "reason": "changed",
                    "last_synced_version": last_version,
                    "current_version": current_version,
                    "changes_since_sync": len(changes),
                    "operations": operations,
                })

    return needs_sync


def sync_changed_table(
    client: WorkspaceClient,
    warehouse_id: str,
    source_catalog: str,
    dest_catalog: str,
    schema: str,
    table_name: str,
    clone_type: str = "DEEP",
    dry_run: bool = False,
) -> bool:
    """Sync a single changed table by re-cloning it."""
    source = f"`{source_catalog}`.`{schema}`.`{table_name}`"
    dest = f"`{dest_catalog}`.`{schema}`.`{table_name}`"

    # Drop and re-clone for deep clone; for shallow, CREATE OR REPLACE works
    if clone_type == "DEEP":
        drop_sql = f"DROP TABLE IF EXISTS {dest}"
        execute_sql(client, warehouse_id, drop_sql, dry_run=dry_run)

    clone_keyword = "DEEP CLONE" if clone_type == "DEEP" else "SHALLOW CLONE"
    sql = f"CREATE OR REPLACE TABLE {dest} {clone_keyword} {source}"

    try:
        execute_sql(client, warehouse_id, sql, dry_run=dry_run)
        logger.info(f"{'[DRY RUN] ' if dry_run else ''}Synced: {source} -> {dest}")

        # Save sync state
        if not dry_run:
            history = get_table_history(client, warehouse_id, source_catalog, schema, table_name)
            if history:
                version = int(history[0].get("version", 0))
                save_sync_version(source_catalog, dest_catalog, schema, table_name, version)

        return True
    except Exception as e:
        logger.error(f"Failed to sync {source}: {e}")
        return False


def _get_state_file(source_catalog: str, dest_catalog: str) -> str:
    """Get the state file path for a source/dest pair."""
    return os.path.join(SYNC_STATE_DIR, f"sync_{source_catalog}_to_{dest_catalog}.json")
