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


def enforce_rbac_for_sync(client, source_catalog: str, dest_catalog: str, config: dict | None = None) -> None:
    """Enforce RBAC for incremental sync operations."""
    rbac_config = config or {}
    if not rbac_config.get("rbac_enabled"):
        return
    from src.rbac import enforce_rbac
    enforce_rbac(client, {
        "source_catalog": source_catalog,
        "destination_catalog": dest_catalog,
        "rbac_policy_path": rbac_config.get("rbac_policy_path", "~/.clone-xs/rbac_policy.yaml"),
    }, operation="sync")


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


# ---------------------------------------------------------------------------
# CDF-based Incremental Sync
# ---------------------------------------------------------------------------

def check_cdf_enabled(
    client: WorkspaceClient, warehouse_id: str,
    catalog: str, schema: str, table_name: str,
) -> bool:
    """Check if Change Data Feed is enabled on a Delta table via SDK."""
    full_name = f"{catalog}.{schema}.{table_name}"
    try:
        table_info = client.tables.get(full_name=full_name)
        props = dict(table_info.properties) if table_info.properties else {}
        return str(props.get("delta.enableChangeDataFeed", "false")).lower() == "true"
    except Exception as e:
        logger.debug(f"Could not check CDF for {full_name}: {e}")
    return False


def get_cdf_changes(
    client: WorkspaceClient, warehouse_id: str,
    catalog: str, schema: str, table_name: str,
    since_version: int,
) -> list[dict]:
    """Get row-level changes from Change Data Feed since a given version.

    Returns rows with _change_type (insert, update_preimage,
    update_postimage, delete), _commit_version, and _commit_timestamp.
    """
    fqn = f"`{catalog}`.`{schema}`.`{table_name}`"
    sql = f"SELECT * FROM table_changes('{catalog}.{schema}.{table_name}', {since_version + 1})"
    try:
        results = execute_sql(client, warehouse_id, sql)
        logger.info(f"CDF: {len(results)} changes for {fqn} since version {since_version}")
        return results
    except Exception as e:
        logger.error(f"Failed to read CDF for {fqn}: {e}")
        return []


def get_cdf_change_summary(
    client: WorkspaceClient, warehouse_id: str,
    catalog: str, schema: str, table_name: str,
    since_version: int,
) -> dict:
    """Get a summary of CDF changes without fetching all rows.

    Returns counts by change type.
    """
    fqn = f"{catalog}.{schema}.{table_name}"
    sql = f"""
        SELECT
            _change_type,
            COUNT(*) AS change_count,
            MAX(_commit_version) AS latest_version
        FROM table_changes('{fqn}', {since_version + 1})
        GROUP BY _change_type
    """
    try:
        results = execute_sql(client, warehouse_id, sql)
        summary = {
            "table": fqn,
            "since_version": since_version,
            "changes": {r["_change_type"]: r["change_count"] for r in results},
            "latest_version": max((r.get("latest_version", 0) for r in results), default=0),
            "total_changes": sum(r["change_count"] for r in results),
        }
        return summary
    except Exception as e:
        logger.debug(f"Could not get CDF summary for {fqn}: {e}")
        return {"table": fqn, "error": str(e)}


def apply_cdf_changes(
    client: WorkspaceClient, warehouse_id: str,
    source_catalog: str, dest_catalog: str,
    schema: str, table_name: str,
    since_version: int,
    dry_run: bool = False,
) -> dict:
    """Apply CDF changes from source to destination using MERGE INTO.

    Reads changes from the source table's CDF and merges inserts, updates,
    and deletes into the destination table.
    """
    source_fqn = f"{source_catalog}.{schema}.{table_name}"
    dest_fqn = f"`{dest_catalog}`.`{schema}`.`{table_name}`"

    # Get primary key columns from destination table
    pk_cols = _get_primary_keys(client, warehouse_id, dest_catalog, schema, table_name)

    if not pk_cols:
        # Fallback: re-clone the table if no PK is available for MERGE
        logger.warning(f"No primary key for {source_fqn} — falling back to full re-clone")
        success = sync_changed_table(
            client, warehouse_id, source_catalog, dest_catalog,
            schema, table_name, "DEEP", dry_run,
        )
        return {"table": source_fqn, "method": "re-clone", "success": success}

    # Build MERGE statement
    join_condition = " AND ".join(f"t.`{col}` = s.`{col}`" for col in pk_cols)

    # Get non-CDF columns via SDK
    dest_full_name = f"{dest_catalog}.{schema}.{table_name}"
    table_info = client.tables.get(full_name=dest_full_name)
    columns = [
        c.name for c in (table_info.columns or [])
        if not c.name.startswith("_change_") and not c.name.startswith("_commit_")
    ]
    update_set = ", ".join(f"t.`{c}` = s.`{c}`" for c in columns)
    insert_cols = ", ".join(f"`{c}`" for c in columns)
    insert_vals = ", ".join(f"s.`{c}`" for c in columns)

    merge_sql = f"""
        MERGE INTO {dest_fqn} AS t
        USING (
            SELECT * FROM table_changes('{source_fqn}', {since_version + 1})
            WHERE _change_type IN ('insert', 'update_postimage', 'delete')
        ) AS s
        ON {join_condition}
        WHEN MATCHED AND s._change_type = 'delete' THEN DELETE
        WHEN MATCHED AND s._change_type = 'update_postimage' THEN UPDATE SET {update_set}
        WHEN NOT MATCHED AND s._change_type IN ('insert', 'update_postimage') THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """

    if dry_run:
        logger.info(f"[DRY RUN] CDF MERGE for {source_fqn}")
        return {"table": source_fqn, "method": "cdf_merge", "dry_run": True, "sql": merge_sql}

    try:
        execute_sql(client, warehouse_id, merge_sql)

        # Update sync state with the latest version
        history = get_table_history(client, warehouse_id, source_catalog, schema, table_name)
        if history:
            version = int(history[0].get("version", 0))
            save_sync_version(source_catalog, dest_catalog, schema, table_name, version)

        logger.info(f"CDF MERGE applied for {source_fqn}")
        return {"table": source_fqn, "method": "cdf_merge", "success": True}
    except Exception as e:
        logger.error(f"CDF MERGE failed for {source_fqn}: {e}")
        return {"table": source_fqn, "method": "cdf_merge", "success": False, "error": str(e)}


def sync_table_cdf(
    client: WorkspaceClient, warehouse_id: str,
    source_catalog: str, dest_catalog: str,
    schema: str, table_name: str,
    clone_type: str = "DEEP",
    dry_run: bool = False,
    sync_mode: str = "auto",
) -> dict:
    """Sync a single table using CDF if available, otherwise fall back to version-based clone.

    sync_mode: "cdf" (force CDF), "version" (force re-clone), "auto" (try CDF, fall back)
    """
    last_version = get_last_sync_version(source_catalog, dest_catalog, schema, table_name)

    if last_version is None:
        # Never synced — do a full clone
        success = sync_changed_table(
            client, warehouse_id, source_catalog, dest_catalog,
            schema, table_name, clone_type, dry_run,
        )
        return {"table": f"{schema}.{table_name}", "method": "initial_clone", "success": success}

    use_cdf = False
    if sync_mode in ("cdf", "auto"):
        use_cdf = check_cdf_enabled(client, warehouse_id, source_catalog, schema, table_name)
        if sync_mode == "cdf" and not use_cdf:
            logger.warning(
                f"CDF not enabled for {source_catalog}.{schema}.{table_name} "
                f"— falling back to version-based clone"
            )

    if use_cdf:
        return apply_cdf_changes(
            client, warehouse_id, source_catalog, dest_catalog,
            schema, table_name, last_version, dry_run,
        )
    else:
        success = sync_changed_table(
            client, warehouse_id, source_catalog, dest_catalog,
            schema, table_name, clone_type, dry_run,
        )
        return {"table": f"{schema}.{table_name}", "method": "version_clone", "success": success}


def _get_primary_keys(
    client: WorkspaceClient, warehouse_id: str,
    catalog: str, schema: str, table_name: str,
) -> list[str]:
    """Get primary key columns for a table via SDK.

    Inspects table_constraints from the SDK TableInfo object.
    Falls back to information_schema SQL if SDK doesn't expose constraints.
    """
    full_name = f"{catalog}.{schema}.{table_name}"
    try:
        table_info = client.tables.get(full_name=full_name)
        # Check SDK table_constraints if available
        if hasattr(table_info, "table_constraints") and table_info.table_constraints:
            for constraint in table_info.table_constraints:
                if hasattr(constraint, "primary_key_constraint") and constraint.primary_key_constraint:
                    pk = constraint.primary_key_constraint
                    if hasattr(pk, "child_columns") and pk.child_columns:
                        return list(pk.child_columns)
    except Exception as e:
        logger.debug(f"SDK table_constraints not available for {full_name}: {e}")

    # Fallback: query information_schema via SQL
    try:
        sql = f"""
            SELECT kcu.column_name
            FROM `{catalog}`.information_schema.table_constraints AS tc
            JOIN `{catalog}`.information_schema.key_column_usage AS kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
             AND tc.table_name = kcu.table_name
            WHERE tc.table_schema = '{schema}'
              AND tc.table_name = '{table_name}'
              AND tc.constraint_type = 'PRIMARY KEY'
            ORDER BY kcu.ordinal_position
        """
        rows = execute_sql(client, warehouse_id, sql)
        return [r["column_name"] for r in rows]
    except Exception:
        return []
