"""Schema evolution handler — detect and apply schema changes without re-cloning."""

import logging

from src.client import execute_sql

logger = logging.getLogger(__name__)


def detect_schema_changes(
    client,
    warehouse_id: str,
    source_catalog: str,
    dest_catalog: str,
    schema_name: str,
    table_name: str,
) -> dict:
    """Compare column schemas between source and dest tables.

    Returns:
        Dict with added_columns, removed_columns, changed_columns, and is_compatible.
    """
    source_fqn = f"{source_catalog}.{schema_name}.{table_name}"
    dest_fqn = f"{dest_catalog}.{schema_name}.{table_name}"

    def _get_columns(fqn):
        sql = f"""
        SELECT column_name, data_type, is_nullable, column_default, ordinal_position
        FROM {fqn.rsplit('.', 1)[0].rsplit('.', 1)[0]}.information_schema.columns
        WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'
        ORDER BY ordinal_position
        """
        catalog = fqn.split(".")[0]
        sql = f"""
        SELECT column_name, data_type, is_nullable, ordinal_position
        FROM {catalog}.information_schema.columns
        WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'
        ORDER BY ordinal_position
        """
        rows = execute_sql(client, warehouse_id, sql)
        return {r["column_name"]: r for r in rows}

    try:
        source_cols = _get_columns(source_fqn)
        dest_cols = _get_columns(dest_fqn)
    except Exception as e:
        return {"error": str(e), "is_compatible": False}

    added = []
    removed = []
    changed = []

    for col_name, col_info in source_cols.items():
        if col_name not in dest_cols:
            added.append({
                "column": col_name,
                "data_type": col_info["data_type"],
                "nullable": col_info["is_nullable"],
            })
        else:
            dest_info = dest_cols[col_name]
            if col_info["data_type"] != dest_info["data_type"]:
                changed.append({
                    "column": col_name,
                    "source_type": col_info["data_type"],
                    "dest_type": dest_info["data_type"],
                })
            if col_info["is_nullable"] != dest_info["is_nullable"]:
                changed.append({
                    "column": col_name,
                    "change": "nullability",
                    "source": col_info["is_nullable"],
                    "dest": dest_info["is_nullable"],
                })

    for col_name in dest_cols:
        if col_name not in source_cols:
            removed.append({
                "column": col_name,
                "data_type": dest_cols[col_name]["data_type"],
            })

    # Schema evolution is "compatible" if it only adds nullable columns
    is_compatible = (
        len(changed) == 0
        and len(removed) == 0
        and all(c.get("nullable", "YES") == "YES" for c in added)
    )

    return {
        "source": source_fqn,
        "dest": dest_fqn,
        "added_columns": added,
        "removed_columns": removed,
        "changed_columns": changed,
        "is_compatible": is_compatible,
        "requires_action": bool(added or removed or changed),
    }


def apply_schema_evolution(
    client,
    warehouse_id: str,
    dest_catalog: str,
    schema_name: str,
    table_name: str,
    changes: dict,
    dry_run: bool = False,
    drop_removed: bool = False,
) -> dict:
    """Apply detected schema changes to the destination table using ALTER TABLE.

    Args:
        changes: Output from detect_schema_changes().
        dry_run: If True, log SQL without executing.
        drop_removed: If True, drop columns that exist in dest but not source.

    Returns:
        Dict with applied changes summary.
    """
    dest_fqn = f"{dest_catalog}.{schema_name}.{table_name}"
    applied = {"added": [], "dropped": [], "altered": [], "skipped": [], "errors": []}

    # Add new columns
    for col in changes.get("added_columns", []):
        col_name = col["column"]
        data_type = col["data_type"]
        sql = f"ALTER TABLE {dest_fqn} ADD COLUMN `{col_name}` {data_type}"

        if dry_run:
            logger.info(f"[DRY RUN] Would execute: {sql}")
            applied["added"].append(col_name)
        else:
            try:
                execute_sql(client, warehouse_id, sql)
                logger.info(f"Added column: {dest_fqn}.{col_name} ({data_type})")
                applied["added"].append(col_name)
            except Exception as e:
                logger.error(f"Failed to add column {col_name}: {e}")
                applied["errors"].append({"column": col_name, "error": str(e)})

    # Drop removed columns (only if explicitly requested)
    if drop_removed:
        for col in changes.get("removed_columns", []):
            col_name = col["column"]
            sql = f"ALTER TABLE {dest_fqn} DROP COLUMN `{col_name}`"

            if dry_run:
                logger.info(f"[DRY RUN] Would execute: {sql}")
                applied["dropped"].append(col_name)
            else:
                try:
                    execute_sql(client, warehouse_id, sql)
                    logger.info(f"Dropped column: {dest_fqn}.{col_name}")
                    applied["dropped"].append(col_name)
                except Exception as e:
                    logger.error(f"Failed to drop column {col_name}: {e}")
                    applied["errors"].append({"column": col_name, "error": str(e)})
    else:
        for col in changes.get("removed_columns", []):
            logger.warning(
                f"Column {col['column']} exists in dest but not source — "
                f"use --drop-removed to remove it"
            )
            applied["skipped"].append(col["column"])

    # Type changes (ALTER COLUMN SET DATA TYPE — limited support in Delta)
    for col in changes.get("changed_columns", []):
        if "source_type" in col:
            col_name = col["column"]
            new_type = col["source_type"]
            sql = f"ALTER TABLE {dest_fqn} ALTER COLUMN `{col_name}` SET DATA TYPE {new_type}"

            if dry_run:
                logger.info(f"[DRY RUN] Would execute: {sql}")
                applied["altered"].append(col_name)
            else:
                try:
                    execute_sql(client, warehouse_id, sql)
                    logger.info(f"Altered column type: {dest_fqn}.{col_name} -> {new_type}")
                    applied["altered"].append(col_name)
                except Exception as e:
                    logger.warning(
                        f"Cannot alter {col_name} from {col.get('dest_type')} to {new_type}: {e}. "
                        f"You may need to re-clone this table."
                    )
                    applied["errors"].append({"column": col_name, "error": str(e)})

    return applied


def evolve_catalog_schema(
    client,
    warehouse_id: str,
    source_catalog: str,
    dest_catalog: str,
    exclude_schemas: list[str] | None = None,
    dry_run: bool = False,
    drop_removed: bool = False,
    max_workers: int = 4,
) -> dict:
    """Detect and apply schema evolution across all tables in a catalog.

    Returns:
        Summary of all changes detected and applied.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    exclude = exclude_schemas or []

    # Get all tables
    sql = f"""
    SELECT table_schema, table_name
    FROM {source_catalog}.information_schema.tables
    WHERE table_schema NOT IN ('information_schema')
      AND table_type != 'VIEW'
    """
    tables = execute_sql(client, warehouse_id, sql)
    tables = [t for t in tables if t["table_schema"] not in exclude]

    logger.info(f"Checking schema evolution for {len(tables)} tables...")

    all_changes = []
    all_applied = []

    def _process_table(t):
        schema = t["table_schema"]
        table = t["table_name"]
        changes = detect_schema_changes(
            client, warehouse_id, source_catalog, dest_catalog, schema, table
        )

        if changes.get("error"):
            return {"table": f"{schema}.{table}", "status": "error", "error": changes["error"]}

        if not changes["requires_action"]:
            return {"table": f"{schema}.{table}", "status": "no_changes"}

        applied = apply_schema_evolution(
            client, warehouse_id, dest_catalog, schema, table,
            changes, dry_run=dry_run, drop_removed=drop_removed,
        )
        return {
            "table": f"{schema}.{table}",
            "status": "applied" if not dry_run else "dry_run",
            "changes": changes,
            "applied": applied,
        }

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_process_table, t): t for t in tables}
        for future in as_completed(futures):
            result = future.result()
            all_changes.append(result)

    tables_changed = sum(1 for c in all_changes if c["status"] in ("applied", "dry_run"))
    tables_errors = sum(1 for c in all_changes if c["status"] == "error")

    summary = {
        "total_tables": len(tables),
        "tables_with_changes": tables_changed,
        "tables_unchanged": len(tables) - tables_changed - tables_errors,
        "tables_with_errors": tables_errors,
        "dry_run": dry_run,
        "details": [c for c in all_changes if c["status"] != "no_changes"],
    }

    logger.info("=" * 60)
    logger.info(f"SCHEMA EVOLUTION: {source_catalog} -> {dest_catalog}")
    logger.info("=" * 60)
    logger.info(f"Tables checked:     {len(tables)}")
    logger.info(f"Tables with changes: {tables_changed}")
    logger.info(f"Tables with errors:  {tables_errors}")

    for detail in summary["details"]:
        if detail["status"] in ("applied", "dry_run"):
            changes = detail.get("changes", {})
            added = len(changes.get("added_columns", []))
            removed = len(changes.get("removed_columns", []))
            changed = len(changes.get("changed_columns", []))
            prefix = "[DRY RUN] " if dry_run else ""
            logger.info(f"  {prefix}{detail['table']}: +{added} added, -{removed} removed, ~{changed} changed")

    return summary
