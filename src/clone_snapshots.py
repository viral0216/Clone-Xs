"""Named clone snapshots (fork points).

A snapshot is a **named capture** of a catalog's Delta-version state at a
point in time. You can later clone *from* a snapshot — the orchestrator uses
the snapshot's captured timestamp as the `as_of_timestamp` for every table,
giving you point-in-time clones without hunting for the right timestamp.

Not to be confused with the analysis "metadata snapshot" in ``src/snapshot.py``
(captures schema DDL to a file for diffing — different feature).

Storage: one Delta row per snapshot in
``<audit_catalog>.<audit_schema>.clone_snapshots``. The table is created on
first use via :func:`ensure_snapshot_table`.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone

from databricks.sdk import WorkspaceClient

from src.client import execute_sql

logger = logging.getLogger(__name__)


def _audit_fqn(config: dict) -> tuple[str, str, str]:
    """Resolve audit catalog/schema/table from clone config."""
    audit = config.get("audit_trail") or {}
    catalog = audit.get("catalog") or config.get("audit_trail_catalog") or ""
    schema = audit.get("schema") or config.get("audit_trail_schema") or "clone_xs"
    if not catalog:
        raise ValueError(
            "audit_trail.catalog is not configured — snapshots need a place to live. "
            "Set it in Settings or config/clone_config.yaml."
        )
    return catalog, schema, "clone_snapshots"


def ensure_snapshot_table(client: WorkspaceClient, warehouse_id: str, config: dict) -> str:
    """Create the snapshot Delta table if it doesn't exist. Returns its FQN."""
    catalog, schema, table = _audit_fqn(config)
    fqn = f"`{catalog}`.`{schema}`.`{table}`"

    execute_sql(client, warehouse_id, f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
    execute_sql(
        client,
        warehouse_id,
        f"""
        CREATE TABLE IF NOT EXISTS {fqn} (
            snapshot_id    STRING,
            name           STRING,
            source_catalog STRING,
            description    STRING,
            captured_at    TIMESTAMP,
            created_by     STRING,
            table_count    INT,
            total_bytes    BIGINT,
            tables_json    STRING
        ) USING DELTA
        """.strip(),
    )
    return fqn


def _enumerate_tables(
    client: WorkspaceClient,
    warehouse_id: str,
    source_catalog: str,
    exclude_schemas: list[str] | None,
) -> list[tuple[str, str]]:
    """Return (schema, table) pairs for MANAGED + EXTERNAL Delta tables."""
    excl = ", ".join(f"'{s}'" for s in (exclude_schemas or ["information_schema", "default"]))
    rows = execute_sql(
        client,
        warehouse_id,
        f"""
        SELECT table_schema, table_name
        FROM {source_catalog}.information_schema.tables
        WHERE table_type IN ('MANAGED','EXTERNAL')
          AND table_schema NOT IN ({excl})
        """.strip(),
    )
    return [(r["table_schema"], r["table_name"]) for r in rows]


def _capture_table_detail(
    client: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
    schema: str,
    table: str,
) -> dict:
    """DESCRIBE DETAIL one table to capture current version + size. Best-effort."""
    try:
        rows = execute_sql(
            client, warehouse_id, f"DESCRIBE DETAIL `{catalog}`.`{schema}`.`{table}`"
        )
        if rows:
            r = rows[0]
            return {
                "schema": schema,
                "table": table,
                "version": int(r.get("version", 0) or 0),
                "size_bytes": int(r.get("sizeInBytes", 0) or 0),
            }
    except Exception as e:
        logger.debug(f"DESCRIBE DETAIL failed for {schema}.{table}: {e}")
    return {"schema": schema, "table": table, "version": None, "size_bytes": 0}


def create_snapshot(
    client: WorkspaceClient,
    warehouse_id: str,
    config: dict,
    *,
    source_catalog: str,
    name: str,
    description: str | None = None,
    created_by: str | None = None,
    exclude_schemas: list[str] | None = None,
) -> dict:
    """Capture a named snapshot of a catalog's current state.

    Returns the inserted row as a dict. The ``snapshot_id`` is a new UUID.
    """
    fqn = ensure_snapshot_table(client, warehouse_id, config)
    snapshot_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)

    tables = _enumerate_tables(client, warehouse_id, source_catalog, exclude_schemas)
    details = [_capture_table_detail(client, warehouse_id, source_catalog, s, t) for s, t in tables]
    total_bytes = sum(d["size_bytes"] or 0 for d in details)

    row = {
        "snapshot_id": snapshot_id,
        "name": name,
        "source_catalog": source_catalog,
        "description": description or "",
        "captured_at": now.isoformat(),
        "created_by": created_by or "",
        "table_count": len(details),
        "total_bytes": total_bytes,
        "tables_json": json.dumps(details),
    }

    safe_desc = (description or "").replace("'", "''")
    safe_by = (created_by or "").replace("'", "''")
    safe_json = row["tables_json"].replace("'", "''")
    execute_sql(
        client,
        warehouse_id,
        f"""
        INSERT INTO {fqn} VALUES (
            '{snapshot_id}', '{name}', '{source_catalog}', '{safe_desc}',
            TIMESTAMP '{now.strftime("%Y-%m-%d %H:%M:%S")}',
            '{safe_by}', {len(details)}, {total_bytes}, '{safe_json}'
        )
        """.strip(),
    )

    logger.info(
        f"Snapshot '{name}' created: {snapshot_id} — {len(details)} tables, "
        f"{total_bytes / (1024**3):.2f} GB"
    )
    return row


def list_snapshots(
    client: WorkspaceClient,
    warehouse_id: str,
    config: dict,
    *,
    source_catalog: str | None = None,
) -> list[dict]:
    """List all snapshots, newest first. Optionally filter by source catalog."""
    fqn = ensure_snapshot_table(client, warehouse_id, config)
    where = f"WHERE source_catalog = '{source_catalog}'" if source_catalog else ""
    rows = execute_sql(
        client,
        warehouse_id,
        f"""
        SELECT snapshot_id, name, source_catalog, description,
               captured_at, created_by, table_count, total_bytes
        FROM {fqn} {where}
        ORDER BY captured_at DESC
        LIMIT 500
        """.strip(),
    )
    return rows


def get_snapshot(
    client: WorkspaceClient,
    warehouse_id: str,
    config: dict,
    snapshot_id: str,
) -> dict | None:
    """Return full snapshot including parsed tables list, or None if missing."""
    fqn = ensure_snapshot_table(client, warehouse_id, config)
    rows = execute_sql(
        client,
        warehouse_id,
        f"SELECT * FROM {fqn} WHERE snapshot_id = '{snapshot_id}' LIMIT 1",
    )
    if not rows:
        return None
    row = dict(rows[0])
    try:
        row["tables"] = json.loads(row.get("tables_json") or "[]")
    except Exception:
        row["tables"] = []
    return row


def delete_snapshot(
    client: WorkspaceClient,
    warehouse_id: str,
    config: dict,
    snapshot_id: str,
) -> bool:
    """Remove a snapshot row. Returns True if anything was deleted."""
    fqn = ensure_snapshot_table(client, warehouse_id, config)
    execute_sql(
        client,
        warehouse_id,
        f"DELETE FROM {fqn} WHERE snapshot_id = '{snapshot_id}'",
    )
    return True


def resolve_snapshot_timestamp(
    client: WorkspaceClient,
    warehouse_id: str,
    config: dict,
    snapshot_id: str,
) -> str | None:
    """Look up a snapshot's captured timestamp for use as `as_of_timestamp`.

    Returns ISO-formatted timestamp string, or None if the snapshot is missing.
    Called by the clone orchestrator when `source_snapshot_id` is on the request.
    """
    snap = get_snapshot(client, warehouse_id, config, snapshot_id)
    if not snap:
        return None
    ts = snap.get("captured_at")
    if ts is None:
        return None
    # Delta returns datetime or string depending on SDK path
    if hasattr(ts, "isoformat"):
        return ts.isoformat()
    return str(ts)
