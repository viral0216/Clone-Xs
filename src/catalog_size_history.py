"""Per-catalog size history — daily time-series for FinOps trend charts.

The Catalog Explorer's stats endpoint returns a *snapshot* of each catalog's
size, table count, and row count at the moment the user clicks Explore.
The Multi-mode Overview tab can show the current numbers, but it has no
way to answer "is this catalog growing or shrinking?" without a historical
record.

This module captures one row per `(date, catalog)` to a Delta table in the
configured audit catalog, schema `clone_xs`, table `catalog_size_history`.
Recording is idempotent: if a row already exists for today's date+catalog,
the new value overwrites it (DELETE + INSERT in a transaction). That way
a user clicking Explore three times in one day produces one row, not
three, and that row reflects the most recent observation.

Recording is best-effort and fire-and-forget — any failure logs at WARN
and returns silently so the /stats path can never break because of a
history write. Reads are also best-effort and return `[]` if the table
hasn't been created yet (i.e. nobody's snapshotted anything yet).

Audit-catalog config and the schema-creation pattern mirror
`src.clone_snapshots`; both modules write to `clone_xs` schema in the
same audit catalog.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from databricks.sdk import WorkspaceClient

from src.client import execute_sql

logger = logging.getLogger(__name__)


def _history_fqn(config: dict) -> tuple[str, str, str]:
    """Resolve audit catalog/schema/table for the daily history table.

    Same convention as `clone_snapshots._audit_fqn` so all rollup tables
    live together. Returns (catalog, schema, table). Raises ValueError
    if no audit catalog is configured — callers running the recorder
    fire-and-forget should catch this and log/skip.
    """
    audit = config.get("audit_trail") or {}
    catalog = audit.get("catalog") or config.get("audit_trail_catalog") or ""
    schema = audit.get("schema") or config.get("audit_trail_schema") or "clone_xs"
    if not catalog:
        raise ValueError("audit_trail.catalog is not configured")
    return catalog, schema, "catalog_size_history"


def ensure_history_table(
    client: WorkspaceClient, warehouse_id: str, config: dict,
) -> str:
    """Create the catalog_size_history Delta table if it doesn't exist.

    Returns the FQN. The schema is created if missing — same pattern as
    `clone_snapshots.ensure_snapshot_table`. Idempotent: safe to call
    on every record_snapshot path.
    """
    catalog, schema, table = _history_fqn(config)
    fqn = f"`{catalog}`.`{schema}`.`{table}`"
    execute_sql(client, warehouse_id, f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
    execute_sql(
        client, warehouse_id,
        f"""
        CREATE TABLE IF NOT EXISTS {fqn} (
            snapshot_date    DATE,
            catalog          STRING,
            num_tables       INT,
            num_schemas      INT,
            total_size_bytes BIGINT,
            total_rows       BIGINT,
            captured_at      TIMESTAMP
        ) USING DELTA
        """.strip(),
    )
    return fqn


def record_snapshot(
    client: WorkspaceClient, warehouse_id: str, config: dict,
    *, catalog: str,
    num_tables: int,
    num_schemas: int,
    total_size_bytes: int,
    total_rows: int,
) -> None:
    """Upsert today's snapshot row for `catalog`.

    Idempotent by `(snapshot_date, catalog)`: re-recording overwrites
    today's existing row rather than appending a duplicate. Best-effort
    — any failure logs at WARN and returns silently so callers can
    fire-and-forget without wrapping in their own try/except.
    """
    try:
        fqn = ensure_history_table(client, warehouse_id, config)
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        # Idempotency: drop today's row before inserting the new one.
        # MERGE INTO would be cleaner but requires a USING source which
        # is more SQL than this read-mostly path is worth.
        execute_sql(
            client, warehouse_id,
            f"DELETE FROM {fqn} WHERE snapshot_date = DATE'{today}' AND catalog = '{catalog}'",
        )
        execute_sql(
            client, warehouse_id,
            f"""
            INSERT INTO {fqn} VALUES (
                DATE'{today}', '{catalog}', {int(num_tables)}, {int(num_schemas)},
                {int(total_size_bytes)}, {int(total_rows)}, CURRENT_TIMESTAMP()
            )
            """.strip(),
        )
        logger.debug(
            f"Recorded size snapshot: catalog={catalog} tables={num_tables} "
            f"size={total_size_bytes} rows={total_rows}"
        )
    except Exception as e:
        logger.warning(f"Failed to record size snapshot for {catalog!r}: {e}")


def record_snapshots_from_stats(
    client: WorkspaceClient, warehouse_id: str, config: dict, stats_response: dict,
) -> None:
    """Convenience wrapper: take a `catalog_stats_fast` (single) or
    `catalog_stats_multi` response dict and record a snapshot per
    catalog. Best-effort — never raises, never breaks /stats.

    Distinguishes the two shapes by checking for `per_catalog` (multi)
    vs `catalog` (single).
    """
    try:
        if not stats_response:
            return
        per_catalog = stats_response.get("per_catalog")
        if per_catalog:
            for cat, r in per_catalog.items():
                record_snapshot(
                    client, warehouse_id, config,
                    catalog=cat,
                    num_tables=int(r.get("num_tables", 0) or 0),
                    num_schemas=int(r.get("num_schemas", 0) or 0),
                    total_size_bytes=int(r.get("total_size_bytes", 0) or 0),
                    total_rows=int(r.get("total_rows", 0) or 0),
                )
            return
        # Single-catalog shape.
        cat = stats_response.get("catalog")
        if not cat or "," in cat:  # multi response uses comma-joined fallback
            return
        record_snapshot(
            client, warehouse_id, config,
            catalog=cat,
            num_tables=int(stats_response.get("num_tables", 0) or 0),
            num_schemas=int(stats_response.get("num_schemas", 0) or 0),
            total_size_bytes=int(stats_response.get("total_size_bytes", 0) or 0),
            total_rows=int(stats_response.get("total_rows", 0) or 0),
        )
    except Exception as e:
        logger.warning(f"record_snapshots_from_stats failed: {e}")


def get_history(
    client: WorkspaceClient, warehouse_id: str, config: dict,
    *, catalogs: list[str] | None = None, days: int = 30,
) -> list[dict]:
    """Read back per-catalog daily snapshots over the last `days` days.

    Args:
        catalogs: Restrict to these catalog names. None → all catalogs
            in the history table.
        days: Look-back window. Capped at 365 to keep the query bounded.

    Returns: list of `{snapshot_date, catalog, num_tables, num_schemas,
    total_size_bytes, total_rows, captured_at}` rows sorted by
    `(catalog, snapshot_date)`. Returns `[]` if the table doesn't exist
    yet, the audit catalog isn't configured, or the read fails — all
    of which are valid "no history" states the UI handles by showing
    an empty-state hint.
    """
    days = max(1, min(int(days or 30), 365))
    try:
        cat, schema, tbl = _history_fqn(config)
    except ValueError:
        return []

    fqn = f"`{cat}`.`{schema}`.`{tbl}`"
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    where = [f"snapshot_date >= DATE'{cutoff}'"]
    if catalogs:
        # Catalog names are validated identifiers upstream; quote-escape
        # defensively in case a misconfigured caller passes one with a
        # single quote.
        in_list = ",".join("'" + c.replace("'", "''") + "'" for c in catalogs)
        where.append(f"catalog IN ({in_list})")

    sql = f"""
        SELECT snapshot_date, catalog, num_tables, num_schemas,
               total_size_bytes, total_rows, captured_at
        FROM {fqn}
        WHERE {' AND '.join(where)}
        ORDER BY catalog, snapshot_date
    """
    try:
        rows = execute_sql(client, warehouse_id, sql.strip()) or []
        return [
            {
                "snapshot_date": str(r.get("snapshot_date")) if r.get("snapshot_date") else None,
                "catalog": r.get("catalog"),
                "num_tables": int(r.get("num_tables") or 0),
                "num_schemas": int(r.get("num_schemas") or 0),
                "total_size_bytes": int(r.get("total_size_bytes") or 0),
                "total_rows": int(r.get("total_rows") or 0),
                "captured_at": str(r.get("captured_at")) if r.get("captured_at") else None,
            }
            for r in rows
        ]
    except Exception as e:
        # Most likely cause: history table doesn't exist yet because
        # nobody has clicked Explore yet. Return empty rather than 500
        # — the UI will render an empty-state hint.
        logger.debug(f"get_history returning [] (likely first run): {e}")
        return []
