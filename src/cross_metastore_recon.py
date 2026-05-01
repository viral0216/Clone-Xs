"""Cross-metastore reconciliation.

For catalogs that have been migrated cross-workspace (typically via the Delta
Sharing → DEEP CLONE pipeline in :mod:`src.clone_cross_workspace`), verify
that every table on the destination matches the source. Row counts first
(cheap, reliable); optional SHA-256 checksums for columns-that-hashable to
catch silent data drift.

Separate from :mod:`src.validation` (same-workspace only) because it needs
a second :class:`WorkspaceClient` for the target and runs one COUNT(*) per
client — not a JOIN across a single metastore.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from databricks.sdk import WorkspaceClient

from src.client import execute_sql

logger = logging.getLogger(__name__)


def _list_tables(client: WorkspaceClient, catalog: str, schema: str) -> list[str]:
    names: list[str] = []
    try:
        for t in client.tables.list(catalog_name=catalog, schema_name=schema):
            kind = str(getattr(t, "table_type", "")).split(".")[-1]
            if kind in ("VIEW", "MATERIALIZED_VIEW"):
                continue
            if t.name:
                names.append(t.name)
    except Exception as e:
        logger.debug(f"tables.list failed for {catalog}.{schema}: {e}")
    return names


def _row_count(client: WorkspaceClient, warehouse_id: str, fqn: str) -> int | None:
    try:
        rows = execute_sql(client, warehouse_id, f"SELECT COUNT(*) AS c FROM {fqn}")
        return int(rows[0]["c"]) if rows else None
    except Exception as e:
        logger.debug(f"COUNT(*) failed on {fqn}: {e}")
        return None


def _checksum(client: WorkspaceClient, warehouse_id: str, fqn: str, columns: list[str]) -> str | None:
    """SHA-256 over a deterministic hash of concatenated column values.

    Relies on xxhash64 → SHA256 ordering-agnostic sum. Not cryptographic
    proof — it's a drift detector.
    """
    if not columns:
        return None
    cols_csv = ", ".join(f"cast(`{c}` as string)" for c in columns)
    sql = (
        f"SELECT sha2(cast(sum(xxhash64(concat_ws('|', {cols_csv}))) as string), 256) AS checksum "
        f"FROM {fqn}"
    )
    try:
        rows = execute_sql(client, warehouse_id, sql)
        return rows[0].get("checksum") if rows else None
    except Exception as e:
        logger.debug(f"Checksum failed on {fqn}: {e}")
        return None


def _get_hashable_columns(client: WorkspaceClient, catalog: str, schema: str, table: str) -> list[str]:
    """Columns suitable for hashing — exclude arrays, maps, structs (harder to cast to string uniformly)."""
    try:
        info = client.tables.get(full_name=f"{catalog}.{schema}.{table}")
        return [
            c.name for c in (info.columns or [])
            if c.name and str(getattr(c, "type_name", "")).upper() not in ("ARRAY", "MAP", "STRUCT")
        ]
    except Exception as e:
        logger.debug(f"tables.get failed for {catalog}.{schema}.{table}: {e}")
        return []


def reconcile_cross_metastore(
    source_client: WorkspaceClient,
    source_warehouse_id: str,
    source_catalog: str,
    target_client: WorkspaceClient,
    target_warehouse_id: str,
    dest_catalog: str,
    *,
    exclude_schemas: list[str] | None = None,
    use_checksum: bool = False,
    max_workers: int = 4,
) -> dict:
    """Run row-count (and optional checksum) reconciliation across two metastores.

    Returns:
        {
          "status": "match" | "partial" | "mismatch" | "failed",
          "table_count": N,
          "matched": M,
          "mismatched": N-M,
          "errors": K,
          "details": [
            {"schema": "...", "table": "...", "source_count": ..., "target_count": ...,
             "match": bool, "source_checksum": ..., "target_checksum": ..., "error": ...}
          ]
        }
    """
    excl = {s.lower() for s in (exclude_schemas or ["information_schema", "default"])}
    logger.info(f"Reconciling {source_catalog} ⇄ {dest_catalog} across metastores...")

    # Collect (schema, table) pairs from source — the definition of "what should match".
    pairs: list[tuple[str, str]] = []
    try:
        for s in source_client.schemas.list(catalog_name=source_catalog):
            if not s.name or s.name.lower() in excl:
                continue
            for name in _list_tables(source_client, source_catalog, s.name):
                pairs.append((s.name, name))
    except Exception as e:
        return {"status": "failed", "error": f"source enumeration failed: {e}"}

    def _compare_one(schema: str, table: str) -> dict:
        src_fqn = f"`{source_catalog}`.`{schema}`.`{table}`"
        dst_fqn = f"`{dest_catalog}`.`{schema}`.`{table}`"
        src_count = _row_count(source_client, source_warehouse_id, src_fqn)
        dst_count = _row_count(target_client, target_warehouse_id, dst_fqn)
        entry = {
            "schema": schema,
            "table": table,
            "source_count": src_count,
            "target_count": dst_count,
            "match": src_count is not None and src_count == dst_count,
        }
        if src_count is None and dst_count is None:
            entry["error"] = "both counts unavailable"
            entry["match"] = False

        if use_checksum and entry["match"]:
            cols = _get_hashable_columns(source_client, source_catalog, schema, table)
            if cols:
                entry["source_checksum"] = _checksum(source_client, source_warehouse_id, src_fqn, cols)
                entry["target_checksum"] = _checksum(target_client, target_warehouse_id, dst_fqn, cols)
                if entry["source_checksum"] != entry["target_checksum"]:
                    entry["match"] = False
                    entry["error"] = "checksum mismatch"
        return entry

    details: list[dict] = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(_compare_one, s, t): (s, t) for s, t in pairs}
        for f in as_completed(futures):
            details.append(f.result())

    matched = sum(1 for d in details if d.get("match"))
    errors = sum(1 for d in details if d.get("error"))
    total = len(details)
    if total == 0:
        status = "failed"
    elif matched == total:
        status = "match"
    elif matched == 0:
        status = "mismatch"
    else:
        status = "partial"

    logger.info(
        f"Reconciliation {status.upper()}: {matched}/{total} tables match, {errors} errors"
    )
    return {
        "status": status,
        "table_count": total,
        "matched": matched,
        "mismatched": total - matched,
        "errors": errors,
        "use_checksum": use_checksum,
        "details": details,
    }
