"""Detailed catalog diff — presence/absence + column drift + size delta.

The existing `src.diff.compare_catalogs` returns "what's in source vs
dest" at the object level (schemas / tables / views / functions /
volumes), and `src.compare.compare_catalogs_deep` runs per-table column
comparisons. Both are useful, but neither answers the question
"compared to source, which dest tables drifted in size, and which
columns were added/removed?" without the user running two endpoints
and joining the results in their head.

This module produces a single response that combines:
  1. The full presence/absence diff from `compare_catalogs` (unchanged).
  2. A per-common-table `drift` block carrying:
     - source_size_bytes / dest_size_bytes / size_delta_bytes (signed)
     - source_row_count / dest_row_count / row_delta (signed)
     - columns_only_in_source / columns_only_in_dest (lists of column names)
     - column_type_changes: [{column, source_type, dest_type}]
  3. A summary rollup: total drifted tables, columns drifted, total
     size delta — drives the headline cards on the new /catalog-diff
     UI page.

Implementation cost: one bulk `information_schema` query per side that
joins `tables` + `columns` + `table_properties`, parallelised. So the
detailed diff for a 500-table catalog takes ~3-5s instead of the
30+s the per-table /compare path takes.

Failure isolation: if either side's bulk query fails, we return the
presence/absence diff with `drift: []` and a single error in
`drift_errors`. The presence/absence diff stays useful even if the
deep query is unavailable (e.g. one side lacks table_properties).
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from databricks.sdk import WorkspaceClient

from src.client import execute_sql
from src.diff import compare_catalogs

logger = logging.getLogger(__name__)


def _bulk_metadata_query(catalog: str, exclude_schemas: list[str]) -> str:
    """One SQL string that returns one row per (schema, table, column)
    with its data_type plus the table's size_bytes / row_count.

    Mirrors the pattern in `src.stats_fast._bulk_tables_query` but at
    column granularity (one row per column instead of per table) so
    the diff can detect both column-level drift and table-level size
    delta from a single query per catalog.
    """
    excl = ",".join(f"'{s}'" for s in (exclude_schemas or ["information_schema", "default"]))
    return f"""
        WITH sizes AS (
            SELECT table_schema, table_name,
                CAST(MAX(CASE WHEN property_key = 'spark.sql.statistics.totalSize' THEN property_value END) AS BIGINT) AS size_bytes,
                CAST(MAX(CASE WHEN property_key = 'spark.sql.statistics.numRows'   THEN property_value END) AS BIGINT) AS row_count
            FROM {catalog}.information_schema.table_properties
            WHERE property_key IN ('spark.sql.statistics.totalSize', 'spark.sql.statistics.numRows')
            GROUP BY table_schema, table_name
        )
        SELECT c.table_schema AS table_schema,
               c.table_name   AS table_name,
               c.column_name  AS column_name,
               c.full_data_type AS data_type,
               s.size_bytes   AS size_bytes,
               s.row_count    AS row_count
        FROM {catalog}.information_schema.columns c
        LEFT JOIN sizes s
          ON s.table_schema = c.table_schema AND s.table_name = c.table_name
        WHERE c.table_schema NOT IN ({excl})
        ORDER BY c.table_schema, c.table_name, c.ordinal_position
    """


def _index_by_table(rows: list[dict]) -> dict[str, dict]:
    """Pivot column-grain rows into a `{schema.table: {columns, size_bytes, row_count}}`
    map, where `columns` is a `{column_name: data_type}` dict preserving
    declaration order via Python's insertion-ordered dicts."""
    out: dict[str, dict] = {}
    for r in rows:
        schema = r.get("table_schema")
        table = r.get("table_name")
        col = r.get("column_name")
        dtype = (r.get("data_type") or "").strip()
        if not schema or not table or not col:
            continue
        key = f"{schema}.{table}"
        if key not in out:
            out[key] = {
                "schema": schema,
                "table": table,
                "columns": {},  # column_name → data_type
                "size_bytes": r.get("size_bytes"),
                "row_count": r.get("row_count"),
            }
        out[key]["columns"][col] = dtype
    return out


def _classify_drift(src: dict, dst: dict) -> dict[str, Any]:
    """Compare one common table's source-side + dest-side metadata,
    returning a drift record. The returned dict is omitted from the
    final list when there's no drift at all (no column changes AND
    size delta == 0) — so callers can treat the drift list as "tables
    that actually changed"."""
    src_cols = src.get("columns") or {}
    dst_cols = dst.get("columns") or {}

    only_src = [c for c in src_cols if c not in dst_cols]
    only_dst = [c for c in dst_cols if c not in src_cols]

    # Type changes: same column name on both sides, but the
    # `full_data_type` differs (e.g. STRING → VARCHAR(50), or INT → BIGINT).
    type_changes = []
    for col in src_cols:
        if col in dst_cols and src_cols[col] != dst_cols[col]:
            type_changes.append(
                {
                    "column": col,
                    "source_type": src_cols[col],
                    "dest_type": dst_cols[col],
                }
            )

    src_size = int(src.get("size_bytes") or 0)
    dst_size = int(dst.get("size_bytes") or 0)
    src_rows = int(src.get("row_count") or 0)
    dst_rows = int(dst.get("row_count") or 0)

    return {
        "schema": src.get("schema") or dst.get("schema"),
        "table": src.get("table") or dst.get("table"),
        "source_size_bytes": src_size,
        "dest_size_bytes": dst_size,
        "size_delta_bytes": dst_size - src_size,
        "source_row_count": src_rows,
        "dest_row_count": dst_rows,
        "row_delta": dst_rows - src_rows,
        "columns_only_in_source": sorted(only_src),
        "columns_only_in_dest": sorted(only_dst),
        "column_type_changes": type_changes,
    }


def _has_drift(record: dict) -> bool:
    """True when at least one signal indicates the tables differ.
    Tables with identical schemas AND no size/row delta are dropped
    from the response so the UI's drifted-tables list is signal-only."""
    return bool(
        record["columns_only_in_source"]
        or record["columns_only_in_dest"]
        or record["column_type_changes"]
        or record["size_delta_bytes"] != 0
        or record["row_delta"] != 0
    )


def compare_catalogs_detailed(
    client: WorkspaceClient,
    warehouse_id: str,
    source_catalog: str,
    dest_catalog: str,
    exclude_schemas: list[str] | None = None,
) -> dict[str, Any]:
    """Detailed cross-catalog diff: presence/absence + per-table drift.

    Wraps the existing `compare_catalogs` (presence/absence at the
    object level) and overlays a per-common-table drift block computed
    from one bulk information_schema query per side.

    Returns a dict shaped:
        {
            "schemas":   {only_in_source, only_in_dest, in_both, source_count, dest_count},
            "tables":    {...same shape...},
            "views":     {...},
            "functions": {...},
            "volumes":   {...},
            "drift":     [{schema, table, ...drift fields...}, ...drifted only],
            "summary": {
                "tables_drifted":      int,
                "columns_added":       int,  # in dest but not source
                "columns_removed":     int,  # in source but not dest
                "type_changes":        int,
                "total_size_delta_bytes": int,  # signed
            },
            "drift_errors": [],   # populated if either bulk query failed
        }
    """
    if exclude_schemas is None:
        exclude_schemas = ["information_schema", "default"]

    logger.info(f"Detailed diff: {source_catalog} vs {dest_catalog}")

    # Step 1: presence/absence diff (existing helper).
    presence = compare_catalogs(
        client,
        warehouse_id,
        source_catalog,
        dest_catalog,
        exclude_schemas,
    )

    # Step 2: bulk metadata query on each side, in parallel.
    drift_errors: list[dict] = []
    src_meta: dict[str, dict] = {}
    dst_meta: dict[str, dict] = {}

    def _fetch(catalog: str) -> dict[str, dict]:
        return _index_by_table(
            execute_sql(
                client,
                warehouse_id,
                _bulk_metadata_query(catalog, exclude_schemas),
            )
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        f_src = executor.submit(_fetch, source_catalog)
        f_dst = executor.submit(_fetch, dest_catalog)
        try:
            src_meta = f_src.result()
        except Exception as e:
            logger.warning(f"Bulk metadata query failed for source {source_catalog!r}: {e}")
            drift_errors.append({"side": "source", "catalog": source_catalog, "error": str(e)})
        try:
            dst_meta = f_dst.result()
        except Exception as e:
            logger.warning(f"Bulk metadata query failed for dest {dest_catalog!r}: {e}")
            drift_errors.append({"side": "dest", "catalog": dest_catalog, "error": str(e)})

    # Step 3: classify drift for tables present on both sides. If
    # either side's bulk query failed we skip classification entirely
    # — comparing an empty side against a populated one would produce
    # phantom "all columns added/removed" entries that the user can't
    # act on. The presence/absence diff above is still useful.
    drift: list[dict] = []
    if not drift_errors:
        in_both = set(presence.get("tables", {}).get("in_both") or [])
        for key in sorted(in_both):
            # `in_both` is keyed `<schema>.<table>` already (see src.diff).
            src = src_meta.get(
                key, {"schema": key.split(".")[0], "table": key.split(".", 1)[1], "columns": {}}
            )
            dst = dst_meta.get(
                key, {"schema": key.split(".")[0], "table": key.split(".", 1)[1], "columns": {}}
            )
            record = _classify_drift(src, dst)
            if _has_drift(record):
                drift.append(record)

    # Step 4: summary rollup for the headline cards.
    summary = {
        "tables_drifted": len(drift),
        "columns_added": sum(len(d["columns_only_in_dest"]) for d in drift),
        "columns_removed": sum(len(d["columns_only_in_source"]) for d in drift),
        "type_changes": sum(len(d["column_type_changes"]) for d in drift),
        "total_size_delta_bytes": sum(d["size_delta_bytes"] for d in drift),
    }

    return {
        **presence,
        "source_catalog": source_catalog,
        "destination_catalog": dest_catalog,
        "drift": drift,
        "summary": summary,
        "drift_errors": drift_errors,
    }
