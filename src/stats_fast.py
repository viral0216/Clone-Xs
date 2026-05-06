"""Fast catalog statistics via bulk information_schema queries.

The default `src/stats.py:catalog_stats` runs three queries per table
(`SELECT COUNT(*)`, `DESCRIBE DETAIL`, SDK column lookup), parallelised
up to ~10 concurrent. For the Catalog Explorer's "show me everything in
this catalog" view that's 30-90 seconds on a 500-table catalog — most of
which is the round-trip overhead of issuing 1,500 distinct SQL statements
through the Statement Execution API.

This module ships a fast path that returns equivalent shape from **one**
bulk query against `<catalog>.information_schema` plus optionally a
second one against `information_schema.table_properties` for size + row
counts. Latency drops from minutes to seconds.

Trade-off: row counts and sizes come from `spark.sql.statistics.numRows`
and `spark.sql.statistics.totalSize` table properties, which are only
populated when `ANALYZE TABLE … COMPUTE STATISTICS` has been run. For
tables without ANALYZE stats, those fields are `None`. The Explorer UI
falls back to "—" in that case; users who need exact counts can switch
to the detailed path (which keeps the existing per-table COUNT(*) work).

The fast path is exposed via `/stats?fast=true` and is the default for
the Catalog Explorer page.
"""

from __future__ import annotations

import logging
from typing import Any

from databricks.sdk import WorkspaceClient

from src.client import execute_sql, list_schemas_sdk

logger = logging.getLogger(__name__)


def _format_bytes(size: int | None) -> str | None:
    """Same shape as the slow path's `_format_bytes` so the UI doesn't
    have to know which path produced the result."""
    if size is None:
        return None
    if size < 1024:
        return f"{size} B"
    if size < 1024**2:
        return f"{size / 1024:.1f} KB"
    if size < 1024**3:
        return f"{size / 1024**2:.1f} MB"
    if size < 1024**4:
        return f"{size / 1024**3:.2f} GB"
    return f"{size / 1024**4:.2f} TB"


def _bulk_tables_query(catalog: str, exclude_schemas: list[str]) -> str:
    """One SQL string that returns one row per table with shape:

      table_schema, table_name, table_type, comment,
      created, last_altered, num_columns, size_bytes, row_count

    `size_bytes` and `row_count` come from
    `spark.sql.statistics.totalSize` / `spark.sql.statistics.numRows`
    table properties (set by `ANALYZE TABLE … COMPUTE STATISTICS`).
    `null` when ANALYZE has not been run for the table — Explorer UI
    renders these as "—".
    """
    excl = ",".join(f"'{s}'" for s in exclude_schemas) or "''"
    return f"""
        WITH cols AS (
            SELECT table_schema, table_name, COUNT(*) AS num_columns
            FROM `{catalog}`.information_schema.columns
            WHERE table_catalog = '{catalog}'
            GROUP BY table_schema, table_name
        ),
        sizes AS (
            SELECT table_schema, table_name,
                CAST(MAX(CASE WHEN property_key = 'spark.sql.statistics.totalSize' THEN property_value END) AS BIGINT) AS size_bytes,
                CAST(MAX(CASE WHEN property_key = 'spark.sql.statistics.numRows'   THEN property_value END) AS BIGINT) AS row_count
            FROM `{catalog}`.information_schema.table_properties
            WHERE table_catalog = '{catalog}'
              AND property_key IN ('spark.sql.statistics.totalSize', 'spark.sql.statistics.numRows')
            GROUP BY table_schema, table_name
        )
        SELECT
            t.table_schema    AS table_schema,
            t.table_name      AS table_name,
            t.table_type      AS table_type,
            t.comment         AS comment,
            t.created         AS created,
            t.last_altered    AS last_altered,
            COALESCE(c.num_columns, 0) AS num_columns,
            s.size_bytes      AS size_bytes,
            s.row_count       AS row_count
        FROM `{catalog}`.information_schema.tables t
        LEFT JOIN cols  c ON c.table_schema = t.table_schema AND c.table_name = t.table_name
        LEFT JOIN sizes s ON s.table_schema = t.table_schema AND s.table_name = t.table_name
        WHERE t.table_catalog = '{catalog}'
          AND t.table_schema NOT IN ({excl})
        ORDER BY t.table_schema, t.table_name
    """


def catalog_stats_fast(
    client: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
    exclude_schemas: list[str],
) -> dict:
    """Fast catalog statistics — equivalent shape to `catalog_stats` but
    served by a single bulk query. Row count and size are `None` for
    tables without ANALYZE statistics; see module docstring.

    Falls back to a tables-only query if the join + table_properties read
    fails (e.g. `table_properties` view not exposed in some metastores).
    The fallback omits size + row count but keeps the table list and
    column counts intact.
    """
    logger.info(f"Gathering fast statistics for catalog: {catalog}")

    try:
        rows = execute_sql(client, warehouse_id, _bulk_tables_query(catalog, exclude_schemas))
    except Exception as e:
        logger.warning(
            f"Bulk information_schema query failed ({e}); "
            f"falling back to tables-only fast path (no size / row counts)."
        )
        rows = _fallback_tables_only(client, warehouse_id, catalog, exclude_schemas)

    return _build_summary(catalog, rows or [])


def _fallback_tables_only(
    client: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
    exclude_schemas: list[str],
) -> list[dict]:
    """When the joined bulk query fails (rare — usually means
    `information_schema.table_properties` isn't readable for this user),
    fall back to a plain tables-only query so the Explorer at least shows
    the table list. Columns / sizes / rows will all be None."""
    excl = ",".join(f"'{s}'" for s in exclude_schemas) or "''"
    sql = f"""
        SELECT table_schema, table_name, table_type, comment, created, last_altered,
               0 AS num_columns,
               CAST(NULL AS BIGINT) AS size_bytes,
               CAST(NULL AS BIGINT) AS row_count
        FROM `{catalog}`.information_schema.tables
        WHERE table_catalog = '{catalog}'
          AND table_schema NOT IN ({excl})
        ORDER BY table_schema, table_name
    """
    try:
        return execute_sql(client, warehouse_id, sql)
    except Exception as e:
        logger.error(f"Fallback tables-only query also failed: {e}")
        return []


def _build_summary(catalog: str, rows: list[dict]) -> dict:
    """Reshape the bulk query rows into the same response shape as the
    slow path so the UI can consume either interchangeably.

    Mirrors the structure produced by `src/stats.py:catalog_stats`:
    `tables[]`, `schema_summaries[]`, `top_tables_by_size[]`,
    `top_tables_by_rows[]`, plus aggregate totals.
    """
    tables: list[dict[str, Any]] = []
    by_schema: dict[str, list[dict]] = {}

    for row in rows:
        size = row.get("size_bytes")
        size_int = int(size) if size is not None else None
        rc = row.get("row_count")
        rc_int = int(rc) if rc is not None else None

        rec = {
            "schema": row.get("table_schema"),
            "table": row.get("table_name"),
            "table_type": row.get("table_type"),
            "row_count": rc_int,
            "size_bytes": size_int,
            "size_display": _format_bytes(size_int),
            "num_columns": int(row.get("num_columns") or 0),
            # Fast path can't supply num_files / last_modified / format
            # without a per-table DESCRIBE DETAIL — set to None and let
            # the UI render "—". The detailed path still has these.
            "num_files": None,
            "last_modified": str(row.get("last_altered")) if row.get("last_altered") else None,
            "format": None,
            "comment": row.get("comment"),
            "error": None,
        }
        tables.append(rec)
        by_schema.setdefault(rec["schema"], []).append(rec)

    schema_summaries = []
    for schema_name, schema_tables in sorted(by_schema.items()):
        schema_size = sum(int(t["size_bytes"] or 0) for t in schema_tables)
        schema_rows = sum(int(t["row_count"] or 0) for t in schema_tables)
        schema_summaries.append(
            {
                "schema": schema_name,
                "num_tables": len(schema_tables),
                "total_size_bytes": schema_size,
                "total_size_display": _format_bytes(schema_size),
                "total_rows": schema_rows,
            }
        )

    total_size = sum(int(t["size_bytes"] or 0) for t in tables)
    total_rows = sum(int(t["row_count"] or 0) for t in tables)

    return {
        "catalog": catalog,
        "num_schemas": len(schema_summaries),
        "num_tables": len(tables),
        "total_size_bytes": total_size,
        "total_size_display": _format_bytes(total_size),
        "total_rows": total_rows,
        "schema_summaries": schema_summaries,
        "tables": tables,
        "top_tables_by_size": sorted(
            [t for t in tables if t["size_bytes"]],
            key=lambda t: t["size_bytes"],
            reverse=True,
        )[:10],
        "top_tables_by_rows": sorted(
            [t for t in tables if t["row_count"]],
            key=lambda t: t["row_count"],
            reverse=True,
        )[:10],
        # Marker so the UI knows which path produced this and can show
        # the "row counts may be null without ANALYZE TABLE" caveat.
        "stats_mode": "fast",
    }


# Public re-exports for parity with the slow path's API surface.
def list_schemas(client: WorkspaceClient, catalog: str, exclude: list[str]) -> list[str]:
    """Convenience wrapper — exposes the SDK's `list_schemas_sdk` so
    callers don't have to import from src.client directly."""
    return list_schemas_sdk(client, catalog, exclude=exclude)
