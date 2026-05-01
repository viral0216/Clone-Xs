"""Multi-catalog statistics — fan-out + merge across N catalogs.

The Catalog Explorer page previously supported analysing **one** catalog
at a time. Cross-catalog audits ("how big is everything across
`prod_us`, `prod_eu`, `prod_apac`?") forced the user to pick each
catalog one by one and mentally aggregate.

This module fans out `catalog_stats_fast` (or `catalog_stats` when
caller wants the slow, exact path) across N catalogs in parallel and
merges the per-catalog responses into one shape that's a strict superset
of the single-catalog one. Wall-clock latency is the slowest catalog,
not the sum.

Shape additions on top of single-catalog:
- Each `tables[]` row is stamped with `catalog: str`
- Each `schema_summaries[]` row is stamped with `catalog: str`
- New top-level `per_catalog: dict[str, {num_tables, total_size_bytes,
  total_rows}]` — drives the per-catalog rollup card on the UI.
- New top-level `errors: list[{catalog, error}]` — one entry per catalog
  whose stats run failed; the rest of the catalogs still surface.
- `stats_mode` is `"fast_multi"` or `"detailed_multi"` so callers can
  branch on which path produced the result.

Failure isolation contract: one catalog inaccessible (auth, deleted
mid-run, etc.) does NOT abort the whole request. Callers see partial
data plus the per-catalog error.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from databricks.sdk import WorkspaceClient

from src.stats_fast import _format_bytes

logger = logging.getLogger(__name__)


# Cap parallelism to avoid hammering the warehouse with too many
# concurrent bulk queries. 5 simultaneous fast-stats calls is comfortable
# on a Small SQL Warehouse — equivalent to ~5 information_schema scans
# in flight, which UC handles cheaply.
_DEFAULT_MAX_PARALLEL = 5


def catalog_stats_multi(
    client: WorkspaceClient,
    warehouse_id: str,
    catalogs: list[str],
    exclude_schemas: list[str],
    fast: bool = True,
    max_parallel: int = _DEFAULT_MAX_PARALLEL,
) -> dict:
    """Run per-catalog stats in parallel and merge into a single response.

    Args:
        catalogs: Non-empty list of catalog names to analyse.
        fast: True → use `src.stats_fast.catalog_stats_fast` (1-3s per catalog).
              False → `src.stats.catalog_stats` (30-90s per catalog, exact).
        max_parallel: Cap on concurrent per-catalog runs. Defaults to 5.

    Returns:
        Merged response dict. `tables` / `schema_summaries` rows are
        stamped with their `catalog` field. `errors` carries per-catalog
        failures (empty when everything succeeded).
    """
    if not catalogs:
        raise ValueError("catalog_stats_multi requires at least one catalog")

    # Lazy-import so the slow path's top-level import cost only fires
    # when the caller actually wants it (the slow path imports things
    # like `progress.ProgressTracker` which we don't want for fast).
    if fast:
        from src.stats_fast import catalog_stats_fast as _per_catalog
    else:
        from src.stats import catalog_stats as _per_catalog

    parallelism = max(1, min(max_parallel, len(catalogs)))
    logger.info(
        f"Fanning out stats across {len(catalogs)} catalog(s) "
        f"(parallelism={parallelism}, fast={fast})"
    )

    per_catalog_results: dict[str, dict] = {}
    errors: list[dict] = []

    with ThreadPoolExecutor(max_workers=parallelism) as executor:
        futures = {
            executor.submit(
                _per_catalog, client, warehouse_id, cat, exclude_schemas,
            ): cat
            for cat in catalogs
        }
        for fut in as_completed(futures):
            cat = futures[fut]
            try:
                per_catalog_results[cat] = fut.result()
            except Exception as e:
                # One catalog failing (auth issue, doesn't exist, mid-deletion)
                # must not kill the whole request — mark it and carry on.
                logger.warning(f"Stats failed for catalog {cat!r}: {e}")
                errors.append({"catalog": cat, "error": str(e)})

    return _merge(per_catalog_results, errors, catalogs, fast)


def _merge(
    per_catalog: dict[str, dict],
    errors: list[dict],
    requested_catalogs: list[str],
    fast: bool,
) -> dict:
    """Merge per-catalog stats responses into one cross-catalog response.

    Tables and schema_summaries get stamped with their owning catalog
    (so the UI can render a `catalog` column / sort by it). Top-N tables
    are recomputed across the merged set so users see the largest tables
    cross-catalog, not per-catalog.
    """
    merged_tables: list[dict[str, Any]] = []
    merged_schemas: list[dict[str, Any]] = []
    rollup: dict[str, dict] = {}

    total_size = 0
    total_rows = 0
    total_tables = 0
    total_schemas = 0

    for cat, result in per_catalog.items():
        # Stamp catalog onto each table + schema row so the UI can group
        # / sort by catalog. We don't drop the original `schema` field —
        # the UI either displays catalog + schema as two columns or
        # combines them as `<catalog>.<schema>` when space is tight.
        for tbl in result.get("tables", []) or []:
            merged_tables.append({**tbl, "catalog": cat})
        for ss in result.get("schema_summaries", []) or []:
            merged_schemas.append({**ss, "catalog": cat})

        cat_tables = int(result.get("num_tables", 0) or 0)
        cat_size = int(result.get("total_size_bytes", 0) or 0)
        cat_rows = int(result.get("total_rows", 0) or 0)
        cat_schemas = int(result.get("num_schemas", 0) or 0)

        rollup[cat] = {
            "num_tables": cat_tables,
            "num_schemas": cat_schemas,
            "total_size_bytes": cat_size,
            "total_size_display": _format_bytes(cat_size),
            "total_rows": cat_rows,
        }

        total_tables += cat_tables
        total_size += cat_size
        total_rows += cat_rows
        total_schemas += cat_schemas

    # Recompute top-N across the merged tables (not just N from each catalog).
    # The UI's "Top tables by size" panel is more informative cross-catalog
    # if it's a true global top-10 vs. a top-10 per catalog mashed together.
    top_by_size = sorted(
        [t for t in merged_tables if t.get("size_bytes")],
        key=lambda t: t["size_bytes"],
        reverse=True,
    )[:10]
    top_by_rows = sorted(
        [t for t in merged_tables if t.get("row_count")],
        key=lambda t: t["row_count"],
        reverse=True,
    )[:10]

    # Sort schema_summaries by (catalog, schema) for stable rendering
    merged_schemas.sort(key=lambda s: (s.get("catalog", ""), s.get("schema", "")))

    return {
        "catalog": ",".join(sorted(requested_catalogs)),  # composite identifier
        "catalogs": list(requested_catalogs),
        "num_schemas": total_schemas,
        "num_tables": total_tables,
        "total_size_bytes": total_size,
        "total_size_display": _format_bytes(total_size),
        "total_rows": total_rows,
        "schema_summaries": merged_schemas,
        "tables": merged_tables,
        "top_tables_by_size": top_by_size,
        "top_tables_by_rows": top_by_rows,
        "per_catalog": rollup,
        "errors": errors,
        "stats_mode": "fast_multi" if fast else "detailed_multi",
    }
