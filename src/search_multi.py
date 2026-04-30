"""Multi-catalog table/column search — fan-out + merge across N catalogs.

Companion to `src.search.search_tables` (single-catalog). The Catalog
Explorer's Multi mode lets users search a regex pattern across several
catalogs at once; this module runs `search_tables` per catalog in
parallel and merges the matches into a single response, stamping each
match with its owning `catalog` so the UI can render a Catalog column.

Failure isolation: a per-catalog search failure is captured under
`errors` and other catalogs still surface — same contract as
`src.stats_multi`.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from databricks.sdk import WorkspaceClient

from src.search import search_tables

logger = logging.getLogger(__name__)

_DEFAULT_MAX_PARALLEL = 5


def search_tables_multi(
    client: WorkspaceClient,
    warehouse_id: str,
    catalogs: list[str],
    pattern: str,
    exclude_schemas: list[str],
    search_columns: bool = False,
    max_parallel: int = _DEFAULT_MAX_PARALLEL,
) -> dict:
    """Run `search_tables` in parallel across N catalogs and merge.

    Returns a dict shaped:
        {
          "pattern": str,
          "catalogs": [...requested catalogs],
          "matched_tables": [...rows, each with `catalog` stamped],
          "matched_columns": [...rows, each with `catalog` stamped],
          "per_catalog": {catalog: {tables: int, columns: int}, ...},
          "errors": [{catalog, error}, ...],
        }
    """
    if not catalogs:
        raise ValueError("search_tables_multi requires at least one catalog")

    parallelism = max(1, min(max_parallel, len(catalogs)))
    logger.info(
        f"Searching {len(catalogs)} catalog(s) for pattern {pattern!r} "
        f"(parallelism={parallelism}, search_columns={search_columns})"
    )

    matched_tables: list[dict] = []
    matched_columns: list[dict] = []
    per_catalog: dict[str, dict] = {}
    errors: list[dict] = []

    with ThreadPoolExecutor(max_workers=parallelism) as executor:
        futures = {
            executor.submit(
                search_tables, client, warehouse_id, cat, pattern,
                exclude_schemas, None, search_columns,
            ): cat
            for cat in catalogs
        }
        for fut in as_completed(futures):
            cat = futures[fut]
            try:
                result = fut.result() or {}
                cat_tables = result.get("matched_tables", []) or []
                cat_columns = result.get("matched_columns", []) or []
                for t in cat_tables:
                    matched_tables.append({**t, "catalog": cat})
                for c in cat_columns:
                    matched_columns.append({**c, "catalog": cat})
                per_catalog[cat] = {"tables": len(cat_tables), "columns": len(cat_columns)}
            except Exception as e:
                logger.warning(f"Search failed for catalog {cat!r}: {e}")
                errors.append({"catalog": cat, "error": str(e)})
                per_catalog[cat] = {"tables": 0, "columns": 0}

    return {
        "pattern": pattern,
        "catalogs": list(catalogs),
        "matched_tables": matched_tables,
        "matched_columns": matched_columns,
        "per_catalog": per_catalog,
        "errors": errors,
    }
