"""User-defined function listing — single + multi catalog.

Single-catalog: lists UDFs from `<catalog>.information_schema.routines`.
Used by both the GET `/functions/{catalog}` route and the multi-catalog
fan-out below. Extracted from `api/routers/deps.py` so the multi path
can compose it without duplicating SQL.

Multi-catalog (`list_functions_multi`): fans the single-catalog query
out across N catalogs in parallel, stamps each result row with its
owning `catalog`, and reports per-catalog errors instead of aborting on
the first failure. Shape mirrors `src.stats_multi.catalog_stats_multi`
so the UI handles both endpoints with a shared pattern.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from databricks.sdk import WorkspaceClient

from src.client import execute_sql

logger = logging.getLogger(__name__)

_DEFAULT_MAX_PARALLEL = 5


def list_functions_for_catalog(
    client: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
) -> list[dict]:
    """List user-defined FUNCTION routines across all schemas in a catalog.

    Returns one dict per UDF with `name`, `schema`, `full_name`,
    `data_type`, and a 200-char preview of the routine definition.
    Returns `[]` on query failure (auth, missing catalog) — the caller
    decides whether that's worth surfacing as an error.
    """
    rows = execute_sql(
        client,
        warehouse_id,
        f"""
        SELECT routine_catalog, routine_schema, routine_name, routine_type,
               data_type, routine_definition
        FROM {catalog}.information_schema.routines
        WHERE routine_type = 'FUNCTION'
        AND routine_schema NOT IN ('information_schema', '__internal')
        ORDER BY routine_schema, routine_name
    """,
    )
    return [
        {
            "name": r.get("routine_name", ""),
            "schema": r.get("routine_schema", ""),
            "full_name": f"{catalog}.{r.get('routine_schema', '')}.{r.get('routine_name', '')}",
            "data_type": r.get("data_type", ""),
            "definition": (r.get("routine_definition", "") or "")[:200],
        }
        for r in rows
    ]


def list_functions_multi(
    client: WorkspaceClient,
    warehouse_id: str,
    catalogs: list[str],
    max_parallel: int = _DEFAULT_MAX_PARALLEL,
) -> dict:
    """Run `list_functions_for_catalog` in parallel across N catalogs and merge.

    Returns:
        {
          "functions": [...rows from each catalog, each stamped with `catalog`],
          "per_catalog": {catalog: count, ...},
          "errors": [{catalog, error}, ...],
          "catalogs": [...requested catalogs],
        }

    Failure isolation contract: one catalog erroring (auth, deleted
    mid-run, etc.) does not abort — the failure is captured in `errors`
    and the rest still surface. Mirrors `src.stats_multi`.
    """
    if not catalogs:
        raise ValueError("list_functions_multi requires at least one catalog")

    parallelism = max(1, min(max_parallel, len(catalogs)))
    logger.info(f"Listing functions across {len(catalogs)} catalog(s) (parallelism={parallelism})")

    merged: list[dict] = []
    per_catalog: dict[str, int] = {}
    errors: list[dict] = []

    with ThreadPoolExecutor(max_workers=parallelism) as executor:
        futures = {
            executor.submit(list_functions_for_catalog, client, warehouse_id, cat): cat
            for cat in catalogs
        }
        for fut in as_completed(futures):
            cat = futures[fut]
            try:
                rows = fut.result() or []
                for r in rows:
                    merged.append({**r, "catalog": cat})
                per_catalog[cat] = len(rows)
            except Exception as e:
                logger.warning(f"list_functions failed for catalog {cat!r}: {e}")
                errors.append({"catalog": cat, "error": str(e)})
                per_catalog[cat] = 0

    return {
        "functions": merged,
        "per_catalog": per_catalog,
        "errors": errors,
        "catalogs": list(catalogs),
    }
