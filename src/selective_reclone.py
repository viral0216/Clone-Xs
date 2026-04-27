"""Selective re-clone orchestrator.

Re-clones only the tables that have drifted between source and target,
leaving in-sync tables untouched. Runtime is proportional to drift size,
not catalog size — useful when:

- A large catalog has been initially cloned and you want to keep it fresh
  without re-transferring TB of static data on every run.
- You know specific tables changed (e.g. an upstream batch job rewrote
  one fact table) and want to re-clone exactly those without manually
  picking them.

Drift detection compares Delta versions on source vs target directly via
DESCRIBE HISTORY (cf. `incremental_sync.find_drifted_tables`). This means:

- Tables present on source but absent from target are treated as drifted
  and cloned in (`reason: never_cloned`).
- Tables on both sides where source.version > target.version are cloned
  with `force_reclone=True` so the target gets the up-to-date copy
  (`reason: version_drift`).
- Tables Clone-Xs can't read a version from on either side
  (Parquet / Iceberg sources, transient SDK errors) are also treated as
  drifted (`reason: unable_to_compare`) — conservative: cheaper than
  silently missing real drift.

Tables on target but not on source are NOT touched — selective re-clone is
additive only. Use a separate compare/cleanup if you need to drop orphans.

Returns the same result shape as `clone_catalog` so it's a drop-in for
the FastAPI router and downstream report generators.
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from databricks.sdk import WorkspaceClient

from src.client import list_tables_sdk
from src.clone_tables import _clone_single_table
from src.incremental_sync import find_drifted_tables
from src.log_formatter import OK, SCHEMA, SKIP, bold, header

logger = logging.getLogger(__name__)


def selective_reclone_catalog(client: WorkspaceClient, config: dict) -> dict:
    """Selective re-clone orchestrator: clones only drifted tables.

    Mirrors `clone_catalog` config shape and result shape — drop-in for
    the JobManager dispatch. Differences from full clone:

    - `load_type` is overridden to FULL internally (selective itself defines
      the scope; falling through to incremental's "skip-if-exists" inside
      `_clone_single_table` would defeat the point).
    - `force_reclone` is set per-table (the whole point: replace target's
      stale copy with source's current state).
    - Schemas with zero drift are reported with `tables.skipped: 0` and
      `tables.success: 0` and a one-line "in sync" log entry.
    """
    from src.clone_catalog import (
        create_catalog_if_not_exists,
        create_schema_if_not_exists,
        get_schemas,
        _build_summary,
        _print_summary,
    )

    start = time.time()
    source = config["source_catalog"]
    dest = config["destination_catalog"]
    warehouse_id = config["sql_warehouse_id"]
    dry_run = config.get("dry_run", False)
    exclude_schemas = config.get("exclude_schemas") or ["information_schema", "default"]
    include_schemas = config.get("include_schemas") or []

    logger.info(header(f"SELECTIVE RE-CLONE: {source} → {dest}"))

    # Ensure target catalog/schemas exist so the per-table CLONE has somewhere
    # to land. No-ops when they already do.
    create_catalog_if_not_exists(
        client, warehouse_id, dest,
        dry_run=dry_run, location=config.get("location", "") or "",
    )
    schemas = get_schemas(client, warehouse_id, source, exclude_schemas, include_schemas)

    all_results: list[dict] = []
    total_drifted = 0

    for schema in schemas:
        schema_start = time.time()
        create_schema_if_not_exists(client, warehouse_id, dest, schema, dry_run=dry_run)

        drifted = find_drifted_tables(client, warehouse_id, source, dest, schema)
        total_drifted += len(drifted)
        if not drifted:
            logger.info(f"{SKIP} {SCHEMA} Schema {bold(schema)} in sync — 0 drifted tables")
            all_results.append({
                "schema": schema,
                "tables": {
                    "success": 0, "failed": 0, "skipped": 0,
                    "bytes_copied": 0, "files_copied": 0,
                    "source_table_size": 0, "source_num_of_files": 0,
                    "formats": {},
                },
                "views": {"success": 0, "failed": 0, "skipped": 0},
                "functions": {"success": 0, "failed": 0, "skipped": 0},
                "volumes": {"success": 0, "failed": 0, "skipped": 0},
                "duration_seconds": round(time.time() - schema_start, 2),
            })
            continue

        logger.info(
            f"{SCHEMA} Schema {bold(schema)}: {len(drifted)} drifted "
            f"{_drift_breakdown(drifted)}"
        )

        tables_result = _reclone_drifted_in_schema(
            client, warehouse_id, source, dest, schema, drifted, config,
        )
        # `_build_summary` expects per-schema results with object-type keys
        # nested (`tables: {...}`), matching the shape `process_schema` emits.
        # The no-drift branch above uses the same shape.
        all_results.append({
            "schema": schema,
            "tables": tables_result,
            "views": {"success": 0, "failed": 0, "skipped": 0},
            "functions": {"success": 0, "failed": 0, "skipped": 0},
            "volumes": {"success": 0, "failed": 0, "skipped": 0},
            "duration_seconds": round(time.time() - schema_start, 2),
        })

    summary = _build_summary(all_results)
    summary["duration_seconds"] = round(time.time() - start, 2)
    summary["total_drifted_tables"] = total_drifted
    summary["mode"] = "selective"
    summary["timestamp"] = datetime.now().isoformat()
    summary["source_catalog"] = source
    summary["destination_catalog"] = dest

    _print_summary(summary, source, dest, dry_run=dry_run)
    if total_drifted == 0:
        logger.info(f"{OK} Source and destination are fully in sync — no tables cloned.")

    return summary


def _drift_breakdown(drifted: list[dict]) -> str:
    """One-line description of why each table is in the drift list."""
    counts: dict[str, int] = {}
    for d in drifted:
        counts[d["reason"]] = counts.get(d["reason"], 0) + 1
    parts = [f"{n} {reason}" for reason, n in sorted(counts.items())]
    return f"({', '.join(parts)})"


def _reclone_drifted_in_schema(
    client: WorkspaceClient,
    warehouse_id: str,
    source_catalog: str,
    dest_catalog: str,
    schema: str,
    drifted: list[dict],
    config: dict,
) -> dict:
    """Re-clone the drifted tables in a single schema.

    Reuses `_clone_single_table` so all per-table fixes (metrics capture,
    TBLPROPERTIES override, mask handling, ownership/tags/permissions
    replay) apply unchanged. Aggregates results in the same shape that
    `clone_tables_in_schema` returns.
    """
    src_format_by_name = {
        r["table_name"]: (r.get("data_source_format") or "DELTA").upper()
        for r in list_tables_sdk(client, source_catalog, schema)
    }

    results = {
        "success": 0, "failed": 0, "skipped": 0,
        "bytes_copied": 0, "files_copied": 0,
        "source_table_size": 0, "source_num_of_files": 0,
        "formats": {},
    }

    def _add(metrics: dict | None, tname: str, success: bool) -> None:
        if success:
            results["success"] += 1
            fmt = src_format_by_name.get(tname, "DELTA")
            results["formats"][fmt] = results["formats"].get(fmt, 0) + 1
        else:
            results["failed"] += 1
        if metrics:
            results["bytes_copied"] += metrics.get("copied_files_size", 0)
            results["files_copied"] += metrics.get("num_copied_files", 0)
            results["source_table_size"] += metrics.get("source_table_size", 0)
            results["source_num_of_files"] += metrics.get("source_num_of_files", 0)

    parallel = max(1, int(config.get("parallel_tables", 1) or 1))
    args_for = lambda tname: (  # noqa: E731 — readability over a one-shot helper
        client, warehouse_id, source_catalog, dest_catalog, schema, tname,
        config.get("clone_type", "DEEP"), config.get("dry_run", False),
        config.get("copy_permissions", False), config.get("copy_ownership", False),
        config.get("copy_tags", False), config.get("copy_properties", False),
        config.get("copy_security", False), config.get("copy_constraints", False),
        config.get("copy_comments", False),
        None,  # rollback_log handled at orchestrator level if needed
        config.get("as_of_timestamp"), config.get("as_of_version"),
        None,  # where_clause not applicable for selective
        True,  # force_reclone — that's the whole point of selective
        False, # schema_only off
        config.get("clone_tbl_properties"),
    )

    drifted_names = [d["table_name"] for d in drifted]

    if parallel > 1 and len(drifted_names) > 1:
        with ThreadPoolExecutor(max_workers=parallel) as executor:
            futures = {
                executor.submit(_clone_single_table, *args_for(t)): t
                for t in drifted_names
            }
            for f in as_completed(futures):
                tname, success, metrics = f.result()
                _add(metrics, tname, success)
    else:
        for tname in drifted_names:
            _, success, metrics = _clone_single_table(*args_for(tname))
            _add(metrics, tname, success)

    return results
