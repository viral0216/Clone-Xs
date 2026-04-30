"""Multi-catalog stale & orphan detection — fan-out + merge across N catalogs.

Companion to `src.stale_detection.detect_stale_tables`. The Catalog
Explorer's Multi mode lets users scan several catalogs at once for
cleanup candidates; this module runs the per-catalog scanner in
parallel and merges findings into a single response, stamping each
finding with its owning `catalog` so the UI can render a Catalog
column.

Parallelism is capped at 3 (vs 5 for `stats_multi`): each per-catalog
run hits two system-table queries (information_schema for stats +
system.access.audit for usage), so a 5-way fan-out would put 10
queries in flight on a Small SQL Warehouse. 3-way is comfortable.

Failure isolation contract: one catalog inaccessible (auth on
system.access.audit, deleted catalog, etc.) does NOT abort the whole
request — the failure is captured under `errors` and other catalogs
still surface. Mirrors `src.stats_multi`.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any

from databricks.sdk import WorkspaceClient

from src.stale_detection import detect_stale_tables
from src.stats_fast import _format_bytes

logger = logging.getLogger(__name__)

_DEFAULT_MAX_PARALLEL = 3


def detect_stale_tables_multi(
    client: WorkspaceClient,
    warehouse_id: str,
    catalogs: list[str],
    days_threshold: int = 90,
    min_age_days: int = 7,
    min_size_bytes: int = 0,
    exclude_schemas: list[str] | None = None,
    max_parallel: int = _DEFAULT_MAX_PARALLEL,
    check_small_files: bool = False,
) -> dict[str, Any]:
    """Run `detect_stale_tables` in parallel across N catalogs and merge.

    Each merged finding carries `catalog` so the UI can render a
    Catalog column without a second lookup. Summary counts are summed
    across catalogs; `per_catalog` keeps the per-catalog breakdown.
    """
    if not catalogs:
        raise ValueError("detect_stale_tables_multi requires at least one catalog")

    parallelism = max(1, min(max_parallel, len(catalogs)))
    logger.info(
        f"Scanning {len(catalogs)} catalog(s) for stale tables "
        f"(parallelism={parallelism}, days_threshold={days_threshold})"
    )

    findings: list[dict[str, Any]] = []
    per_catalog: dict[str, dict] = {}
    errors: list[dict] = []
    total_tables_scanned = 0
    by_risk = {"HIGH": 0, "MEDIUM": 0, "LOW": 0}
    by_action: dict[str, int] = {}
    total_reclaimable_bytes = 0
    never_accessed_count = 0
    no_stats_count = 0

    def _scan_one(cat: str) -> dict:
        return detect_stale_tables(
            client, warehouse_id, cat,
            days_threshold=days_threshold,
            min_age_days=min_age_days,
            min_size_bytes=min_size_bytes,
            exclude_schemas=exclude_schemas,
            check_small_files=check_small_files,
        )

    with ThreadPoolExecutor(max_workers=parallelism) as executor:
        futures = {executor.submit(_scan_one, cat): cat for cat in catalogs}
        for fut in as_completed(futures):
            cat = futures[fut]
            try:
                result = fut.result() or {}
                cat_findings = result.get("findings", []) or []
                cat_summary = result.get("summary", {}) or {}

                # Stamp each finding with its owning catalog.
                for f in cat_findings:
                    findings.append({**f, "catalog": cat})

                per_catalog[cat] = {
                    "total_tables_scanned": result.get("total_tables_scanned", 0),
                    "findings_count": len(cat_findings),
                    "by_risk_level": cat_summary.get("by_risk_level", {}),
                    "total_reclaimable_bytes": cat_summary.get("total_reclaimable_bytes", 0),
                    "total_reclaimable_display": cat_summary.get("total_reclaimable_display"),
                    "never_accessed_count": cat_summary.get("never_accessed_count", 0),
                    "no_stats_count": cat_summary.get("no_stats_count", 0),
                }

                total_tables_scanned += int(result.get("total_tables_scanned", 0) or 0)
                for level, count in (cat_summary.get("by_risk_level") or {}).items():
                    by_risk[level] = by_risk.get(level, 0) + int(count or 0)
                for action, count in (cat_summary.get("by_suggested_action") or {}).items():
                    by_action[action] = by_action.get(action, 0) + int(count or 0)
                total_reclaimable_bytes += int(cat_summary.get("total_reclaimable_bytes", 0) or 0)
                never_accessed_count += int(cat_summary.get("never_accessed_count", 0) or 0)
                no_stats_count += int(cat_summary.get("no_stats_count", 0) or 0)
            except Exception as e:
                logger.warning(f"Stale scan failed for catalog {cat!r}: {e}")
                errors.append({"catalog": cat, "error": str(e)})
                per_catalog[cat] = {
                    "total_tables_scanned": 0, "findings_count": 0,
                    "by_risk_level": {}, "total_reclaimable_bytes": 0,
                    "never_accessed_count": 0, "no_stats_count": 0,
                }

    return {
        "catalogs": list(catalogs),
        "scanned_at": datetime.now(timezone.utc).isoformat(),
        "days_threshold": days_threshold,
        "min_age_days": min_age_days,
        "min_size_bytes": min_size_bytes,
        "total_tables_scanned": total_tables_scanned,
        "findings": findings,
        "summary": {
            "by_risk_level": by_risk,
            "by_suggested_action": by_action,
            "total_reclaimable_bytes": total_reclaimable_bytes,
            "total_reclaimable_display": _format_bytes(total_reclaimable_bytes),
            "never_accessed_count": never_accessed_count,
            "no_stats_count": no_stats_count,
        },
        "per_catalog": per_catalog,
        "errors": errors,
    }
