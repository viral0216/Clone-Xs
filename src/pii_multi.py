"""Multi-catalog PII scan — fan-out + merge across N catalogs.

Companion to `src.pii_detection.scan_catalog_for_pii`. Lets the Catalog
Explorer's Multi mode run a PII detection sweep over several catalogs
in parallel and surface a single merged report. Each detection is
stamped with its owning `catalog`; per-catalog scan summaries land in
`per_catalog`; per-catalog scan failures (auth, missing catalog,
warehouse error) are captured under `errors` instead of aborting.

Risk level rollup is the worst across catalogs (NONE < LOW < MEDIUM <
HIGH) so the UI can show a single "overall risk" badge without losing
the per-catalog detail.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from databricks.sdk import WorkspaceClient

from src.pii_detection import scan_catalog_for_pii

logger = logging.getLogger(__name__)

# PII scans are heavier than stats / search — each catalog kicks off
# information_schema scans plus optional sampling. Cap concurrency to 3
# so a 5-catalog Multi PII scan doesn't saturate a Small SQL Warehouse.
_DEFAULT_MAX_PARALLEL = 3

# Risk ordering — used to pick the worst-case rollup risk level.
_RISK_ORDER = {"NONE": 0, "LOW": 1, "MEDIUM": 2, "HIGH": 3}


def scan_catalogs_for_pii_multi(
    client: WorkspaceClient,
    warehouse_id: str,
    catalogs: list[str],
    exclude_schemas: list[str] | None = None,
    sample_data: bool = False,
    max_workers: int = 4,
    pii_config: dict | None = None,
    read_uc_tags: bool = False,
    save_history: bool = False,
    state_catalog: str = "clone_audit",
    schema_filter: list[str] | None = None,
    table_filter: str | None = None,
    max_parallel: int = _DEFAULT_MAX_PARALLEL,
) -> dict:
    """Run `scan_catalog_for_pii` in parallel across N catalogs and merge.

    Args mirror the single-catalog `scan_catalog_for_pii`, plus:
        catalogs: Non-empty list of catalog names to scan.
        max_parallel: Cap on concurrent per-catalog scans (default 3).

    Returns:
        {
          "scan_ids": [...one per catalog scanned],
          "catalogs": [...requested catalogs],
          "summary": {
            "total_columns_scanned": int,
            "pii_columns_found": int,
            "risk_level": str,                  # worst across catalogs
            "by_pii_type": {pii_type: count, ...},
          },
          "columns": [...detections, each stamped with `catalog`],
          "suggested_masking_config": {<catalog>.<schema>.<table>.<column>: {...}},
          "per_catalog": {catalog: {pii_columns_found, risk_level, ...}},
          "errors": [{catalog, error}, ...],
        }
    """
    if not catalogs:
        raise ValueError("scan_catalogs_for_pii_multi requires at least one catalog")

    parallelism = max(1, min(max_parallel, len(catalogs)))
    logger.info(
        f"Scanning {len(catalogs)} catalog(s) for PII "
        f"(parallelism={parallelism}, sample_data={sample_data})"
    )

    scan_ids: list[str] = []
    merged_columns: list[dict] = []
    masking_rules: dict[str, dict] = {}
    per_catalog: dict[str, dict] = {}
    errors: list[dict] = []
    by_type_total: dict[str, int] = {}
    total_columns_scanned = 0
    worst_risk = "NONE"

    def _scan_one(cat: str) -> dict:
        return scan_catalog_for_pii(
            client,
            warehouse_id,
            cat,
            exclude_schemas,
            sample_data=sample_data,
            max_workers=max_workers,
            pii_config=pii_config,
            read_uc_tags=read_uc_tags,
            save_history=save_history,
            state_catalog=state_catalog,
            schema_filter=schema_filter,
            table_filter=table_filter,
        )

    with ThreadPoolExecutor(max_workers=parallelism) as executor:
        futures = {executor.submit(_scan_one, cat): cat for cat in catalogs}
        for fut in as_completed(futures):
            cat = futures[fut]
            try:
                result = fut.result() or {}
                cat_summary = result.get("summary", {}) or {}
                cat_columns = result.get("columns", []) or []
                cat_masking = result.get("suggested_masking_config", {}) or {}
                cat_risk = cat_summary.get("risk_level", "NONE")

                if result.get("scan_id"):
                    scan_ids.append(result["scan_id"])

                for d in cat_columns:
                    merged_columns.append({**d, "catalog": cat})

                # Re-key masking rules with catalog prefix so we don't
                # collide on `<schema>.<table>.<column>` from two catalogs
                # that happen to share schema/table names.
                for key, rule in cat_masking.items():
                    masking_rules[f"{cat}.{key}"] = {**rule, "catalog": cat}

                per_catalog[cat] = {
                    "scan_id": result.get("scan_id"),
                    "total_columns_scanned": cat_summary.get("total_columns_scanned", 0),
                    "pii_columns_found": cat_summary.get("pii_columns_found", 0),
                    "risk_level": cat_risk,
                    "by_pii_type": cat_summary.get("by_pii_type", {}),
                }

                total_columns_scanned += cat_summary.get("total_columns_scanned", 0)
                for pii_type, count in (cat_summary.get("by_pii_type") or {}).items():
                    by_type_total[pii_type] = by_type_total.get(pii_type, 0) + count
                if _RISK_ORDER.get(cat_risk, 0) > _RISK_ORDER.get(worst_risk, 0):
                    worst_risk = cat_risk
            except Exception as e:
                logger.warning(f"PII scan failed for catalog {cat!r}: {e}")
                errors.append({"catalog": cat, "error": str(e)})
                per_catalog[cat] = {
                    "scan_id": None,
                    "total_columns_scanned": 0,
                    "pii_columns_found": 0,
                    "risk_level": "UNKNOWN",
                    "by_pii_type": {},
                }

    return {
        "scan_ids": scan_ids,
        "catalogs": list(catalogs),
        "summary": {
            "total_columns_scanned": total_columns_scanned,
            "pii_columns_found": len(merged_columns),
            "risk_level": worst_risk,
            "by_pii_type": by_type_total,
        },
        "columns": merged_columns,
        "suggested_masking_config": masking_rules,
        "per_catalog": per_catalog,
        "errors": errors,
    }
