"""Results retrieval and findings filter endpoints."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from ._storage import _list_results, _latest_result, _load_result, _severity_order
from ..waf_constants import CATEGORY_TO_PILLAR, WAF_PILLAR_NAMES

router = APIRouter()


@router.get("/results")
async def list_results():
    """List all past scan results (metadata only), newest first."""
    return _list_results()


@router.get("/results/{scan_id}")
async def get_result(scan_id: str):
    """Get full result JSON for a specific scan."""
    data = _load_result(scan_id)
    if not data:
        raise HTTPException(status_code=404, detail="Scan result not found")
    return data


@router.get("/latest")
async def get_latest():
    """Return metadata + findings summary for the most recent scan."""
    meta = _latest_result()
    if not meta:
        return None

    scan_id = meta.get("scan_id", "")
    result  = _load_result(scan_id)
    if not result:
        return meta

    meta["category_scores"] = result.get("category_scores", {})
    meta["findings_preview"] = [
        f for f in result.get("findings", [])
        if f.get("status") == "FAIL" and f.get("severity", "").lower() == "critical"
    ][:5]
    return meta


@router.get("/findings")
async def get_findings(
    scan_id: str | None = Query(None),
    severity: str | None = Query(None),
    category: str | None = Query(None),
    status: str | None = Query(None),
):
    """Return filtered findings list. Uses latest scan when scan_id is omitted."""
    if scan_id:
        result = _load_result(scan_id)
    else:
        meta   = _latest_result()
        result = _load_result(meta["scan_id"]) if meta else None

    if not result:
        return []

    findings: list[dict] = result.get("findings", [])

    if severity:
        sevs     = {s.strip().lower() for s in severity.split(",")}
        findings = [f for f in findings if f.get("severity", "").lower() in sevs]

    if category:
        requested = {c.strip() for c in category.split(",")}
        expanded_cats: set[str] = set()
        for req in requested:
            req_lower      = req.lower()
            matched_pillar = next((p for p in WAF_PILLAR_NAMES if p.lower() == req_lower), None)
            if matched_pillar:
                for scanner_cat, pillar in CATEGORY_TO_PILLAR.items():
                    if pillar == matched_pillar:
                        expanded_cats.add(scanner_cat.lower())
            else:
                expanded_cats.add(req_lower)
        findings = [f for f in findings if f.get("category", "").lower() in expanded_cats]

    if status:
        statuses = {s.strip().upper() for s in status.split(",")}
        findings = [f for f in findings if f.get("status", "").upper() in statuses]

    findings.sort(key=lambda f: (
        _severity_order(f.get("severity", "")),
        0 if f.get("status") == "FAIL" else 1,
    ))
    return findings
