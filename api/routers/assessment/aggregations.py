"""Score aggregation endpoints: per-category, WAF pillars, recommendations, and inventory summary."""

from __future__ import annotations

import json

from fastapi import APIRouter, Query

from ._storage import _STORE, _latest_result, _load_result, _grade, _severity_order
from ..waf_constants import CATEGORY_TO_PILLAR, WAF_PILLARS, WAF_PILLAR_NAMES, category_to_pillar

router = APIRouter()


@router.get("/categories")
async def get_categories(scan_id: str | None = Query(None)):
    """Return per-category scores for the latest (or specified) scan."""
    if scan_id:
        result = _load_result(scan_id)
    else:
        meta   = _latest_result()
        result = _load_result(meta["scan_id"]) if meta else None

    if not result:
        return []

    scores:   dict      = result.get("category_scores", {})
    findings: list[dict] = result.get("findings", [])

    cat_counts: dict[str, dict] = {}
    for f in findings:
        cat = f.get("category", "Unknown")
        if cat not in cat_counts:
            cat_counts[cat] = {"passed": 0, "failed": 0, "warnings": 0, "not_applicable": 0}
        st = f.get("status", "").upper()
        if st == "PASS":
            cat_counts[cat]["passed"] += 1
        elif st == "FAIL":
            cat_counts[cat]["failed"] += 1
        elif st == "WARN":
            cat_counts[cat]["warnings"] += 1
        else:
            cat_counts[cat]["not_applicable"] += 1

    return [
        {
            "category": cat,
            "score":    scores.get(cat, 0),
            "grade":    _grade(scores.get(cat, 0)),
            **cat_counts.get(cat, {"passed": 0, "failed": 0, "warnings": 0, "not_applicable": 0}),
        }
        for cat in sorted(scores.keys())
    ]


@router.get("/waf-pillars")
async def get_waf_pillars(scan_id: str | None = Query(None)):
    """Return scores aggregated by 7 Databricks Well-Architected Framework pillars.

    Derives pillar scores from the existing per-category scores in result.json
    using a weighted average, so no scanner changes are required.
    """
    if scan_id:
        result = _load_result(scan_id)
    else:
        meta   = _latest_result()
        result = _load_result(meta["scan_id"]) if meta else None

    if not result:
        return []

    category_scores: dict      = result.get("category_scores", {})
    findings:        list[dict] = result.get("findings", [])

    pillar_counts: dict[str, dict] = {
        p: {"passed": 0, "failed": 0, "warnings": 0, "not_applicable": 0, "check_count": 0}
        for p in WAF_PILLAR_NAMES
    }
    pillar_cat_scores: dict[str, list[float]] = {p: [] for p in WAF_PILLAR_NAMES}

    for f in findings:
        cat    = f.get("category", "")
        pillar = category_to_pillar(cat)
        st     = f.get("status", "").upper()
        pillar_counts[pillar]["check_count"] += 1
        if st == "PASS":
            pillar_counts[pillar]["passed"] += 1
        elif st == "FAIL":
            pillar_counts[pillar]["failed"] += 1
        elif st == "WARN":
            pillar_counts[pillar]["warnings"] += 1
        else:
            pillar_counts[pillar]["not_applicable"] += 1

    for cat, score in category_scores.items():
        pillar = category_to_pillar(cat)
        pillar_cat_scores[pillar].append(score)

    output = []
    for pillar_name, icon, description in WAF_PILLARS:
        scores_for_pillar = pillar_cat_scores[pillar_name]
        pillar_score      = round(sum(scores_for_pillar) / len(scores_for_pillar), 1) if scores_for_pillar else 0
        counts            = pillar_counts[pillar_name]
        output.append({
            "pillar":      pillar_name,
            "icon":        icon,
            "description": description,
            "score":       pillar_score,
            "grade":       _grade(pillar_score),
            **counts,
        })
    return output


@router.get("/recommendations")
async def get_recommendations(scan_id: str | None = Query(None)):
    """Return prioritised recommendations (FAIL/WARN findings, deduplicated by title)."""
    if scan_id:
        result = _load_result(scan_id)
    else:
        meta   = _latest_result()
        result = _load_result(meta["scan_id"]) if meta else None

    if not result:
        return []

    findings: list[dict] = result.get("findings", [])

    seen: dict[str, dict] = {}
    for f in findings:
        if f.get("status") not in ("FAIL", "WARN"):
            continue
        key = f.get("title", f.get("check_id", ""))
        if key not in seen:
            seen[key] = {
                "title":          f.get("title", ""),
                "category":       f.get("category", ""),
                "severity":       f.get("severity", ""),
                "status":         f.get("status", ""),
                "recommendation": f.get("recommendation", ""),
                "effort":         f.get("effort", ""),
                "benefits":       f.get("benefits", ""),
                "reference_url":  f.get("reference_url", ""),
                "count":          1,
                "priority":       _severity_order(f.get("severity", "")),
            }
        else:
            seen[key]["count"] += 1

    recs = sorted(seen.values(), key=lambda r: (r["priority"], r["status"] != "FAIL"))
    for i, r in enumerate(recs, 1):
        r["rank"] = i
    return recs


@router.get("/inventory")
async def get_inventory(scan_id: str | None = Query(None)):
    """Return UC inventory summary JSON."""
    sid = scan_id
    if not sid:
        meta = _latest_result()
        sid  = meta["scan_id"] if meta else None
    if not sid:
        return None

    p = _STORE / sid / "inventory.json"
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text())
    except Exception:
        return None
