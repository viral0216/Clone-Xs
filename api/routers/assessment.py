"""Assessment portal endpoints — wraps the sat_scanner package.

Runs Databricks security/compliance scans and UC inventory in the background,
persists results as JSON under ~/.clone-xs/assessment/, and exposes them via
REST endpoints for the /assessment/* frontend portal.
"""

from __future__ import annotations

import asyncio
import json
import re
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import APIRouter, BackgroundTasks, Header, HTTPException, Query
from fastapi.responses import HTMLResponse, FileResponse, Response

# ---------------------------------------------------------------------------
# Ensure sat_scanner is importable
# ---------------------------------------------------------------------------
_SAT_PATH = Path.home() / ".clone-xs" / "sat_scanner_path.txt"
_DEFAULT_SAT = Path("/Users/viralkumarjpatel/source/databricks-assesment-tool/assessment")

def _find_sat_path() -> Path | None:
    """Locate the sat_scanner package directory."""
    if _DEFAULT_SAT.exists():
        return _DEFAULT_SAT
    if _SAT_PATH.exists():
        p = Path(_SAT_PATH.read_text().strip())
        if p.exists():
            return p
    return None

_sat_root = _find_sat_path()
if _sat_root and str(_sat_root) not in sys.path:
    sys.path.insert(0, str(_sat_root))

# ---------------------------------------------------------------------------
# Storage paths
# ---------------------------------------------------------------------------
_STORE = Path.home() / ".clone-xs" / "assessment"
_STORE.mkdir(parents=True, exist_ok=True)

# In-memory job tracker: job_id → {"status", "progress", "error", "result_id"}
_JOBS: dict[str, dict] = {}

router = APIRouter()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _scan_dir(scan_id: str) -> Path:
    d = _STORE / scan_id
    d.mkdir(parents=True, exist_ok=True)
    return d


def _list_results() -> list[dict]:
    """Return scan metadata sorted newest-first."""
    items = []
    for p in _STORE.iterdir():
        meta = p / "meta.json"
        if p.is_dir() and meta.exists():
            try:
                items.append(json.loads(meta.read_text()))
            except Exception:
                pass
    return sorted(items, key=lambda x: x.get("scanned_at", ""), reverse=True)


def _latest_result() -> dict | None:
    results = _list_results()
    return results[0] if results else None


def _load_result(scan_id: str) -> dict | None:
    p = _STORE / scan_id / "result.json"
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text())
    except Exception:
        return None


def _html_path(scan_id: str, view: str) -> Path | None:
    """Return the HTML file path for a given view name.

    The exporter generates dynamic filenames like:
      sat-uc-inventory[-{workspace}]-{date}-tree.html
      sat-uc-inventory[-{workspace}]-{date}-star.html   (sunburst)
      sat-uc-inventory[-{workspace}]-{date}-hubspoke.html
      sat[-{workspace}]-{date}.html                     (security report)
    We glob by suffix to find them regardless of workspace name or date.
    """
    scan_dir = _STORE / scan_id
    if not scan_dir.exists():
        return None

    # Inventory views: search for files ending with the known suffix
    suffix_map = {
        "tree":     "-tree.html",
        "sunburst": "-star.html",     # exporter uses "star" not "sunburst"
        "hubspoke": "-hubspoke.html",
        "overview": "-overview.html",
        "topology": "-topology.html",
    }
    if view in suffix_map:
        suffix = suffix_map[view]
        matches = sorted(scan_dir.glob(f"*{suffix}"))
        return matches[0] if matches else None

    if view == "report":
        # Security report: sat[-workspace]-date.html — does NOT end with an
        # inventory suffix.  Prefer an explicitly renamed report.html first.
        explicit = scan_dir / "report.html"
        if explicit.exists():
            return explicit
        inventory_suffixes = set(suffix_map.values())
        candidates = [
            p for p in sorted(scan_dir.glob("sat*.html"))
            if not any(p.name.endswith(s) for s in inventory_suffixes)
        ]
        return candidates[0] if candidates else None

    return None


def _grade(score: float | int) -> str:
    if score >= 90:
        return "A"
    if score >= 75:
        return "B"
    if score >= 60:
        return "C"
    if score >= 45:
        return "D"
    return "F"


def _severity_order(s: str) -> int:
    return {"critical": 0, "high": 1, "medium": 2, "low": 3}.get(s.lower(), 4)


# ---------------------------------------------------------------------------
# Background scan runner
# ---------------------------------------------------------------------------

async def _run_inventory_only(
    job_id: str,
    scan_id: str,
    host: str,
    token: str,
    workspace_name: str,
) -> None:
    """Run UC inventory scan only — no security checks."""
    try:
        _JOBS[job_id]["status"] = "running"
        _JOBS[job_id]["progress"] = "Importing sat_scanner…"

        try:
            from sat_scanner.inventory import run_inventory
            from sat_scanner.exporters import export_inventory_hierarchy_html
        except ImportError as e:
            _JOBS[job_id]["status"] = "error"
            _JOBS[job_id]["error"] = f"sat_scanner package not found: {e}."
            return

        _JOBS[job_id]["progress"] = "Scanning Unity Catalog objects…"
        inv = await run_inventory(
            host=host.rstrip("/"),
            token=token,
            workspace_name=workspace_name,
            quiet=True,
            grants="coarse",
        )

        out_dir = _scan_dir(scan_id)
        inv_dict = inv.to_dict()

        _JOBS[job_id]["progress"] = "Generating visualisations…"
        (out_dir / "inventory.json").write_text(json.dumps(inv_dict, default=str, indent=2))
        export_inventory_hierarchy_html(inv, out_dir)

        now = datetime.now(timezone.utc).isoformat()
        meta = {
            "scan_id": scan_id,
            "workspace_url": host,
            "workspace_name": workspace_name,
            "scanned_at": inv_dict.get("scanned_at", now),
            "overall_score": None,
            "grade": None,
            "total_checks": 0,
            "passed": 0,
            "failed": 0,
            "warnings": 0,
            "not_applicable": 0,
            "scan_type": "inventory",
            "with_inventory": True,
            "catalog_count": inv_dict.get("catalog_count", 0),
            "schema_count": inv_dict.get("schema_count", 0),
            "table_count": inv_dict.get("table_count", 0),
        }
        (out_dir / "meta.json").write_text(json.dumps(meta, indent=2))

        _JOBS[job_id]["status"] = "completed"
        _JOBS[job_id]["progress"] = "Done"
        _JOBS[job_id]["result_id"] = scan_id

    except Exception as exc:
        _JOBS[job_id]["status"] = "error"
        _JOBS[job_id]["error"] = str(exc)


async def _run_scan_task(
    job_id: str,
    scan_id: str,
    host: str,
    token: str,
    workspace_name: str,
    scan_type: str,  # "full" | "security" | "inventory"
) -> None:
    """Run sat_scanner scan (and optionally UC inventory) then persist results."""
    if scan_type == "inventory":
        await _run_inventory_only(job_id, scan_id, host, token, workspace_name)
        return

    with_inventory = scan_type == "full"

    try:
        _JOBS[job_id]["status"] = "running"
        _JOBS[job_id]["progress"] = "Importing sat_scanner…"

        try:
            from sat_scanner.scanner import run_scan
            from sat_scanner.exporters import (
                export_json, export_html, export_inventory_hierarchy_html,
            )
            from sat_scanner.models import SATScanResult
        except ImportError as e:
            _JOBS[job_id]["status"] = "error"
            _JOBS[job_id]["error"] = (
                f"sat_scanner package not found: {e}. "
                f"Expected at {_DEFAULT_SAT}"
            )
            return

        _JOBS[job_id]["progress"] = "Running security scan (345 checks)…"
        result: SATScanResult = await run_scan(
            host=host.rstrip("/"),
            token=token,
            workspace_name=workspace_name,
            quiet=True,
        )

        out_dir = _scan_dir(scan_id)

        _JOBS[job_id]["progress"] = "Saving results…"
        result_dict = result.to_dict()
        (out_dir / "result.json").write_text(json.dumps(result_dict, default=str, indent=2))

        # Generate HTML report — exporter writes sat[-workspace]-{date}.html;
        # _html_path("report") finds it via glob so no rename needed.
        try:
            export_html(result, out_dir)
        except Exception:
            pass

        # UC Inventory
        if with_inventory:
            _JOBS[job_id]["progress"] = "Running UC inventory scan…"
            try:
                from sat_scanner.inventory import run_inventory
                inv = await run_inventory(
                    host=host.rstrip("/"),
                    token=token,
                    workspace_name=workspace_name,
                    quiet=True,
                    grants="coarse",
                )
                (out_dir / "inventory.json").write_text(
                    json.dumps(inv.to_dict(), default=str, indent=2)
                )
                export_inventory_hierarchy_html(inv, out_dir)
            except Exception as inv_err:
                _JOBS[job_id]["inventory_error"] = str(inv_err)

        overall = result_dict.get("overall_score", 0)
        meta = {
            "scan_id": scan_id,
            "workspace_url": host,
            "workspace_name": workspace_name,
            "scanned_at": result_dict.get("scanned_at", datetime.now(timezone.utc).isoformat()),
            "overall_score": overall,
            "grade": _grade(overall),
            "total_checks": result_dict.get("total_checks", 0),
            "passed": result_dict.get("passed", 0),
            "failed": result_dict.get("failed", 0),
            "warnings": result_dict.get("warnings", 0),
            "not_applicable": result_dict.get("not_applicable", 0),
            "scan_type": scan_type,
            "with_inventory": with_inventory,
        }
        (out_dir / "meta.json").write_text(json.dumps(meta, indent=2))

        _JOBS[job_id]["status"] = "completed"
        _JOBS[job_id]["progress"] = "Done"
        _JOBS[job_id]["result_id"] = scan_id

    except Exception as exc:
        _JOBS[job_id]["status"] = "error"
        _JOBS[job_id]["error"] = str(exc)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post("/run")
async def run_assessment(
    background_tasks: BackgroundTasks,
    x_databricks_host: str | None = Header(None),
    x_databricks_token: str | None = Header(None),
    workspace_name: str = Query(""),
    scan_type: str = Query("full"),  # "full" | "security" | "inventory"
):
    """Trigger an async assessment scan. Returns job_id to poll.

    scan_type:
      - full       — 345 security checks + UC inventory (default)
      - security   — 345 security checks only
      - inventory  — UC inventory only (no security checks)
    """
    host = x_databricks_host or ""
    token = x_databricks_token or ""
    if not host or not token:
        raise HTTPException(status_code=401, detail="Databricks host and token required")
    if scan_type not in ("full", "security", "inventory"):
        raise HTTPException(status_code=400, detail="scan_type must be full, security, or inventory")

    job_id = str(uuid.uuid4())
    scan_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S") + "_" + job_id[:8]
    _JOBS[job_id] = {
        "job_id": job_id,
        "scan_id": scan_id,
        "scan_type": scan_type,
        "status": "queued",
        "progress": "Queued…",
        "submitted_at": datetime.now(timezone.utc).isoformat(),
        "result_id": None,
        "error": None,
    }
    background_tasks.add_task(
        _run_scan_task, job_id, scan_id, host, token, workspace_name, scan_type
    )
    return {"job_id": job_id, "scan_id": scan_id, "scan_type": scan_type, "status": "queued"}


@router.get("/status/{job_id}")
async def get_job_status(job_id: str):
    """Poll scan job status."""
    job = _JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


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
    result = _load_result(scan_id)
    if not result:
        return meta

    # Attach category scores and top findings to the meta
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
        meta = _latest_result()
        result = _load_result(meta["scan_id"]) if meta else None

    if not result:
        return []

    findings: list[dict] = result.get("findings", [])

    if severity:
        sevs = {s.strip().lower() for s in severity.split(",")}
        findings = [f for f in findings if f.get("severity", "").lower() in sevs]
    if category:
        cats = {c.strip().lower() for c in category.split(",")}
        findings = [f for f in findings if f.get("category", "").lower() in cats]
    if status:
        statuses = {s.strip().upper() for s in status.split(",")}
        findings = [f for f in findings if f.get("status", "").upper() in statuses]

    # Sort by severity then status
    findings.sort(key=lambda f: (
        _severity_order(f.get("severity", "")),
        0 if f.get("status") == "FAIL" else 1,
    ))
    return findings


@router.get("/categories")
async def get_categories(scan_id: str | None = Query(None)):
    """Return per-category scores for the latest (or specified) scan."""
    if scan_id:
        result = _load_result(scan_id)
    else:
        meta = _latest_result()
        result = _load_result(meta["scan_id"]) if meta else None

    if not result:
        return []

    scores: dict = result.get("category_scores", {})
    findings: list[dict] = result.get("findings", [])

    # Build per-category count breakdown
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
            "score": scores.get(cat, 0),
            "grade": _grade(scores.get(cat, 0)),
            **cat_counts.get(cat, {"passed": 0, "failed": 0, "warnings": 0, "not_applicable": 0}),
        }
        for cat in sorted(scores.keys())
    ]


@router.get("/recommendations")
async def get_recommendations(scan_id: str | None = Query(None)):
    """Return prioritised recommendations (FAIL findings, deduplicated by title)."""
    if scan_id:
        result = _load_result(scan_id)
    else:
        meta = _latest_result()
        result = _load_result(meta["scan_id"]) if meta else None

    if not result:
        return []

    findings: list[dict] = result.get("findings", [])

    # Group FAIL/WARN findings by title and deduplicate
    seen: dict[str, dict] = {}
    for f in findings:
        if f.get("status") not in ("FAIL", "WARN"):
            continue
        key = f.get("title", f.get("check_id", ""))
        if key not in seen:
            seen[key] = {
                "title": f.get("title", ""),
                "category": f.get("category", ""),
                "severity": f.get("severity", ""),
                "status": f.get("status", ""),
                "recommendation": f.get("recommendation", ""),
                "effort": f.get("effort", ""),
                "benefits": f.get("benefits", ""),
                "reference_url": f.get("reference_url", ""),
                "count": 1,
                "priority": _severity_order(f.get("severity", "")),
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
        sid = meta["scan_id"] if meta else None
    if not sid:
        return None

    p = _STORE / sid / "inventory.json"
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text())
    except Exception:
        return None


@router.get("/html/{view}", response_class=HTMLResponse)
async def serve_html(
    view: str,
    scan_id: str | None = Query(None),
):
    """Serve a generated HTML dashboard view.

    view options: tree | sunburst | hubspoke | overview | report
    """
    sid = scan_id
    if not sid:
        meta = _latest_result()
        sid = meta["scan_id"] if meta else None

    if not sid:
        return HTMLResponse(
            "<html><body style='font-family:sans-serif;padding:2rem'>"
            "<h2>No assessment results yet</h2>"
            "<p>Run an assessment from the <a href='/assessment/run'>Run Scan</a> page first.</p>"
            "</body></html>",
            status_code=200,
        )

    html_file = _html_path(sid, view)
    if not html_file or not html_file.exists():
        if view == "report":
            msg = (
                "<h2 style='color:#b45309'>No Security Report Available</h2>"
                "<p>This scan was an <strong>inventory-only</strong> scan — no security checks were run.</p>"
                "<p>To generate a security report, go back to "
                "<a href='/assessment/run'>Run Scan</a> and choose "
                "<strong>Full Assessment</strong> or <strong>Security Checks Only</strong>.</p>"
            )
        else:
            msg = (
                f"<h2>{view.title()} view not available</h2>"
                "<p>This view requires a UC inventory scan. Re-run with "
                "<strong>Include UC Inventory</strong> enabled, or check that "
                "the sat_scanner package is installed.</p>"
            )
        return HTMLResponse(
            f"<html><body style='font-family:sans-serif;padding:2rem'>{msg}</body></html>",
            status_code=200,
        )

    html_content = html_file.read_text(encoding="utf-8")
    html_content = _rewrite_nav_links(html_content)
    return HTMLResponse(html_content)


def _rewrite_nav_links(html: str) -> str:
    """Replace relative cross-view file hrefs with portal API endpoint URLs.

    The sat_scanner exporter writes links like:
      href="sat-uc-inventory-2026-06-21-star.html"
    which break when served via /api/assessment/html/{view}.
    Rewrite them to the canonical API paths so in-iframe nav works.
    """
    replacements = [
        (r'href="[^"]*-tree\.html"',     'href="/api/assessment/html/tree"'),
        (r'href="[^"]*-star\.html"',     'href="/api/assessment/html/sunburst"'),
        (r'href="[^"]*-hubspoke\.html"', 'href="/api/assessment/html/hubspoke"'),
        (r'href="[^"]*-overview\.html"', 'href="/api/assessment/html/overview"'),
        (r'href="[^"]*-topology\.html"', 'href="/api/assessment/html/topology"'),
        # "Report" link: sat-uc-inventory-YYYY-MM-DD.html (date-only suffix, no view name)
        (r'href="sat-[^"]*-\d{4}-\d{2}-\d{2}\.html"', 'href="/api/assessment/html/report"'),
    ]
    for pattern, replacement in replacements:
        html = re.sub(pattern, replacement, html)
    return html


@router.get("/export/{fmt}")
async def export_result(
    fmt: str,
    scan_id: str | None = Query(None),
):
    """Download a scan result in the requested format: json | csv | excel | html."""
    sid = scan_id
    if not sid:
        meta = _latest_result()
        sid = meta["scan_id"] if meta else None

    if not sid:
        raise HTTPException(status_code=404, detail="No assessment results available")

    out_dir = _STORE / sid

    if fmt == "json":
        p = out_dir / "result.json"
        if not p.exists():
            raise HTTPException(status_code=404, detail="Result not found")
        return FileResponse(str(p), media_type="application/json", filename=f"assessment_{sid}.json")

    if fmt == "html":
        p = _html_path(sid, "report")
        if not p or not p.exists():
            raise HTTPException(status_code=404, detail="HTML report not generated — run a full or security-only scan first")
        return FileResponse(str(p), media_type="text/html", filename=f"assessment_{sid}.html")

    # For CSV and Excel we need to regenerate from result
    result_data = _load_result(sid)
    if not result_data:
        raise HTTPException(status_code=404, detail="Result data not found")

    try:
        from sat_scanner.models import SATScanResult, SATFinding
        from sat_scanner.exporters import export_csv, export_excel
        import tempfile

        # SATFinding uses __slots__ (not a dataclass), so filter keys against __slots__
        _slots = set(SATFinding.__slots__)
        findings = [
            SATFinding(**{k: v for k, v in f.items() if k in _slots})
            for f in result_data.get("findings", [])
        ]
        result_obj = SATScanResult(
            workspace_url=result_data.get("workspace_url", ""),
            workspace_name=result_data.get("workspace_name", ""),
            scanned_at=result_data.get("scanned_at", ""),
            overall_score=result_data.get("overall_score", 0),
            total_checks=result_data.get("total_checks", 0),
            passed=result_data.get("passed", 0),
            failed=result_data.get("failed", 0),
            warnings=result_data.get("warnings", 0),
            not_applicable=result_data.get("not_applicable", 0),
            findings=findings,
            category_scores=result_data.get("category_scores", {}),
        )

        # Read file into bytes inside the temp dir before it's cleaned up
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            if fmt == "csv":
                out = export_csv(result_obj, tmp_path)
                content = Path(out).read_bytes()
                return Response(
                    content=content,
                    media_type="text/csv",
                    headers={"Content-Disposition": f'attachment; filename="assessment_{sid}.csv"'},
                )
            elif fmt == "excel":
                out = export_excel(result_obj, tmp_path)
                content = Path(out).read_bytes()
                return Response(
                    content=content,
                    media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    headers={"Content-Disposition": f'attachment; filename="assessment_{sid}.xlsx"'},
                )
            else:
                raise HTTPException(status_code=400, detail=f"Unknown format: {fmt}")
    except ImportError:
        raise HTTPException(status_code=503, detail="sat_scanner not available for export")
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Export failed: {exc}")
