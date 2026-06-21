"""HTML dashboard viewer and multi-format export endpoints."""

from __future__ import annotations

import re
from pathlib import Path

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse, HTMLResponse, Response

from ._storage import _STORE, _html_path, _latest_result, _load_result

router = APIRouter()


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
        (r'href="sat-[^"]*-\d{4}-\d{2}-\d{2}\.html"', 'href="/api/assessment/html/report"'),
    ]
    for pattern, replacement in replacements:
        html = re.sub(pattern, replacement, html)
    return html


@router.get("/html/{view}", response_class=HTMLResponse)
async def serve_html(
    view: str,
    scan_id: str | None = Query(None),
):
    """Serve a generated HTML dashboard view.

    view options: tree | sunburst | hubspoke | overview | topology | report
    """
    sid = scan_id
    if not sid:
        meta = _latest_result()
        sid  = meta["scan_id"] if meta else None

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
                "<strong>Include UC Inventory</strong> enabled.</p>"
            )
        return HTMLResponse(
            f"<html><body style='font-family:sans-serif;padding:2rem'>{msg}</body></html>",
            status_code=200,
        )

    html_content = html_file.read_text(encoding="utf-8")
    html_content = _rewrite_nav_links(html_content)
    return HTMLResponse(html_content)


@router.get("/export/{fmt}")
async def export_result(
    fmt: str,
    scan_id: str | None = Query(None),
):
    """Download a scan result: json | csv | excel | html."""
    sid = scan_id
    if not sid:
        meta = _latest_result()
        sid  = meta["scan_id"] if meta else None

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
            raise HTTPException(
                status_code=404,
                detail="HTML report not generated — run a full or security-only scan first",
            )
        return FileResponse(str(p), media_type="text/html", filename=f"assessment_{sid}.html")

    result_data = _load_result(sid)
    if not result_data:
        raise HTTPException(status_code=404, detail="Result data not found")

    try:
        from sat_scanner.models   import SATScanResult, SATFinding
        from sat_scanner.exporters import export_csv, export_excel
        import tempfile

        _slots  = set(SATFinding.__slots__)
        findings = [
            SATFinding(**{k: v for k, v in f.items() if k in _slots})
            for f in result_data.get("findings", [])
        ]
        result_obj = SATScanResult(
            workspace_url   = result_data.get("workspace_url",   ""),
            workspace_name  = result_data.get("workspace_name",  ""),
            scanned_at      = result_data.get("scanned_at",      ""),
            overall_score   = result_data.get("overall_score",   0),
            total_checks    = result_data.get("total_checks",    0),
            passed          = result_data.get("passed",          0),
            failed          = result_data.get("failed",          0),
            warnings        = result_data.get("warnings",        0),
            not_applicable  = result_data.get("not_applicable",  0),
            findings        = findings,
            category_scores = result_data.get("category_scores", {}),
        )

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            if fmt == "csv":
                out     = export_csv(result_obj, tmp_path)
                content = Path(out).read_bytes()
                return Response(
                    content=content,
                    media_type="text/csv",
                    headers={"Content-Disposition": f'attachment; filename="assessment_{sid}.csv"'},
                )
            if fmt == "excel":
                out     = export_excel(result_obj, tmp_path)
                content = Path(out).read_bytes()
                return Response(
                    content=content,
                    media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    headers={"Content-Disposition": f'attachment; filename="assessment_{sid}.xlsx"'},
                )
            raise HTTPException(status_code=400, detail=f"Unknown format: {fmt}")
    except ImportError:
        raise HTTPException(status_code=503, detail="sat_scanner not available for export")
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Export failed: {exc}")
