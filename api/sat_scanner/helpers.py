"""SAT Scanner — utility and rendering functions."""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from typing import Any

from .models import SATFinding, SATScanResult
from .checks import (
    PORTAL_LINKS, _WORKSPACE_ACCOUNT_IDS, _WORKSPACE_ARM_INFO,
    EXCEL_CELL_LIMIT,
)

# ── Module-level logger ──
logger = logging.getLogger("sat_scanner")


def setup_logging(quiet: bool = False) -> None:
    """Configure SAT Scanner logging. Call once from CLI entry point."""
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("  %(asctime)s %(message)s", datefmt="%H:%M:%S"))
    logger.addHandler(handler)
    logger.setLevel(logging.WARNING if quiet else logging.INFO)


def _log(msg: str) -> None:
    """Log an info message via the sat_scanner logger."""
    logger.info(msg)


def _pl(n: int, singular: str, plural: str | None = None) -> str:
    """Return 'N thing' or 'N things' based on count."""
    p = plural or (singular + "s")
    return f"{n} {singular}" if n == 1 else f"{n} {p}"


def _sanitize_name(name: str) -> str:
    return re.sub(r'_+', '_', re.sub(r'[^a-zA-Z0-9_-]', '_', name)).strip('_')[:60]


def _file_prefix(result: SATScanResult) -> str:
    prefix = "sat"
    if result.workspace_name:
        prefix += f"-{_sanitize_name(result.workspace_name)}"
    prefix += f"-{datetime.now().strftime('%Y-%m-%d')}"
    return prefix


def _extract_org_id(host: str) -> str:
    """Extract the workspace org ID from the host URL.

    E.g. https://adb-1134642475632994.14.azuredatabricks.net → 1134642475632994
    """
    m = re.search(r"adb-(\d+)", host)
    return m.group(1) if m else ""


def _resolve_portal_link(check_id: str, host: str) -> str:
    """Resolve the portal link for a given check_id.

    Returns workspace UI links (prepended with host) for workspace-level
    settings, or full URLs for Azure Portal / Account Console items.
    URLs containing {account_id} are resolved using the cached account ID.
    Settings paths use the new /settings/... format with ?o={org_id}.
    """
    host = host.rstrip("/")
    path = ""
    # Exact match first
    if check_id in PORTAL_LINKS:
        path = PORTAL_LINKS[check_id]
    else:
        # Prefix match — try longest prefix first
        best = ""
        for key, p in PORTAL_LINKS.items():
            if check_id.startswith(key) and len(key) > len(best):
                best = key
        if best:
            path = PORTAL_LINKS[best]
    if not path:
        return ""
    # Full URLs (Azure Portal, Account Console)
    if path.startswith("https://"):
        # Substitute {account_id} if present
        if "{account_id}" in path:
            acct_id = _WORKSPACE_ACCOUNT_IDS.get(host, "")
            if not acct_id:
                return ""
            path = path.replace("{account_id}", acct_id)
        # Substitute {resource_id} and {tenant} for Azure Portal deep links
        if "{resource_id}" in path or "{tenant}" in path:
            arm = _WORKSPACE_ARM_INFO.get(host, {})
            if not arm:
                return ""
            path = path.replace("{resource_id}", arm.get("resource_id", ""))
            path = path.replace("{tenant}", arm.get("tenant", ""))
        return path
    # Workspace UI paths — append ?o={org_id}
    org_id = _extract_org_id(host)
    suffix = f"?o={org_id}" if org_id else ""
    return f"{host}/{path}{suffix}"


def _auto_extract_evidence(f: SATFinding) -> dict | None:
    """Auto-extract evidence from current_state string and api_response."""
    cs = f.current_state
    resp = f.details.get("api_response")

    # Pattern 1: "X/Y items have Z" or "X/Y items use Z"
    m = re.match(r"(\d+)/(\d+)\s+(.+?)(?:\s+have|\s+use|\s+with)\s+(.+)", cs)
    if m:
        return {"field": m.group(4).strip(), "value": f"{m.group(1)}/{m.group(2)}", "source": "current_state"}

    # Pattern 2: "Feature: enabled/disabled/not enabled/not configured"
    m = re.match(r"(.+?):\s+(enabled|disabled|not enabled|not configured|configured|workspace-level enabled|not confirmed)", cs)
    if m:
        return {"field": m.group(1).strip(), "value": m.group(2), "source": "current_state"}

    # Pattern 3: "N item(s) found/configured/detected"
    m = re.match(r"(\d+)\s+(.+?)\s*(?:found|configured|detected|defined)", cs)
    if m:
        return {"field": m.group(2).strip(), "value": int(m.group(1)), "source": "current_state"}

    # Pattern 4: small workspace-conf dict (≤5 keys) — the whole response IS the evidence
    if resp and isinstance(resp, dict) and len(resp) <= 5 and all(isinstance(v, (str, bool, int, float, type(None))) for v in resp.values()):
        return {"field": ", ".join(resp.keys()), "value": resp, "source": "api_response"}

    # Fallback: use current_state as the evidence value
    return {"field": "current_state", "value": cs, "source": "current_state"}


def _details_str(details: dict, excel_safe: bool = False) -> str:
    """Serialize a finding's details dict for display in CSV/Excel/HTML exports.

    When *excel_safe* is True the string is capped at Excel's 32 767-char
    cell limit to prevent openpyxl from raising an error.
    """
    if not details:
        return ""
    s = json.dumps(details, default=str, ensure_ascii=False)
    if excel_safe and len(s) > EXCEL_CELL_LIMIT:
        return s[: EXCEL_CELL_LIMIT - 30] + "... [truncated for Excel]"
    return s


def _format_scan_items(details: dict | None) -> str:
    """Format scanned items list for CSV/Excel columns."""
    if not details:
        return ""
    items = details.get("items", [])
    if not items:
        return ""
    api_ep = details.get("api_endpoint", "")
    count = details.get("items_scanned", len(items))
    header = f"API: {api_ep} | {count} item(s)\n" if api_ep else f"{count} item(s)\n"
    return header + "\n".join(f"  {i}. {name}" for i, name in enumerate(items[:100], 1))


def _render_secret_details_html(details: dict) -> str:
    """Render secret scan findings as a formatted HTML table instead of raw JSON."""
    import html as _html
    _esc = _html.escape
    if not details or "findings" not in details:
        return ""
    findings = details.get("findings", [])
    total = details.get("secrets_found", len(findings))
    verified = details.get("verified_count", 0)
    detectors = details.get("detectors", [])
    ws_host = details.get("workspace_host", "")  # e.g. https://adb-xxx.azuredatabricks.net

    # Known file extensions added during export (to strip when building workspace link)
    _EXT_SUFFIXES = (".py", ".sql", ".scala", ".r", ".sh", ".txt")

    def _make_ws_link(src_file: str) -> str:
        """Build a clickable Databricks workspace URL from the exported filename."""
        if not ws_host or src_file.endswith(".json"):
            return ""
        # Strip the extension we added during export
        name = src_file
        for ext in _EXT_SUFFIXES:
            if name.endswith(ext):
                name = name[: -len(ext)]
                break
        # Restore path separators (__ was used to flatten the path)
        ws_path = "/" + name.replace("__", "/")
        return f"{ws_host.rstrip('/')}/#workspace{ws_path}"

    # Summary bar
    html = f"""<div style="margin-top:10px;padding:12px 16px;background:#fef2f2;border:1px solid #fecaca;border-radius:8px">
<div style="display:flex;gap:20px;font-size:13px;margin-bottom:8px;flex-wrap:wrap">
<span style="font-weight:700;color:#991b1b">{total} secret(s) found</span>
<span style="color:#6b7280">{verified} verified</span>
<span style="color:#6b7280">Detector(s): {', '.join(detectors) if detectors else 'N/A'}</span></div>"""

    if findings:
        html += """<table style="width:100%;font-size:12px;border-collapse:collapse;margin-top:4px">
<tr style="background:#fee2e2;text-align:left">
<th style="padding:6px 8px;border-bottom:2px solid #fca5a5">#</th>
<th style="padding:6px 8px;border-bottom:2px solid #fca5a5">Detector</th>
<th style="padding:6px 8px;border-bottom:2px solid #fca5a5">Source File</th>
<th style="padding:6px 8px;border-bottom:2px solid #fca5a5">Line</th>
<th style="padding:6px 8px;border-bottom:2px solid #fca5a5">Severity</th>
<th style="padding:6px 8px;border-bottom:2px solid #fca5a5">Secret Hash (SHA-256)</th></tr>"""
        for i, f in enumerate(findings, 1):
            src = f.get("source_file", "unknown")
            readable_path = src.replace("__", "/")
            ws_link = _make_ws_link(src)
            line = f.get("line_number", "—")
            det = _esc(str(f.get("detector_name", "unknown")))
            sev = f.get("detector_type", "")
            sev_color = {"HIGH": "#dc2626", "CRITICAL": "#7f1d1d", "MEDIUM": "#ca8a04", "LOW": "#6b7280"}.get(sev.upper(), "#6b7280")
            sev_badge = f'<span style="color:{sev_color};font-weight:600;font-size:11px">{_esc(sev.upper() or "—")}</span>'
            secret_hash = f.get("secret_hash", "N/A")
            short_hash = secret_hash[:16] + "..." if len(secret_hash) > 16 else secret_hash
            row_bg = "#fff" if i % 2 == 1 else "#fef2f2"
            # Clickable link to the notebook in Databricks workspace
            if ws_link:
                src_cell = f'<a href="{_esc(ws_link)}" target="_blank" style="color:#2563eb;text-decoration:underline" title="Open in Databricks">{_esc(readable_path)}</a>'
            else:
                src_cell = _esc(readable_path)
            html += f"""<tr style="background:{row_bg}">
<td style="padding:5px 8px;border-bottom:1px solid #fde8e8">{i}</td>
<td style="padding:5px 8px;border-bottom:1px solid #fde8e8;font-weight:600">{det}</td>
<td style="padding:5px 8px;border-bottom:1px solid #fde8e8;font-family:monospace;font-size:11px;word-break:break-all">{src_cell}</td>
<td style="padding:5px 8px;border-bottom:1px solid #fde8e8;text-align:center">{line}</td>
<td style="padding:5px 8px;border-bottom:1px solid #fde8e8">{sev_badge}</td>
<td style="padding:5px 8px;border-bottom:1px solid #fde8e8;font-family:monospace;font-size:10px" title="{_esc(secret_hash)}">{_esc(short_hash)}</td></tr>"""
        html += "</table>"
        if total > len(findings):
            html += f'<p style="font-size:11px;color:#6b7280;margin:6px 0 0">Showing {len(findings)} of {total} findings (capped for report size)</p>'
    html += "</div>"
    return html


def _render_scan_items_html(details: dict) -> str:
    """Render the scanned-items list as a formatted HTML table."""
    import html as _html
    _esc = _html.escape
    if not details:
        return ""

    items = details.get("items", [])
    api_ep = details.get("api_endpoint", "")
    items_scanned = details.get("items_scanned", 0)
    ws_host = details.get("workspace_host", "")

    # Determine endpoint status
    err_code = details.get("api_error_code", 0)
    api_resp = details.get("api_response")
    if items:
        ep_status = "items"
        ep_badge = f'<span style="background:#16a34a;color:#fff;padding:1px 7px;border-radius:9999px;font-size:10px;font-weight:600">&#10003; {items_scanned} item{"s" if items_scanned != 1 else ""}</span>'
    elif err_code:
        ep_status = "error"
        ep_badge = f'<span style="background:#dc2626;color:#fff;padding:1px 7px;border-radius:9999px;font-size:10px;font-weight:600">&#10007; HTTP {err_code}</span>'
    elif isinstance(api_resp, dict) and api_resp:
        ep_status = "config"
        ep_badge = '<span style="background:#ca8a04;color:#fff;padding:1px 7px;border-radius:9999px;font-size:10px;font-weight:600">&#9881; Config</span>'
    else:
        ep_status = "empty"
        ep_badge = '<span style="background:#94a3b8;color:#fff;padding:1px 7px;border-radius:9999px;font-size:10px;font-weight:600">&#9675; Empty</span>'

    # Choose border/background color based on status
    border_colors = {"items": "#bae6fd", "config": "#fef08a", "empty": "#e2e8f0", "error": "#fca5a5"}
    bg_colors = {"items": "#f0f9ff", "config": "#fefce8", "empty": "#f8fafc", "error": "#fef2f2"}
    border_c = border_colors.get(ep_status, "#bae6fd")
    bg_c = bg_colors.get(ep_status, "#f0f9ff")

    # Summary line
    html = f'<div style="margin-top:8px;padding:10px 14px;background:{bg_c};border:1px solid {border_c};border-radius:8px">'
    html += '<div style="display:flex;gap:16px;font-size:12px;margin-bottom:6px;flex-wrap:wrap;align-items:center;color:#0c4a6e">'
    if api_ep:
        html += f'<span><strong>API:</strong> <code style="background:#e0f2fe;padding:1px 5px;border-radius:3px">{_esc(api_ep)}</code></span>'
    html += f'<span>{ep_badge}</span>'
    if ws_host:
        html += f'<span><strong>Workspace:</strong> {_esc(ws_host)}</span>'
    html += '</div>'

    if items:
        html += """<table style="width:100%;font-size:12px;border-collapse:collapse;margin-top:4px">
<tr style="background:#e0f2fe;text-align:left">
<th style="padding:5px 8px;border-bottom:2px solid #7dd3fc;width:40px">#</th>
<th style="padding:5px 8px;border-bottom:2px solid #7dd3fc">Item</th></tr>"""
        for i, name in enumerate(items[:50], 1):
            row_bg = "#fff" if i % 2 == 1 else "#f0f9ff"
            html += f"""<tr style="background:{row_bg}">
<td style="padding:4px 8px;border-bottom:1px solid #e0f2fe;color:#6b7280">{i}</td>
<td style="padding:4px 8px;border-bottom:1px solid #e0f2fe;font-family:monospace;font-size:11px;word-break:break-all">{_esc(str(name))}</td></tr>"""
        html += "</table>"
        if len(items) > 50:
            html += f'<p style="font-size:11px;color:#6b7280;margin:4px 0 0">Showing 50 of {len(items)} items</p>'
    elif ep_status == "error":
        _HTTP_REASONS = {400: "Bad Request", 401: "Unauthorized", 403: "Permission Denied", 404: "Not Found"}
        reason = _HTTP_REASONS.get(err_code, "Error")
        html += f'<p style="font-size:12px;color:#dc2626;margin:2px 0 0;font-weight:600">API returned HTTP {err_code} ({reason})</p>'
        _HTTP_HINTS = {
            401: "Token is invalid, expired, or revoked. Regenerate a PAT token or re-login via Azure.",
            403: "Token lacks the required admin role for this endpoint. Use a Workspace Admin token.",
            404: "Endpoint does not exist on this workspace (feature not enabled or requires Premium pricing tier).",
            400: "Feature is not configured or workspace does not support this configuration.",
        }
        hint = _HTTP_HINTS.get(err_code)
        if hint:
            html += f'<p style="font-size:11px;color:#6b7280;margin:4px 0 0;font-style:italic">{_esc(hint)}</p>'
    elif ep_status == "config":
        html += """<table style="width:100%;font-size:12px;border-collapse:collapse;margin-top:4px">
<tr style="background:#fef9c3;text-align:left">
<th style="padding:5px 8px;border-bottom:2px solid #facc15">Setting</th>
<th style="padding:5px 8px;border-bottom:2px solid #facc15">Value</th></tr>"""
        for i, (k, v) in enumerate(api_resp.items()):
            row_bg = "#fff" if i % 2 == 0 else "#fefce8"
            val_str = str(v) if v is not None else "null"
            html += f"""<tr style="background:{row_bg}">
<td style="padding:4px 8px;border-bottom:1px solid #fef08a;font-family:monospace;font-size:11px;font-weight:600">{_esc(str(k))}</td>
<td style="padding:4px 8px;border-bottom:1px solid #fef08a;font-family:monospace;font-size:11px">{_esc(val_str)}</td></tr>"""
        html += "</table>"
    else:
        html += '<p style="font-size:12px;color:#94a3b8;margin:2px 0 0;font-style:italic">API returned 200 OK with empty list — feature not in use on this workspace.</p>'

    html += "</div>"
    return html
