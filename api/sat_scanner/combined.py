"""SAT Scanner — combined multi-workspace summary and HTML export."""

from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path

from .models import SATFinding, SATScanResult
from .checks import SAT_CHECKS, _get_effort, CHECK_BENEFITS, CATEGORY_DEFINITIONS
from .scoring import _build_prioritised_recommendations
from .helpers import _pl, _sanitize_name, _details_str, _render_secret_details_html, _render_scan_items_html
from .exporters import export_recommendation_summary


def _print_combined_summary(
    results: list[tuple[str, SATScanResult]],
    skipped: list[str],
    output_dir: Path,
    formats: set[str],
    show_scan_items: bool = False,
    show_effort: bool = False,
    show_cost: bool = False,
    report_profile: str = "modern",
) -> None:
    """Print and export a combined summary across all scanned workspaces."""
    print(f"\n{'='*70}")
    print(f"  COMBINED RESULTS — {len(results)} workspace(s) scanned")
    if skipped:
        print(f"  ⚠️  {len(skipped)} workspace(s) skipped: {', '.join(skipped)}")
    print(f"{'='*70}")

    # Dynamic column width: fit the longest workspace name (min 20, max 60)
    _nw = max((len(n) for n, _ in results), default=20)
    _nw = max(20, min(_nw + 4, 60))  # +4 for icon prefix

    print(f"\n  {'Workspace':<{_nw}} {'Score':>7}  {'Total':>5}  {'Pass':>5}  {'Fail':>5}  {'Warn':>5}  {'N/A':>5}  {'Err':>5}  {'Grade'}")
    print(f"  {'─'*_nw} {'─'*7}  {'─'*5}  {'─'*5}  {'─'*5}  {'─'*5}  {'─'*5}  {'─'*5}  {'─'*19}")
    all_findings: list[SATFinding] = []
    for name, r in sorted(results, key=lambda x: x[1].overall_score):
        grade = "Good" if r.overall_score >= 80 else ("Needs Improvement" if r.overall_score >= 60 else "Critical")
        icon = "✅" if r.overall_score >= 80 else ("⚠️ " if r.overall_score >= 60 else "❌")
        label = f"{icon} {name}"
        print(f"  {label:<{_nw}} {r.overall_score:>4}/100  {r.total_checks:>5}  {r.passed:>5}  {r.failed:>5}  {r.warnings:>5}  {r.not_applicable:>5}  {r.api_errors:>5}  {grade}")
        all_findings.extend(r.findings)

    avg_score = round(sum(r.overall_score for _, r in results) / len(results))
    total_checks = sum(r.total_checks for _, r in results)
    total_pass = sum(r.passed for _, r in results)
    total_fail = sum(r.failed for _, r in results)
    total_warn = sum(r.warnings for _, r in results)
    total_na = sum(r.not_applicable for _, r in results)
    total_err = sum(r.api_errors for _, r in results)
    print(f"\n  {'AVERAGE':<{_nw}} {avg_score:>4}/100  {total_checks:>5}  {total_pass:>5}  {total_fail:>5}  {total_warn:>5}  {total_na:>5}  {total_err:>5}")

    # Export combined CSV summary
    summary_path = output_dir / f"sat-combined-summary-{datetime.now().strftime('%Y-%m-%d')}.csv"
    with open(summary_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Workspace", "URL", "Score", "Grade", "Total Checks", "Passed", "Failed", "Warnings", "N/A", "API Errors", "Scanned At"])
        for name, r in results:
            grade = "Good" if r.overall_score >= 80 else ("Needs Improvement" if r.overall_score >= 60 else "Critical")
            writer.writerow([name, r.workspace_url, r.overall_score, grade,
                r.total_checks, r.passed, r.failed, r.warnings, r.not_applicable, r.api_errors, r.scanned_at])
    print(f"\n  📊 Combined summary → {summary_path}")

    # Export combined HTML summary
    if report_profile == "modern":
        from .profiles.modern import export_combined_html_modern, export_recommendation_summary_modern
        export_combined_html_modern(results, skipped, avg_score, output_dir, show_scan_items=show_scan_items, show_effort=show_effort, show_cost=show_cost)
    else:
        _export_combined_html(results, skipped, avg_score, output_dir, show_scan_items=show_scan_items, show_effort=show_effort, show_cost=show_cost)

    # Export combined Recommendation Summary (deduplicated across workspaces)
    all_findings = []
    for _, r in results:
        all_findings.extend(r.findings)
    if report_profile == "modern":
        _recom_path = export_recommendation_summary_modern(None, output_dir, show_cost=show_cost, findings=all_findings)
    else:
        _recom_path = export_recommendation_summary(None, output_dir, show_cost=show_cost, findings=all_findings)
    if _recom_path:
        print(f"  📋 Recommendation Summary → {_recom_path}")


def _export_combined_html(
    results: list[tuple[str, SATScanResult]],
    skipped: list[str],
    avg_score: int,
    output_dir: Path,
    show_scan_items: bool = False,
    show_effort: bool = False,
    show_cost: bool = False,
) -> None:
    """Create a combined HTML dashboard across all workspaces."""
    import html as _html
    _esc = _html.escape
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _icon(name: str, size: int = 16) -> str:
        return f'<i data-lucide="{name}" style="width:{size}px;height:{size}px;vertical-align:middle;margin-right:4px"></i>'

    def _svg_gauge(score: int, size: int = 160) -> str:
        gc = "#16a34a" if score >= 80 else ("#ca8a04" if score >= 60 else "#dc2626")
        g = "Good" if score >= 80 else ("Needs Improvement" if score >= 60 else "Critical")
        r = size * 0.4375
        circ = round(2 * 3.14159 * r, 1)
        dash = round(score / 100 * circ, 1)
        half = size // 2
        return f"""<div style="text-align:center">
<svg width="{size}" height="{size}" viewBox="0 0 {size} {size}">
  <circle cx="{half}" cy="{half}" r="{r}" fill="none" stroke="#e2e8f0" stroke-width="12"/>
  <circle cx="{half}" cy="{half}" r="{r}" fill="none" stroke="{gc}" stroke-width="12"
    stroke-dasharray="{dash} {circ}" stroke-dashoffset="0"
    transform="rotate(-90 {half} {half})" stroke-linecap="round"/>
  <text x="{half}" y="{half - 8}" text-anchor="middle" font-size="32" font-weight="700" fill="{gc}">{score}</text>
  <text x="{half}" y="{half + 16}" text-anchor="middle" font-size="13" fill="#64748b">{_esc(g)}</text>
</svg></div>"""

    _SEV_TIPS = {
        "critical": "Immediate security risk — must be remediated immediately (weight: 10)",
        "high": "Significant security weakness — remediate within days (weight: 7)",
        "medium": "Moderate risk — remediate within the current sprint (weight: 4)",
        "low": "Minor improvement opportunity — plan for upcoming cycles (weight: 2)",
        "pass": "Check passed — no action needed",
    }
    _STATUS_TIPS = {
        "PASS": "Compliant — security control is correctly configured",
        "FAIL": "Action Required — confirmed security gap that must be fixed",
        "WARN": "Review Needed — borderline result that needs manual investigation",
        "NOT_APPLICABLE": "Skipped — feature not in use on this workspace, excluded from score",
        "API ERROR": "Could Not Evaluate — API failure prevented this check from running",
    }

    def sev_badge(s: str) -> str:
        colors = {"critical": "#dc2626", "high": "#ea580c", "medium": "#ca8a04", "low": "#2563eb", "pass": "#16a34a"}
        tip = _esc(_SEV_TIPS.get(s, ""))
        return f'<span class="tip" style="background:{colors.get(s,"#6b7280")};color:#fff;padding:2px 8px;border-radius:9999px;font-size:11px;font-weight:600;text-transform:uppercase"><span class="tip-text">{tip}</span>{_esc(s)}</span>'

    def status_badge(s: str) -> str:
        colors = {"PASS": "#16a34a", "FAIL": "#dc2626", "WARN": "#ca8a04", "NOT_APPLICABLE": "#6b7280"}
        tip = _esc(_STATUS_TIPS.get(s, ""))
        return f'<span class="tip" style="background:{colors.get(s,"#6b7280")};color:#fff;padding:2px 8px;border-radius:9999px;font-size:11px;font-weight:600"><span class="tip-text">{tip}</span>{_esc(s)}</span>'

    total_checks_all = sum(r.total_checks for _, r in results)
    total_passed = sum(r.passed for _, r in results)
    total_failed = sum(r.failed for _, r in results)
    total_warnings = sum(r.warnings for _, r in results)
    total_na = sum(r.not_applicable for _, r in results)
    total_api_errors = sum(r.api_errors for _, r in results)

    # ── Summary tab ──
    skipped_html = ""
    if skipped:
        skipped_html = f"""<div style="background:#fef2f2;border:1px solid #fca5a5;border-radius:8px;padding:14px;margin-bottom:24px">
<strong style="color:#dc2626">{_icon('alert-triangle',14)} Skipped ({len(skipped)}):</strong> {', '.join(_esc(s) for s in skipped)}</div>"""

    # Build filename lookup for workspace reports
    date_str = datetime.now().strftime('%Y-%m-%d')
    def _ws_report_file(ws_name: str) -> str:
        sn = _sanitize_name(ws_name)
        fname = f"sat-{sn}-{date_str}.html" if sn else f"sat-{date_str}.html"
        # Multi-workspace: reports live in per-workspace subfolders
        if len(results) > 1 and sn:
            return f"{sn}/{fname}"
        return fname

    # Workspace score cards with mini gauges
    ws_cards = ""
    for name, r in sorted(results, key=lambda x: x[1].overall_score):
        report_link = _ws_report_file(name)
        ws_cards += f"""<div style="background:#fff;border:1px solid #e2e8f0;border-radius:10px;padding:20px;margin-bottom:12px">
<div style="display:flex;justify-content:space-between;align-items:center">
<div style="flex:1">
  <h3 style="margin:0;font-size:16px;color:#0f172a;font-weight:600">{_esc(name)}</h3>
  <div style="font-size:12px;color:#64748b;margin-top:2px">{_esc(r.workspace_url)}</div>
  <div style="display:flex;gap:16px;margin-top:12px;font-size:13px">
    <span style="color:#374151;font-weight:600">{r.total_checks} checks</span>
    <span style="color:#16a34a;font-weight:600">{_pl(r.passed, 'passed', 'passed')}</span>
    <span style="color:#dc2626;font-weight:600">{_pl(r.failed, 'failed', 'failed')}</span>
    <span style="color:#ca8a04;font-weight:600">{_pl(r.warnings, 'warning')}</span>
    <span style="color:#6b7280">{r.not_applicable} N/A</span>
    <span style="color:#7c3aed;font-weight:600">{_pl(r.api_errors, 'API error')}</span>
  </div>
  <a href="{_esc(report_link)}" style="display:inline-flex;align-items:center;gap:4px;margin-top:10px;font-size:12px;color:#3b82f6;text-decoration:none;font-weight:600">{_icon('external-link',13)} View Full Report</a>
</div>
{_svg_gauge(r.overall_score, 100)}
</div></div>\n"""

    # ── Effort summary for combined (only when --effort) ──
    comb_effort_html = ""
    if show_effort:
        _all_actionable = []
        for _, r in results:
            _all_actionable.extend(f for f in r.findings if f.status in ("FAIL", "WARN") and not f.is_api_error)
        _ceff_counts: dict[str, int] = {}
        for f in _all_actionable:
            e = f.effort or "Moderate (1–4 hrs)"
            _ceff_counts[e] = _ceff_counts.get(e, 0) + 1
        _ceff_order = ["Quick Fix (5–15 min)", "Moderate (1–4 hrs)", "Significant (1–3 days)", "Project (1+ weeks)"]
        _ceff_colors = {"Quick Fix (5–15 min)": "#16a34a", "Moderate (1–4 hrs)": "#2563eb", "Significant (1–3 days)": "#ca8a04", "Project (1+ weeks)": "#dc2626"}
        _ceff_icons = {"Quick Fix (5–15 min)": "zap", "Moderate (1–4 hrs)": "wrench", "Significant (1–3 days)": "hard-hat", "Project (1+ weeks)": "building"}
        _ceff_bars = ""
        for eff in _ceff_order:
            cnt = _ceff_counts.get(eff, 0)
            if cnt == 0:
                continue
            pct = round(cnt / len(_all_actionable) * 100) if _all_actionable else 0
            c = _ceff_colors.get(eff, "#6b7280")
            _ceff_bars += f"""<tr><td style="white-space:nowrap;font-size:13px;padding:6px 12px 6px 0">{_icon(_ceff_icons.get(eff, 'clock'), 14)} {_esc(eff)}</td>
<td style="font-weight:700;color:{c};text-align:right;padding:6px 12px 6px 0;font-size:14px">{cnt}</td>
<td style="width:200px;padding:6px 0"><div style="background:#e2e8f0;border-radius:4px;height:8px;width:100%"><div style="background:{c};border-radius:4px;height:8px;width:{max(2, pct)}%"></div></div></td>
<td style="color:#64748b;font-size:12px;padding:6px 0 6px 8px">{pct}%</td></tr>"""
        if _all_actionable:
            comb_effort_html = f"""
<div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:12px;padding:20px;margin:24px auto;max-width:560px">
<h4 style="margin:0 0 12px;font-size:14px;font-weight:700;color:#0f172a">{_icon('clock', 16)} Remediation Effort Summary (All Workspaces)</h4>
<p style="font-size:12px;color:#64748b;margin:0 0 12px">{len(_all_actionable)} actionable finding{"s" if len(_all_actionable) != 1 else ""} (FAIL + WARN) across {len(results)} workspace(s):</p>
<table style="border:none;width:100%">{_ceff_bars}</table>
<div style="background:#fffbeb;border:1px solid #fcd34d;border-radius:8px;padding:12px 14px;margin-top:14px">
<p style="margin:0 0 8px;font-size:12px;font-weight:600;color:#92400e">{_icon('alert-triangle', 13)} Prerequisites Not Included in Estimates</p>
<p style="margin:0 0 6px;font-size:11px;color:#78350f">The times above reflect core remediation work only. Factor in additional time for:</p>
<div style="display:flex;flex-wrap:wrap;gap:6px;font-size:11px">
<span style="background:#fef3c7;color:#92400e;padding:2px 8px;border-radius:9999px">Terraform / IaC updates</span>
<span style="background:#fef3c7;color:#92400e;padding:2px 8px;border-radius:9999px">CI/CD pipeline runs</span>
<span style="background:#fef3c7;color:#92400e;padding:2px 8px;border-radius:9999px">Change management / CAB approval</span>
<span style="background:#fef3c7;color:#92400e;padding:2px 8px;border-radius:9999px">Non-prod testing</span>
<span style="background:#fef3c7;color:#92400e;padding:2px 8px;border-radius:9999px">Provider / module upgrades</span>
<span style="background:#fef3c7;color:#92400e;padding:2px 8px;border-radius:9999px">Security review</span>
<span style="background:#fef3c7;color:#92400e;padding:2px 8px;border-radius:9999px">Documentation updates</span>
</div>
<p style="margin:6px 0 0;font-size:11px;color:#78350f;font-style:italic">See the <strong>Definitions</strong> tab for full prerequisite details and typical additional times.</p>
</div>
</div>"""

    summary_content = f"""{skipped_html}
{_svg_gauge(avg_score, 160)}
<div style="text-align:center;font-size:13px;color:#64748b;margin-bottom:24px">Average across {len(results)} workspace(s)</div>
{comb_effort_html}
<h3 style="font-size:15px;font-weight:700;color:#0f172a;margin:0 0 16px">{_icon('server',16)} Workspace Scores</h3>
{ws_cards}"""

    # ── Workspace Comparison tab ── (category heatmap table)
    all_cats: list[str] = []
    for _, r in results:
        for cat in r.category_scores:
            if cat not in all_cats:
                all_cats.append(cat)

    comp_header = "<th>Category</th>"
    for name, _ in results:
        comp_header += f"<th style='text-align:center'>{_esc(name)}</th>"

    comp_rows = ""
    for cat in all_cats:
        comp_rows += f"<tr><td>{_esc(cat)}</td>"
        for _, r in results:
            cs = r.category_scores.get(cat, -1)
            if cs < 0:
                comp_rows += "<td style='text-align:center;color:#94a3b8'>—</td>"
            else:
                c = "#16a34a" if cs >= 80 else ("#ca8a04" if cs >= 60 else "#dc2626")
                comp_rows += f"<td style='text-align:center;font-weight:700;color:{c}'>{cs}</td>"
        comp_rows += "</tr>"

    comparison_content = f"""<div style="overflow-x:auto"><table style="min-width:500px">
<tr>{comp_header}</tr>
{comp_rows}</table></div>"""

    # ── Common Issues tab ── (checks that fail/warn across multiple workspaces)
    check_status_map: dict[str, list[tuple[str, str, str, str, str]]] = {}
    for name, r in results:
        for f in r.findings:
            if f.status in ("FAIL", "WARN"):
                check_status_map.setdefault(f.check_id, []).append(
                    (name, f.status, f.title, f.severity, f.current_state)
                )

    # Sort: most widespread issues first, then by severity
    sev_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    sorted_issues = sorted(
        check_status_map.items(),
        key=lambda x: (-len(x[1]), sev_order.get(x[1][0][3], 9)),
    )

    if sorted_issues:
        issues_rows = ""
        for check_id, entries in sorted_issues:
            ws_names = ", ".join(e[0] for e in entries)
            title = entries[0][2]
            sev = entries[0][3]
            statuses = set(e[1] for e in entries)
            worst = "FAIL" if "FAIL" in statuses else "WARN"
            ci_benefit = _esc(CHECK_BENEFITS.get(check_id, ""))
            issues_rows += f"""<tr>
<td style="font-family:monospace;font-size:12px">{_esc(check_id)}</td>
<td>{_esc(title)}</td>
<td>{sev_badge(sev)}</td>
<td>{status_badge(worst)}</td>
<td style="font-weight:700;font-size:22px;text-align:center">{len(entries)}/{len(results)}</td>
<td style="font-size:12px">{_esc(ws_names)}</td>
<td style="font-size:12px">{ci_benefit}</td></tr>"""
        common_issues_content = f"""<div style="overflow-x:auto"><table style="min-width:700px">
<tr><th>Check ID</th><th>Title</th><th>Severity</th><th>Worst Status</th><th>Affected</th><th>Workspaces</th><th>Why It Matters</th></tr>
{issues_rows}</table></div>"""
    else:
        common_issues_content = '<p style="color:#16a34a;font-size:14px;font-weight:600">No issues found across workspaces.</p>'

    # ── Per-workspace tabs ──
    ws_tab_content: dict[str, str] = {}
    for name, r in results:
        ws_gauge = _svg_gauge(r.overall_score, 140)
        pass_rate = round(r.passed / max(r.total_checks, 1) * 100, 1)

        na_cell = f"""
<div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;padding:14px;text-align:center">
  <div style="font-size:11px;color:#64748b;text-transform:uppercase;letter-spacing:.05em">N/A</div>
  <div style="font-size:22px;font-weight:700;color:#64748b">{r.not_applicable}</div></div>""" if r.not_applicable else ""
        grid_cols = 5 + (1 if r.not_applicable else 0)
        ws_kpis = f"""<div style="display:grid;grid-template-columns:repeat({grid_cols},1fr);gap:12px;margin:16px 0 20px">
<div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;padding:14px;text-align:center">
  <div style="font-size:11px;color:#64748b;text-transform:uppercase;letter-spacing:.05em">Score</div>
  <div style="font-size:22px;font-weight:700;color:{'#16a34a' if r.overall_score >= 80 else ('#ca8a04' if r.overall_score >= 60 else '#dc2626')}">{r.overall_score}/100</div></div>
<div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;padding:14px;text-align:center">
  <div style="font-size:11px;color:#64748b;text-transform:uppercase;letter-spacing:.05em">Checks</div>
  <div style="font-size:22px;font-weight:700;color:#374151">{r.total_checks}</div></div>
<div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;padding:14px;text-align:center">
  <div style="font-size:11px;color:#64748b;text-transform:uppercase;letter-spacing:.05em">Passed</div>
  <div style="font-size:22px;font-weight:700;color:#16a34a">{r.passed}</div>
  <div style="font-size:11px;color:#64748b">{pass_rate}%</div></div>
<div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;padding:14px;text-align:center">
  <div style="font-size:11px;color:#64748b;text-transform:uppercase;letter-spacing:.05em">Failed</div>
  <div style="font-size:22px;font-weight:700;color:#dc2626">{r.failed}</div></div>
<div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;padding:14px;text-align:center">
  <div style="font-size:11px;color:#64748b;text-transform:uppercase;letter-spacing:.05em">Warnings</div>
  <div style="font-size:22px;font-weight:700;color:#ca8a04">{r.warnings}</div></div>{na_cell}</div>"""

        # Score breakdown for this workspace
        _SWt = {"critical": 10, "high": 7, "medium": 4, "low": 2}
        _ws_scorable = [f for f in r.findings if not f.is_api_error]
        _ws_applicable = [f for f in _ws_scorable if f.status != "NOT_APPLICABLE"]
        _ws_fails = [f for f in _ws_applicable if f.status == "FAIL"]
        _ws_warns = [f for f in _ws_applicable if f.status == "WARN"]
        _ws_passes = [f for f in _ws_applicable if f.status == "PASS"]
        _ws_tot = sum(_SWt.get(SAT_CHECKS.get(f.check_id, {}).get("severity", "low"), 2) for f in _ws_applicable)
        _ws_fp = sum(_SWt.get(SAT_CHECKS.get(f.check_id, {}).get("severity", "low"), 2) for f in _ws_fails)
        _ws_wp = sum(_SWt.get(SAT_CHECKS.get(f.check_id, {}).get("severity", "low"), 2) * 0.5 for f in _ws_warns)
        _ws_tp = _ws_fp + _ws_wp
        _ws_gc = "#16a34a" if r.overall_score >= 80 else ("#ca8a04" if r.overall_score >= 60 else "#dc2626")
        ws_breakdown = f"""<div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:12px;padding:16px;margin:0 auto 20px;max-width:520px">
<h4 style="margin:0 0 10px;font-size:13px;font-weight:700;color:#0f172a">Score Breakdown</h4>
<table style="font-size:12px;border:none">
<tr><td style="border:none;padding:3px 10px 3px 0;color:#64748b">Scored checks</td><td style="border:none;padding:3px 0;font-weight:600">{len(_ws_applicable)} <span style="color:#94a3b8;font-weight:400">(excl. {r.not_applicable} N/A + {r.api_errors} API Error)</span></td></tr>
<tr><td style="border:none;padding:3px 10px 3px 0;color:#64748b">Total weight pool</td><td style="border:none;padding:3px 0;font-weight:600">{_ws_tot:.0f} pts</td></tr>
<tr><td style="border:none;padding:3px 10px 3px 0;color:#dc2626">FAIL penalty <span style="color:#94a3b8">({len(_ws_fails)} &times; full)</span></td><td style="border:none;padding:3px 0;font-weight:600;color:#dc2626">&minus;{_ws_fp:.0f} pts</td></tr>
<tr><td style="border:none;padding:3px 10px 3px 0;color:#ca8a04">WARN penalty <span style="color:#94a3b8">({len(_ws_warns)} &times; half)</span></td><td style="border:none;padding:3px 0;font-weight:600;color:#ca8a04">&minus;{_ws_wp:.1f} pts</td></tr>
<tr><td style="border:none;padding:3px 10px 3px 0;color:#16a34a">PASS <span style="color:#94a3b8">({len(_ws_passes)})</span></td><td style="border:none;padding:3px 0;font-weight:600;color:#16a34a">0 pts</td></tr>
<tr style="border-top:1px solid #e2e8f0"><td style="border:none;padding:6px 10px 3px 0;color:#0f172a;font-weight:600">Formula</td><td style="border:none;padding:6px 0 3px;font-family:monospace;font-size:11px">(1 &minus; {_ws_tp:.1f} / {_ws_tot:.0f}) &times; 100 = <strong style="color:{_ws_gc};font-size:13px">{r.overall_score}</strong></td></tr>
</table>
<div style="display:flex;gap:6px;margin-top:10px;font-size:11px">
<span style="padding:2px 8px;border-radius:9999px;font-weight:600;{'background:#dcfce7;color:#16a34a' if r.overall_score >= 80 else 'background:#f1f5f9;color:#94a3b8'}">Good 80&ndash;100</span>
<span style="padding:2px 8px;border-radius:9999px;font-weight:600;{'background:#fef9c3;color:#ca8a04' if 60 <= r.overall_score < 80 else 'background:#f1f5f9;color:#94a3b8'}">Needs Improvement 60&ndash;79</span>
<span style="padding:2px 8px;border-radius:9999px;font-weight:600;{'background:#fee2e2;color:#dc2626' if r.overall_score < 60 else 'background:#f1f5f9;color:#94a3b8'}">Critical 0&ndash;59</span>
</div></div>"""

        # Category score bars
        cat_rows = ""
        for cat, cs in sorted(r.category_scores.items(), key=lambda x: x[1]):
            c = "#16a34a" if cs >= 80 else ("#ca8a04" if cs >= 60 else "#dc2626")
            cat_rows += f"""<tr><td>{_esc(cat)}</td>
<td style="font-weight:700;color:{c};text-align:right">{cs}</td>
<td><div style="background:#e2e8f0;border-radius:4px;height:8px;width:100%"><div style="background:{c};border-radius:4px;height:8px;width:{max(2,cs)}%"></div></div></td></tr>"""

        # Failed checks for this workspace
        ws_failed = [f for f in r.findings if f.status == "FAIL"]
        ws_warns = [f for f in r.findings if f.status == "WARN"]

        failed_html = ""
        if ws_failed:
            f_rows = ""
            for f in ws_failed:
                secret_detail_row = ""
                _comb_fail_cols = 8 if show_effort else 7
                if f.details and "findings" in f.details:
                    secret_detail_row = f'<tr><td colspan="{_comb_fail_cols}" style="padding:0 8px 12px">{_render_secret_details_html(f.details)}</td></tr>'
                scan_items_row = ""
                if f.details and "items" in f.details:
                    scan_items_row = f'<tr><td colspan="{_comb_fail_cols}" style="padding:0 8px 12px">{_render_scan_items_html(f.details)}</td></tr>'
                f_rows += f"""<tr>
<td style="font-family:monospace;font-size:12px">{_esc(f.check_id)}</td>
<td>{sev_badge(f.severity)}</td>
{'<td style="font-size:12px;white-space:nowrap">' + _esc(f.effort) + '</td>' if show_effort else ''}
<td>{_esc(f.title)}</td>
<td style="font-size:12px;max-width:250px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{_esc(f.current_state)}</td>
<td style="font-size:12px">{_esc(f.recommendation)}</td>
<td style="font-size:12px">{_esc(f.benefits) if f.benefits else ''}</td></tr>{secret_detail_row}{scan_items_row}"""
            failed_html = f"""<h3 style="font-size:15px;font-weight:700;color:#dc2626;margin:20px 0 12px">{_icon('x-circle',16)} {_pl(len(ws_failed), 'Failed Check')}</h3>
<div style="overflow-x:auto"><table style="min-width:600px">
<tr><th>Check ID</th><th>Severity</th>{'<th>Effort</th>' if show_effort else ''}<th>Title</th><th>Current State</th><th>Recommendation</th><th>Why It Matters</th></tr>
{f_rows}</table></div>"""
        else:
            failed_html = '<p style="color:#16a34a;font-size:13px;margin:16px 0;font-weight:600">No failed checks.</p>'

        warn_html = ""
        if ws_warns:
            w_rows = ""
            for f in ws_warns:
                w_rows += f"""<tr>
<td style="font-family:monospace;font-size:12px">{_esc(f.check_id)}</td>
<td>{sev_badge(f.severity)}</td>
{'<td style="font-size:12px;white-space:nowrap">' + _esc(f.effort) + '</td>' if show_effort else ''}
<td>{_esc(f.title)}</td>
<td style="font-size:12px;max-width:250px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{_esc(f.current_state)}</td>
<td style="font-size:12px">{_esc(f.benefits) if f.benefits else ''}</td></tr>"""
            warn_html = f"""<h3 style="font-size:15px;font-weight:700;color:#ca8a04;margin:20px 0 12px">{_icon('alert-triangle',16)} {_pl(len(ws_warns), 'Warning')}</h3>
<div style="overflow-x:auto"><table style="min-width:500px">
<tr><th>Check ID</th><th>Severity</th>{'<th>Effort</th>' if show_effort else ''}<th>Title</th><th>Current State</th><th>Why It Matters</th></tr>
{w_rows}</table></div>"""

        ws_report_link = _ws_report_file(name)
        ws_tab_content[name] = f"""{ws_gauge}
<div style="text-align:center;margin-bottom:12px">
  <div style="font-size:12px;color:#64748b">{_esc(r.workspace_url)}</div>
  <a href="{_esc(ws_report_link)}" style="display:inline-flex;align-items:center;gap:4px;margin-top:6px;font-size:13px;color:#3b82f6;text-decoration:none;font-weight:600">{_icon('external-link',14)} Open Full Workspace Report</a>
</div>
{ws_kpis}
{ws_breakdown}
<h3 style="font-size:15px;font-weight:700;color:#0f172a;margin:20px 0 12px">{_icon('bar-chart-3',16)} Category Scores</h3>
<table><tr><th>Category</th><th style="text-align:right">Score</th><th style="width:200px">Progress</th></tr>
{cat_rows}</table>
{failed_html}
{warn_html}"""

    # ── Definitions tab ── (reused from workspace report)
    definitions_content = """
<h3 style="font-size:15px;font-weight:700;color:#0f172a;margin:0 0 12px">Finding Statuses</h3>
<table>
<tr><th>Status</th><th>Label</th><th>Definition</th></tr>
<tr><td><span style="background:#16a34a;color:#fff;padding:2px 8px;border-radius:9999px;font-size:11px;font-weight:600">PASS</span></td><td>Compliant</td><td>The check ran successfully and confirmed the security control is in place. No action needed.</td></tr>
<tr><td><span style="background:#dc2626;color:#fff;padding:2px 8px;border-radius:9999px;font-size:11px;font-weight:600">FAIL</span></td><td>Action Required</td><td>The check found a concrete security gap &mdash; confirmed bad configuration that must be fixed.</td></tr>
<tr><td><span style="background:#ca8a04;color:#fff;padding:2px 8px;border-radius:9999px;font-size:11px;font-weight:600">WARN</span></td><td>Review Needed</td><td>The check returned a borderline result that needs manual investigation. Genuine security warnings only (API errors are separated).</td></tr>
<tr><td><span style="background:#6b7280;color:#fff;padding:2px 8px;border-radius:9999px;font-size:11px;font-weight:600">N/A</span></td><td>Skipped</td><td>The feature being checked is not in use on this workspace (e.g. no Delta Sharing recipients, no MLflow models, Unity Catalog not enabled). Excluded from score.</td></tr>
<tr><td><span style="background:#7c3aed;color:#fff;padding:2px 8px;border-radius:9999px;font-size:11px;font-weight:600">API ERROR</span></td><td>Could Not Evaluate</td><td>The check could not run due to an API failure (HTTP 400/401/403/404, timeout, or connection error). Excluded from score. Fix the underlying issue and re-scan.</td></tr>
</table>
<h3 style="font-size:15px;font-weight:700;color:#0f172a;margin:24px 0 12px">Severity Levels</h3>
<table>
<tr><th>Severity</th><th>Weight</th><th>Definition</th></tr>
<tr><td><span style="background:#dc2626;color:#fff;padding:2px 8px;border-radius:9999px;font-size:11px;font-weight:600">CRITICAL</span></td><td>10</td><td>Immediate security risk that could lead to data breach, unauthorized access, or compliance violation. Must be remediated immediately.</td></tr>
<tr><td><span style="background:#ea580c;color:#fff;padding:2px 8px;border-radius:9999px;font-size:11px;font-weight:600">HIGH</span></td><td>7</td><td>Significant security weakness that could be exploited. Should be remediated within days.</td></tr>
<tr><td><span style="background:#ca8a04;color:#fff;padding:2px 8px;border-radius:9999px;font-size:11px;font-weight:600">MEDIUM</span></td><td>4</td><td>Moderate risk that weakens overall security posture. Should be remediated within the current sprint/cycle.</td></tr>
<tr><td><span style="background:#2563eb;color:#fff;padding:2px 8px;border-radius:9999px;font-size:11px;font-weight:600">LOW</span></td><td>2</td><td>Minor improvement opportunity or best-practice deviation. Plan remediation for upcoming cycles.</td></tr>
</table>
<h3 style="font-size:15px;font-weight:700;color:#0f172a;margin:24px 0 12px">Grade Definitions</h3>
<table>
<tr><th>Grade</th><th>Score Range</th><th>Definition</th></tr>
<tr><td style="color:#16a34a;font-weight:700">Good</td><td>80&ndash;100</td><td>Strong security posture. Address remaining findings as maintenance items.</td></tr>
<tr><td style="color:#ca8a04;font-weight:700">Needs Improvement</td><td>60&ndash;79</td><td>Gaps exist that weaken security posture. Prioritize High and Critical findings.</td></tr>
<tr><td style="color:#dc2626;font-weight:700">Critical</td><td>0&ndash;59</td><td>Significant security risks are present. Immediate remediation is required.</td></tr>
</table>
<h3 style="font-size:15px;font-weight:700;color:#0f172a;margin:24px 0 12px">Scoring Formula</h3>
<table>
<tr><th>Component</th><th>Detail</th></tr>
<tr><td>Weight per check</td><td>Critical=10, High=7, Medium=4, Low=2</td></tr>
<tr><td>FAIL penalty</td><td>Full weight &mdash; Critical=10, High=7, Medium=4, Low=2</td></tr>
<tr><td>WARN penalty</td><td>Half weight &mdash; Critical=5, High=3.5, Medium=2, Low=1</td></tr>
<tr><td>PASS penalty</td><td>0 (no penalty)</td></tr>
<tr><td>Excluded</td><td>NOT_APPLICABLE and API Error findings are excluded from weight totals</td></tr>
<tr><td>Formula</td><td>Score = (1 &minus; penalty_sum / total_weights) &times; 100<br><em>penalty_sum = &sum;(FAIL &times; full_weight) + &sum;(WARN &times; half_weight)</em></td></tr>
<tr><td>Interpretation</td><td>A FAIL on a Critical check costs 10 pts; a WARN on a Critical check costs 5 pts. A FAIL on Low costs 2 pts; a WARN on Low costs 1 pt.</td></tr>
</table>
<h3 style="font-size:15px;font-weight:700;color:#0f172a;margin:24px 0 12px">Category Definitions</h3>
<table>
<tr><th>Category</th><th>Definition</th></tr>
""" + "".join(f'<tr><td>{_esc(cat)}</td><td>{_esc(defn)}</td></tr>' for cat, defn in CATEGORY_DEFINITIONS) + """
</table>
<h3 style="font-size:15px;font-weight:700;color:#0f172a;margin:24px 0 12px">HTTP Error Codes in Findings</h3>
<table>
<tr><th>Code</th><th>Meaning</th><th>What to do</th></tr>
<tr><td>401</td><td>Unauthorized &mdash; token is invalid, expired, or revoked.</td><td>Regenerate a new PAT token in Databricks &rarr; User Settings &rarr; Access Tokens, or re-login via Azure to refresh.</td></tr>
<tr><td>403</td><td>Permission Denied &mdash; token authenticated but lacks the required admin role for this endpoint.</td><td>Use a Workspace Admin PAT token. Admin-only endpoints: token-management, ip-access-lists, workspace-conf, Settings API.</td></tr>
<tr><td>404</td><td>Not Found &mdash; the API endpoint does not exist on this workspace (feature not enabled or requires Premium pricing tier).</td><td>Check the Azure Databricks Account Console for account-level settings. Verify your workspace pricing tier (Premium required for many features).</td></tr>
<tr><td>400</td><td>Bad Request &mdash; feature is not configured or workspace does not support that configuration key.</td><td>Review raw details. This usually means the feature is managed differently (e.g. Unity Catalog instead of Table ACLs).</td></tr>
</table>"""
    if show_effort:
        definitions_content += """
<h3 style="font-size:15px;font-weight:700;color:#0f172a;margin:24px 0 12px">Remediation Effort Levels</h3>
<table>
<tr><th>Level</th><th>Time Range</th><th>What&rsquo;s Included</th><th>Examples</th></tr>
<tr><td style="color:#16a34a;font-weight:700">Quick Fix</td><td>5&ndash;15 min</td><td>Single configuration toggle or setting change in the admin console. No cross-team coordination or testing required.</td><td>Enable a workspace flag, toggle an admin setting, update a single policy value</td></tr>
<tr><td style="color:#2563eb;font-weight:700">Moderate</td><td>1&ndash;4 hrs</td><td>Multi-step configuration, policy creation, or changes that require testing and validation. May involve creating new resources, updating IaC code, or running build/deploy pipelines.</td><td>Create cluster policies, configure IP access lists, set up secret scopes, update Terraform modules, run CI/CD pipelines</td></tr>
<tr><td style="color:#ca8a04;font-weight:700">Significant</td><td>1&ndash;3 days</td><td>Architecture changes requiring cross-team coordination, downtime planning, IaC refactoring, and phased rollout. Includes prerequisite work like updating Terraform providers, modifying ARM/Bicep templates, and change management approvals.</td><td>Enable Unity Catalog, configure VNet peering, migrate to private endpoints, implement SSO/SCIM, refactor Terraform state</td></tr>
<tr><td style="color:#dc2626;font-weight:700">Project</td><td>1+ weeks</td><td>Major infrastructure migration or org-wide policy rollout. Requires project planning, stakeholder approval, IaC redesign, multi-phase implementation, and extensive testing across environments.</td><td>Full workspace migration, enterprise-wide compliance overhaul, network architecture redesign, Terraform module rewrite</td></tr>
</table>
<h3 style="font-size:15px;font-weight:700;color:#0f172a;margin:24px 0 12px">Common Prerequisites (not included in estimates)</h3>
<p style="font-size:12px;color:#64748b;margin:0 0 10px">The effort estimates above reflect the <strong>core remediation work</strong>. Depending on your organization, you may also need to account for these additional prerequisites:</p>
<table>
<tr><th>Prerequisite</th><th>When Needed</th><th>Typical Additional Time</th></tr>
<tr><td>Terraform / IaC Updates</td><td>When infrastructure is managed via Terraform, Pulumi, ARM/Bicep, or other IaC tools. Changes must be codified, reviewed, and applied through the IaC pipeline rather than the UI.</td><td>+30 min &ndash; 2 hrs per change</td></tr>
<tr><td>CI/CD Pipeline Runs</td><td>When changes must flow through build, test, and deploy pipelines before reaching production.</td><td>+15 min &ndash; 1 hr per deployment</td></tr>
<tr><td>Change Management / CAB Approval</td><td>When your organization requires change tickets, CAB review, or ITSM approvals before production changes.</td><td>+1 &ndash; 5 business days</td></tr>
<tr><td>Non-prod Testing</td><td>When changes must be validated in dev/staging environments before production rollout.</td><td>+1 &ndash; 4 hrs per environment</td></tr>
<tr><td>Provider / Module Upgrades</td><td>When the Terraform azurerm/databricks provider or shared modules must be upgraded to support the required resource type or argument.</td><td>+1 &ndash; 4 hrs</td></tr>
<tr><td>Security Review</td><td>When network, identity, or security teams must review and approve the change before implementation.</td><td>+1 &ndash; 3 business days</td></tr>
<tr><td>Documentation Updates</td><td>When internal runbooks, architecture diagrams, or compliance documentation must be updated to reflect the change.</td><td>+30 min &ndash; 2 hrs</td></tr>
</table>
<h3 style="font-size:15px;font-weight:700;color:#0f172a;margin:24px 0 12px">How Effort Is Estimated</h3>
<table>
<tr><th>Factor</th><th>Description</th></tr>
<tr><td>Configuration Steps</td><td>Number of settings, APIs, or UI steps required to remediate the finding.</td></tr>
<tr><td>Access Requirements</td><td>Whether changes need admin console access, API/IaC changes, or Azure Portal configuration.</td></tr>
<tr><td>Testing &amp; Validation</td><td>Whether the fix needs functional testing, security validation, or user acceptance testing.</td></tr>
<tr><td>Cross-team Coordination</td><td>Whether network, security, identity, or platform teams need to be involved.</td></tr>
<tr><td>Blast Radius</td><td>Whether the change affects a single workspace, multiple workspaces, or the entire organization.</td></tr>
</table>
<p style="font-size:12px;color:#64748b;margin-top:12px;font-style:italic">Note: Effort estimates reflect core remediation work and are approximate. Actual time will vary based on your organization&rsquo;s change management processes, IaC maturity, CI/CD pipeline complexity, and team familiarity with Databricks administration. Add prerequisite time from the table above as applicable.</p>"""

    # ── Build tabs ──
    tabs: list[tuple[str, str, str, str]] = [
        ("summary", "shield", "Summary", summary_content),
        ("comparison", "columns", "Workspace Comparison", comparison_content),
        ("common-issues", "alert-octagon", "Common Issues", common_issues_content),
    ]
    for name, _ in results:
        tid = "ws-" + name.lower().replace(" ", "-").replace(".", "-").replace("_", "-")
        tabs.append((tid, "server", name, ws_tab_content[name]))

    # ── Prioritised Recommendations tab (combined across all workspaces) ──
    # Deduplicate: same check_id across workspaces → one row with workspace count
    all_findings_combined = []
    for _, r in results:
        all_findings_combined.extend(r.findings)
    _raw_prio = _build_prioritised_recommendations(all_findings_combined)
    # Keep worst status (FAIL > WARN) and highest score per check_id
    _STATUS_RANK = {"FAIL": 2, "WARN": 1}
    _dedup: dict[str, dict] = {}
    for item in _raw_prio:
        cid = item["check_id"]
        if cid not in _dedup:
            _dedup[cid] = {**item, "_ws_count": 1}
        else:
            _dedup[cid]["_ws_count"] += 1
            if _STATUS_RANK.get(item["status"], 0) > _STATUS_RANK.get(_dedup[cid]["status"], 0):
                _dedup[cid]["status"] = item["status"]
            if item["priority_score"] > _dedup[cid]["priority_score"]:
                _dedup[cid]["priority_score"] = item["priority_score"]
                _dedup[cid]["priority_label"] = item["priority_label"]
    comb_prio_items = sorted(_dedup.values(), key=lambda x: x["priority_score"], reverse=True)
    if comb_prio_items:
        _CPRIO_COLORS = {"P1": "#dc2626", "P2": "#ea580c", "P3": "#ca8a04", "P4": "#6b7280"}
        _CPRIO_BG = {"P1": "#fef2f2", "P2": "#fff7ed", "P3": "#fefce8", "P4": "#f8fafc"}
        _cprio_counts: dict[str, int] = {}
        for item in comb_prio_items:
            prefix = item["priority_label"][:2]
            _cprio_counts[prefix] = _cprio_counts.get(prefix, 0) + 1
        cprio_dist = ""
        for p in ["P1", "P2", "P3", "P4"]:
            cnt = _cprio_counts.get(p, 0)
            if cnt == 0:
                continue
            cprio_dist += f'<div style="background:{_CPRIO_BG[p]};border:1px solid {_CPRIO_COLORS[p]}33;border-radius:8px;padding:12px 18px;text-align:center"><div style="font-size:24px;font-weight:700;color:{_CPRIO_COLORS[p]}">{cnt}</div><div style="font-size:11px;color:#64748b;text-transform:uppercase">{p}</div></div>'
        cprio_rows_html = ""
        _ccost_total_low = 0
        _ccost_total_high = 0
        _ccost_count = 0
        for item in comb_prio_items:
            prefix = item["priority_label"][:2]
            pc = _CPRIO_COLORS.get(prefix, "#6b7280")
            prio_badge = f'<span style="background:{pc};color:#fff;padding:2px 8px;border-radius:9999px;font-size:11px;font-weight:600">{_esc(item["priority_label"])}</span>'
            cost_td = ""
            if show_cost:
                if item["cost_low"]:
                    cost_td = f'<td style="font-size:12px;text-align:right;white-space:nowrap;color:#b45309">${item["cost_low"]:,} &ndash; ${item["cost_high"]:,}</td>'
                    _ccost_total_low += item["cost_low"]
                    _ccost_total_high += item["cost_high"]
                    _ccost_count += 1
                else:
                    cost_td = '<td style="font-size:11px;color:#94a3b8;text-align:center">&mdash;</td>'
            cprio_rows_html += f"""<tr>
<td>{prio_badge}</td>
<td style="font-weight:700;color:{pc};text-align:center">{item['priority_score']}</td>
<td style="font-family:monospace;font-size:12px">{_esc(item['check_id'])}</td>
<td style="font-size:12px">{_esc(item['category'])}</td>
<td>{sev_badge(item['severity'])}</td><td>{status_badge(item['status'])}</td>
<td style="font-size:12px;white-space:nowrap">{_esc(item['effort'])}</td>
{cost_td}
<td style="text-align:center;font-weight:600">{item['_ws_count']}/{len(results)}</td>
<td>{_esc(item['title'])}</td>
<td style="font-size:12px">{_esc(item['recommendation'])}</td>
<td style="font-size:12px">{_esc(item['benefits']) if item['benefits'] else ''}</td></tr>"""
        _ccost_card = ""
        if show_cost and _ccost_count:
            _ccost_card = f'<div style="background:#fffbeb;border:1px solid #fbbf2433;border-radius:8px;padding:12px 18px;text-align:center"><div style="font-size:20px;font-weight:700;color:#b45309">${_ccost_total_low:,} &ndash; ${_ccost_total_high:,}</div><div style="font-size:11px;color:#64748b;text-transform:uppercase">Est. Monthly Cost</div></div>'
        _ccost_explain = ""
        if show_cost:
            _ccost_explain = """<br><strong>Est. Cost</strong> = estimated monthly cloud operational cost of the misconfiguration (per workspace).
<em>Cost figures are illustrative examples only &mdash; actual costs vary with usage, region, and pricing tier.</em>"""
        _ccost_th = '<th>Est. Cost ($/mo)</th>' if show_cost else ''
        cprio_content = f"""<p style="font-size:13px;color:#475569;margin-bottom:16px">
{len(comb_prio_items)} unique actionable check{"s" if len(comb_prio_items) != 1 else ""} across {len(results)} workspace(s), ranked by <strong>Priority Score</strong>
(severity weight &times; effort multiplier). High-severity quick fixes appear first.
</p>
<div style="display:flex;gap:16px;margin-bottom:20px;flex-wrap:wrap">{cprio_dist}{_ccost_card}</div>
<div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;padding:14px 18px;margin-bottom:18px;font-size:12px;color:#64748b">
<strong>How it works:</strong> Priority Score = Severity Weight (Critical=10, High=7, Medium=4, Low=2)
&times; Effort Multiplier (Quick Fix=4&times;, Moderate=3&times;, Significant=2&times;, Project=1&times;).
Higher score = fix first.
<br><strong>P1</strong> &ge;28 &middot; <strong>P2</strong> 16&ndash;27 &middot; <strong>P3</strong> 7&ndash;15 &middot; <strong>P4</strong> &lt;7
{_ccost_explain}
</div>
<div style="overflow-x:auto"><table style="min-width:{'1000' if show_cost else '900'}px">
<tr><th>Priority</th><th>Score</th><th>Check ID</th><th>Category</th><th>Severity</th><th>Status</th><th>Effort</th>{_ccost_th}<th>Workspaces</th><th>Title</th><th>Recommendation</th><th>Why It Matters</th></tr>
{cprio_rows_html}</table></div>"""
        tabs.append(("prioritised", "arrow-up-circle", f"Prioritised ({len(comb_prio_items)})", cprio_content))

    # ── API Endpoints tab (combined across all workspaces) ──
    all_ep_summaries = [r.endpoint_summary for _, r in results if r.endpoint_summary and r.endpoint_summary.get("endpoints")]
    if all_ep_summaries:
        # Merge endpoint summaries across workspaces
        merged_ep: dict[str, dict] = {}
        for ep_sum in all_ep_summaries:
            for e in ep_sum["endpoints"]:
                key = e["endpoint"]
                if key not in merged_ep:
                    merged_ep[key] = {"endpoint": key, "status": e["status"], "items_count": e["items_count"], "error_code": e.get("error_code", 0)}
                else:
                    existing = merged_ep[key]
                    existing["items_count"] = max(existing["items_count"], e["items_count"])
                    if e["status"] == "items":
                        existing["status"] = "items"
                    elif e["status"] == "config" and existing["status"] not in ("items",):
                        existing["status"] = "config"
                    elif e["status"] == "error" and existing["status"] not in ("items", "config"):
                        existing["status"] = "error"
                        existing["error_code"] = e.get("error_code", 0)
        ep_with = sum(1 for e in merged_ep.values() if e["status"] == "items")
        ep_conf = sum(1 for e in merged_ep.values() if e["status"] == "config")
        ep_empty = sum(1 for e in merged_ep.values() if e["status"] == "empty")
        ep_err = sum(1 for e in merged_ep.values() if e["status"] == "error")
        ep_total = len(merged_ep)
        ep_rows = ""
        for e in sorted(merged_ep.values(), key=lambda x: ({"items": 0, "config": 1, "empty": 2, "error": 3}.get(x["status"], 4), x["endpoint"])):
            if e["status"] == "items":
                icon = '<span style="color:#16a34a;font-weight:700">&#10003;</span>'
                count_text = f'{e["items_count"]} item{"s" if e["items_count"] != 1 else ""}'
                count_style = "color:#16a34a;font-weight:600"
            elif e["status"] == "config":
                icon = '<span style="color:#ca8a04;font-weight:700">&#9881;</span>'
                count_text = "config/settings"
                count_style = "color:#ca8a04;font-weight:600"
            elif e["status"] == "error":
                icon = '<span style="color:#dc2626;font-weight:700">&#10007;</span>'
                count_text = f'HTTP {e["error_code"]}'
                count_style = "color:#dc2626;font-weight:600"
            else:
                icon = '<span style="color:#94a3b8">&#9675;</span>'
                count_text = "0 items"
                count_style = "color:#94a3b8"
            ep_rows += f'<tr><td>{icon}</td><td style="font-family:monospace;font-size:12px">{_esc(e["endpoint"])}</td><td style="{count_style}">{count_text}</td></tr>'
        ep_summary_content = f"""<div style="display:flex;gap:24px;margin-bottom:20px;font-size:14px;flex-wrap:wrap">
<div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:8px;padding:12px 18px;text-align:center"><div style="font-size:24px;font-weight:700;color:#16a34a">{ep_with}</div><div style="font-size:11px;color:#64748b;text-transform:uppercase">With Items</div></div>
<div style="background:#fefce8;border:1px solid #fef08a;border-radius:8px;padding:12px 18px;text-align:center"><div style="font-size:24px;font-weight:700;color:#ca8a04">{ep_conf}</div><div style="font-size:11px;color:#64748b;text-transform:uppercase">Config/Settings</div></div>
<div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;padding:12px 18px;text-align:center"><div style="font-size:24px;font-weight:700;color:#94a3b8">{ep_empty}</div><div style="font-size:11px;color:#64748b;text-transform:uppercase">Empty</div></div>
<div style="background:#fef2f2;border:1px solid #fca5a5;border-radius:8px;padding:12px 18px;text-align:center"><div style="font-size:24px;font-weight:700;color:#dc2626">{ep_err}</div><div style="font-size:11px;color:#64748b;text-transform:uppercase">Errors</div></div>
<div style="background:#eff6ff;border:1px solid #bfdbfe;border-radius:8px;padding:12px 18px;text-align:center"><div style="font-size:24px;font-weight:700;color:#3b82f6">{ep_total}</div><div style="font-size:11px;color:#64748b;text-transform:uppercase">Total</div></div>
</div>
<div style="overflow-x:auto"><table style="min-width:500px">
<tr><th style="width:30px"></th><th>Endpoint</th><th>Result</th></tr>
{ep_rows}</table></div>"""
        tabs.append(("api-endpoints", "globe", "API Endpoints", ep_summary_content))

    # ── All Checks Reference tab ──
    _cat_order_cref: list[str] = []
    _cat_checks_cref: dict[str, list[str]] = {}
    for cid, cdata in SAT_CHECKS.items():
        cat = cdata.get("category", "Other")
        if cat not in _cat_checks_cref:
            _cat_order_cref.append(cat)
            _cat_checks_cref[cat] = []
        _cat_checks_cref[cat].append(cid)
    cref_rows = ""
    _sev_sort_c = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    for cat in _cat_order_cref:
        sorted_ids = sorted(_cat_checks_cref[cat], key=lambda c: (_sev_sort_c.get(SAT_CHECKS[c].get("severity", "low"), 3), c))
        for cid in sorted_ids:
            ck = SAT_CHECKS[cid]
            ref_url = ck.get("reference_url", "")
            ref_link = f'<a href="{_esc(ref_url)}" target="_blank" style="color:#3b82f6;text-decoration:none;font-size:11px">Docs</a>' if ref_url else ""
            _cref_effort_td = f'<td style="font-size:12px;white-space:nowrap">{_esc(_get_effort(cid))}</td>' if show_effort else ''
            cref_rows += f'<tr><td style="font-family:monospace;font-size:12px;white-space:nowrap">{_esc(cid)}</td><td>{_esc(cat)}</td><td>{sev_badge(ck.get("severity", "low"))}</td>{_cref_effort_td}<td><strong>{_esc(ck.get("title", ""))}</strong></td><td style="font-size:12px">{_esc(ck.get("description", ""))}</td><td style="font-size:12px">{_esc(ck.get("recommendation", ""))}</td><td>{ref_link}</td></tr>\n'
    _sev_counts_c = {}
    for ck in SAT_CHECKS.values():
        s = ck.get("severity", "low")
        _sev_counts_c[s] = _sev_counts_c.get(s, 0) + 1
    sev_chips_c = " ".join(f'{sev_badge(s)} <span style="font-size:13px;margin-right:12px">{_sev_counts_c.get(s,0)}</span>' for s in ["critical", "high", "medium", "low"])
    checks_ref_content_c = f"""<div style="margin-bottom:16px;display:flex;gap:16px;align-items:center;flex-wrap:wrap">
<div style="font-size:15px;font-weight:600;color:#1e293b">Total: {len(SAT_CHECKS)} checks</div>
{sev_chips_c}
</div>
<div style="overflow-x:auto"><table style="min-width:900px">
<tr><th>Check ID</th><th>Category</th><th>Severity</th>{'<th>Effort</th>' if show_effort else ''}<th>Title</th><th>Description</th><th>Recommendation</th><th>Docs</th></tr>
{cref_rows}</table></div>"""
    tabs.append(("checks-reference", "list-checks", "All Checks ({})".format(len(SAT_CHECKS)), checks_ref_content_c))

    tabs.append(("definitions", "book-open", "Definitions", definitions_content))

    # Tab buttons & panels
    tab_buttons = ""
    for i, (tid, icon_name, label, _) in enumerate(tabs):
        active = " active" if i == 0 else ""
        tab_buttons += f'<button class="tab-btn{active}" data-tab="{tid}">{_icon(icon_name, 15)} {_esc(label)}</button>\n'

    tab_panels = ""
    for i, (tid, icon_name, label, content) in enumerate(tabs):
        display = "block" if i == 0 else "none"
        tab_panels += f'<div class="tab-panel" id="panel-{tid}" style="display:{display}">\n  <div class="card">\n    <h2>{_icon(icon_name, 18)} {_esc(label)}</h2>\n    {content}\n  </div>\n</div>\n'

    # ── KPI cards ──
    avg_color = "#16a34a" if avg_score >= 80 else ("#ca8a04" if avg_score >= 60 else "#dc2626")
    avg_grade = "Good" if avg_score >= 80 else ("Needs Improvement" if avg_score >= 60 else "Critical")
    _sub2 = '<div style="font-size:12px;color:#64748b;margin-top:4px">'
    kpis_html = f"""<div class="kpis">
<div class="kpi"><div class="label">Average Score</div><div class="value" style="color:{avg_color}">{avg_score}/100</div>{_sub2}{_esc(avg_grade)}</div></div>
<div class="kpi"><div class="label">Workspaces</div><div class="value" style="color:#374151">{len(results)}</div>{_sub2}scanned</div></div>
<div class="kpi"><div class="label">Total Checks</div><div class="value" style="color:#374151">{total_checks_all}</div>{_sub2}across all workspaces</div></div>
<div class="kpi"><div class="label">Total Passed</div><div class="value" style="color:#16a34a">{total_passed}</div>{_sub2}no penalty</div></div>
<div class="kpi"><div class="label">Total Failed</div><div class="value" style="color:#dc2626">{total_failed}</div>{_sub2}full penalty</div></div>
<div class="kpi"><div class="label">Total {"Warning" if total_warnings == 1 else "Warnings"}</div><div class="value" style="color:#ca8a04">{total_warnings}</div>{_sub2}half penalty</div></div>
<div class="kpi"><div class="label">N/A</div><div class="value" style="color:#6b7280">{total_na}</div>{_sub2}excluded from score</div></div>
<div class="kpi"><div class="label">{"API Error" if total_api_errors == 1 else "API Errors"}</div><div class="value" style="color:#7c3aed">{total_api_errors}</div>{_sub2}excluded from score</div></div>
</div>"""

    # ── Full HTML ──
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>SAT &mdash; Subscription Summary</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
          background: #f8fafc; color: #1e293b; }}
  .header {{ background: linear-gradient(135deg, #1e3a5f 0%, #0f2447 100%);
             color: white; padding: 24px 32px; }}
  .header h1 {{ font-size: 22px; margin-bottom: 4px; }}
  .header .sub {{ opacity: .8; font-size: 13px; }}
  .layout {{ display: flex; height: calc(100vh - 80px); }}
  .sidebar {{ width: 220px; min-width: 220px; background: #0f172a;
              overflow-y: auto; padding: 12px 0; }}
  .tab-btn {{ display: flex; align-items: center; width: 100%; padding: 10px 16px;
              border: none; background: none; color: #94a3b8; font-size: 13px;
              cursor: pointer; text-align: left; transition: all .15s; gap: 4px; }}
  .tab-btn:hover {{ background: #1e293b; color: #e2e8f0; }}
  .tab-btn.active {{ background: #1e3a5f; color: #ffffff; font-weight: 600;
                     border-left: 3px solid #3b82f6; }}
  .sidebar .sep {{ height: 1px; background: #1e293b; margin: 8px 16px; }}
  .sidebar .sep-label {{ font-size: 10px; color: #475569; text-transform: uppercase;
                         letter-spacing: .08em; padding: 8px 16px 4px; }}
  .main {{ flex: 1; overflow-y: auto; padding: 24px 32px; }}
  .kpis {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(130px, 1fr)); gap: 16px; margin-bottom: 24px; }}
  .kpi {{ background: white; border: 1px solid #e2e8f0; border-radius: 10px; padding: 18px 22px; }}
  .kpi .value {{ font-size: 26px; font-weight: 700; margin: 4px 0; }}
  .kpi .label {{ font-size: 11px; color: #64748b; text-transform: uppercase; letter-spacing: .05em; }}
  .card {{ background: white; border: 1px solid #e2e8f0; border-radius: 10px; padding: 24px; }}
  h2 {{ font-size: 16px; font-weight: 600; margin-bottom: 16px; color: #0f172a; }}
  h3 {{ font-size: 15px; font-weight: 700; color: #0f172a; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
  th {{ text-align: left; padding: 8px 10px; background: #f1f5f9;
        font-size: 11px; text-transform: uppercase; letter-spacing: .05em; color: #475569;
        border-bottom: 1px solid #e2e8f0; position: sticky; top: 0; z-index: 1; }}
  td {{ padding: 8px 10px; border-bottom: 1px solid #f1f5f9; }}
  tr:hover td {{ background: #f8fafc; }}
  .footer {{ text-align: center; color: #94a3b8; font-size: 11px; padding: 16px; }}
  @media (max-width: 900px) {{
    .layout {{ flex-direction: column; height: auto; }}
    .sidebar {{ width: 100%; min-width: unset; display: flex; flex-wrap: wrap;
                padding: 8px; gap: 4px; }}
    .sidebar .sep, .sidebar .sep-label {{ display: none; }}
    .tab-btn {{ width: auto; padding: 6px 12px; font-size: 12px;
                border-radius: 6px; }}
    .tab-btn.active {{ border-left: none; border-bottom: 2px solid #3b82f6; }}
    .main {{ padding: 16px; }}
    .kpis {{ grid-template-columns: repeat(2, 1fr); }}
  }}
  .tip {{ position: relative; cursor: help; }}
  .tip .tip-text {{ visibility: hidden; opacity: 0; position: absolute; bottom: calc(100% + 8px);
    left: 50%; transform: translateX(-50%); background: #1e293b; color: #f1f5f9;
    padding: 6px 10px; border-radius: 6px; font-size: 11px; font-weight: 400;
    white-space: nowrap; z-index: 100; pointer-events: none;
    transition: opacity .15s; text-transform: none; letter-spacing: normal; }}
  .tip .tip-text::after {{ content: ''; position: absolute; top: 100%; left: 50%;
    margin-left: -5px; border: 5px solid transparent; border-top-color: #1e293b; }}
  .tip:hover .tip-text {{ visibility: visible; opacity: 1; }}
  .search-bar {{ position: relative; margin-bottom: 16px; }}
  .search-bar input {{ width: 100%; padding: 10px 16px 10px 40px; border: 1px solid #e2e8f0;
    border-radius: 8px; font-size: 14px; outline: none; transition: border-color .15s;
    background: #fff url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='16' height='16' fill='none' stroke='%2394a3b8' stroke-width='2' stroke-linecap='round'%3E%3Ccircle cx='7' cy='7' r='5'/%3E%3Cline x1='11' y1='11' x2='15' y2='15'/%3E%3C/svg%3E") 12px center no-repeat; }}
  .search-bar input:focus {{ border-color: #3b82f6; box-shadow: 0 0 0 3px rgba(59,130,246,.15); }}
  .search-count {{ position: absolute; right: 12px; top: 50%; transform: translateY(-50%);
    font-size: 12px; color: #64748b; }}
  .search-badge {{ display: none; margin-left: auto; padding: 1px 7px; border-radius: 10px;
    font-size: 11px; font-weight: 600; background: #3b82f6; color: #fff; }}
</style>
<script src="https://unpkg.com/lucide@latest"></script>
</head>
<body>
<div class="header">
  <h1>{_icon('shield', 24)} SAT Subscription Summary</h1>
  <div class="sub">Security Analysis Tool &middot; {len(results)} workspace(s) scanned &middot; {_esc(ts)}</div>
</div>
<div class="layout">
  <nav class="sidebar">
    {tab_buttons}
  </nav>
  <div class="main">
    {kpis_html}
    <div class="search-bar">
      <input type="text" id="searchInput" placeholder="Search checks, categories, keywords..." oninput="doSearch(this.value)">
      <span id="searchCount" class="search-count"></span>
    </div>
    {tab_panels}
    <div class="footer">Generated by SAT Scanner CLI &middot; {datetime.now().strftime('%Y-%m-%d')}</div>
  </div>
</div>
<script>
lucide.createIcons();
function doSearch(q) {{
  q = q.toLowerCase().trim();
  var total = 0;
  document.querySelectorAll('.tab-panel').forEach(function(panel) {{
    var tabCount = 0;
    var rows = panel.querySelectorAll('tr');
    for (var i = 0; i < rows.length; i++) {{
      if (rows[i].querySelector('th')) continue;
      var text = rows[i].textContent.toLowerCase();
      if (!q || text.indexOf(q) >= 0) {{
        rows[i].style.display = '';
        if (q) tabCount++;
      }} else {{
        rows[i].style.display = 'none';
      }}
    }}
    var cards = panel.querySelectorAll('.finding-card');
    for (var j = 0; j < cards.length; j++) {{
      var ct = cards[j].textContent.toLowerCase();
      if (!q || ct.indexOf(q) >= 0) {{
        cards[j].style.display = '';
        if (q) tabCount++;
      }} else {{
        cards[j].style.display = 'none';
      }}
    }}
    total += tabCount;
    var tabId = panel.id.replace('panel-', '');
    var btn = document.querySelector('.tab-btn[data-tab="' + tabId + '"]');
    if (btn) {{
      var b = btn.querySelector('.search-badge');
      if (!b) {{ b = document.createElement('span'); b.className = 'search-badge'; btn.appendChild(b); }}
      b.textContent = (q && tabCount) ? tabCount : '';
      b.style.display = (q && tabCount) ? 'inline-block' : 'none';
    }}
  }});
  var badge = document.getElementById('searchCount');
  if (badge) badge.textContent = q ? total + ' match' + (total !== 1 ? 'es' : '') : '';
}}
document.querySelectorAll('.tab-btn').forEach(function(btn) {{
  btn.addEventListener('click', function() {{
    document.querySelectorAll('.tab-btn').forEach(function(b) {{ b.classList.remove('active'); }});
    document.querySelectorAll('.tab-panel').forEach(function(p) {{ p.style.display = 'none'; }});
    btn.classList.add('active');
    var panel = document.getElementById('panel-' + btn.getAttribute('data-tab'));
    if (panel) panel.style.display = 'block';
    var si = document.getElementById('searchInput');
    if (si) doSearch(si.value);
  }});
}});
</script>
</body></html>"""

    path = output_dir / f"sat-combined-summary-{datetime.now().strftime('%Y-%m-%d')}.html"
    path.write_text(html, encoding="utf-8")
    print(f"  📊 Combined HTML  → {path}")
