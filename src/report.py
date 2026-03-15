import json
import logging
import os
from datetime import datetime

logger = logging.getLogger(__name__)


def generate_report(summary: dict, config: dict, output_dir: str = "reports") -> dict:
    """Generate JSON and HTML reports from clone summary.

    Returns dict with paths to generated report files.
    """
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    source = config["source_catalog"]
    dest = config["destination_catalog"]

    report_data = {
        "timestamp": datetime.now().isoformat(),
        "source_catalog": source,
        "destination_catalog": dest,
        "clone_type": config["clone_type"],
        "load_type": config["load_type"],
        "dry_run": config.get("dry_run", False),
        "summary": summary,
    }

    # Generate JSON report
    json_path = os.path.join(output_dir, f"clone_report_{timestamp}.json")
    with open(json_path, "w") as f:
        json.dump(report_data, f, indent=2)
    logger.info(f"JSON report saved: {json_path}")

    # Generate HTML report
    html_path = os.path.join(output_dir, f"clone_report_{timestamp}.html")
    html_content = _build_html_report(report_data)
    with open(html_path, "w") as f:
        f.write(html_content)
    logger.info(f"HTML report saved: {html_path}")

    return {"json": json_path, "html": html_path}


def _build_html_report(data: dict) -> str:
    """Build an HTML report from report data."""
    summary = data["summary"]
    dry_run_badge = '<span class="badge dry-run">DRY RUN</span>' if data["dry_run"] else ""

    # Build object type rows
    obj_rows = ""
    total_success = 0
    total_failed = 0
    total_skipped = 0
    for obj_type in ("tables", "views", "functions", "volumes"):
        stats = summary.get(obj_type, {})
        s = stats.get("success", 0)
        f = stats.get("failed", 0)
        sk = stats.get("skipped", 0)
        total_success += s
        total_failed += f
        total_skipped += sk
        row_class = "error-row" if f > 0 else ""
        obj_rows += f"""
            <tr class="{row_class}">
                <td>{obj_type.capitalize()}</td>
                <td class="success">{s}</td>
                <td class="failed">{f}</td>
                <td class="skipped">{sk}</td>
                <td>{s + f + sk}</td>
            </tr>"""

    # Build error rows
    error_rows = ""
    errors = summary.get("errors", [])
    if errors:
        for err in errors:
            error_rows += f"<tr><td>{err}</td></tr>"
    else:
        error_rows = '<tr><td class="no-errors">No errors</td></tr>'

    status_class = "status-success" if total_failed == 0 and not errors else "status-failed"
    status_text = "COMPLETED" if total_failed == 0 and not errors else "COMPLETED WITH ERRORS"

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Clone Report - {data['source_catalog']} to {data['destination_catalog']}</title>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
               max-width: 900px; margin: 40px auto; padding: 0 20px; background: #f5f5f5; }}
        .report {{ background: white; border-radius: 8px; padding: 30px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
        h1 {{ color: #1a1a2e; border-bottom: 2px solid #e94560; padding-bottom: 10px; }}
        h2 {{ color: #16213e; margin-top: 30px; }}
        .meta {{ display: grid; grid-template-columns: 1fr 1fr; gap: 10px; margin: 20px 0;
                 background: #f8f9fa; padding: 15px; border-radius: 6px; }}
        .meta-item {{ display: flex; justify-content: space-between; }}
        .meta-label {{ font-weight: 600; color: #555; }}
        .meta-value {{ color: #1a1a2e; }}
        table {{ width: 100%; border-collapse: collapse; margin: 15px 0; }}
        th {{ background: #1a1a2e; color: white; padding: 10px 15px; text-align: left; }}
        td {{ padding: 10px 15px; border-bottom: 1px solid #eee; }}
        tr:hover {{ background: #f8f9fa; }}
        .success {{ color: #27ae60; font-weight: 600; }}
        .failed {{ color: #e74c3c; font-weight: 600; }}
        .skipped {{ color: #f39c12; font-weight: 600; }}
        .error-row {{ background: #fdf2f2; }}
        .no-errors {{ color: #27ae60; font-style: italic; }}
        .badge {{ padding: 4px 12px; border-radius: 12px; font-size: 12px; font-weight: 600; }}
        .dry-run {{ background: #fff3cd; color: #856404; }}
        .status-success {{ background: #d4edda; color: #155724; padding: 8px 16px; border-radius: 6px;
                          display: inline-block; font-weight: 600; }}
        .status-failed {{ background: #f8d7da; color: #721c24; padding: 8px 16px; border-radius: 6px;
                         display: inline-block; font-weight: 600; }}
        .totals {{ font-weight: 700; background: #f0f0f0; }}
        .footer {{ margin-top: 30px; text-align: center; color: #999; font-size: 12px; }}
    </style>
</head>
<body>
<div class="report">
    <h1>Unity Catalog Clone Report {dry_run_badge}</h1>

    <div class="meta">
        <div class="meta-item"><span class="meta-label">Source Catalog</span>
            <span class="meta-value">{data['source_catalog']}</span></div>
        <div class="meta-item"><span class="meta-label">Destination Catalog</span>
            <span class="meta-value">{data['destination_catalog']}</span></div>
        <div class="meta-item"><span class="meta-label">Clone Type</span>
            <span class="meta-value">{data['clone_type']}</span></div>
        <div class="meta-item"><span class="meta-label">Load Type</span>
            <span class="meta-value">{data['load_type']}</span></div>
        <div class="meta-item"><span class="meta-label">Timestamp</span>
            <span class="meta-value">{data['timestamp']}</span></div>
        <div class="meta-item"><span class="meta-label">Schemas Processed</span>
            <span class="meta-value">{summary.get('schemas_processed', 0)}</span></div>
    </div>

    <span class="{status_class}">{status_text}</span>

    <h2>Clone Summary</h2>
    <table>
        <thead>
            <tr><th>Object Type</th><th>Success</th><th>Failed</th><th>Skipped</th><th>Total</th></tr>
        </thead>
        <tbody>
            {obj_rows}
            <tr class="totals">
                <td>Total</td>
                <td class="success">{total_success}</td>
                <td class="failed">{total_failed}</td>
                <td class="skipped">{total_skipped}</td>
                <td>{total_success + total_failed + total_skipped}</td>
            </tr>
        </tbody>
    </table>

    <h2>Errors</h2>
    <table>
        <thead><tr><th>Error Details</th></tr></thead>
        <tbody>{error_rows}</tbody>
    </table>

    <div class="footer">Generated by Clone-Xs</div>
</div>
</body>
</html>"""
