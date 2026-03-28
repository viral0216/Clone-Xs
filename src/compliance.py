"""Compliance report generation — audit-ready reports combining multiple data sources."""

import json
import logging
import os
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


def generate_compliance_report(
    client, warehouse_id: str, config: dict,
    from_date: str | None = None, to_date: str | None = None,
    output_dir: str = "reports/compliance",
    output_format: str = "all",
) -> dict:
    """Generate a comprehensive compliance report.

    Combines data from audit trail, PII detection, permissions, lineage, and validation.
    """
    report = {
        "report_metadata": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "generated_by": _get_user(client),
            "from_date": from_date,
            "to_date": to_date,
            "clone_xs_version": "0.4.0",
        },
        "clone_operations_summary": _gather_audit_data(client, warehouse_id, config, from_date, to_date),
        "pii_handling": _gather_pii_data(client, warehouse_id, config),
        "permission_audit": _gather_permission_data(client, warehouse_id, config),
        "data_lineage": _gather_lineage_data(client, warehouse_id, config, from_date, to_date),
        "validation_results": _gather_validation_data(client, warehouse_id, config),
        "rtbf_compliance": _gather_rtbf_data(client, warehouse_id, config),
    }

    os.makedirs(output_dir, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    paths = {}

    if output_format in ("json", "all"):
        json_path = os.path.join(output_dir, f"compliance_report_{ts}.json")
        with open(json_path, "w") as f:
            json.dump(report, f, indent=2, default=str)
        paths["json"] = json_path
        logger.info(f"Compliance report (JSON): {json_path}")

    if output_format in ("html", "all"):
        html_path = os.path.join(output_dir, f"compliance_report_{ts}.html")
        html_content = _build_compliance_html(report)
        with open(html_path, "w") as f:
            f.write(html_content)
        paths["html"] = html_path
        logger.info(f"Compliance report (HTML): {html_path}")

    return {"report": report, "paths": paths}


def _get_user(client) -> str:
    try:
        me = client.current_user.me()
        return me.user_name or me.display_name or "unknown"
    except Exception:
        return "unknown"


def _gather_audit_data(client, warehouse_id: str, config: dict,
                       from_date: str | None, to_date: str | None) -> dict:
    """Query audit trail for clone operations."""
    from src.client import execute_sql

    audit_config = config.get("audit")
    if not audit_config:
        return {"available": False, "message": "Audit trail not configured"}

    catalog = audit_config.get("catalog", "clone_audit")
    schema = audit_config.get("schema", "logs")
    table = audit_config.get("table", "clone_audit_log")
    fqn = f"`{catalog}`.`{schema}`.`{table}`"

    conditions = []
    if from_date:
        conditions.append(f"started_at >= '{from_date}'")
    if to_date:
        conditions.append(f"started_at <= '{to_date}'")
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    try:
        sql = f"SELECT * FROM {fqn} {where} ORDER BY started_at DESC LIMIT 1000"
        operations = execute_sql(client, warehouse_id, sql)
        total_ops = len(operations)
        successful = sum(1 for o in operations if o.get("status") == "SUCCESS")
        failed = total_ops - successful

        return {
            "available": True,
            "total_operations": total_ops,
            "successful": successful,
            "failed": failed,
            "operations": operations[:50],  # limit detail
        }
    except Exception as e:
        return {"available": False, "message": str(e)}


def _gather_pii_data(client, warehouse_id: str, config: dict) -> dict:
    """Gather PII detection and masking data."""
    try:
        from src.pii_detection import detect_pii_by_column_names
        dest = config.get("destination_catalog", "")
        if not dest:
            return {"available": False, "message": "No destination catalog configured"}

        findings = detect_pii_by_column_names(client, warehouse_id, dest,
                                               config.get("exclude_schemas", []))

        masking_rules = config.get("masking_rules", [])
        masked_columns = {r.get("column", "") for r in masking_rules} if masking_rules else set()

        unmasked_pii = [f for f in findings if f.get("column") not in masked_columns]

        return {
            "available": True,
            "total_pii_columns": len(findings),
            "masking_rules_applied": len(masking_rules) if masking_rules else 0,
            "unmasked_pii_warnings": len(unmasked_pii),
            "findings": findings[:100],
            "unmasked": unmasked_pii[:20],
        }
    except Exception as e:
        return {"available": False, "message": str(e)}


def _gather_permission_data(client, warehouse_id: str, config: dict) -> dict:
    """Gather permission audit data."""
    from src.client import execute_sql

    dest = config.get("destination_catalog", "")
    if not dest:
        return {"available": False, "message": "No destination catalog configured"}

    try:
        sql = f"SHOW GRANTS ON CATALOG `{dest}`"
        grants = execute_sql(client, warehouse_id, sql)
        return {
            "available": True,
            "catalog": dest,
            "total_grants": len(grants),
            "grants": grants,
        }
    except Exception as e:
        return {"available": False, "message": str(e)}


def _gather_lineage_data(client, warehouse_id: str, config: dict,
                         from_date: str | None, to_date: str | None) -> dict:
    """Gather data lineage information."""
    from src.client import execute_sql

    lineage_config = config.get("lineage")
    if not lineage_config:
        return {"available": False, "message": "Lineage tracking not configured"}

    catalog = lineage_config.get("catalog", "clone_audit")
    schema = lineage_config.get("schema", "lineage")
    table = lineage_config.get("table", "clone_lineage")
    fqn = f"`{catalog}`.`{schema}`.`{table}`"

    conditions = []
    if from_date:
        conditions.append(f"cloned_at >= '{from_date}'")
    if to_date:
        conditions.append(f"cloned_at <= '{to_date}'")
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    try:
        sql = f"SELECT * FROM {fqn} {where} ORDER BY cloned_at DESC LIMIT 500"
        records = execute_sql(client, warehouse_id, sql)
        return {
            "available": True,
            "total_records": len(records),
            "records": records[:100],
        }
    except Exception as e:
        return {"available": False, "message": str(e)}


def _gather_validation_data(client, warehouse_id: str, config: dict) -> dict:
    """Gather validation results."""
    return {
        "available": config.get("validate_after_clone", False),
        "checksum_enabled": config.get("validate_checksum", False),
        "message": "Validation data from latest clone operation" if config.get("validate_after_clone") else "Validation not enabled",
    }


def _gather_rtbf_data(client, warehouse_id: str, config: dict) -> dict:
    """Gather RTBF (Right to Be Forgotten) compliance data."""
    from src.client import execute_sql

    audit_catalog = config.get("audit_trail", {}).get("catalog", "clone_audit")
    requests_table = f"{audit_catalog}.rtbf.rtbf_requests"

    try:
        sql = f"""
        SELECT
            COUNT(*) AS total_requests,
            SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed,
            SUM(CASE WHEN status NOT IN ('completed', 'cancelled') AND deadline < current_timestamp()
                THEN 1 ELSE 0 END) AS overdue,
            AVG(CASE WHEN completed_at IS NOT NULL
                THEN DATEDIFF(completed_at, created_at) END) AS avg_processing_days
        FROM {requests_table}
        """
        rows = execute_sql(client, warehouse_id, sql)
        stats = rows[0] if rows else {}

        total = int(stats.get("total_requests", 0) or 0)
        completed = int(stats.get("completed", 0) or 0)
        overdue = int(stats.get("overdue", 0) or 0)

        return {
            "available": True,
            "total_requests": total,
            "completed": completed,
            "overdue": overdue,
            "completion_rate": f"{(completed / total * 100):.1f}%" if total > 0 else "N/A",
            "avg_processing_days": round(float(stats.get("avg_processing_days", 0) or 0), 1),
            "compliant": overdue == 0,
        }
    except Exception as e:
        return {"available": False, "message": str(e)}


def _build_compliance_html(report: dict) -> str:
    """Generate HTML compliance report."""
    meta = report["report_metadata"]
    ops = report["clone_operations_summary"]
    pii = report["pii_handling"]
    perms = report["permission_audit"]
    lineage = report["data_lineage"]
    validation = report["validation_results"]

    html = f"""<!DOCTYPE html>
<html>
<head>
<title>Clone-Xs Compliance Report</title>
<style>
body {{ font-family: -apple-system, sans-serif; margin: 20px; background: #f5f5f5; color: #333; }}
.container {{ max-width: 1000px; margin: 0 auto; background: white; padding: 30px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
h1 {{ color: #1a1a1a; border-bottom: 3px solid #2196F3; padding-bottom: 10px; }}
h2 {{ color: #1976D2; margin-top: 30px; border-bottom: 1px solid #e0e0e0; padding-bottom: 8px; }}
table {{ border-collapse: collapse; width: 100%; margin: 15px 0; }}
th, td {{ border: 1px solid #ddd; padding: 10px 12px; text-align: left; }}
th {{ background: #f0f0f0; font-weight: 600; }}
.badge {{ display: inline-block; padding: 3px 10px; border-radius: 12px; font-size: 12px; font-weight: 600; }}
.badge-success {{ background: #d4edda; color: #155724; }}
.badge-warning {{ background: #fff3cd; color: #856404; }}
.badge-danger {{ background: #f8d7da; color: #721c24; }}
.badge-info {{ background: #cce5ff; color: #004085; }}
.meta {{ color: #666; font-size: 14px; margin-bottom: 20px; }}
.stat {{ font-size: 24px; font-weight: 700; color: #1976D2; }}
.stat-label {{ font-size: 12px; color: #666; text-transform: uppercase; }}
.stats-grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 15px; margin: 15px 0; }}
.stat-card {{ background: #f8f9fa; padding: 15px; border-radius: 6px; text-align: center; }}
</style>
</head>
<body>
<div class="container">
<h1>Compliance Report</h1>
<p class="meta">Generated: {meta.get('generated_at', '')} | By: {meta.get('generated_by', '')} | Period: {meta.get('from_date', 'all')} to {meta.get('to_date', 'present')}</p>

<h2>Clone Operations</h2>
"""
    if ops.get("available"):
        html += f"""
<div class="stats-grid">
<div class="stat-card"><div class="stat">{ops.get('total_operations', 0)}</div><div class="stat-label">Total Operations</div></div>
<div class="stat-card"><div class="stat">{ops.get('successful', 0)}</div><div class="stat-label">Successful</div></div>
<div class="stat-card"><div class="stat">{ops.get('failed', 0)}</div><div class="stat-label">Failed</div></div>
</div>"""
    else:
        html += f'<p>{ops.get("message", "Not available")}</p>'

    html += "<h2>PII Handling</h2>"
    if pii.get("available"):
        html += f"""
<div class="stats-grid">
<div class="stat-card"><div class="stat">{pii.get('total_pii_columns', 0)}</div><div class="stat-label">PII Columns Detected</div></div>
<div class="stat-card"><div class="stat">{pii.get('masking_rules_applied', 0)}</div><div class="stat-label">Masking Rules Applied</div></div>
<div class="stat-card"><div class="stat">{pii.get('unmasked_pii_warnings', 0)}</div><div class="stat-label">Unmasked PII Warnings</div></div>
</div>"""
        if pii.get("unmasked"):
            html += '<p><span class="badge badge-warning">Warning</span> The following PII columns have no masking rules:</p><ul>'
            for u in pii["unmasked"]:
                html += f'<li>{u.get("column", "?")} ({u.get("pii_type", "?")})</li>'
            html += "</ul>"
    else:
        html += f'<p>{pii.get("message", "Not available")}</p>'

    html += "<h2>Permission Audit</h2>"
    if perms.get("available"):
        html += f'<p>Catalog: <strong>{perms.get("catalog", "")}</strong> | Total grants: {perms.get("total_grants", 0)}</p>'
        if perms.get("grants"):
            html += "<table><tr><th>Principal</th><th>Privilege</th></tr>"
            for g in perms["grants"][:30]:
                html += f'<tr><td>{g.get("Principal", g.get("principal", ""))}</td><td>{g.get("ActionType", g.get("privilege", ""))}</td></tr>'
            html += "</table>"
    else:
        html += f'<p>{perms.get("message", "Not available")}</p>'

    html += "<h2>Data Lineage</h2>"
    if lineage.get("available"):
        html += f'<p>Total lineage records: {lineage.get("total_records", 0)}</p>'
    else:
        html += f'<p>{lineage.get("message", "Not available")}</p>'

    html += "<h2>Validation</h2>"
    html += f'<p>Validation enabled: <span class="badge {"badge-success" if validation.get("available") else "badge-warning"}">'
    html += f'{"Yes" if validation.get("available") else "No"}</span></p>'
    html += f'<p>Checksum validation: <span class="badge {"badge-info" if validation.get("checksum_enabled") else "badge-warning"}">'
    html += f'{"Enabled" if validation.get("checksum_enabled") else "Disabled"}</span></p>'

    html += """
<hr>
<p class="meta">Generated by Clone-Xs Compliance Report Engine</p>
</div>
</body>
</html>"""
    return html


def apply_retention_policy(report_dir: str, retention_days: int) -> int:
    """Delete compliance reports older than retention_days. Returns count deleted."""
    if not os.path.exists(report_dir):
        return 0

    import time
    cutoff = time.time() - (retention_days * 86400)
    deleted = 0

    for filename in os.listdir(report_dir):
        filepath = os.path.join(report_dir, filename)
        if os.path.isfile(filepath) and os.path.getmtime(filepath) < cutoff:
            os.remove(filepath)
            deleted += 1
            logger.debug(f"Deleted old report: {filename}")

    if deleted:
        logger.info(f"Retention policy: deleted {deleted} reports older than {retention_days} days")
    return deleted
