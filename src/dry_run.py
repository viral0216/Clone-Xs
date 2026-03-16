"""Enhanced dry-run mode with execution plan generation."""

import json
import logging
import time
from datetime import datetime

logger = logging.getLogger(__name__)


class SqlCapture:
    """Captures SQL statements during a dry-run clone."""

    def __init__(self):
        self.statements: list[dict] = []
        self._start_time = time.time()

    def capture(self, sql: str, category: str = "unknown"):
        """Record a SQL statement."""
        self.statements.append({
            "sql": sql.strip(),
            "category": classify_sql(sql),
            "timestamp": time.time() - self._start_time,
        })

    def get_summary(self) -> dict:
        """Get summary of captured statements."""
        categories = {}
        for stmt in self.statements:
            cat = stmt["category"]
            categories[cat] = categories.get(cat, 0) + 1
        return {
            "total_statements": len(self.statements),
            "by_category": categories,
        }


def classify_sql(sql: str) -> str:
    """Classify SQL into operation categories."""
    sql_upper = sql.strip().upper()
    if sql_upper.startswith("CREATE"):
        if "CLONE" in sql_upper:
            return "CLONE"
        elif "VIEW" in sql_upper:
            return "CREATE_VIEW"
        elif "FUNCTION" in sql_upper:
            return "CREATE_FUNCTION"
        elif "VOLUME" in sql_upper:
            return "CREATE_VOLUME"
        elif "SCHEMA" in sql_upper or "DATABASE" in sql_upper:
            return "CREATE_SCHEMA"
        elif "CATALOG" in sql_upper:
            return "CREATE_CATALOG"
        elif "TABLE" in sql_upper:
            return "CREATE_TABLE"
        return "CREATE_OTHER"
    elif sql_upper.startswith("GRANT"):
        return "GRANT"
    elif sql_upper.startswith("ALTER"):
        return "ALTER"
    elif sql_upper.startswith("DROP"):
        return "DROP"
    elif sql_upper.startswith("SELECT") or sql_upper.startswith("SHOW") or sql_upper.startswith("DESCRIBE"):
        return "READ"
    elif sql_upper.startswith("UPDATE"):
        return "UPDATE"
    elif sql_upper.startswith("INSERT") or sql_upper.startswith("MERGE"):
        return "INSERT"
    return "OTHER"


def build_execution_plan(client, config: dict) -> dict:
    """Run clone_catalog with dry_run=True and capture all SQL operations.

    Returns a structured execution plan.
    """
    from src.clone_catalog import clone_catalog

    # Ensure dry_run is True
    plan_config = {**config, "dry_run": True}

    # Use the global capture hook in client.py to record all SQL statements.
    # This fires before the dry_run check, so write statements are captured
    # even though they won't be executed.
    capture = SqlCapture()

    from src.client import set_sql_capture

    def on_sql(sql):
        sql_upper = sql.strip().upper()
        is_read = sql_upper.startswith(("SELECT", "SHOW", "DESCRIBE", "LIST"))
        if not is_read:
            capture.capture(sql)

    try:
        set_sql_capture(on_sql)
        summary = clone_catalog(client, plan_config)
    finally:
        set_sql_capture(None)

    # Build cost estimate
    cost_estimate = None
    try:
        from src.clone_cost_estimator import estimate_clone_cost
        cost_estimate = estimate_clone_cost(
            client, config["sql_warehouse_id"],
            config["source_catalog"],
            config.get("exclude_schemas", []),
            config.get("clone_type", "DEEP"),
        )
    except Exception as e:
        logger.debug(f"Could not estimate cost: {e}")

    plan = {
        "generated_at": datetime.utcnow().isoformat(),
        "source_catalog": config["source_catalog"],
        "destination_catalog": config["destination_catalog"],
        "clone_type": config.get("clone_type", "DEEP"),
        "load_type": config.get("load_type", "FULL"),
        "sql_statements": capture.statements,
        "sql_summary": capture.get_summary(),
        "clone_summary": summary,
        "cost_estimate": cost_estimate,
    }

    return plan


def format_plan_console(plan: dict) -> str:
    """Format execution plan for console output."""
    lines = []
    lines.append("=" * 70)
    lines.append("CLONE EXECUTION PLAN")
    lines.append("=" * 70)
    lines.append(f"  Source:      {plan['source_catalog']}")
    lines.append(f"  Destination: {plan['destination_catalog']}")
    lines.append(f"  Clone Type:  {plan['clone_type']}")
    lines.append(f"  Load Type:   {plan['load_type']}")
    lines.append("")

    # SQL Summary
    summary = plan["sql_summary"]
    lines.append(f"  Total SQL Statements: {summary['total_statements']}")
    lines.append("  By Category:")
    for cat, count in sorted(summary["by_category"].items()):
        lines.append(f"    {cat:20s}: {count}")
    lines.append("")

    # Cost estimate
    if plan.get("cost_estimate"):
        ce = plan["cost_estimate"]
        lines.append("  Cost Estimate:")
        if isinstance(ce, dict):
            for key, val in ce.items():
                if key not in ("schemas",):
                    lines.append(f"    {key}: {val}")
        lines.append("")

    # Clone summary
    cs = plan.get("clone_summary", {})
    lines.append(f"  Schemas: {cs.get('schemas_processed', 0)}")
    for obj_type in ("tables", "views", "functions", "volumes"):
        stats = cs.get(obj_type, {})
        if stats:
            lines.append(
                f"  {obj_type.capitalize():12s}: "
                f"{stats.get('success', 0)} to clone, "
                f"{stats.get('skipped', 0)} to skip"
            )

    lines.append("")
    lines.append("  SQL Statements:")
    lines.append("  " + "-" * 66)
    for stmt in plan["sql_statements"]:
        cat = stmt["category"]
        sql_preview = stmt["sql"][:100].replace("\n", " ")
        if len(stmt["sql"]) > 100:
            sql_preview += "..."
        lines.append(f"  [{cat:15s}] {sql_preview}")

    lines.append("=" * 70)
    return "\n".join(lines)


def format_plan_json(plan: dict) -> str:
    """Format execution plan as JSON."""
    return json.dumps(plan, indent=2, default=str)


def format_plan_html(plan: dict) -> str:
    """Format execution plan as HTML."""
    summary = plan["sql_summary"]
    cs = plan.get("clone_summary", {})

    html = f"""<!DOCTYPE html>
<html>
<head>
<title>Clone Execution Plan</title>
<style>
body {{ font-family: -apple-system, sans-serif; margin: 20px; background: #f5f5f5; }}
.container {{ max-width: 1000px; margin: 0 auto; background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
h1 {{ color: #1a1a1a; border-bottom: 2px solid #e0e0e0; padding-bottom: 10px; }}
h2 {{ color: #333; margin-top: 24px; }}
table {{ border-collapse: collapse; width: 100%; margin: 10px 0; }}
th, td {{ border: 1px solid #ddd; padding: 8px 12px; text-align: left; }}
th {{ background: #f0f0f0; font-weight: 600; }}
.sql {{ background: #f8f8f8; padding: 8px; font-family: monospace; font-size: 12px; white-space: pre-wrap; word-break: break-all; border-left: 3px solid #4a90d9; margin: 4px 0; }}
.badge {{ display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 12px; font-weight: 600; }}
.badge-create {{ background: #d4edda; color: #155724; }}
.badge-grant {{ background: #fff3cd; color: #856404; }}
.badge-read {{ background: #cce5ff; color: #004085; }}
.meta {{ color: #666; font-size: 14px; }}
</style>
</head>
<body>
<div class="container">
<h1>Clone Execution Plan</h1>
<p class="meta">Generated: {plan['generated_at']}</p>

<h2>Configuration</h2>
<table>
<tr><th>Source</th><td>{plan['source_catalog']}</td></tr>
<tr><th>Destination</th><td>{plan['destination_catalog']}</td></tr>
<tr><th>Clone Type</th><td>{plan['clone_type']}</td></tr>
<tr><th>Load Type</th><td>{plan['load_type']}</td></tr>
</table>

<h2>Summary</h2>
<table>
<tr><th>Total SQL Statements</th><td>{summary['total_statements']}</td></tr>
<tr><th>Schemas</th><td>{cs.get('schemas_processed', 0)}</td></tr>
</table>

<h2>SQL by Category</h2>
<table>
<tr><th>Category</th><th>Count</th></tr>
"""
    for cat, count in sorted(summary["by_category"].items()):
        html += f"<tr><td>{cat}</td><td>{count}</td></tr>\n"

    html += """</table>

<h2>SQL Statements</h2>
"""
    for stmt in plan["sql_statements"]:
        cat = stmt["category"]
        badge_class = "badge-create" if "CREATE" in cat or "CLONE" in cat else "badge-grant" if cat == "GRANT" else "badge-read"
        sql_escaped = stmt["sql"].replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        html += f'<div><span class="badge {badge_class}">{cat}</span></div>\n'
        html += f'<div class="sql">{sql_escaped}</div>\n'

    html += """
</div>
</body>
</html>"""
    return html


def format_plan_sql(plan: dict) -> str:
    """Format execution plan as a .sql file with all statements."""
    lines = []
    lines.append(f"-- Clone-Xs Execution Plan")
    lines.append(f"-- Source: {plan['source_catalog']} -> Destination: {plan['destination_catalog']}")
    lines.append(f"-- Clone Type: {plan['clone_type']}")
    lines.append(f"-- Generated: {plan['generated_at']}")
    lines.append(f"-- Total statements: {plan['sql_summary']['total_statements']}")
    lines.append("")

    for i, stmt in enumerate(plan.get("sql_statements", []), 1):
        sql = stmt["sql"] if isinstance(stmt, dict) else str(stmt)
        lines.append(f"-- [{stmt.get('category', 'SQL')}] Statement {i}")
        lines.append(f"{sql};")
        lines.append("")

    return "\n".join(lines)


def output_plan(plan: dict, fmt: str = "console", output_path: str | None = None):
    """Output the plan in the requested format."""
    if fmt == "json":
        content = format_plan_json(plan)
    elif fmt == "html":
        content = format_plan_html(plan)
    elif fmt == "sql":
        content = format_plan_sql(plan)
    else:
        content = format_plan_console(plan)

    if output_path:
        with open(output_path, "w") as f:
            f.write(content)
        logger.info(f"Execution plan written to: {output_path}")
    else:
        print(content)
