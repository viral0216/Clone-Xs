"""Clone impact analysis — analyze downstream effects before cloning."""

import logging
import re

from src.client import execute_sql

logger = logging.getLogger(__name__)

# Databricks identifiers: alphanumeric, underscores, hyphens (no dots — full names use backticks)
_IDENT_RE = re.compile(r"^[A-Za-z0-9_\-]+$")


def _sanitize_ident(name: str) -> str:
    """Validate and return a safe identifier, or raise ValueError."""
    if not name or not _IDENT_RE.match(name):
        raise ValueError(f"Invalid identifier: {name!r}")
    return name


def analyze_impact(client, warehouse_id: str, catalog: str, config: dict) -> dict:
    """Analyze the downstream impact of cloning to a destination catalog.

    Returns impact report dict with dependent objects and risk assessment.
    """
    catalog = _sanitize_ident(catalog)
    schema_filter = config.get("schema") or None
    table_filter = config.get("table") or None
    if schema_filter:
        schema_filter = _sanitize_ident(schema_filter)
    if table_filter:
        table_filter = _sanitize_ident(table_filter)

    impact = {
        "catalog": catalog,
        "dependent_views": [],
        "dependent_functions": [],
        "referencing_jobs": [],
        "active_queries": [],
        "dashboard_references": [],
    }

    # Find dependent views
    impact["dependent_views"] = _find_dependent_views(client, warehouse_id, catalog, schema_filter, table_filter)

    # Find dependent functions
    impact["dependent_functions"] = _find_dependent_functions(client, warehouse_id, catalog, schema_filter, table_filter)

    # Find referencing jobs
    impact["referencing_jobs"] = _find_referencing_jobs(client, catalog)

    # Find active queries
    impact["active_queries"] = _find_active_queries(client, warehouse_id, catalog)

    # Find dashboard references
    impact["dashboard_references"] = _find_dashboard_references(client, catalog)

    # Assess risk
    threshold = config.get("impact_high_threshold", 10)
    impact["risk_level"] = _assess_risk(impact, threshold)

    total = (len(impact["dependent_views"]) + len(impact["dependent_functions"]) +
             len(impact["referencing_jobs"]) + len(impact["active_queries"]) +
             len(impact["dashboard_references"]))
    impact["total_dependent_objects"] = total

    return impact


def _find_dependent_views(client, warehouse_id: str, catalog: str,
                          schema_filter: str = None, table_filter: str = None) -> list[dict]:
    """Find views that reference this catalog across all accessible catalogs."""
    try:
        # Build the LIKE pattern for the target objects
        if table_filter and schema_filter:
            like_target = f"{catalog}.{schema_filter}.{table_filter}"
        elif schema_filter:
            like_target = f"{catalog}.{schema_filter}."
        else:
            like_target = f"{catalog}."

        sql = f"""
            SELECT table_catalog, table_schema, table_name, view_definition
            FROM system.information_schema.views
            WHERE view_definition LIKE '%{like_target}%'
              AND table_catalog != '{catalog}'
            LIMIT 200
        """
        rows = execute_sql(client, warehouse_id, sql)
        return [
            {
                "catalog": r.get("table_catalog"),
                "schema": r.get("table_schema"),
                "view": r.get("table_name"),
                "references": _extract_references(r.get("view_definition", ""), like_target),
            }
            for r in rows
        ]
    except Exception as e:
        logger.debug(f"Could not query system views: {e}")
        return []


def _find_dependent_functions(client, warehouse_id: str, catalog: str,
                              schema_filter: str = None, table_filter: str = None) -> list[dict]:
    """Find functions that reference this catalog."""
    try:
        if table_filter and schema_filter:
            like_target = f"{catalog}.{schema_filter}.{table_filter}"
        elif schema_filter:
            like_target = f"{catalog}.{schema_filter}."
        else:
            like_target = f"{catalog}."

        sql = f"""
            SELECT routine_catalog, routine_schema, routine_name
            FROM system.information_schema.routines
            WHERE routine_definition LIKE '%{like_target}%'
              AND routine_catalog != '{catalog}'
            LIMIT 200
        """
        rows = execute_sql(client, warehouse_id, sql)
        return [
            {
                "catalog": r.get("routine_catalog"),
                "schema": r.get("routine_schema"),
                "function": r.get("routine_name"),
            }
            for r in rows
        ]
    except Exception as e:
        logger.debug(f"Could not query system routines: {e}")
        return []


def _find_referencing_jobs(client, catalog: str) -> list[dict]:
    """Find Databricks jobs that reference the catalog."""
    results = []
    try:
        jobs = client.jobs.list()
        for job in jobs:
            job_name = job.settings.name if job.settings else ""
            # Check task configurations for catalog references
            if job.settings and job.settings.tasks:
                for task in job.settings.tasks:
                    task_str = str(task)
                    if catalog in task_str:
                        results.append({
                            "job_id": job.job_id,
                            "job_name": job_name,
                            "task": task.task_key if hasattr(task, 'task_key') else "",
                        })
                        break  # One match per job is enough
    except Exception as e:
        logger.debug(f"Could not list jobs: {e}")

    return results[:50]  # Limit results


def _find_active_queries(client, warehouse_id: str, catalog: str) -> list[dict]:
    """Find currently running queries against this catalog."""
    try:
        sql = f"""
            SELECT query_id, statement_text, executed_by, start_time
            FROM system.query.history
            WHERE status = 'RUNNING'
              AND LOWER(statement_text) LIKE '%{catalog.lower()}%'
            LIMIT 20
        """
        return execute_sql(client, warehouse_id, sql)
    except Exception as e:
        logger.debug(f"Could not query active queries: {e}")
        return []


def _find_dashboard_references(client, catalog: str) -> list[dict]:
    """Find dashboards that reference the catalog."""
    results = []
    try:
        # Try Lakeview dashboards first
        dashboards = client.lakeview.list()
        for dash in dashboards:
            dash_str = str(dash)
            if catalog in dash_str:
                results.append({
                    "dashboard_id": dash.dashboard_id if hasattr(dash, 'dashboard_id') else "",
                    "name": dash.display_name if hasattr(dash, 'display_name') else "",
                })
    except Exception as e:
        logger.debug(f"Could not list dashboards: {e}")

    return results[:50]


def _extract_references(definition: str, target: str) -> str:
    """Extract a snippet showing how target is referenced in the definition."""
    if not definition:
        return ""
    lower = definition.lower()
    idx = lower.find(target.lower())
    if idx == -1:
        return ""
    start = max(0, idx - 20)
    end = min(len(definition), idx + len(target) + 30)
    snippet = definition[start:end].strip()
    if start > 0:
        snippet = "..." + snippet
    if end < len(definition):
        snippet = snippet + "..."
    return snippet


def _assess_risk(impact: dict, threshold: int) -> str:
    """Assess risk level based on total dependent objects."""
    total = (len(impact.get("dependent_views", [])) +
             len(impact.get("dependent_functions", [])) +
             len(impact.get("referencing_jobs", [])) +
             len(impact.get("active_queries", [])) +
             len(impact.get("dashboard_references", [])))

    if total == 0:
        return "low"
    elif total <= threshold:
        return "medium"
    else:
        return "high"


def print_impact_report(impact: dict) -> None:
    """Pretty-print the impact analysis results."""
    print("=" * 60)
    print(f"IMPACT ANALYSIS: {impact['catalog']}")
    print("=" * 60)
    print(f"  Risk Level: {impact['risk_level'].upper()}")
    print(f"  Total Dependent Objects: {impact['total_dependent_objects']}")
    print()

    if impact["dependent_views"]:
        print(f"  Dependent Views ({len(impact['dependent_views'])}):")
        for v in impact["dependent_views"][:10]:
            print(f"    {v['catalog']}.{v['schema']}.{v['view']}")

    if impact["dependent_functions"]:
        print(f"  Dependent Functions ({len(impact['dependent_functions'])}):")
        for f_item in impact["dependent_functions"][:10]:
            print(f"    {f_item['catalog']}.{f_item['schema']}.{f_item['function']}")

    if impact["referencing_jobs"]:
        print(f"  Referencing Jobs ({len(impact['referencing_jobs'])}):")
        for j in impact["referencing_jobs"][:10]:
            print(f"    Job {j['job_id']}: {j['job_name']}")

    if impact["active_queries"]:
        print(f"  Active Queries ({len(impact['active_queries'])}):")
        for q in impact["active_queries"][:5]:
            print(f"    {q.get('query_id', '?')}: {q.get('executed_by', '?')}")

    if impact["dashboard_references"]:
        print(f"  Dashboard References ({len(impact['dashboard_references'])}):")
        for d in impact["dashboard_references"][:10]:
            print(f"    {d.get('name', d.get('dashboard_id', '?'))}")

    if impact["total_dependent_objects"] == 0:
        print("  No downstream dependencies found.")

    print("=" * 60)
