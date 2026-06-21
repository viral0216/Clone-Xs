"""SAT Scanner — Lakeview Dashboard generation from Delta tables."""

from __future__ import annotations

import json
import uuid
from typing import Any

import httpx


# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

DASHBOARD_NAME = "SAT Scanner Dashboard"


# ─────────────────────────────────────────────────────────────────────────────
# Lakeview API helpers
# ─────────────────────────────────────────────────────────────────────────────

def _lakeview_headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def _find_existing_dashboard(host: str, token: str) -> dict | None:
    """Return the existing SAT Scanner dashboard dict, or None."""
    resp = httpx.get(
        f"{host}/api/2.0/lakeview/dashboards",
        headers=_lakeview_headers(token),
        params={"page_size": 100},
        timeout=15,
    )
    if resp.status_code != 200:
        return None
    for d in resp.json().get("dashboards", []):
        if d.get("display_name") == DASHBOARD_NAME:
            return d
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Dataset SQL queries
# ─────────────────────────────────────────────────────────────────────────────

def _datasets(fqn: str) -> list[dict]:
    """Return Lakeview dataset definitions with SQL queries against {fqn}.*."""
    return [
        {
            "name": "ds_score_trend",
            "displayName": "Score Trend",
            "query": (
                f"SELECT scanned_at, overall_score, workspace_name "
                f"FROM {fqn}.scan_runs ORDER BY scanned_at"
            ),
        },
        {
            "name": "ds_latest_scores",
            "displayName": "Latest Scores",
            "query": (
                f"SELECT * FROM ("
                f"  SELECT *, ROW_NUMBER() OVER (PARTITION BY workspace_name ORDER BY scanned_at DESC) AS rn"
                f"  FROM {fqn}.scan_runs"
                f") WHERE rn = 1"
            ),
        },
        {
            "name": "ds_category_scores",
            "displayName": "Category Scores",
            "query": (
                f"SELECT * FROM ("
                f"  SELECT *, ROW_NUMBER() OVER (PARTITION BY workspace_name, category ORDER BY run_id DESC) AS rn"
                f"  FROM {fqn}.category_scores"
                f") WHERE rn = 1"
            ),
        },
        {
            "name": "ds_findings_by_status",
            "displayName": "Findings by Status",
            "query": (
                f"SELECT status, COUNT(*) AS count FROM {fqn}.findings "
                f"WHERE run_id = (SELECT run_id FROM {fqn}.scan_runs ORDER BY scanned_at DESC LIMIT 1) "
                f"GROUP BY status"
            ),
        },
        {
            "name": "ds_findings_by_severity",
            "displayName": "Findings by Severity",
            "query": (
                f"SELECT severity, COUNT(*) AS count FROM {fqn}.findings "
                f"WHERE run_id = (SELECT run_id FROM {fqn}.scan_runs ORDER BY scanned_at DESC LIMIT 1) "
                f"AND status = 'FAIL' "
                f"GROUP BY severity"
            ),
        },
        {
            "name": "ds_top_failed",
            "displayName": "Top Failed Checks",
            "query": (
                f"SELECT check_id, title, severity, category, current_state FROM {fqn}.findings "
                f"WHERE run_id = (SELECT run_id FROM {fqn}.scan_runs ORDER BY scanned_at DESC LIMIT 1) "
                f"AND status = 'FAIL' "
                f"ORDER BY CASE severity "
                f"  WHEN 'critical' THEN 1 WHEN 'high' THEN 2 WHEN 'medium' THEN 3 ELSE 4 END"
            ),
        },
        {
            "name": "ds_changes",
            "displayName": "Recent Changes",
            "query": (
                f"SELECT change_type, check_id, title, severity, category, "
                f"status_before, status_after, score_before, score_after, score_delta, "
                f"scanned_at, workspace_name "
                f"FROM {fqn}.scan_changes ORDER BY scanned_at DESC LIMIT 100"
            ),
        },
        {
            "name": "ds_score_delta",
            "displayName": "Score Delta",
            "query": (
                f"SELECT scanned_at, workspace_name, score_before, score_after, score_delta "
                f"FROM {fqn}.scan_changes WHERE change_type = 'SCORE_CHANGE' ORDER BY scanned_at"
            ),
        },
        {
            "name": "ds_api_endpoints",
            "displayName": "API Endpoints",
            "query": (
                f"SELECT endpoint, status, items_count, error_code FROM {fqn}.api_endpoints "
                f"WHERE run_id = (SELECT run_id FROM {fqn}.scan_runs ORDER BY scanned_at DESC LIMIT 1) "
                f"ORDER BY status, items_count DESC"
            ),
        },
        {
            "name": "ds_prioritised",
            "displayName": "Prioritised Recommendations",
            "query": (
                f"SELECT priority_label, priority_score, check_id, category, severity, "
                f"status, effort, title, recommendation, benefits, "
                f"cost_low, cost_high, cost_reason "
                f"FROM {fqn}.prioritised_recommendations "
                f"WHERE run_id = (SELECT run_id FROM {fqn}.scan_runs ORDER BY scanned_at DESC LIMIT 1) "
                f"ORDER BY priority_score DESC"
            ),
        },
        {
            "name": "ds_prioritised_by_priority",
            "displayName": "By Priority Level",
            "query": (
                f"SELECT priority_label, COUNT(*) AS count, "
                f"SUM(cost_low) AS cost_low, SUM(cost_high) AS cost_high "
                f"FROM {fqn}.prioritised_recommendations "
                f"WHERE run_id = (SELECT run_id FROM {fqn}.scan_runs ORDER BY scanned_at DESC LIMIT 1) "
                f"GROUP BY priority_label ORDER BY priority_label"
            ),
        },
        {
            "name": "ds_prioritised_by_effort",
            "displayName": "By Effort Level",
            "query": (
                f"SELECT effort, COUNT(*) AS count, "
                f"SUM(cost_low) AS cost_low, SUM(cost_high) AS cost_high "
                f"FROM {fqn}.prioritised_recommendations "
                f"WHERE run_id = (SELECT run_id FROM {fqn}.scan_runs ORDER BY scanned_at DESC LIMIT 1) "
                f"GROUP BY effort"
            ),
        },
    ]


# ─────────────────────────────────────────────────────────────────────────────
# Widget builders (matching Lakeview lvdash.json schema)
# ─────────────────────────────────────────────────────────────────────────────

def _w_id() -> str:
    return str(uuid.uuid4()).replace("-", "")[:16]


def _text_widget(text: str) -> dict:
    return {"name": _w_id(), "textbox_spec": text}


def _counter_widget(dataset: str, field: str, label: str) -> dict:
    """Counter widget — version 2 with frame title."""
    return {
        "name": _w_id(),
        "queries": [
            {
                "name": "main_query",
                "query": {
                    "datasetName": dataset,
                    "fields": [{"name": field, "expression": f"`{field}`"}],
                    "disaggregated": True,
                },
            }
        ],
        "spec": {
            "version": 2,
            "widgetType": "counter",
            "encodings": {
                "value": {"fieldName": field, "displayName": label},
            },
            "frame": {
                "showTitle": True,
                "title": label,
            },
        },
    }


def _line_widget(dataset: str, x: str, y: str, color: str | None = None, title: str = "") -> dict:
    encodings: dict[str, Any] = {
        "x": {"fieldName": x, "scale": {"type": "temporal"}, "displayName": x},
        "y": {"fieldName": y, "scale": {"type": "quantitative"}, "displayName": y},
    }
    if color:
        encodings["color"] = {
            "fieldName": color,
            "scale": {"type": "categorical"},
            "legend": {"position": "bottom"},
            "displayName": color,
        }
    fields = [
        {"name": x, "expression": f"`{x}`"},
        {"name": y, "expression": f"`{y}`"},
    ]
    if color:
        fields.append({"name": color, "expression": f"`{color}`"})
    spec: dict[str, Any] = {
        "version": 3,
        "widgetType": "line",
        "encodings": encodings,
    }
    if title:
        spec["frame"] = {"showTitle": True, "title": title}
    return {
        "name": _w_id(),
        "queries": [
            {
                "name": "main_query",
                "query": {"datasetName": dataset, "fields": fields, "disaggregated": True},
            }
        ],
        "spec": spec,
    }


def _bar_widget(dataset: str, x: str, y: str, color: str | None = None, title: str = "") -> dict:
    encodings: dict[str, Any] = {
        "x": {"fieldName": x, "scale": {"type": "categorical"}, "displayName": x},
        "y": {"fieldName": y, "scale": {"type": "quantitative"}, "displayName": y},
    }
    if color:
        encodings["color"] = {
            "fieldName": color,
            "scale": {"type": "categorical"},
            "legend": {"position": "bottom"},
            "displayName": color,
        }
    fields = [
        {"name": x, "expression": f"`{x}`"},
        {"name": y, "expression": f"`{y}`"},
    ]
    if color:
        fields.append({"name": color, "expression": f"`{color}`"})
    spec: dict[str, Any] = {
        "version": 3,
        "widgetType": "bar",
        "encodings": encodings,
    }
    if title:
        spec["frame"] = {"showTitle": True, "title": title}
    return {
        "name": _w_id(),
        "queries": [
            {
                "name": "main_query",
                "query": {"datasetName": dataset, "fields": fields, "disaggregated": True},
            }
        ],
        "spec": spec,
    }


def _table_widget(dataset: str, columns: list[tuple[str, str]], title: str = "") -> dict:
    """Table widget — minimal spec matching REST API schema."""
    fields = [{"name": c[0], "expression": f"`{c[0]}`"} for c in columns]
    col_encodings = [
        {"fieldName": fn, "displayName": dn} for fn, dn in columns
    ]
    spec: dict[str, Any] = {
        "version": 2,
        "widgetType": "table",
        "encodings": {"columns": col_encodings},
    }
    if title:
        spec["frame"] = {"showTitle": True, "title": title}
    return {
        "name": _w_id(),
        "queries": [
            {
                "name": "main_query",
                "query": {"datasetName": dataset, "fields": fields, "disaggregated": True},
            }
        ],
        "spec": spec,
    }


def _filter_widget(
    datasets: list[str],
    field: str,
    title: str,
    widget_type: str = "filter-multi-select",
) -> dict:
    """Filter widget — references one or more datasets for cross-filtering."""
    wid = _w_id()
    queries = []
    enc_fields = []
    for ds in datasets:
        qname = f"{wid}_{ds}_{field}"
        queries.append({
            "name": qname,
            "query": {
                "datasetName": ds,
                "disaggregated": False,
                "fields": [
                    {"name": field, "expression": f"`{field}`"},
                    {"name": f"{field}_associativity", "expression": "COUNT_IF(`associative_filter_predicate_group`)"},
                ],
            },
        })
        enc_fields.append({
            "fieldName": field,
            "displayName": title,
            "queryName": qname,
        })
    return {
        "name": wid,
        "queries": queries,
        "spec": {
            "version": 2,
            "widgetType": widget_type,
            "encodings": {"fields": enc_fields},
            "frame": {"showTitle": True, "title": title},
        },
    }


def _pos(x: int, y: int, w: int, h: int) -> dict:
    return {"x": x, "y": y, "width": w, "height": h}


# ─────────────────────────────────────────────────────────────────────────────
# Dashboard layout
# ─────────────────────────────────────────────────────────────────────────────

def _build_serialized_dashboard(fqn: str) -> str:
    """Build the complete Lakeview serialized_dashboard JSON string."""
    datasets = _datasets(fqn)

    # ── Page 1: Overview ──
    overview_layout = [
        # Row 0: Title
        {"widget": _text_widget("# SAT Scanner Dashboard\nSecurity posture overview powered by Delta tables."), "position": _pos(0, 0, 6, 2)},
        # Row 2: Filters
        {"widget": _filter_widget(
            ["ds_score_trend", "ds_latest_scores", "ds_category_scores"],
            "workspace_name", "Workspace",
        ), "position": _pos(0, 2, 3, 1)},
        # Row 3: KPI counters
        {"widget": _counter_widget("ds_latest_scores", "overall_score", "Latest Score"), "position": _pos(0, 3, 2, 8)},
        {"widget": _counter_widget("ds_latest_scores", "total_checks", "Total Checks"), "position": _pos(2, 3, 2, 8)},
        {"widget": _counter_widget("ds_latest_scores", "failed", "Failed Checks"), "position": _pos(4, 3, 2, 8)},
        # Row 11: Score trend line chart
        {"widget": _line_widget("ds_score_trend", "scanned_at", "overall_score", "workspace_name", title="Overall Score Trend"), "position": _pos(0, 11, 6, 10)},
        # Row 21: Findings by status / severity bar charts
        {"widget": _bar_widget("ds_findings_by_status", "status", "count", title="Findings by Status"), "position": _pos(0, 21, 3, 10)},
        {"widget": _bar_widget("ds_findings_by_severity", "severity", "count", title="Failed by Severity"), "position": _pos(3, 21, 3, 10)},
        # Row 31: Category scores table
        {"widget": _table_widget("ds_category_scores", [
            ("workspace_name", "Workspace"),
            ("category", "Category"),
            ("score", "Score"),
            ("grade", "Grade"),
        ], title="Category Scores"), "position": _pos(0, 31, 6, 14)},
    ]

    # ── Page 2: Findings Detail ──
    findings_layout = [
        {"widget": _text_widget("# Failed Checks\nAll checks with FAIL status from the latest scan, ordered by severity."), "position": _pos(0, 0, 6, 2)},
        # Row 2: Filters
        {"widget": _filter_widget(
            ["ds_top_failed"], "severity", "Severity",
        ), "position": _pos(0, 2, 2, 1)},
        {"widget": _filter_widget(
            ["ds_top_failed"], "category", "Category",
        ), "position": _pos(2, 2, 2, 1)},
        # Row 3: Table
        {"widget": _table_widget("ds_top_failed", [
            ("check_id", "Check ID"),
            ("title", "Title"),
            ("severity", "Severity"),
            ("category", "Category"),
            ("current_state", "Details"),
        ], title="Top Failed Checks"), "position": _pos(0, 3, 6, 18)},
    ]

    # ── Page 3: Changes ──
    changes_layout = [
        {"widget": _text_widget("# Change Tracking\nFixes, new failures, regressions, and score deltas between scans."), "position": _pos(0, 0, 6, 2)},
        # Row 2: Filters
        {"widget": _filter_widget(
            ["ds_changes", "ds_score_delta"], "workspace_name", "Workspace",
        ), "position": _pos(0, 2, 2, 1)},
        {"widget": _filter_widget(
            ["ds_changes"], "change_type", "Change Type",
        ), "position": _pos(2, 2, 2, 1)},
        {"widget": _filter_widget(
            ["ds_changes"], "severity", "Severity",
        ), "position": _pos(4, 2, 2, 1)},
        # Row 3: Score delta bar chart
        {"widget": _bar_widget("ds_score_delta", "scanned_at", "score_delta", "workspace_name", title="Score Delta Over Time"), "position": _pos(0, 3, 6, 10)},
        # Row 13: Changes table
        {"widget": _table_widget("ds_changes", [
            ("scanned_at", "Scanned At"),
            ("workspace_name", "Workspace"),
            ("change_type", "Change"),
            ("check_id", "Check ID"),
            ("title", "Title"),
            ("severity", "Severity"),
            ("status_before", "Before"),
            ("status_after", "After"),
            ("score_delta", "Score Delta"),
        ], title="Recent Changes"), "position": _pos(0, 13, 6, 16)},
    ]

    # ── Page 4: API Endpoints ──
    api_layout = [
        {"widget": _text_widget("# API Endpoint Health\nStatus and item counts for each API endpoint checked during the latest scan."), "position": _pos(0, 0, 6, 2)},
        # Row 2: Filters
        {"widget": _filter_widget(
            ["ds_api_endpoints"], "status", "Status",
        ), "position": _pos(0, 2, 2, 1)},
        # Row 3: Table
        {"widget": _table_widget("ds_api_endpoints", [
            ("endpoint", "Endpoint"),
            ("status", "Status"),
            ("items_count", "Items"),
            ("error_code", "Error Code"),
        ], title="API Endpoints"), "position": _pos(0, 3, 6, 18)},
    ]

    # ── Page 5: Prioritised Recommendations ──
    prio_layout = [
        {"widget": _text_widget("# Prioritised Recommendations\nActionable findings ranked by Priority Score (severity × effort multiplier). Fix high-impact, low-effort items first."), "position": _pos(0, 0, 6, 2)},
        # Row 2: Filters
        {"widget": _filter_widget(
            ["ds_prioritised"], "priority_label", "Priority",
        ), "position": _pos(0, 2, 2, 1)},
        {"widget": _filter_widget(
            ["ds_prioritised"], "severity", "Severity",
        ), "position": _pos(2, 2, 2, 1)},
        {"widget": _filter_widget(
            ["ds_prioritised"], "effort", "Effort",
        ), "position": _pos(4, 2, 2, 1)},
        # Row 3: Priority distribution bar chart + Effort distribution
        {"widget": _bar_widget("ds_prioritised_by_priority", "priority_label", "count", title="By Priority Level"), "position": _pos(0, 3, 3, 10)},
        {"widget": _bar_widget("ds_prioritised_by_effort", "effort", "count", title="By Effort Level"), "position": _pos(3, 3, 3, 10)},
        # Row 13: Full table
        {"widget": _table_widget("ds_prioritised", [
            ("priority_label", "Priority"),
            ("priority_score", "Score"),
            ("check_id", "Check ID"),
            ("category", "Category"),
            ("severity", "Severity"),
            ("status", "Status"),
            ("effort", "Effort"),
            ("cost_low", "Cost Low ($/mo)"),
            ("cost_high", "Cost High ($/mo)"),
            ("title", "Title"),
            ("recommendation", "Recommendation"),
        ], title="Prioritised Recommendations"), "position": _pos(0, 13, 6, 18)},
    ]

    dashboard_def = {
        "pages": [
            {"name": "overview", "displayName": "Overview", "layout": overview_layout},
            {"name": "findings", "displayName": "Failed Checks", "layout": findings_layout},
            {"name": "prioritised", "displayName": "Prioritised", "layout": prio_layout},
            {"name": "changes", "displayName": "Changes", "layout": changes_layout},
            {"name": "api_health", "displayName": "API Health", "layout": api_layout},
        ],
        "datasets": datasets,
    }

    return json.dumps(dashboard_def)


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def create_or_update_dashboard(
    catalog: str,
    schema: str,
    host: str,
    token: str,
    warehouse_id: str,
    quiet: bool = False,
) -> str:
    """Create or update the SAT Scanner Lakeview dashboard.

    Returns the dashboard URL (e.g. https://host/sql/dashboardsv3/dashboard-id).
    """
    fqn = f"`{catalog}`.`{schema}`"
    headers = _lakeview_headers(token)

    # 1. Check if dashboard already exists
    existing = _find_existing_dashboard(host, token)
    serialized = _build_serialized_dashboard(fqn)

    if existing:
        dashboard_id = existing["dashboard_id"]
        if not quiet:
            print(f"  Updating existing dashboard: {DASHBOARD_NAME}")
        # Update the dashboard definition
        resp = httpx.patch(
            f"{host}/api/2.0/lakeview/dashboards/{dashboard_id}",
            headers=headers,
            json={
                "display_name": DASHBOARD_NAME,
                "serialized_dashboard": serialized,
                "warehouse_id": warehouse_id,
            },
            timeout=30,
        )
        if resp.status_code != 200:
            raise RuntimeError(f"Dashboard update failed ({resp.status_code}): {resp.text}")
    else:
        if not quiet:
            print(f"  Creating dashboard: {DASHBOARD_NAME}")
        resp = httpx.post(
            f"{host}/api/2.0/lakeview/dashboards",
            headers=headers,
            json={
                "display_name": DASHBOARD_NAME,
                "warehouse_id": warehouse_id,
                "serialized_dashboard": serialized,
                "parent_path": "/Shared",
            },
            timeout=30,
        )
        if resp.status_code != 200:
            raise RuntimeError(f"Dashboard creation failed ({resp.status_code}): {resp.text}")
        dashboard_id = resp.json()["dashboard_id"]

    # 2. Publish the dashboard so it's viewable
    if not quiet:
        print("  Publishing dashboard...")
    pub_resp = httpx.post(
        f"{host}/api/2.0/lakeview/dashboards/{dashboard_id}/published",
        headers=headers,
        json={"embed_credentials": True, "warehouse_id": warehouse_id},
        timeout=30,
    )
    if pub_resp.status_code not in (200, 409):
        # 409 = already published, that's fine
        if not quiet:
            print(f"  Warning: publish returned {pub_resp.status_code} (dashboard may need manual publish)")

    url = f"{host}/sql/dashboardsv3/{dashboard_id}"
    return url
