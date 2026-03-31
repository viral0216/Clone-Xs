"""Data Observability — unified health scoring across freshness, volume, anomaly, SLA, and DQ."""

import logging
from datetime import datetime, timedelta, timezone

from src.client import execute_sql

logger = logging.getLogger(__name__)

DEFAULT_WEIGHTS = {
    "freshness": 0.25,
    "volume": 0.15,
    "anomaly": 0.20,
    "sla": 0.25,
    "dq": 0.15,
}


class ObservabilityService:
    """Read-only aggregation service for data observability metrics."""

    def __init__(self, client, warehouse_id: str, config: dict | None = None):
        self.client = client
        self.warehouse_id = warehouse_id
        self.config = config or {}

        obs_config = self.config.get("observability", {})
        self.weights = obs_config.get("health_score_weights", DEFAULT_WEIGHTS)
        self.issue_lookback_hours = obs_config.get("issue_lookback_hours", 24)
        self.trend_days = obs_config.get("trend_days", 30)

        audit_catalog = self.config.get("audit_trail", {}).get("catalog", "clone_audit")
        self._freshness_table = f"{audit_catalog}.data_quality.freshness_history"
        self._baselines_table = f"{audit_catalog}.data_quality.metric_baselines"
        self._sla_checks_table = f"{audit_catalog}.governance.sla_checks"
        self._dq_results_table = f"{audit_catalog}.governance.dq_results"
        self._incidents_table = f"{audit_catalog}.data_quality.freshness_history"
        self._anomaly_table = f"{audit_catalog}.data_quality.metric_baselines"

    def get_dashboard(self) -> dict:
        """Get the full observability dashboard in a single call."""
        summary = self.get_summary()
        health = self._compute_health_score(summary)
        issues = self.get_top_issues(limit=10)
        categories = self.get_category_breakdown(summary)
        return {
            "health_score": health,
            "summary": summary,
            "top_issues": issues,
            "categories": categories,
        }

    def get_health_score(self) -> int:
        """Compute a composite health score (0-100)."""
        summary = self.get_summary()
        return self._compute_health_score(summary)

    def get_summary(self) -> dict:
        """Get stat card values: pass rates, counts, totals."""
        lookback = (datetime.now(timezone.utc) - timedelta(hours=self.issue_lookback_hours)).strftime("%Y-%m-%d %H:%M:%S")

        freshness = self._safe_query(f"""
            SELECT COUNT(*) AS total,
                   SUM(CASE WHEN is_fresh = true THEN 1 ELSE 0 END) AS fresh
            FROM {self._freshness_table}
            WHERE checked_at >= '{lookback}'
        """)

        sla = self._safe_query(f"""
            SELECT COUNT(*) AS total,
                   SUM(CASE WHEN passed = true THEN 1 ELSE 0 END) AS passed
            FROM {self._sla_checks_table}
            WHERE checked_at >= '{lookback}'
        """)

        dq = self._safe_query(f"""
            SELECT COUNT(*) AS total,
                   SUM(CASE WHEN passed = true THEN 1 ELSE 0 END) AS passed
            FROM {self._dq_results_table}
            WHERE executed_at >= '{lookback}'
        """)

        anomaly = self._safe_query(f"""
            SELECT COUNT(*) AS total,
                   SUM(CASE WHEN is_anomaly = true THEN 1 ELSE 0 END) AS anomalies
            FROM {self._baselines_table}
            WHERE updated_at >= '{lookback}'
        """)

        f_row = freshness[0] if freshness else {}
        s_row = sla[0] if sla else {}
        d_row = dq[0] if dq else {}
        a_row = anomaly[0] if anomaly else {}

        f_total = int(f_row.get("total", 0) or 0)
        f_fresh = int(f_row.get("fresh", 0) or 0)
        s_total = int(s_row.get("total", 0) or 0)
        s_passed = int(s_row.get("passed", 0) or 0)
        d_total = int(d_row.get("total", 0) or 0)
        d_passed = int(d_row.get("passed", 0) or 0)
        a_total = int(a_row.get("total", 0) or 0)
        a_anomalies = int(a_row.get("anomalies", 0) or 0)

        return {
            "freshness_total": f_total,
            "freshness_pass": f_fresh,
            "freshness_rate": round(f_fresh / f_total * 100, 1) if f_total > 0 else 100.0,
            "sla_total": s_total,
            "sla_pass": s_passed,
            "sla_rate": round(s_passed / s_total * 100, 1) if s_total > 0 else 100.0,
            "dq_total": d_total,
            "dq_pass": d_passed,
            "dq_rate": round(d_passed / d_total * 100, 1) if d_total > 0 else 100.0,
            "anomaly_total": a_total,
            "anomaly_count": a_anomalies,
            "anomaly_rate": round((a_total - a_anomalies) / a_total * 100, 1) if a_total > 0 else 100.0,
            "volume_rate": 100.0,  # placeholder — volume health derived from anomaly absence
            "lookback_hours": self.issue_lookback_hours,
        }

    def get_top_issues(self, limit: int = 10) -> list[dict]:
        """Get the most critical current issues across all categories."""
        lookback = (datetime.now(timezone.utc) - timedelta(hours=self.issue_lookback_hours)).strftime("%Y-%m-%d %H:%M:%S")
        issues = []

        # Freshness failures
        stale = self._safe_query(f"""
            SELECT catalog, schema_name, table_name, checked_at, hours_since_update
            FROM {self._freshness_table}
            WHERE is_fresh = false AND checked_at >= '{lookback}'
            ORDER BY checked_at DESC LIMIT {limit}
        """)
        for r in (stale or []):
            issues.append({
                "category": "freshness",
                "severity": "warning",
                "table": f"{r.get('catalog','')}.{r.get('schema_name','')}.{r.get('table_name','')}",
                "message": f"Stale data — {r.get('hours_since_update', '?')} hours since last update",
                "time": str(r.get("checked_at", "")),
            })

        # SLA violations
        sla_fail = self._safe_query(f"""
            SELECT rule_name, catalog, schema_name, table_name, checked_at
            FROM {self._sla_checks_table}
            WHERE passed = false AND checked_at >= '{lookback}'
            ORDER BY checked_at DESC LIMIT {limit}
        """)
        for r in (sla_fail or []):
            issues.append({
                "category": "sla",
                "severity": "critical",
                "table": f"{r.get('catalog','')}.{r.get('schema_name','')}.{r.get('table_name','')}",
                "message": f"SLA violation — {r.get('rule_name', 'unknown rule')}",
                "time": str(r.get("checked_at", "")),
            })

        # DQ failures
        dq_fail = self._safe_query(f"""
            SELECT rule_name, catalog, schema_name, table_name, executed_at
            FROM {self._dq_results_table}
            WHERE passed = false AND executed_at >= '{lookback}'
            ORDER BY executed_at DESC LIMIT {limit}
        """)
        for r in (dq_fail or []):
            issues.append({
                "category": "dq",
                "severity": "warning",
                "table": f"{r.get('catalog','')}.{r.get('schema_name','')}.{r.get('table_name','')}",
                "message": f"DQ check failed — {r.get('rule_name', 'unknown')}",
                "time": str(r.get("executed_at", "")),
            })

        # Sort by severity (critical first) then recency
        severity_order = {"critical": 0, "warning": 1, "info": 2}
        issues.sort(key=lambda x: (severity_order.get(x["severity"], 9), x.get("time", "")))
        return issues[:limit]

    def get_trend_data(self, metric: str, days: int | None = None) -> list[dict]:
        """Get daily aggregated trend data for sparklines."""
        days = days or self.trend_days
        start = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")

        if metric == "freshness":
            return self._safe_query(f"""
                SELECT DATE(checked_at) AS day,
                       COUNT(*) AS total,
                       SUM(CASE WHEN is_fresh = true THEN 1 ELSE 0 END) AS passed
                FROM {self._freshness_table}
                WHERE checked_at >= '{start}'
                GROUP BY DATE(checked_at) ORDER BY day
            """) or []
        elif metric == "sla":
            return self._safe_query(f"""
                SELECT DATE(checked_at) AS day,
                       COUNT(*) AS total,
                       SUM(CASE WHEN passed = true THEN 1 ELSE 0 END) AS passed
                FROM {self._sla_checks_table}
                WHERE checked_at >= '{start}'
                GROUP BY DATE(checked_at) ORDER BY day
            """) or []
        elif metric == "dq":
            return self._safe_query(f"""
                SELECT DATE(executed_at) AS day,
                       COUNT(*) AS total,
                       SUM(CASE WHEN passed = true THEN 1 ELSE 0 END) AS passed
                FROM {self._dq_results_table}
                WHERE executed_at >= '{start}'
                GROUP BY DATE(executed_at) ORDER BY day
            """) or []
        return []

    def get_category_breakdown(self, summary: dict | None = None) -> dict:
        """Per-category health percentages."""
        s = summary or self.get_summary()
        return {
            "freshness": {"rate": s["freshness_rate"], "label": "Data Freshness", "weight": self.weights.get("freshness", 0.25)},
            "volume": {"rate": s["volume_rate"], "label": "Volume Health", "weight": self.weights.get("volume", 0.15)},
            "anomaly": {"rate": s["anomaly_rate"], "label": "Anomaly Free", "weight": self.weights.get("anomaly", 0.20)},
            "sla": {"rate": s["sla_rate"], "label": "SLA Compliance", "weight": self.weights.get("sla", 0.25)},
            "dq": {"rate": s["dq_rate"], "label": "Data Quality", "weight": self.weights.get("dq", 0.15)},
        }

    def _compute_health_score(self, summary: dict) -> int:
        """Weighted average of category pass rates."""
        rates = {
            "freshness": summary.get("freshness_rate", 100),
            "volume": summary.get("volume_rate", 100),
            "anomaly": summary.get("anomaly_rate", 100),
            "sla": summary.get("sla_rate", 100),
            "dq": summary.get("dq_rate", 100),
        }
        score = sum(self.weights.get(k, 0) * v for k, v in rates.items())
        total_weight = sum(self.weights.values())
        return max(0, min(100, round(score / total_weight if total_weight > 0 else 100)))

    def _safe_query(self, sql: str) -> list[dict] | None:
        """Execute SQL and return results, or None on error."""
        try:
            return execute_sql(self.client, self.warehouse_id, sql)
        except Exception as e:
            logger.debug(f"Observability query failed (table may not exist): {e}")
            return None
