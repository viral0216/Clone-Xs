"""Tests for Data Observability service."""

from unittest.mock import patch, MagicMock
from src.observability import ObservabilityService


class TestObservabilityService:

    def setup_method(self):
        self.client = MagicMock()
        self.config = {"audit_trail": {"catalog": "audit_cat"}, "observability": {
            "health_score_weights": {"freshness": 0.25, "volume": 0.15, "anomaly": 0.20, "sla": 0.25, "dq": 0.15},
            "issue_lookback_hours": 24, "trend_days": 30,
        }}
        self.svc = ObservabilityService(self.client, "wh-1", config=self.config)

    def test_health_score_all_perfect(self):
        summary = {"freshness_rate": 100, "volume_rate": 100, "anomaly_rate": 100, "sla_rate": 100, "dq_rate": 100}
        assert self.svc._compute_health_score(summary) == 100

    def test_health_score_all_zero(self):
        summary = {"freshness_rate": 0, "volume_rate": 0, "anomaly_rate": 0, "sla_rate": 0, "dq_rate": 0}
        assert self.svc._compute_health_score(summary) == 0

    def test_health_score_mixed(self):
        summary = {"freshness_rate": 80, "volume_rate": 100, "anomaly_rate": 90, "sla_rate": 70, "dq_rate": 60}
        score = self.svc._compute_health_score(summary)
        assert 70 < score < 90

    def test_health_score_clamped_to_100(self):
        summary = {"freshness_rate": 150, "volume_rate": 100, "anomaly_rate": 100, "sla_rate": 100, "dq_rate": 100}
        assert self.svc._compute_health_score(summary) == 100

    def test_category_breakdown_structure(self):
        summary = {"freshness_rate": 90, "volume_rate": 100, "anomaly_rate": 85, "sla_rate": 95, "dq_rate": 80}
        breakdown = self.svc.get_category_breakdown(summary)
        assert "freshness" in breakdown
        assert "sla" in breakdown
        assert breakdown["freshness"]["rate"] == 90
        assert breakdown["freshness"]["weight"] == 0.25

    @patch("src.observability.execute_sql")
    def test_get_summary_with_data(self, mock_sql):
        mock_sql.side_effect = [
            [{"total": 100, "fresh": 90}],   # freshness
            [{"total": 50, "passed": 48}],    # sla
            [{"total": 200, "passed": 180}],  # dq
            [{"total": 30, "anomalies": 2}],  # anomaly
        ]
        summary = self.svc.get_summary()
        assert summary["freshness_rate"] == 90.0
        assert summary["sla_rate"] == 96.0
        assert summary["dq_rate"] == 90.0

    @patch("src.observability.execute_sql")
    def test_get_summary_empty_tables(self, mock_sql):
        mock_sql.return_value = None  # all queries fail
        summary = self.svc.get_summary()
        assert summary["freshness_rate"] == 100.0  # default when no data

    @patch("src.observability.execute_sql")
    def test_get_top_issues_empty(self, mock_sql):
        mock_sql.return_value = []
        issues = self.svc.get_top_issues()
        assert issues == []

    @patch("src.observability.execute_sql")
    def test_get_top_issues_sorted_by_severity(self, mock_sql):
        mock_sql.side_effect = [
            [{"catalog": "c", "schema_name": "s", "table_name": "t", "checked_at": "2025-01-01", "hours_since_update": 48}],  # freshness (warning)
            [{"rule_name": "latency", "catalog": "c", "schema_name": "s", "table_name": "t", "checked_at": "2025-01-01"}],  # sla (critical)
            [],  # dq
        ]
        issues = self.svc.get_top_issues()
        assert len(issues) == 2
        assert issues[0]["severity"] == "critical"  # SLA first
        assert issues[1]["severity"] == "warning"

    @patch("src.observability.execute_sql")
    def test_get_dashboard_returns_all_sections(self, mock_sql):
        mock_sql.return_value = [{"total": 0, "fresh": 0, "passed": 0, "anomalies": 0}]
        dashboard = self.svc.get_dashboard()
        assert "health_score" in dashboard
        assert "summary" in dashboard
        assert "top_issues" in dashboard
        assert "categories" in dashboard

    @patch("src.observability.execute_sql")
    def test_trend_data_freshness(self, mock_sql):
        mock_sql.return_value = [{"day": "2025-01-01", "total": 10, "passed": 9}]
        trends = self.svc.get_trend_data("freshness", days=7)
        assert len(trends) == 1
        assert trends[0]["total"] == 10

    @patch("src.observability.execute_sql")
    def test_trend_data_unknown_metric(self, mock_sql):
        trends = self.svc.get_trend_data("unknown")
        assert trends == []
