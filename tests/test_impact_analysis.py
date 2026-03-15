"""Tests for impact analysis."""

from unittest.mock import MagicMock, patch

from src.impact_analysis import analyze_impact, _assess_risk, print_impact_report


class TestAssessRisk:
    def test_low_risk(self):
        impact = {"dependent_views": [], "dependent_functions": [], "referencing_jobs": [],
                   "active_queries": [], "dashboard_references": []}
        assert _assess_risk(impact, 10) == "low"

    def test_medium_risk(self):
        impact = {"dependent_views": [{"v": 1}] * 5, "dependent_functions": [],
                   "referencing_jobs": [], "active_queries": [], "dashboard_references": []}
        assert _assess_risk(impact, 10) == "medium"

    def test_high_risk(self):
        impact = {"dependent_views": [{"v": 1}] * 15, "dependent_functions": [],
                   "referencing_jobs": [], "active_queries": [], "dashboard_references": []}
        assert _assess_risk(impact, 10) == "high"


class TestAnalyzeImpact:
    @patch("src.impact_analysis._find_dashboard_references", return_value=[])
    @patch("src.impact_analysis._find_active_queries", return_value=[])
    @patch("src.impact_analysis._find_referencing_jobs", return_value=[])
    @patch("src.impact_analysis._find_dependent_functions", return_value=[])
    @patch("src.impact_analysis._find_dependent_views", return_value=[{"catalog": "c", "schema": "s", "view": "v"}])
    def test_analyze_with_views(self, *mocks):
        client = MagicMock()
        result = analyze_impact(client, "wh-123", "test_catalog", {"impact_high_threshold": 10})
        assert len(result["dependent_views"]) == 1
        assert result["risk_level"] == "medium"
        assert result["total_dependent_objects"] == 1
