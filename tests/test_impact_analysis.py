"""Tests for impact analysis."""

from unittest.mock import MagicMock, patch

from src.impact_analysis import (
    analyze_impact,
    _assess_risk,
    _find_dependent_views,
    _find_dependent_functions,
    _find_referencing_jobs,
    _find_active_queries,
    _find_dashboard_references,
    print_impact_report,
)


class TestAssessRisk:
    def test_low_risk_zero_deps(self):
        impact = {
            "dependent_views": [],
            "dependent_functions": [],
            "referencing_jobs": [],
            "active_queries": [],
            "dashboard_references": [],
        }
        assert _assess_risk(impact, 10) == "low"

    def test_medium_risk(self):
        impact = {
            "dependent_views": [{"v": 1}] * 5,
            "dependent_functions": [],
            "referencing_jobs": [],
            "active_queries": [],
            "dashboard_references": [],
        }
        assert _assess_risk(impact, 10) == "medium"

    def test_high_risk_exceeds_threshold(self):
        impact = {
            "dependent_views": [{"v": 1}] * 15,
            "dependent_functions": [],
            "referencing_jobs": [],
            "active_queries": [],
            "dashboard_references": [],
        }
        assert _assess_risk(impact, 10) == "high"

    def test_boundary_at_threshold_is_medium(self):
        impact = {
            "dependent_views": [{"v": 1}] * 10,
            "dependent_functions": [],
            "referencing_jobs": [],
            "active_queries": [],
            "dashboard_references": [],
        }
        assert _assess_risk(impact, 10) == "medium"

    def test_one_above_threshold_is_high(self):
        impact = {
            "dependent_views": [{"v": 1}] * 11,
            "dependent_functions": [],
            "referencing_jobs": [],
            "active_queries": [],
            "dashboard_references": [],
        }
        assert _assess_risk(impact, 10) == "high"


class TestFindDependentViews:
    @patch("src.impact_analysis.execute_sql")
    def test_returns_mapped_rows(self, mock_sql):
        mock_sql.return_value = [
            {"table_catalog": "other", "table_schema": "s1", "table_name": "v1", "view_definition": "..."},
        ]
        result = _find_dependent_views(MagicMock(), "wh-123", "my_cat")
        assert len(result) == 1
        assert result[0]["catalog"] == "other"
        assert result[0]["schema"] == "s1"
        assert result[0]["view"] == "v1"

    @patch("src.impact_analysis.execute_sql", side_effect=Exception("query error"))
    def test_returns_empty_on_error(self, mock_sql):
        result = _find_dependent_views(MagicMock(), "wh-123", "my_cat")
        assert result == []


class TestFindDependentFunctions:
    @patch("src.impact_analysis.execute_sql")
    def test_returns_mapped_rows(self, mock_sql):
        mock_sql.return_value = [
            {"routine_catalog": "other", "routine_schema": "s1", "routine_name": "fn1"},
        ]
        result = _find_dependent_functions(MagicMock(), "wh-123", "my_cat")
        assert len(result) == 1
        assert result[0]["function"] == "fn1"

    @patch("src.impact_analysis.execute_sql", side_effect=Exception("fail"))
    def test_returns_empty_on_error(self, mock_sql):
        result = _find_dependent_functions(MagicMock(), "wh-123", "my_cat")
        assert result == []


class TestFindReferencingJobs:
    def test_finds_matching_jobs(self):
        client = MagicMock()
        task = MagicMock()
        task.task_key = "etl_task"
        task.__str__ = lambda self: "my_catalog.schema.table"

        settings = MagicMock()
        settings.name = "ETL Job"
        settings.tasks = [task]

        job = MagicMock()
        job.job_id = 42
        job.settings = settings

        client.jobs.list.return_value = [job]
        result = _find_referencing_jobs(client, "my_catalog")
        assert len(result) == 1
        assert result[0]["job_id"] == 42

    def test_returns_empty_on_error(self):
        client = MagicMock()
        client.jobs.list.side_effect = Exception("API error")
        result = _find_referencing_jobs(client, "my_catalog")
        assert result == []

    def test_no_match(self):
        client = MagicMock()
        task = MagicMock()
        task.__str__ = lambda self: "other_catalog.schema.table"

        settings = MagicMock()
        settings.name = "Other Job"
        settings.tasks = [task]

        job = MagicMock()
        job.job_id = 99
        job.settings = settings

        client.jobs.list.return_value = [job]
        result = _find_referencing_jobs(client, "my_catalog")
        assert result == []


class TestFindActiveQueries:
    @patch("src.impact_analysis.execute_sql")
    def test_returns_rows(self, mock_sql):
        mock_sql.return_value = [{"query_id": "q1", "statement_text": "SELECT ..."}]
        result = _find_active_queries(MagicMock(), "wh-123", "my_cat")
        assert len(result) == 1

    @patch("src.impact_analysis.execute_sql", side_effect=Exception("fail"))
    def test_returns_empty_on_error(self, mock_sql):
        result = _find_active_queries(MagicMock(), "wh-123", "my_cat")
        assert result == []


class TestFindDashboardReferences:
    def test_finds_matching_dashboards(self):
        client = MagicMock()
        dash = MagicMock()
        dash.dashboard_id = "d1"
        dash.display_name = "Sales Dashboard"
        dash.__str__ = lambda self: "my_catalog references"
        client.lakeview.list.return_value = [dash]

        result = _find_dashboard_references(client, "my_catalog")
        assert len(result) == 1
        assert result[0]["dashboard_id"] == "d1"

    def test_returns_empty_on_error(self):
        client = MagicMock()
        client.lakeview.list.side_effect = Exception("API error")
        result = _find_dashboard_references(client, "my_catalog")
        assert result == []


class TestAnalyzeImpact:
    @patch("src.impact_analysis._find_dashboard_references", return_value=[])
    @patch("src.impact_analysis._find_active_queries", return_value=[])
    @patch("src.impact_analysis._find_referencing_jobs", return_value=[])
    @patch("src.impact_analysis._find_dependent_functions", return_value=[])
    @patch("src.impact_analysis._find_dependent_views", return_value=[
        {"catalog": "c", "schema": "s", "view": "v"},
    ])
    def test_analyze_with_views(self, *mocks):
        client = MagicMock()
        result = analyze_impact(client, "wh-123", "test_catalog", {"impact_high_threshold": 10})
        assert len(result["dependent_views"]) == 1
        assert result["risk_level"] == "medium"
        assert result["total_dependent_objects"] == 1
        assert result["catalog"] == "test_catalog"

    @patch("src.impact_analysis._find_dashboard_references", return_value=[])
    @patch("src.impact_analysis._find_active_queries", return_value=[])
    @patch("src.impact_analysis._find_referencing_jobs", return_value=[])
    @patch("src.impact_analysis._find_dependent_functions", return_value=[])
    @patch("src.impact_analysis._find_dependent_views", return_value=[])
    def test_analyze_no_deps_low_risk(self, *mocks):
        result = analyze_impact(MagicMock(), "wh-123", "cat", {})
        assert result["risk_level"] == "low"
        assert result["total_dependent_objects"] == 0

    @patch("src.impact_analysis._find_dashboard_references", return_value=[{"dashboard_id": "d1", "name": "D"}] * 12)
    @patch("src.impact_analysis._find_active_queries", return_value=[])
    @patch("src.impact_analysis._find_referencing_jobs", return_value=[])
    @patch("src.impact_analysis._find_dependent_functions", return_value=[])
    @patch("src.impact_analysis._find_dependent_views", return_value=[])
    def test_analyze_high_risk_from_dashboards(self, *mocks):
        result = analyze_impact(MagicMock(), "wh-123", "cat", {"impact_high_threshold": 10})
        assert result["risk_level"] == "high"

    @patch("src.impact_analysis._find_dashboard_references", return_value=[])
    @patch("src.impact_analysis._find_active_queries", return_value=[])
    @patch("src.impact_analysis._find_referencing_jobs", return_value=[])
    @patch("src.impact_analysis._find_dependent_functions", return_value=[])
    @patch("src.impact_analysis._find_dependent_views", return_value=[])
    def test_uses_default_threshold(self, *mocks):
        # Config without impact_high_threshold should default to 10
        result = analyze_impact(MagicMock(), "wh-123", "cat", {})
        assert result["risk_level"] == "low"


class TestPrintImpactReport:
    def test_prints_without_error(self, capsys):
        impact = {
            "catalog": "test",
            "dependent_views": [{"catalog": "c", "schema": "s", "view": "v"}],
            "dependent_functions": [],
            "referencing_jobs": [],
            "active_queries": [],
            "dashboard_references": [],
            "risk_level": "medium",
            "total_dependent_objects": 1,
        }
        print_impact_report(impact)
        out = capsys.readouterr().out
        assert "IMPACT ANALYSIS" in out
        assert "MEDIUM" in out

    def test_prints_no_deps_message(self, capsys):
        impact = {
            "catalog": "test",
            "dependent_views": [],
            "dependent_functions": [],
            "referencing_jobs": [],
            "active_queries": [],
            "dashboard_references": [],
            "risk_level": "low",
            "total_dependent_objects": 0,
        }
        print_impact_report(impact)
        out = capsys.readouterr().out
        assert "No downstream dependencies" in out
