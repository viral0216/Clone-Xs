"""Tests for Delta Live Tables (DLT) management."""

from unittest.mock import MagicMock, patch
import pytest

from src.dlt_management import (
    list_pipelines, get_pipeline_details, list_pipeline_events,
    list_pipeline_updates, get_dlt_dashboard, get_dlt_lineage,
    clone_pipeline, clone_pipeline_cross_workspace,
    trigger_pipeline, stop_pipeline,
    query_expectation_results,
)


def _mock_pipeline(pid="p1", name="Test Pipeline", state="IDLE", health="HEALTHY", creator="user@co.com"):
    p = MagicMock()
    p.pipeline_id = pid
    p.name = name
    p.state = state
    p.health = health
    p.creator_user_name = creator
    p.cluster_id = "cls-1"
    p.latest_updates = []
    return p


def _mock_event(eid="e1", event_type="flow_progress", level="INFO", message="Table updated", ts="2025-01-01T00:00:00Z"):
    ev = MagicMock()
    ev.id = eid
    ev.event_type = event_type
    ev.level = level
    ev.message = message
    ev.timestamp = ts
    ev.maturity_level = None
    return ev


class TestListPipelines:

    def test_returns_pipeline_list(self):
        client = MagicMock()
        client.pipelines.list_pipelines.return_value = [_mock_pipeline(), _mock_pipeline("p2", "Second")]
        result = list_pipelines(client)
        assert len(result) == 2
        assert result[0]["pipeline_id"] == "p1"
        assert result[1]["name"] == "Second"

    def test_handles_empty_workspace(self):
        client = MagicMock()
        client.pipelines.list_pipelines.return_value = []
        assert list_pipelines(client) == []

    def test_handles_api_error(self):
        client = MagicMock()
        client.pipelines.list_pipelines.side_effect = Exception("API error")
        assert list_pipelines(client) == []


class TestGetPipelineDetails:

    def test_returns_details(self):
        client = MagicMock()
        p = _mock_pipeline()
        spec = MagicMock()
        spec.catalog = "my_catalog"
        spec.target = "my_schema"
        spec.continuous = False
        spec.serverless = True
        spec.development = True
        spec.libraries = []
        spec.clusters = []
        spec.configuration = {"key": "value"}
        spec.notifications = []
        p.spec = spec
        p.run_as_user_name = "admin"
        client.pipelines.get.return_value = p

        result = get_pipeline_details(client, "p1")
        assert result["pipeline_id"] == "p1"
        assert result["spec"]["catalog"] == "my_catalog"
        assert result["spec"]["serverless"] is True

    def test_returns_none_on_error(self):
        client = MagicMock()
        client.pipelines.get.side_effect = Exception("Not found")
        assert get_pipeline_details(client, "bad") is None


class TestPipelineEvents:

    def test_returns_events(self):
        client = MagicMock()
        client.pipelines.list_pipeline_events.return_value = [_mock_event(), _mock_event("e2")]
        result = list_pipeline_events(client, "p1", max_events=10)
        assert len(result) == 2
        assert result[0]["event_type"] == "flow_progress"

    def test_handles_error(self):
        client = MagicMock()
        client.pipelines.list_pipeline_events.side_effect = Exception("fail")
        assert list_pipeline_events(client, "p1") == []


class TestPipelineUpdates:

    def test_returns_updates(self):
        client = MagicMock()
        u = MagicMock()
        u.update_id = "u1"
        u.state = "COMPLETED"
        u.creation_time = "2025-01-01"
        u.full_refresh = False
        u.cause = "USER_ACTION"
        response = MagicMock()
        response.updates = [u]
        client.pipelines.list_updates.return_value = response
        result = list_pipeline_updates(client, "p1")
        assert len(result) == 1
        assert result[0]["update_id"] == "u1"


class TestClonePipeline:

    def test_clone_dry_run(self):
        client = MagicMock()
        p = _mock_pipeline()
        spec = MagicMock()
        spec.catalog = "cat"
        spec.target = "sch"
        spec.libraries = [MagicMock()]
        spec.clusters = []
        p.spec = spec
        client.pipelines.get.return_value = p

        result = clone_pipeline(client, "p1", "Clone of Test", dry_run=True)
        assert result["dry_run"] is True
        assert result["new_name"] == "Clone of Test"

    def test_clone_creates_new(self):
        client = MagicMock()
        p = _mock_pipeline()
        spec = MagicMock()
        spec.catalog = "cat"
        spec.target = "sch"
        lib = MagicMock()
        lib.notebook = MagicMock(path="/Repos/user/pipeline")
        spec.libraries = [lib]
        spec.clusters = []
        spec.continuous = False
        spec.development = True
        spec.serverless = False
        spec.configuration = {}
        spec.notifications = []
        p.spec = spec
        client.pipelines.get.return_value = p
        client.pipelines.create.return_value = MagicMock(pipeline_id="new-p")

        result = clone_pipeline(client, "p1", "Clone")
        assert result["new_pipeline_id"] == "new-p"
        assert result["status"] == "created"


class TestCrossWorkspaceClone:

    @patch("src.auth.get_client")
    def test_cross_workspace_clone_dry_run(self, mock_get_client):
        source = MagicMock()
        p = _mock_pipeline()
        spec = MagicMock()
        spec.catalog = "prod"
        spec.target = "bronze"
        spec.libraries = [MagicMock()]
        spec.clusters = []
        spec.continuous = False
        spec.serverless = True
        p.spec = spec
        source.pipelines.get.return_value = p
        source.config.host = "https://source.databricks.com"

        result = clone_pipeline_cross_workspace(
            source, "p1", "https://dest.databricks.com", "dapi_dest", "Clone", dry_run=True,
        )
        assert result["dry_run"] is True
        assert result["dest_workspace"] == "https://dest.databricks.com"
        assert result["catalog"] == "prod"
        mock_get_client.assert_not_called()  # dry run should NOT create dest client

    @patch("src.auth.get_client")
    def test_cross_workspace_clone_creates_in_dest(self, mock_get_client):
        source = MagicMock()
        p = _mock_pipeline()
        spec = MagicMock()
        spec.catalog = "prod"
        spec.target = "bronze"
        # Empty libraries — triggers placeholder notebook path
        spec.libraries = []
        spec.clusters = []
        spec.continuous = False
        spec.serverless = False
        spec.development = True
        spec.configuration = {}
        spec.notifications = []
        p.spec = spec
        source.pipelines.get.return_value = p
        source.config.host = "https://source.databricks.com"

        dest = MagicMock()
        dest.pipelines.create.return_value = MagicMock(pipeline_id="new-dest-p")
        mock_get_client.return_value = dest

        result = clone_pipeline_cross_workspace(
            source, "p1", "https://dest.databricks.com", "dapi_dest", "Clone",
        )
        assert result["status"] == "created"
        assert result["dest_pipeline_id"] == "new-dest-p"
        mock_get_client.assert_called_once_with(host="https://dest.databricks.com", token="dapi_dest")
        dest.pipelines.create.assert_called_once()

    @patch("src.auth.get_client")
    def test_cross_workspace_clone_source_not_found(self, mock_get_client):
        source = MagicMock()
        source.pipelines.get.side_effect = Exception("Pipeline not found")

        with pytest.raises(Exception):
            clone_pipeline_cross_workspace(source, "bad", "https://dest", "tok", "Clone")


class TestTriggerStop:

    def test_trigger(self):
        client = MagicMock()
        client.pipelines.start_update.return_value = MagicMock(update_id="u1")
        result = trigger_pipeline(client, "p1")
        assert result["status"] == "triggered"

    def test_stop(self):
        client = MagicMock()
        result = stop_pipeline(client, "p1")
        assert result["status"] == "stopping"


class TestDltDashboard:

    def test_dashboard_summary(self):
        client = MagicMock()
        client.pipelines.list_pipelines.return_value = [
            _mock_pipeline("p1", state="RUNNING", health="HEALTHY"),
            _mock_pipeline("p2", state="FAILED", health="UNHEALTHY"),
            _mock_pipeline("p3", state="IDLE", health="HEALTHY"),
        ]
        client.pipelines.list_pipeline_events.return_value = []

        result = get_dlt_dashboard(client)
        assert result["summary"]["total"] == 3
        assert result["summary"]["running"] == 1
        assert result["summary"]["failed"] == 1
        assert result["summary"]["healthy"] >= 2


class TestDltLineage:

    @patch("src.dlt_management.execute_sql")
    def test_lineage_maps_datasets(self, mock_sql):
        client = MagicMock()
        p = _mock_pipeline()
        spec = MagicMock()
        spec.catalog = "prod"
        spec.target = "bronze"
        p.spec = spec
        p.run_as_user_name = None
        client.pipelines.get.return_value = p

        mock_sql.return_value = [
            {"table_name": "raw_events", "table_type": "TABLE", "data_source_format": "DELTA", "comment": None},
            {"table_name": "raw_users", "table_type": "TABLE", "data_source_format": "DELTA", "comment": "User data"},
        ]

        result = get_dlt_lineage(client, "wh-1", "p1")
        assert result["total_datasets"] == 2
        assert result["datasets"][0]["fqn"] == "prod.bronze.raw_events"


class TestExpectationResults:

    @patch("src.dlt_management.execute_sql")
    def test_queries_system_table(self, mock_sql):
        mock_sql.return_value = [{"pipeline_id": "p1", "event_type": "quality_violation"}]
        result = query_expectation_results(MagicMock(), "wh-1", "p1", days=7)
        assert len(result) == 1
        sql = mock_sql.call_args[0][2]
        assert "system.lakeflow.pipeline_events" in sql

    @patch("src.dlt_management.execute_sql")
    def test_handles_missing_system_table(self, mock_sql):
        mock_sql.side_effect = Exception("table not found")
        result = query_expectation_results(MagicMock(), "wh-1")
        assert result == []
