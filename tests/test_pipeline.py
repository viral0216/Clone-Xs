"""Tests for Clone Pipelines feature."""

import json
from unittest.mock import patch, MagicMock
import pytest

from src.pipeline_store import PipelineStore
from src.pipeline_engine import PipelineEngine, BUILTIN_TEMPLATES, STEP_TYPES


class TestPipelineStore:

    def setup_method(self):
        self.store = PipelineStore(MagicMock(), "wh-1", "audit")

    @patch("src.pipeline_store.execute_sql")
    def test_init_tables_creates_schema_and_3_tables(self, mock_sql):
        self.store.init_tables()
        assert mock_sql.call_count == 4  # schema + 3 tables
        calls = [c[0][2] for c in mock_sql.call_args_list]
        assert "CREATE SCHEMA" in calls[0]
        assert "pipelines" in calls[1]
        assert "pipeline_runs" in calls[2]
        assert "pipeline_step_results" in calls[3]

    @patch("src.pipeline_store.execute_sql")
    def test_save_pipeline(self, mock_sql):
        self.store.save_pipeline("p1", "Test", "A test pipeline", [{"type": "clone"}], "user")
        sql = mock_sql.call_args[0][2]
        assert "INSERT INTO" in sql
        assert "p1" in sql

    @patch("src.pipeline_store.execute_sql")
    def test_get_pipeline(self, mock_sql):
        mock_sql.return_value = [{"pipeline_id": "p1", "name": "Test"}]
        p = self.store.get_pipeline("p1")
        assert p["pipeline_id"] == "p1"

    @patch("src.pipeline_store.execute_sql")
    def test_list_pipelines(self, mock_sql):
        mock_sql.return_value = [{"pipeline_id": "p1"}, {"pipeline_id": "p2"}]
        result = self.store.list_pipelines()
        assert len(result) == 2

    @patch("src.pipeline_store.execute_sql")
    def test_save_run(self, mock_sql):
        self.store.save_run("r1", "p1", "Test Pipeline", 3, "user")
        sql = mock_sql.call_args[0][2]
        assert "r1" in sql
        assert "running" in sql


class TestPipelineEngine:

    def setup_method(self):
        self.client = MagicMock()
        self.config = {
            "source_catalog": "src", "destination_catalog": "dst",
            "audit_trail": {"catalog": "audit"},
            "pipelines": {"default_on_failure": "abort", "retry_max_attempts": 2, "retry_backoff_seconds": 0},
        }
        self.engine = PipelineEngine(self.client, "wh-1", config=self.config)

    def test_list_templates(self):
        templates = self.engine.list_templates()
        assert len(templates) == len(BUILTIN_TEMPLATES)
        names = {t["name"] for t in templates}
        assert "Production to Dev" in names

    def test_builtin_templates_have_valid_step_types(self):
        for name, tmpl in BUILTIN_TEMPLATES.items():
            for step in tmpl["steps"]:
                assert step["type"] in STEP_TYPES, f"Template {name} has invalid step type: {step['type']}"

    @patch("src.pipeline_store.execute_sql")
    def test_create_pipeline(self, mock_sql):
        pid = self.engine.create_pipeline("Test", "desc", [{"type": "clone", "name": "Clone"}])
        assert pid  # UUID string
        assert mock_sql.called

    @patch("src.pipeline_store.execute_sql")
    def test_create_from_template(self, mock_sql):
        pid = self.engine.create_from_template("clone-and-validate")
        assert pid
        sql = mock_sql.call_args[0][2]
        assert "Clone & Validate" in sql

    @patch("src.pipeline_store.execute_sql")
    def test_create_from_invalid_template(self, mock_sql):
        with pytest.raises(ValueError, match="not found"):
            self.engine.create_from_template("nonexistent")

    @patch("src.pipeline_store.execute_sql")
    def test_run_pipeline_not_found(self, mock_sql):
        mock_sql.return_value = []  # get_pipeline returns None
        with pytest.raises(ValueError, match="not found"):
            self.engine.run_pipeline("nonexistent")

    @patch("src.pipeline_engine.execute_sql")
    @patch("src.pipeline_store.execute_sql")
    def test_run_pipeline_notify_step(self, mock_store_sql, mock_engine_sql):
        """Test running a simple pipeline with just a notify step."""
        mock_store_sql.side_effect = [
            # get_pipeline
            [{"pipeline_id": "p1", "name": "Test", "steps_json": json.dumps([
                {"type": "notify", "name": "Send alert", "config": {"message": "done"}, "on_failure": "skip"}
            ])}],
            None,  # save_run
            None,  # save_step_result
            None,  # update_run (running)
            None,  # update_run (completed)
        ]
        result = self.engine.run_pipeline("p1")
        assert result["status"] == "completed"
        assert result["completed_steps"] == 1

    @patch("src.pipeline_store.execute_sql")
    def test_cancel_run(self, mock_sql):
        result = self.engine.cancel_run("r1")
        assert result["status"] == "cancelled"

    @patch("src.pipeline_store.execute_sql")
    def test_delete_pipeline(self, mock_sql):
        result = self.engine.delete_pipeline("p1")
        assert result["status"] == "deleted"

    def test_execute_step_notify(self):
        result = self.engine._execute_step({"type": "notify", "config": {"message": "test"}})
        assert result["notified"] is True

    @patch("src.pipeline_engine.execute_sql")
    def test_execute_step_custom_sql(self, mock_sql):
        result = self.engine._execute_step({"type": "custom_sql", "config": {"sql": "SELECT 1"}})
        assert result["sql_executed"] == "SELECT 1"
        mock_sql.assert_called_once()

    def test_execute_step_unknown_type(self):
        with pytest.raises(ValueError, match="Unknown step type"):
            self.engine._execute_step({"type": "invalid_step", "config": {}})
