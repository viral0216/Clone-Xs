"""Tests for hooks module."""

import pytest
from unittest.mock import MagicMock, patch

from src.hooks import run_hooks, run_pre_clone_hooks, run_post_clone_hooks, run_post_schema_hooks


class TestRunHooks:
    @patch("src.hooks.execute_sql")
    def test_successful_hook(self, mock_sql):
        hooks = [{"sql": "SELECT 1", "description": "Test hook"}]
        results = run_hooks(MagicMock(), "wh", hooks, "pre")

        assert len(results) == 1
        assert results[0]["status"] == "success"
        assert results[0]["hook"] == "Test hook"
        mock_sql.assert_called_once()

    @patch("src.hooks.execute_sql")
    def test_multiple_hooks(self, mock_sql):
        hooks = [
            {"sql": "SELECT 1", "description": "Hook 1"},
            {"sql": "SELECT 2", "description": "Hook 2"},
        ]
        results = run_hooks(MagicMock(), "wh", hooks, "pre")
        assert len(results) == 2
        assert all(r["status"] == "success" for r in results)

    @patch("src.hooks.execute_sql")
    def test_empty_sql_skipped(self, mock_sql):
        hooks = [{"sql": "", "description": "Empty hook"}]
        results = run_hooks(MagicMock(), "wh", hooks, "pre")
        assert len(results) == 0
        mock_sql.assert_not_called()

    @patch("src.hooks.execute_sql")
    def test_variable_substitution(self, mock_sql):
        hooks = [{"sql": "USE ${source_catalog}; COPY TO ${dest_catalog}.${schema}"}]
        context = {"source_catalog": "src_cat", "dest_catalog": "dst_cat", "schema": "s1"}
        run_hooks(MagicMock(), "wh", hooks, "pre", context=context)

        called_sql = mock_sql.call_args[0][2]
        assert "src_cat" in called_sql
        assert "dst_cat" in called_sql
        assert "s1" in called_sql
        assert "${" not in called_sql

    @patch("src.hooks.execute_sql", side_effect=Exception("sql error"))
    def test_on_error_warn_continues(self, mock_sql):
        hooks = [
            {"sql": "BAD SQL", "description": "Bad hook", "on_error": "warn"},
        ]
        results = run_hooks(MagicMock(), "wh", hooks, "pre")
        assert len(results) == 1
        assert results[0]["status"] == "failed"
        assert "sql error" in results[0]["error"]

    @patch("src.hooks.execute_sql", side_effect=Exception("sql error"))
    def test_on_error_fail_raises(self, mock_sql):
        hooks = [
            {"sql": "BAD SQL", "description": "Fatal hook", "on_error": "fail"},
        ]
        with pytest.raises(Exception, match="sql error"):
            run_hooks(MagicMock(), "wh", hooks, "pre")

    @patch("src.hooks.execute_sql", side_effect=Exception("sql error"))
    def test_on_error_ignore_continues(self, mock_sql):
        hooks = [
            {"sql": "BAD SQL", "description": "Ignored hook", "on_error": "ignore"},
        ]
        results = run_hooks(MagicMock(), "wh", hooks, "pre")
        assert len(results) == 1
        assert results[0]["status"] == "failed"

    @patch("src.hooks.execute_sql")
    def test_dry_run(self, mock_sql):
        hooks = [{"sql": "DROP TABLE x", "description": "Dangerous"}]
        results = run_hooks(MagicMock(), "wh", hooks, "pre", dry_run=True)
        assert len(results) == 1
        # execute_sql should still be called with dry_run=True
        mock_sql.assert_called_once()
        _, kwargs = mock_sql.call_args
        assert kwargs.get("dry_run") is True

    @patch("src.hooks.execute_sql")
    def test_default_description(self, mock_sql):
        hooks = [{"sql": "SELECT 1"}]
        results = run_hooks(MagicMock(), "wh", hooks, "pre")
        assert results[0]["hook"] == "Hook 1"

    def test_empty_hooks_list(self):
        results = run_hooks(MagicMock(), "wh", [], "pre")
        assert results == []


class TestRunPreCloneHooks:
    @patch("src.hooks.execute_sql")
    def test_runs_pre_hooks(self, mock_sql):
        config = {
            "source_catalog": "src",
            "destination_catalog": "dst",
            "pre_clone_hooks": [{"sql": "SELECT 1", "description": "Pre hook"}],
        }
        results = run_pre_clone_hooks(MagicMock(), "wh", config)
        assert len(results) == 1
        assert results[0]["status"] == "success"

    def test_no_hooks_returns_empty(self):
        config = {"source_catalog": "src", "destination_catalog": "dst"}
        results = run_pre_clone_hooks(MagicMock(), "wh", config)
        assert results == []

    @patch("src.hooks.execute_sql")
    def test_dry_run_passed_through(self, mock_sql):
        config = {
            "source_catalog": "src",
            "destination_catalog": "dst",
            "pre_clone_hooks": [{"sql": "SELECT 1"}],
        }
        run_pre_clone_hooks(MagicMock(), "wh", config, dry_run=True)
        _, kwargs = mock_sql.call_args
        assert kwargs.get("dry_run") is True


class TestRunPostCloneHooks:
    @patch("src.hooks.execute_sql")
    def test_runs_post_hooks(self, mock_sql):
        config = {
            "source_catalog": "src",
            "destination_catalog": "dst",
            "post_clone_hooks": [{"sql": "OPTIMIZE dst.s.t", "description": "Optimize"}],
        }
        results = run_post_clone_hooks(MagicMock(), "wh", config)
        assert len(results) == 1

    def test_no_hooks_returns_empty(self):
        config = {}
        results = run_post_clone_hooks(MagicMock(), "wh", config)
        assert results == []


class TestRunPostSchemaHooks:
    @patch("src.hooks.execute_sql")
    def test_runs_schema_hooks_with_context(self, mock_sql):
        config = {
            "source_catalog": "src",
            "destination_catalog": "dst",
            "post_schema_hooks": [{"sql": "ANALYZE TABLE ${dest_catalog}.${schema}.t1"}],
        }
        results = run_post_schema_hooks(MagicMock(), "wh", config, schema="my_schema")
        assert len(results) == 1
        called_sql = mock_sql.call_args[0][2]
        assert "dst" in called_sql
        assert "my_schema" in called_sql

    def test_no_hooks_returns_empty(self):
        config = {}
        results = run_post_schema_hooks(MagicMock(), "wh", config, schema="s1")
        assert results == []
