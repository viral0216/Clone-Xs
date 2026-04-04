"""Tests for src.client helper functions — sql_escape, utc_now, query_sql, run_sql, get_workspace_client."""

import re
from unittest.mock import patch, MagicMock

import pytest

from src.client import sql_escape, utc_now, query_sql, run_sql, get_workspace_client


# ── sql_escape ────────────────────────────────────────────────────


class TestSqlEscape:
    def test_empty_string(self):
        assert sql_escape("") == ""

    def test_none(self):
        assert sql_escape(None) == ""

    def test_plain_string(self):
        assert sql_escape("hello") == "hello"

    def test_single_quote(self):
        assert sql_escape("it's") == "it''s"

    def test_multiple_quotes(self):
        assert sql_escape("it's a 'test'") == "it''s a ''test''"

    def test_no_quotes(self):
        assert sql_escape("no quotes here") == "no quotes here"

    def test_integer_input(self):
        assert sql_escape(42) == "42"

    def test_zero_is_falsy(self):
        """0 is falsy, so sql_escape(0) returns empty string."""
        assert sql_escape(0) == ""


# ── utc_now ───────────────────────────────────────────────────────


class TestUtcNow:
    def test_format(self):
        result = utc_now()
        assert len(result) == 19
        # Validate it matches the expected ISO format
        assert re.match(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$", result)

    def test_no_offset_suffix(self):
        result = utc_now()
        assert "+" not in result
        assert "Z" not in result


# ── query_sql ─────────────────────────────────────────────────────


class TestQuerySql:
    @patch("src.client._get_spark_safe", return_value=None)
    @patch("src.client.execute_sql")
    def test_fallback_to_warehouse(self, mock_execute_sql, mock_spark):
        mock_client = MagicMock()
        mock_execute_sql.return_value = [{"id": 1}, {"id": 2}]

        result = query_sql("SELECT 1", client=mock_client, warehouse_id="wh123")

        mock_execute_sql.assert_called_once_with(mock_client, "wh123", "SELECT 1")
        assert result == [{"id": 1}, {"id": 2}]

    @patch("src.client._get_spark_safe", return_value=None)
    def test_no_spark_no_client_raises(self, mock_spark):
        with pytest.raises(RuntimeError, match="No Spark session or SQL warehouse"):
            query_sql("SELECT 1")

    @patch("src.client._get_spark_safe", return_value=None)
    def test_no_spark_no_warehouse_id_raises(self, mock_spark):
        mock_client = MagicMock()
        with pytest.raises(RuntimeError, match="No Spark session or SQL warehouse"):
            query_sql("SELECT 1", client=mock_client, warehouse_id="")

    @patch("src.client._get_spark_safe")
    def test_spark_path(self, mock_spark_safe):
        mock_spark = MagicMock()
        mock_spark_safe.return_value = mock_spark

        mock_row = MagicMock()
        mock_row.asDict.return_value = {"col1": "val1"}
        mock_spark.sql.return_value.limit.return_value.collect.return_value = [mock_row]

        result = query_sql("SELECT 1")

        mock_spark.sql.assert_called_once_with("SELECT 1")
        assert result == [{"col1": "val1"}]

    @patch("src.client._get_spark_safe", return_value=None)
    @patch("src.client.execute_sql")
    def test_limit_applied_to_warehouse_results(self, mock_execute_sql, mock_spark):
        mock_client = MagicMock()
        mock_execute_sql.return_value = [{"id": i} for i in range(100)]

        result = query_sql("SELECT 1", limit=5, client=mock_client, warehouse_id="wh123")
        assert len(result) == 5


# ── run_sql ───────────────────────────────────────────────────────


class TestRunSql:
    @patch("src.client._get_spark_safe", return_value=None)
    @patch("src.client.execute_sql")
    def test_fallback_to_warehouse(self, mock_execute_sql, mock_spark):
        mock_client = MagicMock()
        run_sql("CREATE TABLE t", client=mock_client, warehouse_id="wh123")
        mock_execute_sql.assert_called_once_with(mock_client, "wh123", "CREATE TABLE t")

    @patch("src.client._get_spark_safe", return_value=None)
    def test_no_spark_no_client_raises(self, mock_spark):
        with pytest.raises(RuntimeError, match="No Spark session or SQL warehouse"):
            run_sql("CREATE TABLE t")

    @patch("src.client._get_spark_safe")
    def test_spark_path(self, mock_spark_safe):
        mock_spark = MagicMock()
        mock_spark_safe.return_value = mock_spark

        run_sql("CREATE TABLE t")
        mock_spark.sql.assert_called_once_with("CREATE TABLE t")


# ── get_workspace_client ─────────────────────────────────────────


class TestGetWorkspaceClient:
    @patch("src.env.get_databricks_host", return_value="https://host.com")
    @patch("src.env.get_databricks_token", return_value="tok123")
    @patch("src.auth.get_client")
    def test_delegates_to_auth_get_client(self, mock_get_client, mock_token, mock_host):
        mock_get_client.return_value = MagicMock()

        result = get_workspace_client()

        mock_get_client.assert_called_once_with("https://host.com", "tok123", None)
        assert result is mock_get_client.return_value

    @patch("src.env.get_databricks_host", return_value="")
    @patch("src.env.get_databricks_token", return_value="")
    @patch("src.auth.get_client")
    def test_passes_none_when_env_empty(self, mock_get_client, mock_token, mock_host):
        mock_get_client.return_value = MagicMock()

        get_workspace_client()

        # Empty strings become None via `or None`
        mock_get_client.assert_called_once_with(None, None, None)

    @patch("src.env.get_databricks_host", return_value="")
    @patch("src.env.get_databricks_token", return_value="")
    @patch("src.auth.get_client")
    def test_explicit_args_override_env(self, mock_get_client, mock_token, mock_host):
        mock_get_client.return_value = MagicMock()

        get_workspace_client(host="https://explicit.com", token="explicit_tok", profile="myprofile")

        mock_get_client.assert_called_once_with("https://explicit.com", "explicit_tok", "myprofile")
