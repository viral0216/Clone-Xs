"""Unit tests for src/data_freshness.py — table freshness checks."""

from unittest.mock import patch, MagicMock
from datetime import datetime, timezone, timedelta
import pytest


@patch("src.data_freshness._get_schema_prefix", return_value="clone_audit.data_quality")
class TestCheckFreshness:
    """Tests for check_freshness."""

    def test_skipped_catalog_returns_empty(self, mock_schema):
        """system, hive_metastore, etc. should be skipped with zero tables."""
        from src.data_freshness import check_freshness

        client = MagicMock()
        result = check_freshness(client, "system", warehouse_id="wh-1")

        assert result["catalog"] == "system"
        assert result["total_tables"] == 0
        assert result["tables"] == []

    def test_skipped_catalog_dunder_prefix(self, mock_schema):
        from src.data_freshness import check_freshness

        client = MagicMock()
        result = check_freshness(client, "__databricks_internal", warehouse_id="wh-1")

        assert result["total_tables"] == 0

    @patch("src.data_freshness._run_sql")
    @patch("src.data_freshness._query_sql")
    def test_fresh_and_stale_classification(
        self, mock_query, mock_run, mock_schema
    ):
        """Tables within max_stale_hours are fresh; older ones are stale."""
        from src.data_freshness import check_freshness

        now = datetime.now(timezone.utc)
        recent_time = (now - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%S")
        old_time = (now - timedelta(hours=48)).strftime("%Y-%m-%dT%H:%M:%S")

        # First call = SHOW SCHEMAS (verification), second = information_schema query
        mock_query.side_effect = [
            [{"databaseName": "sales"}],  # SHOW SCHEMAS
            [
                {
                    "table_catalog": "my_catalog",
                    "table_schema": "sales",
                    "table_name": "orders",
                    "last_altered": recent_time,
                },
                {
                    "table_catalog": "my_catalog",
                    "table_schema": "sales",
                    "table_name": "archive",
                    "last_altered": old_time,
                },
            ],
        ]

        client = MagicMock()
        result = check_freshness(
            client, "my_catalog", max_stale_hours=24, warehouse_id="wh-1"
        )

        assert result["catalog"] == "my_catalog"
        assert result["total_tables"] == 2
        assert result["fresh"] == 1
        assert result["stale"] == 1

        tables = result["tables"]
        fresh_table = [t for t in tables if t["status"] == "fresh"]
        stale_table = [t for t in tables if t["status"] == "stale"]
        assert len(fresh_table) == 1
        assert fresh_table[0]["table_fqn"] == "my_catalog.sales.orders"
        assert len(stale_table) == 1
        assert stale_table[0]["table_fqn"] == "my_catalog.sales.archive"
        assert stale_table[0]["is_stale"] is True

    @patch("src.data_freshness._run_sql")
    @patch("src.data_freshness._query_sql")
    def test_query_failure_returns_error_key(
        self, mock_query, mock_run, mock_schema
    ):
        """When the information_schema query fails, result has an error key."""
        from src.data_freshness import check_freshness

        # SHOW SCHEMAS succeeds, but information_schema query fails
        mock_query.side_effect = [
            [{"databaseName": "sales"}],
            RuntimeError("SQL error"),
        ]

        client = MagicMock()
        result = check_freshness(client, "bad_catalog", warehouse_id="wh-1")

        assert result["total_tables"] == 0
        assert "error" in result
        assert result["tables"] == []

    @patch("src.data_freshness._run_sql")
    @patch("src.data_freshness._query_sql")
    def test_catalog_not_accessible_returns_error(
        self, mock_query, mock_run, mock_schema
    ):
        """When SHOW SCHEMAS fails, result has an error key."""
        from src.data_freshness import check_freshness

        mock_query.side_effect = RuntimeError("catalog not found")

        client = MagicMock()
        result = check_freshness(client, "nonexistent_cat", warehouse_id="wh-1")

        assert result["total_tables"] == 0
        assert "error" in result

    @patch("src.data_freshness._run_sql")
    @patch("src.data_freshness._query_sql")
    def test_unknown_status_when_last_altered_is_none(
        self, mock_query, mock_run, mock_schema
    ):
        from src.data_freshness import check_freshness

        mock_query.side_effect = [
            [{"databaseName": "s"}],
            [
                {
                    "table_catalog": "cat",
                    "table_schema": "s",
                    "table_name": "t",
                    "last_altered": None,
                },
            ],
        ]

        client = MagicMock()
        result = check_freshness(client, "cat", warehouse_id="wh-1")

        assert result["unknown"] == 1
        assert result["tables"][0]["status"] == "unknown"
        assert result["tables"][0]["is_stale"] is False
