"""Tests for src/search.py — table and column search."""

from unittest.mock import MagicMock, patch

from src.search import search_tables, _search_schema

import re


class TestSearchSchema:
    @patch("src.search.execute_sql")
    def test_finds_matching_table(self, mock_sql):
        mock_sql.return_value = [
            {"table_name": "orders", "table_type": "MANAGED", "comment": ""},
            {"table_name": "customers", "table_type": "MANAGED", "comment": ""},
        ]
        compiled = re.compile("order", re.IGNORECASE)
        tables, columns = _search_schema(
            MagicMock(), "wh-123", "cat", "sales", compiled, search_columns=False,
        )
        assert len(tables) == 1
        assert tables[0]["table"] == "orders"
        assert columns == []

    @patch("src.search.execute_sql")
    def test_searches_columns_when_enabled(self, mock_sql):
        mock_sql.side_effect = [
            # tables
            [{"table_name": "users", "table_type": "MANAGED", "comment": ""}],
            # columns
            [
                {"table_name": "users", "column_name": "email", "data_type": "STRING", "comment": ""},
                {"table_name": "users", "column_name": "name", "data_type": "STRING", "comment": ""},
            ],
        ]
        compiled = re.compile("email", re.IGNORECASE)
        tables, columns = _search_schema(
            MagicMock(), "wh-123", "cat", "sch", compiled, search_columns=True,
        )
        assert len(columns) == 1
        assert columns[0]["column"] == "email"

    @patch("src.search.execute_sql")
    def test_no_matches(self, mock_sql):
        mock_sql.return_value = [
            {"table_name": "abc", "table_type": "MANAGED", "comment": ""},
        ]
        compiled = re.compile("zzz", re.IGNORECASE)
        tables, columns = _search_schema(
            MagicMock(), "wh-123", "cat", "sch", compiled, search_columns=False,
        )
        assert tables == []
        assert columns == []


class TestSearchTables:
    @patch("src.search.ProgressTracker")
    @patch("src.search.get_max_parallel_queries", return_value=1)
    @patch("src.search.execute_sql")
    def test_search_tables_match(self, mock_sql, mock_max, mock_progress):
        mock_progress.return_value = MagicMock()
        mock_sql.side_effect = [
            # schemas
            [{"schema_name": "sales"}],
            # tables in sales
            [
                {"table_name": "orders", "table_type": "MANAGED", "comment": ""},
                {"table_name": "customers", "table_type": "MANAGED", "comment": ""},
                {"table_name": "products", "table_type": "MANAGED", "comment": ""},
            ],
        ]

        result = search_tables(
            MagicMock(), "wh-123", "my_catalog", "order",
            exclude_schemas=["information_schema"],
        )

        assert len(result["matched_tables"]) == 1
        assert result["matched_tables"][0]["table"] == "orders"
        assert result["pattern"] == "order"
        assert result["catalog"] == "my_catalog"

    @patch("src.search.ProgressTracker")
    @patch("src.search.get_max_parallel_queries", return_value=1)
    @patch("src.search.execute_sql")
    def test_search_tables_no_match(self, mock_sql, mock_max, mock_progress):
        mock_progress.return_value = MagicMock()
        mock_sql.side_effect = [
            [{"schema_name": "sales"}],
            [{"table_name": "customers", "table_type": "MANAGED", "comment": ""}],
        ]

        result = search_tables(
            MagicMock(), "wh-123", "my_catalog", "xyz_nonexistent",
            exclude_schemas=["information_schema"],
        )

        assert len(result["matched_tables"]) == 0

    @patch("src.search.ProgressTracker")
    @patch("src.search.get_max_parallel_queries", return_value=1)
    @patch("src.search.execute_sql")
    def test_search_with_columns(self, mock_sql, mock_max, mock_progress):
        mock_progress.return_value = MagicMock()
        mock_sql.side_effect = [
            [{"schema_name": "hr"}],
            [{"table_name": "employees", "table_type": "MANAGED", "comment": ""}],
            # columns
            [
                {"table_name": "employees", "column_name": "email", "data_type": "STRING", "comment": ""},
                {"table_name": "employees", "column_name": "name", "data_type": "STRING", "comment": ""},
                {"table_name": "employees", "column_name": "email_verified", "data_type": "BOOLEAN", "comment": ""},
            ],
        ]

        result = search_tables(
            MagicMock(), "wh-123", "my_catalog", "email",
            exclude_schemas=["information_schema"],
            search_columns=True,
        )

        assert len(result["matched_columns"]) == 2
        assert result["matched_columns"][0]["column"] == "email"
        assert result["matched_columns"][1]["column"] == "email_verified"

    @patch("src.search.ProgressTracker")
    @patch("src.search.get_max_parallel_queries", return_value=1)
    @patch("src.search.execute_sql")
    def test_with_include_schemas(self, mock_sql, mock_max, mock_progress):
        mock_progress.return_value = MagicMock()
        mock_sql.return_value = [
            {"table_name": "t1", "table_type": "MANAGED", "comment": ""},
        ]

        result = search_tables(
            MagicMock(), "wh-123", "cat", "t1",
            exclude_schemas=["information_schema"],
            include_schemas=["sales", "information_schema"],
        )

        # information_schema should be excluded even though it's in include list
        assert len(result["matched_tables"]) == 1

    @patch("src.search.ProgressTracker")
    @patch("src.search.get_max_parallel_queries", return_value=1)
    @patch("src.search.execute_sql")
    def test_case_insensitive_match(self, mock_sql, mock_max, mock_progress):
        mock_progress.return_value = MagicMock()
        mock_sql.side_effect = [
            [{"schema_name": "sch"}],
            [{"table_name": "MyTable", "table_type": "MANAGED", "comment": ""}],
        ]

        result = search_tables(
            MagicMock(), "wh-123", "cat", "mytable",
            exclude_schemas=[],
        )

        assert len(result["matched_tables"]) == 1
