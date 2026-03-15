"""Tests for data filtering with WHERE clause."""

from unittest.mock import MagicMock, patch, call

from src.clone_tables import clone_table


class TestDataFiltering:
    @patch("src.clone_tables.execute_sql")
    @patch("src.clone_tables.record_object")
    def test_where_clause_generates_ctas(self, mock_record, mock_sql):
        mock_sql.return_value = []
        clone_table(
            MagicMock(), "wh-123", "src_cat", "dst_cat", "schema1", "table1",
            clone_type="DEEP", where_clause="year >= 2024",
        )
        # Verify the SQL uses CREATE TABLE AS SELECT ... WHERE
        sql_calls = [c[0][2] for c in mock_sql.call_args_list]
        assert any("SELECT * FROM" in s and "WHERE year >= 2024" in s for s in sql_calls)

    @patch("src.clone_tables.execute_sql")
    @patch("src.clone_tables.record_object")
    def test_no_where_clause_uses_clone(self, mock_record, mock_sql):
        mock_sql.return_value = []
        clone_table(
            MagicMock(), "wh-123", "src_cat", "dst_cat", "schema1", "table1",
            clone_type="DEEP",
        )
        sql_calls = [c[0][2] for c in mock_sql.call_args_list]
        assert any("CLONE" in s for s in sql_calls)

    @patch("src.clone_tables.execute_sql")
    @patch("src.clone_tables.record_object")
    def test_where_clause_with_shallow_clone_ignored(self, mock_record, mock_sql):
        """WHERE clause should be ignored for SHALLOW clone."""
        mock_sql.return_value = []
        clone_table(
            MagicMock(), "wh-123", "src_cat", "dst_cat", "schema1", "table1",
            clone_type="SHALLOW", where_clause="year >= 2024",
        )
        sql_calls = [c[0][2] for c in mock_sql.call_args_list]
        # Should use CLONE, not CTAS
        assert any("CLONE" in s for s in sql_calls)
