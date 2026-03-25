"""Tests for src/compare.py — deep catalog comparison."""

from unittest.mock import MagicMock, patch

from src.compare import compare_table_deep, compare_catalogs_deep, _get_tblproperties


class TestGetTblproperties:
    @patch("src.compare.execute_sql")
    def test_returns_properties_dict(self, mock_sql):
        mock_sql.return_value = [
            {"key": "owner", "value": "user@example.com"},
            {"key": "comment", "value": "test table"},
        ]
        result = _get_tblproperties(MagicMock(), "wh-123", "cat", "sch", "tbl")
        assert result == {"owner": "user@example.com", "comment": "test table"}

    @patch("src.compare.execute_sql", side_effect=Exception("access denied"))
    def test_returns_empty_on_error(self, mock_sql):
        result = _get_tblproperties(MagicMock(), "wh-123", "cat", "sch", "tbl")
        assert result == {}

    @patch("src.compare.execute_sql")
    def test_skips_rows_without_key(self, mock_sql):
        mock_sql.return_value = [
            {"key": "", "value": "ignored"},
            {"key": "valid", "value": "kept"},
        ]
        result = _get_tblproperties(MagicMock(), "wh-123", "cat", "sch", "tbl")
        assert result == {"valid": "kept"}


class TestCompareTableDeep:
    @patch("src.compare._get_tblproperties")
    @patch("src.compare.execute_sql")
    @patch("src.compare.compare_table_schema")
    def test_happy_path_no_issues(self, mock_schema_cmp, mock_sql, mock_props):
        mock_schema_cmp.return_value = {"has_drift": False}
        mock_sql.side_effect = [
            [{"cnt": 100}],  # source rows
            [{"cnt": 100}],  # dest rows
        ]
        mock_props.side_effect = [
            {"owner": "user"},  # source props
            {"owner": "user"},  # dest props
        ]

        result = compare_table_deep(
            MagicMock(), "wh-123", "src_cat", "dst_cat", "sch", "tbl",
        )

        assert result["schema"] == "sch"
        assert result["table"] == "tbl"
        assert result["row_count_match"] is True
        assert result["source_rows"] == 100
        assert result["dest_rows"] == 100
        assert result["issues"] == []

    @patch("src.compare._get_tblproperties")
    @patch("src.compare.execute_sql")
    @patch("src.compare.compare_table_schema")
    def test_detects_schema_drift(self, mock_schema_cmp, mock_sql, mock_props):
        mock_schema_cmp.return_value = {"has_drift": True}
        mock_sql.side_effect = [
            [{"cnt": 50}],
            [{"cnt": 50}],
        ]
        mock_props.side_effect = [{}, {}]

        result = compare_table_deep(
            MagicMock(), "wh-123", "src_cat", "dst_cat", "sch", "tbl",
        )

        assert "schema_drift" in result["issues"]

    @patch("src.compare._get_tblproperties")
    @patch("src.compare.execute_sql")
    @patch("src.compare.compare_table_schema")
    def test_detects_row_count_mismatch(self, mock_schema_cmp, mock_sql, mock_props):
        mock_schema_cmp.return_value = {"has_drift": False}
        mock_sql.side_effect = [
            [{"cnt": 100}],
            [{"cnt": 95}],
        ]
        mock_props.side_effect = [{}, {}]

        result = compare_table_deep(
            MagicMock(), "wh-123", "src_cat", "dst_cat", "sch", "tbl",
        )

        assert result["row_count_match"] is False
        assert "row_count_mismatch" in result["issues"]

    @patch("src.compare._get_tblproperties")
    @patch("src.compare.execute_sql")
    @patch("src.compare.compare_table_schema")
    def test_detects_properties_mismatch(self, mock_schema_cmp, mock_sql, mock_props):
        mock_schema_cmp.return_value = {"has_drift": False}
        mock_sql.side_effect = [
            [{"cnt": 10}],
            [{"cnt": 10}],
        ]
        mock_props.side_effect = [
            {"owner": "alice", "comment": "prod"},
            {"owner": "bob", "comment": "prod"},
        ]

        result = compare_table_deep(
            MagicMock(), "wh-123", "src_cat", "dst_cat", "sch", "tbl",
        )

        assert "properties_mismatch" in result["issues"]
        assert "owner" in result["properties_diff"]

    @patch("src.compare._get_tblproperties")
    @patch("src.compare.execute_sql")
    @patch("src.compare.compare_table_schema", side_effect=Exception("schema error"))
    def test_schema_compare_error_captured(self, mock_schema_cmp, mock_sql, mock_props):
        mock_sql.side_effect = [
            [{"cnt": 10}],
            [{"cnt": 10}],
        ]
        mock_props.side_effect = [{}, {}]

        result = compare_table_deep(
            MagicMock(), "wh-123", "src_cat", "dst_cat", "sch", "tbl",
        )

        assert any("schema_compare_error" in i for i in result["issues"])

    @patch("src.compare._get_tblproperties")
    @patch("src.compare.execute_sql")
    @patch("src.compare.compare_table_schema")
    def test_delta_properties_skipped(self, mock_schema_cmp, mock_sql, mock_props):
        mock_schema_cmp.return_value = {"has_drift": False}
        mock_sql.side_effect = [[{"cnt": 5}], [{"cnt": 5}]]
        mock_props.side_effect = [
            {"delta.minReaderVersion": "1", "owner": "x"},
            {"delta.minReaderVersion": "2", "owner": "x"},
        ]

        result = compare_table_deep(
            MagicMock(), "wh-123", "src_cat", "dst_cat", "sch", "tbl",
        )

        # delta.* properties should be ignored
        assert result["issues"] == []


class TestCompareCatalogsDeep:
    @patch("src.compare.ProgressTracker")
    @patch("src.compare.compare_table_deep")
    @patch("src.compare.list_tables_sdk")
    @patch("src.compare.list_schemas_sdk")
    def test_happy_path(self, mock_schemas, mock_tables, mock_compare, mock_progress):
        mock_schemas.return_value = ["sales"]
        mock_tables.return_value = [
            {"table_name": "orders", "table_type": "MANAGED"},
        ]
        mock_compare.return_value = {
            "schema": "sales",
            "table": "orders",
            "issues": [],
        }
        mock_progress.return_value = MagicMock()

        result = compare_catalogs_deep(
            MagicMock(), "wh-123", "src", "dst",
            exclude_schemas=["information_schema"],
        )

        assert result["total_tables"] == 1
        assert result["tables_ok"] == 1
        assert result["tables_with_issues"] == 0

    @patch("src.compare.ProgressTracker")
    @patch("src.compare.compare_table_deep")
    @patch("src.compare.list_tables_sdk")
    @patch("src.compare.list_schemas_sdk")
    def test_filters_views(self, mock_schemas, mock_tables, mock_compare, mock_progress):
        mock_schemas.return_value = ["sch"]
        mock_tables.return_value = [
            {"table_name": "t1", "table_type": "MANAGED"},
            {"table_name": "v1", "table_type": "VIEW"},
        ]
        mock_compare.return_value = {
            "schema": "sch", "table": "t1", "issues": [],
        }
        mock_progress.return_value = MagicMock()

        result = compare_catalogs_deep(
            MagicMock(), "wh-123", "src", "dst", exclude_schemas=[],
        )

        # VIEW should be filtered out
        assert result["total_tables"] == 1

    @patch("src.compare.ProgressTracker")
    @patch("src.compare.list_tables_sdk")
    @patch("src.compare.list_schemas_sdk")
    def test_empty_catalog(self, mock_schemas, mock_tables, mock_progress):
        mock_schemas.return_value = []
        mock_progress.return_value = MagicMock()

        result = compare_catalogs_deep(
            MagicMock(), "wh-123", "src", "dst", exclude_schemas=[],
        )

        assert result["total_tables"] == 0
        assert result["tables_ok"] == 0
