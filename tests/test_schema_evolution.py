"""Tests for the schema evolution handler module."""

from unittest.mock import MagicMock, patch

from src.schema_evolution import detect_schema_changes, apply_schema_evolution


class TestDetectSchemaChanges:
    @patch("src.schema_evolution.execute_sql")
    def test_no_changes(self, mock_sql):
        columns = [
            {"column_name": "id", "data_type": "INT", "is_nullable": "NO", "ordinal_position": "1"},
            {"column_name": "name", "data_type": "STRING", "is_nullable": "YES", "ordinal_position": "2"},
        ]
        # Source and dest return same columns
        mock_sql.side_effect = [columns, columns]

        result = detect_schema_changes(
            MagicMock(), "wh-123", "src_cat", "dst_cat", "schema1", "table1"
        )

        assert result["added_columns"] == []
        assert result["removed_columns"] == []
        assert result["changed_columns"] == []
        assert result["is_compatible"] is True
        assert result["requires_action"] is False

    @patch("src.schema_evolution.execute_sql")
    def test_added_nullable_column(self, mock_sql):
        source_cols = [
            {"column_name": "id", "data_type": "INT", "is_nullable": "NO", "ordinal_position": "1"},
            {"column_name": "name", "data_type": "STRING", "is_nullable": "YES", "ordinal_position": "2"},
            {"column_name": "email", "data_type": "STRING", "is_nullable": "YES", "ordinal_position": "3"},
        ]
        dest_cols = [
            {"column_name": "id", "data_type": "INT", "is_nullable": "NO", "ordinal_position": "1"},
            {"column_name": "name", "data_type": "STRING", "is_nullable": "YES", "ordinal_position": "2"},
        ]
        mock_sql.side_effect = [source_cols, dest_cols]

        result = detect_schema_changes(
            MagicMock(), "wh-123", "src_cat", "dst_cat", "schema1", "table1"
        )

        assert len(result["added_columns"]) == 1
        assert result["added_columns"][0]["column"] == "email"
        assert result["is_compatible"] is True
        assert result["requires_action"] is True

    @patch("src.schema_evolution.execute_sql")
    def test_removed_column(self, mock_sql):
        source_cols = [
            {"column_name": "id", "data_type": "INT", "is_nullable": "NO", "ordinal_position": "1"},
        ]
        dest_cols = [
            {"column_name": "id", "data_type": "INT", "is_nullable": "NO", "ordinal_position": "1"},
            {"column_name": "old_col", "data_type": "STRING", "is_nullable": "YES", "ordinal_position": "2"},
        ]
        mock_sql.side_effect = [source_cols, dest_cols]

        result = detect_schema_changes(
            MagicMock(), "wh-123", "src_cat", "dst_cat", "schema1", "table1"
        )

        assert len(result["removed_columns"]) == 1
        assert result["removed_columns"][0]["column"] == "old_col"
        assert result["is_compatible"] is False

    @patch("src.schema_evolution.execute_sql")
    def test_type_change(self, mock_sql):
        source_cols = [
            {"column_name": "amount", "data_type": "DOUBLE", "is_nullable": "YES", "ordinal_position": "1"},
        ]
        dest_cols = [
            {"column_name": "amount", "data_type": "INT", "is_nullable": "YES", "ordinal_position": "1"},
        ]
        mock_sql.side_effect = [source_cols, dest_cols]

        result = detect_schema_changes(
            MagicMock(), "wh-123", "src_cat", "dst_cat", "schema1", "table1"
        )

        assert len(result["changed_columns"]) == 1
        assert result["changed_columns"][0]["source_type"] == "DOUBLE"
        assert result["changed_columns"][0]["dest_type"] == "INT"
        assert result["is_compatible"] is False


class TestApplySchemaEvolution:
    @patch("src.schema_evolution.execute_sql")
    def test_add_column_dry_run(self, mock_sql):
        changes = {
            "added_columns": [{"column": "email", "data_type": "STRING", "nullable": "YES"}],
            "removed_columns": [],
            "changed_columns": [],
        }

        result = apply_schema_evolution(
            MagicMock(), "wh-123", "dst_cat", "schema1", "table1",
            changes, dry_run=True,
        )

        assert "email" in result["added"]
        mock_sql.assert_not_called()

    @patch("src.schema_evolution.execute_sql")
    def test_add_column_live(self, mock_sql):
        mock_sql.return_value = []
        changes = {
            "added_columns": [{"column": "phone", "data_type": "STRING", "nullable": "YES"}],
            "removed_columns": [],
            "changed_columns": [],
        }

        result = apply_schema_evolution(
            MagicMock(), "wh-123", "dst_cat", "schema1", "table1",
            changes, dry_run=False,
        )

        assert "phone" in result["added"]
        assert mock_sql.called
        sql = mock_sql.call_args[0][2]
        assert "ALTER TABLE" in sql
        assert "ADD COLUMN" in sql
