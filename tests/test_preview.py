"""Tests for data sampling preview."""

from unittest.mock import patch

from src.preview import preview_comparison, format_side_by_side


class TestPreviewComparison:
    @patch("src.preview.execute_sql")
    def test_matching_rows(self, mock_sql):
        mock_sql.side_effect = [
            [{"id": 1, "name": "Alice"}],
            [{"id": 1, "name": "Alice"}],
        ]
        result = preview_comparison(
            None, "wh", "src", "dst", "schema1", "table1", limit=5,
        )
        assert result["match"] is True
        assert len(result["differences"]) == 0

    @patch("src.preview.execute_sql")
    def test_different_rows(self, mock_sql):
        mock_sql.side_effect = [
            [{"id": 1, "name": "Alice"}],
            [{"id": 1, "name": "Bob"}],
        ]
        result = preview_comparison(
            None, "wh", "src", "dst", "schema1", "table1", limit=5,
        )
        assert result["match"] is False
        assert len(result["differences"]) == 1

    @patch("src.preview.execute_sql")
    def test_empty_tables(self, mock_sql):
        mock_sql.side_effect = [[], []]
        result = preview_comparison(
            None, "wh", "src", "dst", "schema1", "table1",
        )
        assert result["match"] is True
        assert result["source_rows"] == 0


class TestFormatSideBySide:
    def test_matching_output(self):
        comparison = {
            "schema": "s1", "table": "t1",
            "source_rows": 1, "dest_rows": 1,
            "match": True, "differences": [],
            "source_data": [{"id": 1}],
            "dest_data": [{"id": 1}],
        }
        output = format_side_by_side(comparison)
        assert "PREVIEW" in output
        assert "matches" in output

    def test_diff_output(self):
        comparison = {
            "schema": "s1", "table": "t1",
            "source_rows": 1, "dest_rows": 1,
            "match": False,
            "differences": [{"row_index": 0, "source": {"id": 1}, "destination": {"id": 2}}],
            "source_data": [{"id": 1}],
            "dest_data": [{"id": 2}],
        }
        output = format_side_by_side(comparison)
        assert "Differences" in output
