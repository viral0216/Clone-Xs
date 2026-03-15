from unittest.mock import MagicMock, patch

from src.validation import get_row_count, validate_table


@patch("src.validation.execute_sql")
def test_get_row_count(mock_sql):
    mock_sql.return_value = [{"cnt": "42"}]
    count = get_row_count(MagicMock(), "wh", "cat", "schema", "table")
    assert count == 42


@patch("src.validation.execute_sql")
def test_get_row_count_failure(mock_sql):
    mock_sql.side_effect = Exception("table not found")
    count = get_row_count(MagicMock(), "wh", "cat", "schema", "table")
    assert count is None


@patch("src.validation.get_row_count")
def test_validate_table_match(mock_count):
    mock_count.side_effect = [100, 100]
    result = validate_table(MagicMock(), "wh", "src", "dst", "s1", "t1")
    assert result["match"] is True
    assert result["source_count"] == 100
    assert result["dest_count"] == 100


@patch("src.validation.get_row_count")
def test_validate_table_mismatch(mock_count):
    mock_count.side_effect = [100, 50]
    result = validate_table(MagicMock(), "wh", "src", "dst", "s1", "t1")
    assert result["match"] is False
