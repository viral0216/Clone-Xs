from unittest.mock import MagicMock, call, patch

from src.clone_tables import clone_table


@patch("src.clone_tables.execute_sql")
def test_clone_table_deep(mock_sql):
    mock_sql.return_value = []
    result = clone_table(
        MagicMock(), "wh-123", "src", "dst", "schema1", "table1", "DEEP",
    )
    assert result is True
    sql_called = mock_sql.call_args[0][2]
    assert "DEEP CLONE" in sql_called


@patch("src.clone_tables.execute_sql")
def test_clone_table_shallow(mock_sql):
    mock_sql.return_value = []
    result = clone_table(
        MagicMock(), "wh-123", "src", "dst", "schema1", "table1", "SHALLOW",
    )
    assert result is True
    sql_called = mock_sql.call_args[0][2]
    assert "SHALLOW CLONE" in sql_called


@patch("src.clone_tables.execute_sql")
def test_clone_table_failure(mock_sql):
    mock_sql.side_effect = Exception("permission denied")
    result = clone_table(
        MagicMock(), "wh-123", "src", "dst", "schema1", "table1", "DEEP",
    )
    assert result is False


@patch("src.clone_tables.execute_sql")
def test_clone_table_dry_run(mock_sql):
    mock_sql.return_value = []
    result = clone_table(
        MagicMock(), "wh-123", "src", "dst", "schema1", "table1", "DEEP",
        dry_run=True,
    )
    assert result is True
    sql_called = mock_sql.call_args[0][2]
    assert "DEEP CLONE" in sql_called


@patch("src.clone_tables.execute_sql")
def test_clone_table_with_timestamp(mock_sql):
    mock_sql.return_value = []
    result = clone_table(
        MagicMock(), "wh-123", "src", "dst", "schema1", "table1", "DEEP",
        as_of_timestamp="2024-01-15T00:00:00",
    )
    assert result is True
    sql_called = mock_sql.call_args[0][2]
    assert "TIMESTAMP AS OF" in sql_called
    assert "2024-01-15" in sql_called


@patch("src.clone_tables.execute_sql")
def test_clone_table_with_version(mock_sql):
    mock_sql.return_value = []
    result = clone_table(
        MagicMock(), "wh-123", "src", "dst", "schema1", "table1", "DEEP",
        as_of_version=5,
    )
    assert result is True
    sql_called = mock_sql.call_args[0][2]
    assert "VERSION AS OF 5" in sql_called
