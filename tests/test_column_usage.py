from unittest.mock import MagicMock, patch

from src.column_usage import (
    query_column_usage,
    query_column_users,
    query_column_stats_fallback,
    get_column_usage_summary,
)


# ---------- query_column_usage ----------

@patch("src.column_usage.execute_sql")
def test_query_column_usage_happy(mock_sql):
    mock_sql.return_value = [
        {"column_name": "id", "table_name": "cat.s.t", "usage_count": 10,
         "downstream_count": 2, "last_used": "2025-01-01"},
    ]
    rows = query_column_usage(MagicMock(), "wh-1", "cat")
    assert len(rows) == 1
    assert rows[0]["column_name"] == "id"


@patch("src.column_usage.execute_sql")
def test_query_column_usage_with_table_fqn(mock_sql):
    mock_sql.return_value = []
    query_column_usage(MagicMock(), "wh-1", "cat", table_fqn="cat.s.t")
    sql_arg = mock_sql.call_args[0][2]
    assert "source_table_full_name = 'cat.s.t'" in sql_arg


@patch("src.column_usage.execute_sql")
def test_query_column_usage_system_table_unavailable(mock_sql):
    mock_sql.side_effect = Exception("access denied")
    rows = query_column_usage(MagicMock(), "wh-1", "cat")
    assert rows == []


# ---------- query_column_users ----------

@patch("src.column_usage.execute_sql")
def test_query_column_users_happy(mock_sql):
    mock_sql.return_value = [
        {
            "executed_by": "alice",
            "statement_text": "SELECT col1, col2 FROM cat.schema1.table1 WHERE col1 > 0",
            "start_time": "2025-01-01",
        },
    ]
    result = query_column_users(MagicMock(), "wh-1", "cat")
    assert "columns" in result
    assert "top_users" in result
    assert len(result["top_users"]) >= 1
    assert result["top_users"][0]["user"] == "alice"


@patch("src.column_usage.execute_sql")
def test_query_column_users_no_rows(mock_sql):
    mock_sql.return_value = []
    result = query_column_users(MagicMock(), "wh-1", "cat")
    assert result == {"columns": [], "top_users": []}


@patch("src.column_usage.execute_sql")
def test_query_column_users_system_table_unavailable(mock_sql):
    mock_sql.side_effect = Exception("not available")
    result = query_column_users(MagicMock(), "wh-1", "cat")
    assert result == {"columns": [], "top_users": []}


# ---------- query_column_stats_fallback ----------

@patch("src.column_usage.execute_sql")
def test_query_column_stats_fallback_happy(mock_sql):
    mock_sql.return_value = [
        {"column_name": "id", "table_name": "cat.s.t", "data_type": "BIGINT", "name_frequency": 5},
    ]
    rows = query_column_stats_fallback(MagicMock(), "wh-1", "cat")
    assert len(rows) == 1


@patch("src.column_usage.execute_sql")
def test_query_column_stats_fallback_failure(mock_sql):
    mock_sql.side_effect = Exception("nope")
    rows = query_column_stats_fallback(MagicMock(), "wh-1", "cat")
    assert rows == []


# ---------- get_column_usage_summary ----------

@patch("src.column_usage.query_column_stats_fallback")
@patch("src.column_usage.query_column_usage")
def test_get_column_usage_summary_fast_path(mock_lineage, mock_fallback):
    """Default (use_system_tables=False) uses fallback."""
    mock_lineage.return_value = []
    mock_fallback.return_value = [
        {"column_name": "id", "table_name": "cat.s.t", "data_type": "BIGINT", "name_frequency": 3},
    ]
    result = get_column_usage_summary(MagicMock(), "wh-1", "cat")
    assert len(result["top_columns"]) == 1
    assert result["top_columns"][0]["column"] == "id"
    # lineage should NOT have been called (use_system_tables=False)
    mock_lineage.assert_not_called()


@patch("src.column_usage.query_column_users")
@patch("src.column_usage.query_column_usage")
def test_get_column_usage_summary_full_mode(mock_lineage, mock_users):
    mock_lineage.return_value = [
        {"column_name": "id", "table_name": "cat.s.t", "usage_count": 5,
         "downstream_count": 1, "last_used": "2025-01-01"},
    ]
    mock_users.return_value = {"columns": [], "top_users": []}
    result = get_column_usage_summary(
        MagicMock(), "wh-1", "cat", use_system_tables=True,
    )
    assert len(result["top_columns"]) == 1
    assert result["top_columns"][0]["lineage_count"] == 5
