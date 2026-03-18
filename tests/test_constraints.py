from unittest.mock import MagicMock, call, patch

from src.constraints import (
    get_table_constraints,
    copy_table_constraints,
    get_column_comments,
    get_table_comment,
    copy_table_comments,
)


# ---------- get_table_constraints ----------

@patch("src.constraints.execute_sql")
def test_get_table_constraints_happy(mock_sql):
    mock_sql.return_value = [
        {"constraint_name": "ck_positive", "constraint_type": "CHECK", "check_clause": "id > 0"},
    ]
    result = get_table_constraints(MagicMock(), "wh-1", "cat", "s1", "t1")
    assert len(result) == 1
    assert result[0]["constraint_name"] == "ck_positive"


@patch("src.constraints.execute_sql")
def test_get_table_constraints_failure(mock_sql):
    mock_sql.side_effect = Exception("not found")
    result = get_table_constraints(MagicMock(), "wh-1", "cat", "s1", "t1")
    assert result == []


# ---------- copy_table_constraints ----------

@patch("src.constraints.execute_sql")
@patch("src.constraints.get_table_constraints")
def test_copy_table_constraints_happy(mock_get, mock_sql):
    mock_get.return_value = [
        {"constraint_name": "ck_pos", "check_clause": "id > 0"},
        {"constraint_name": "ck_name", "check_clause": "name IS NOT NULL"},
    ]
    mock_sql.return_value = []
    copy_table_constraints(MagicMock(), "wh-1", "src", "dst", "s1", "t1")
    assert mock_sql.call_count == 2
    first_sql = mock_sql.call_args_list[0][0][2]
    assert "ALTER TABLE" in first_sql
    assert "ck_pos" in first_sql
    assert "id > 0" in first_sql


@patch("src.constraints.execute_sql")
@patch("src.constraints.get_table_constraints")
def test_copy_table_constraints_no_constraints(mock_get, mock_sql):
    mock_get.return_value = []
    copy_table_constraints(MagicMock(), "wh-1", "src", "dst", "s1", "t1")
    mock_sql.assert_not_called()


@patch("src.constraints.execute_sql")
@patch("src.constraints.get_table_constraints")
def test_copy_table_constraints_empty_check_clause(mock_get, mock_sql):
    mock_get.return_value = [{"constraint_name": "ck_empty", "check_clause": ""}]
    copy_table_constraints(MagicMock(), "wh-1", "src", "dst", "s1", "t1")
    mock_sql.assert_not_called()


@patch("src.constraints.execute_sql")
@patch("src.constraints.get_table_constraints")
def test_copy_table_constraints_sql_failure_no_raise(mock_get, mock_sql):
    mock_get.return_value = [
        {"constraint_name": "ck_pos", "check_clause": "id > 0"},
    ]
    mock_sql.side_effect = Exception("already exists")
    # Should not raise — errors are logged
    copy_table_constraints(MagicMock(), "wh-1", "src", "dst", "s1", "t1")


# ---------- get_table_comment ----------

@patch("src.constraints.execute_sql")
def test_get_table_comment_happy(mock_sql):
    mock_sql.return_value = [{"comment": "A useful table"}]
    result = get_table_comment(MagicMock(), "wh-1", "cat", "s1", "t1")
    assert result == "A useful table"


@patch("src.constraints.execute_sql")
def test_get_table_comment_none(mock_sql):
    mock_sql.return_value = []
    result = get_table_comment(MagicMock(), "wh-1", "cat", "s1", "t1")
    assert result is None


@patch("src.constraints.execute_sql")
def test_get_table_comment_failure(mock_sql):
    mock_sql.side_effect = Exception("access denied")
    result = get_table_comment(MagicMock(), "wh-1", "cat", "s1", "t1")
    assert result is None


# ---------- get_column_comments ----------

@patch("src.constraints.execute_sql")
def test_get_column_comments_happy(mock_sql):
    mock_sql.return_value = [
        {"column_name": "id", "comment": "Primary key"},
    ]
    result = get_column_comments(MagicMock(), "wh-1", "cat", "s1", "t1")
    assert len(result) == 1


@patch("src.constraints.execute_sql")
def test_get_column_comments_failure(mock_sql):
    mock_sql.side_effect = Exception("nope")
    result = get_column_comments(MagicMock(), "wh-1", "cat", "s1", "t1")
    assert result == []


# ---------- copy_table_comments ----------

@patch("src.constraints.get_column_comments")
@patch("src.constraints.get_table_comment")
@patch("src.constraints.execute_sql")
def test_copy_table_comments_happy(mock_sql, mock_tbl_comment, mock_col_comments):
    mock_tbl_comment.return_value = "My table"
    mock_col_comments.return_value = [
        {"column_name": "id", "comment": "PK"},
        {"column_name": "name", "comment": "User's name"},
    ]
    mock_sql.return_value = []

    copy_table_comments(MagicMock(), "wh-1", "src", "dst", "s1", "t1")

    # 1 table comment + 2 column comments = 3 SQL calls
    assert mock_sql.call_count == 3
    table_comment_sql = mock_sql.call_args_list[0][0][2]
    assert "COMMENT ON TABLE" in table_comment_sql


@patch("src.constraints.get_column_comments")
@patch("src.constraints.get_table_comment")
@patch("src.constraints.execute_sql")
def test_copy_table_comments_no_comments(mock_sql, mock_tbl_comment, mock_col_comments):
    mock_tbl_comment.return_value = None
    mock_col_comments.return_value = []
    copy_table_comments(MagicMock(), "wh-1", "src", "dst", "s1", "t1")
    mock_sql.assert_not_called()


@patch("src.constraints.get_column_comments")
@patch("src.constraints.get_table_comment")
@patch("src.constraints.execute_sql")
def test_copy_table_comments_sql_failure_no_raise(mock_sql, mock_tbl_comment, mock_col_comments):
    mock_tbl_comment.return_value = "My table"
    mock_col_comments.return_value = []
    mock_sql.side_effect = Exception("permission denied")
    # Should not raise
    copy_table_comments(MagicMock(), "wh-1", "src", "dst", "s1", "t1")
