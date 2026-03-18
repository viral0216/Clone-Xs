from unittest.mock import MagicMock, patch

from src.clone_tags import (
    get_table_tags,
    get_column_tags,
    get_schema_tags,
    get_catalog_tags,
    copy_catalog_tags,
    copy_schema_tags,
    copy_table_tags,
    get_table_properties,
    copy_table_properties,
)


# ---------- get_table_tags ----------

@patch("src.clone_tags.execute_sql")
def test_get_table_tags_happy(mock_sql):
    mock_sql.return_value = [
        {"tag_name": "env", "tag_value": "prod"},
    ]
    result = get_table_tags(MagicMock(), "wh-1", "cat", "s1", "t1")
    assert len(result) == 1
    assert result[0]["tag_name"] == "env"


@patch("src.clone_tags.execute_sql")
def test_get_table_tags_failure(mock_sql):
    mock_sql.side_effect = Exception("nope")
    result = get_table_tags(MagicMock(), "wh-1", "cat", "s1", "t1")
    assert result == []


# ---------- get_column_tags ----------

@patch("src.clone_tags.execute_sql")
def test_get_column_tags_happy(mock_sql):
    mock_sql.return_value = [
        {"column_name": "email", "tag_name": "pii", "tag_value": "true"},
    ]
    result = get_column_tags(MagicMock(), "wh-1", "cat", "s1", "t1")
    assert len(result) == 1


@patch("src.clone_tags.execute_sql")
def test_get_column_tags_failure(mock_sql):
    mock_sql.side_effect = Exception("nope")
    assert get_column_tags(MagicMock(), "wh-1", "cat", "s1", "t1") == []


# ---------- get_schema_tags ----------

@patch("src.clone_tags.execute_sql")
def test_get_schema_tags_happy(mock_sql):
    mock_sql.return_value = [{"tag_name": "team", "tag_value": "data"}]
    result = get_schema_tags(MagicMock(), "wh-1", "cat", "s1")
    assert len(result) == 1


@patch("src.clone_tags.execute_sql")
def test_get_schema_tags_failure(mock_sql):
    mock_sql.side_effect = Exception("nope")
    assert get_schema_tags(MagicMock(), "wh-1", "cat", "s1") == []


# ---------- get_catalog_tags ----------

@patch("src.clone_tags.execute_sql")
def test_get_catalog_tags_happy(mock_sql):
    mock_sql.return_value = [{"tag_name": "org", "tag_value": "engineering"}]
    result = get_catalog_tags(MagicMock(), "wh-1", "cat")
    assert len(result) == 1


@patch("src.clone_tags.execute_sql")
def test_get_catalog_tags_failure(mock_sql):
    mock_sql.side_effect = Exception("nope")
    assert get_catalog_tags(MagicMock(), "wh-1", "cat") == []


# ---------- copy_catalog_tags ----------

@patch("src.clone_tags.get_catalog_tags")
@patch("src.clone_tags.execute_sql")
def test_copy_catalog_tags_happy(mock_sql, mock_get):
    mock_get.return_value = [
        {"tag_name": "env", "tag_value": "prod"},
        {"tag_name": "team", "tag_value": "data"},
    ]
    mock_sql.return_value = []
    copy_catalog_tags(MagicMock(), "wh-1", "src", "dst")
    assert mock_sql.call_count == 2
    sql_arg = mock_sql.call_args_list[0][0][2]
    assert "ALTER CATALOG" in sql_arg
    assert "dst" in sql_arg


@patch("src.clone_tags.get_catalog_tags")
@patch("src.clone_tags.execute_sql")
def test_copy_catalog_tags_no_tags(mock_sql, mock_get):
    mock_get.return_value = []
    copy_catalog_tags(MagicMock(), "wh-1", "src", "dst")
    mock_sql.assert_not_called()


@patch("src.clone_tags.get_catalog_tags")
@patch("src.clone_tags.execute_sql")
def test_copy_catalog_tags_sql_failure_no_raise(mock_sql, mock_get):
    mock_get.return_value = [{"tag_name": "env", "tag_value": "prod"}]
    mock_sql.side_effect = Exception("permission denied")
    # Should not raise
    copy_catalog_tags(MagicMock(), "wh-1", "src", "dst")


# ---------- copy_schema_tags ----------

@patch("src.clone_tags.get_schema_tags")
@patch("src.clone_tags.execute_sql")
def test_copy_schema_tags_happy(mock_sql, mock_get):
    mock_get.return_value = [{"tag_name": "team", "tag_value": "data"}]
    mock_sql.return_value = []
    copy_schema_tags(MagicMock(), "wh-1", "src", "dst", "s1")
    assert mock_sql.call_count == 1
    sql_arg = mock_sql.call_args[0][2]
    assert "ALTER SCHEMA" in sql_arg
    assert "`dst`.`s1`" in sql_arg


@patch("src.clone_tags.get_schema_tags")
@patch("src.clone_tags.execute_sql")
def test_copy_schema_tags_no_tags(mock_sql, mock_get):
    mock_get.return_value = []
    copy_schema_tags(MagicMock(), "wh-1", "src", "dst", "s1")
    mock_sql.assert_not_called()


# ---------- copy_table_tags ----------

@patch("src.clone_tags.get_column_tags")
@patch("src.clone_tags.get_table_tags")
@patch("src.clone_tags.execute_sql")
def test_copy_table_tags_happy(mock_sql, mock_tbl_tags, mock_col_tags):
    mock_tbl_tags.return_value = [{"tag_name": "env", "tag_value": "prod"}]
    mock_col_tags.return_value = [
        {"column_name": "email", "tag_name": "pii", "tag_value": "true"},
    ]
    mock_sql.return_value = []
    copy_table_tags(MagicMock(), "wh-1", "src", "dst", "s1", "t1")
    # 1 table tag + 1 column tag = 2 SQL calls
    assert mock_sql.call_count == 2
    table_tag_sql = mock_sql.call_args_list[0][0][2]
    assert "ALTER TABLE" in table_tag_sql
    assert "SET TAGS" in table_tag_sql
    col_tag_sql = mock_sql.call_args_list[1][0][2]
    assert "ALTER COLUMN" in col_tag_sql


@patch("src.clone_tags.get_column_tags")
@patch("src.clone_tags.get_table_tags")
@patch("src.clone_tags.execute_sql")
def test_copy_table_tags_no_tags(mock_sql, mock_tbl_tags, mock_col_tags):
    mock_tbl_tags.return_value = []
    mock_col_tags.return_value = []
    copy_table_tags(MagicMock(), "wh-1", "src", "dst", "s1", "t1")
    mock_sql.assert_not_called()


@patch("src.clone_tags.get_column_tags")
@patch("src.clone_tags.get_table_tags")
@patch("src.clone_tags.execute_sql")
def test_copy_table_tags_sql_failure_no_raise(mock_sql, mock_tbl_tags, mock_col_tags):
    mock_tbl_tags.return_value = [{"tag_name": "env", "tag_value": "prod"}]
    mock_col_tags.return_value = []
    mock_sql.side_effect = Exception("tag already exists")
    # Should not raise
    copy_table_tags(MagicMock(), "wh-1", "src", "dst", "s1", "t1")


# ---------- get_table_properties ----------

@patch("src.clone_tags.execute_sql")
def test_get_table_properties_happy(mock_sql):
    mock_sql.return_value = [
        {"key": "custom.prop", "value": "abc"},
        {"key": "delta.minReaderVersion", "value": "1"},
        {"key": "spark.sql.something", "value": "x"},
    ]
    result = get_table_properties(MagicMock(), "wh-1", "cat", "s1", "t1")
    # delta. and spark. prefixes should be filtered out
    assert len(result) == 1
    assert result[0]["key"] == "custom.prop"


@patch("src.clone_tags.execute_sql")
def test_get_table_properties_failure(mock_sql):
    mock_sql.side_effect = Exception("nope")
    assert get_table_properties(MagicMock(), "wh-1", "cat", "s1", "t1") == []


# ---------- copy_table_properties ----------

@patch("src.clone_tags.get_table_properties")
@patch("src.clone_tags.execute_sql")
def test_copy_table_properties_happy(mock_sql, mock_get):
    mock_get.return_value = [
        {"key": "custom.owner", "value": "team-a"},
    ]
    mock_sql.return_value = []
    copy_table_properties(MagicMock(), "wh-1", "src", "dst", "s1", "t1")
    mock_sql.assert_called_once()
    sql_arg = mock_sql.call_args[0][2]
    assert "SET TBLPROPERTIES" in sql_arg
    assert "custom.owner" in sql_arg


@patch("src.clone_tags.get_table_properties")
@patch("src.clone_tags.execute_sql")
def test_copy_table_properties_no_props(mock_sql, mock_get):
    mock_get.return_value = []
    copy_table_properties(MagicMock(), "wh-1", "src", "dst", "s1", "t1")
    mock_sql.assert_not_called()


@patch("src.clone_tags.get_table_properties")
@patch("src.clone_tags.execute_sql")
def test_copy_table_properties_sql_failure_no_raise(mock_sql, mock_get):
    mock_get.return_value = [{"key": "k", "value": "v"}]
    mock_sql.side_effect = Exception("nope")
    # Should not raise
    copy_table_properties(MagicMock(), "wh-1", "src", "dst", "s1", "t1")
