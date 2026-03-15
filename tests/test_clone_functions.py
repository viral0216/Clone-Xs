from unittest.mock import MagicMock, patch

from src.clone_functions import (
    clone_function,
    clone_functions_in_schema,
    get_function_details,
)


@patch("src.clone_functions.execute_sql")
def test_get_function_details_extracts_ddl(mock_sql):
    mock_sql.return_value = [
        {"function_desc": "CREATE FUNCTION `cat`.`s`.`fn`(x INT) RETURNS INT RETURN x + 1"},
    ]
    ddl = get_function_details(MagicMock(), "wh", "cat", "s", "fn")
    assert "CREATE FUNCTION" in ddl


@patch("src.clone_functions.execute_sql")
def test_get_function_details_skips_spark_config(mock_sql):
    mock_sql.return_value = [
        {"function_desc": "spark.databricks.sql.functions.aiFunctions.createBatchSessionOnExecutor=false"},
    ]
    ddl = get_function_details(MagicMock(), "wh", "cat", "s", "fn")
    assert ddl == ""


@patch("src.clone_functions.execute_sql")
def test_get_function_details_error(mock_sql):
    mock_sql.side_effect = Exception("not found")
    ddl = get_function_details(MagicMock(), "wh", "cat", "s", "fn")
    assert ddl == ""


@patch("src.clone_functions.get_function_details")
@patch("src.clone_functions.execute_sql")
def test_clone_function_success(mock_sql, mock_details):
    mock_details.return_value = "CREATE FUNCTION `src`.`s`.`fn`(x INT) RETURNS INT RETURN x + 1"
    mock_sql.return_value = []
    result = clone_function(MagicMock(), "wh", "src", "dst", "s", "fn")
    assert result is True
    sql = mock_sql.call_args[0][2]
    assert "CREATE OR REPLACE FUNCTION" in sql
    assert "`dst`" in sql
    assert "`src`" not in sql


@patch("src.clone_functions.get_function_details")
def test_clone_function_no_ddl(mock_details):
    mock_details.return_value = ""
    result = clone_function(MagicMock(), "wh", "src", "dst", "s", "fn")
    assert result is False


@patch("src.clone_functions.get_function_details")
def test_clone_function_invalid_ddl(mock_details):
    mock_details.return_value = "spark.databricks.config=true"
    result = clone_function(MagicMock(), "wh", "src", "dst", "s", "fn")
    assert result is False


@patch("src.clone_functions.execute_sql")
@patch("src.clone_functions.clone_function")
def test_clone_functions_in_schema(mock_clone, mock_sql):
    mock_sql.side_effect = [
        # get_functions
        [{"function_name": "fn1"}, {"function_name": "fn2"}],
    ]
    mock_clone.side_effect = [True, False]

    result = clone_functions_in_schema(
        MagicMock(), "wh", "src", "dst", "schema1", "FULL",
    )
    assert result["success"] == 1
    assert result["failed"] == 1


@patch("src.clone_functions.execute_sql")
@patch("src.clone_functions.clone_function")
def test_clone_functions_incremental(mock_clone, mock_sql):
    mock_sql.side_effect = [
        # get_functions
        [{"function_name": "fn1"}, {"function_name": "fn2"}],
        # get_existing_functions
        [{"function_name": "fn1"}],
    ]
    mock_clone.return_value = True

    result = clone_functions_in_schema(
        MagicMock(), "wh", "src", "dst", "s", "INCREMENTAL",
    )
    assert result["skipped"] == 1
    assert result["success"] == 1
