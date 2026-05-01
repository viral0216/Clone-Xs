from unittest.mock import MagicMock, patch

from src.client import _normalize_format, execute_sql


@patch("src.client.WorkspaceClient")
def test_execute_sql_returns_rows(mock_ws_class):
    mock_client = MagicMock()

    # Set up mock response
    mock_col1 = MagicMock()
    mock_col1.name = "schema_name"
    mock_col2 = MagicMock()
    mock_col2.name = "table_name"

    mock_response = MagicMock()
    mock_response.status.state.value = "SUCCEEDED"
    mock_response.result.data_array = [["my_schema", "my_table"]]
    mock_response.manifest.schema.columns = [mock_col1, mock_col2]
    mock_client.statement_execution.execute_statement.return_value = mock_response

    rows = execute_sql(mock_client, "warehouse-123", "SELECT 1")

    assert len(rows) == 1
    assert rows[0] == {"schema_name": "my_schema", "table_name": "my_table"}


@patch("src.client.WorkspaceClient")
def test_execute_sql_empty_result(mock_ws_class):
    mock_client = MagicMock()

    mock_response = MagicMock()
    mock_response.status.state.value = "SUCCEEDED"
    mock_response.result.data_array = []
    mock_response.result.external_links = None
    mock_response.manifest.schema.columns = []
    mock_client.statement_execution.execute_statement.return_value = mock_response

    rows = execute_sql(mock_client, "warehouse-123", "SELECT 1")
    assert rows == []


# `_normalize_format` is a boundary helper that turns whatever shape the
# Databricks SDK returns for `data_source_format` (enum across versions,
# plain string from REST fallback, None when unset) into a single string
# downstream code can `.upper()` / compare against. Regression coverage
# for the production crash where SDK returned an enum and clone_tables
# tried to call `.upper()` on it ("'DataSourceFormat' object has no
# attribute 'upper'").


def test_normalize_format_handles_sdk_enum():
    """SDK ``DataSourceFormat`` enum exposes the canonical string via
    ``.value``. Our helper unwraps that so downstream sees ``"DELTA"``."""
    enum_like = MagicMock()
    enum_like.value = "DELTA"
    assert _normalize_format(enum_like) == "DELTA"


def test_normalize_format_passes_strings_through():
    """REST fallback returns plain strings — must not break or be re-wrapped."""
    assert _normalize_format("ICEBERG") == "ICEBERG"
    assert _normalize_format("parquet") == "parquet"


def test_normalize_format_returns_none_for_none():
    """Unset field stays None — caller's ``or "DELTA"`` defaulting still works."""
    assert _normalize_format(None) is None
