from unittest.mock import MagicMock, patch

from src.client import execute_sql


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
