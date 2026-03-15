from unittest.mock import MagicMock, patch

from src.search import search_tables


@patch("src.search.execute_sql")
def test_search_tables_match(mock_sql):
    mock_sql.side_effect = [
        # schemas
        [{"schema_name": "sales"}],
        # tables in sales
        [
            {"table_name": "orders", "table_type": "MANAGED", "comment": ""},
            {"table_name": "customers", "table_type": "MANAGED", "comment": ""},
            {"table_name": "products", "table_type": "MANAGED", "comment": ""},
        ],
    ]

    result = search_tables(
        MagicMock(), "wh-123", "my_catalog", "order",
        exclude_schemas=["information_schema"],
    )

    assert len(result["matched_tables"]) == 1
    assert result["matched_tables"][0]["table"] == "orders"


@patch("src.search.execute_sql")
def test_search_tables_no_match(mock_sql):
    mock_sql.side_effect = [
        [{"schema_name": "sales"}],
        [{"table_name": "customers", "table_type": "MANAGED", "comment": ""}],
    ]

    result = search_tables(
        MagicMock(), "wh-123", "my_catalog", "xyz_nonexistent",
        exclude_schemas=["information_schema"],
    )

    assert len(result["matched_tables"]) == 0


@patch("src.search.execute_sql")
def test_search_with_columns(mock_sql):
    mock_sql.side_effect = [
        [{"schema_name": "hr"}],
        [{"table_name": "employees", "table_type": "MANAGED", "comment": ""}],
        # columns
        [
            {"table_name": "employees", "column_name": "email", "data_type": "STRING", "comment": ""},
            {"table_name": "employees", "column_name": "name", "data_type": "STRING", "comment": ""},
            {"table_name": "employees", "column_name": "email_verified", "data_type": "BOOLEAN", "comment": ""},
        ],
    ]

    result = search_tables(
        MagicMock(), "wh-123", "my_catalog", "email",
        exclude_schemas=["information_schema"],
        search_columns=True,
    )

    assert len(result["matched_columns"]) == 2
    assert result["matched_columns"][0]["column"] == "email"
    assert result["matched_columns"][1]["column"] == "email_verified"
