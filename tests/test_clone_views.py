from unittest.mock import MagicMock, patch

from src.clone_views import clone_view, clone_views_in_schema, get_views


@patch("src.clone_views.execute_sql")
def test_clone_view_replaces_catalog(mock_sql):
    mock_sql.return_value = []
    result = clone_view(
        MagicMock(), "wh", "dst_cat", "src_cat", "schema1", "view1",
        "SELECT * FROM `src_cat`.`schema1`.`orders`",
    )
    assert result is True
    sql = mock_sql.call_args[0][2]
    assert "CREATE OR REPLACE VIEW" in sql
    assert "`dst_cat`" in sql
    assert "`src_cat`" not in sql


@patch("src.clone_views.execute_sql")
def test_clone_view_failure(mock_sql):
    mock_sql.side_effect = Exception("invalid view")
    result = clone_view(
        MagicMock(), "wh", "dst", "src", "s", "v1",
        "SELECT invalid",
    )
    assert result is False


@patch("src.clone_views.execute_sql")
def test_get_views(mock_sql):
    mock_sql.return_value = [
        {"table_name": "v1", "view_definition": "SELECT 1"},
        {"table_name": "v2", "view_definition": "SELECT 2"},
    ]
    views = get_views(MagicMock(), "wh", "cat", "schema1")
    assert len(views) == 2
    assert views[0]["table_name"] == "v1"


@patch("src.clone_views.execute_sql")
@patch("src.clone_views.clone_view")
def test_clone_views_in_schema_incremental(mock_clone, mock_sql):
    mock_sql.side_effect = [
        # get_views
        [
            {"table_name": "v1", "view_definition": "SELECT 1"},
            {"table_name": "v2", "view_definition": "SELECT 2"},
        ],
        # get_existing_views
        [{"table_name": "v1"}],
    ]
    mock_clone.return_value = True

    result = clone_views_in_schema(
        MagicMock(), "wh", "src", "dst", "schema1", "INCREMENTAL",
    )
    assert result["skipped"] == 1  # v1 already exists
    mock_clone.assert_called_once()  # Only v2 cloned


@patch("src.clone_views.execute_sql")
@patch("src.clone_views.clone_view")
def test_clone_views_regex_filter(mock_clone, mock_sql):
    mock_sql.return_value = [
        {"table_name": "dim_view", "view_definition": "SELECT 1"},
        {"table_name": "tmp_view", "view_definition": "SELECT 2"},
    ]
    mock_clone.return_value = True

    result = clone_views_in_schema(
        MagicMock(), "wh", "src", "dst", "s", "FULL",
        include_regex="^dim_",
    )
    assert result["skipped"] == 1  # tmp_view filtered out
    assert result["success"] == 1
