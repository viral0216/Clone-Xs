from unittest.mock import MagicMock, patch, call
import pytest

from src.clone_catalog import (
    get_schemas,
    create_catalog_if_not_exists,
    create_schema_if_not_exists,
    process_schema,
    _build_summary,
)


# ── get_schemas ──────────────────────────────────────────────────────

@patch("src.clone_catalog.execute_sql")
def test_get_schemas_returns_names(mock_sql):
    mock_sql.return_value = [
        {"schema_name": "sales"},
        {"schema_name": "marketing"},
    ]
    result = get_schemas(MagicMock(), "wh", "prod", ["information_schema", "default"])
    assert result == ["sales", "marketing"]


@patch("src.clone_catalog.execute_sql")
def test_get_schemas_excludes_system_schemas(mock_sql):
    mock_sql.return_value = [
        {"schema_name": "sales"},
    ]
    result = get_schemas(MagicMock(), "wh", "prod", ["information_schema", "default"])
    sql = mock_sql.call_args[0][2]
    assert "information_schema" in sql
    assert "default" in sql


def test_get_schemas_with_include_list():
    result = get_schemas(MagicMock(), "wh", "prod", ["default"], include=["sales", "hr"])
    assert result == ["sales", "hr"]


def test_get_schemas_include_excludes_system():
    result = get_schemas(
        MagicMock(), "wh", "prod", ["default"],
        include=["sales", "default", "information_schema"],
    )
    assert "default" not in result
    assert "information_schema" not in result
    assert "sales" in result


# ── create_catalog_if_not_exists ─────────────────────────────────────

@patch("src.clone_catalog.execute_sql")
def test_create_catalog_dry_run(mock_sql):
    create_catalog_if_not_exists(MagicMock(), "wh", "test_cat", dry_run=True)
    mock_sql.assert_not_called()


@patch("src.clone_catalog.execute_sql")
def test_create_catalog_already_exists(mock_sql):
    client = MagicMock()
    client.catalogs.get.return_value = MagicMock()  # catalog exists
    create_catalog_if_not_exists(client, "wh", "test_cat")
    mock_sql.assert_not_called()


@patch("src.clone_catalog.execute_sql")
def test_create_catalog_with_location(mock_sql):
    client = MagicMock()
    client.catalogs.get.side_effect = Exception("not found")
    client.current_user.me.return_value = MagicMock(user_name="user@test.com")
    create_catalog_if_not_exists(client, "wh", "test_cat", location="abfss://storage/path")
    # First call should be CREATE CATALOG with MANAGED LOCATION
    create_sql = mock_sql.call_args_list[0][0][2]
    assert "CREATE CATALOG" in create_sql
    assert "MANAGED LOCATION" in create_sql
    assert "abfss://storage/path" in create_sql


@patch("src.clone_catalog.execute_sql")
def test_create_catalog_sets_owner(mock_sql):
    client = MagicMock()
    client.catalogs.get.side_effect = Exception("not found")
    client.current_user.me.return_value = MagicMock(user_name="user@test.com")
    create_catalog_if_not_exists(client, "wh", "test_cat")
    # Should have ALTER CATALOG SET OWNER
    all_sql = [c[0][2] for c in mock_sql.call_args_list]
    owner_sql = [s for s in all_sql if "SET OWNER" in s]
    assert len(owner_sql) >= 1
    assert "user@test.com" in owner_sql[0]


@patch("src.clone_catalog.execute_sql")
def test_create_catalog_grants_access(mock_sql):
    client = MagicMock()
    client.catalogs.get.side_effect = Exception("not found")
    client.current_user.me.return_value = MagicMock(user_name="user@test.com")
    create_catalog_if_not_exists(client, "wh", "test_cat")
    all_sql = [c[0][2] for c in mock_sql.call_args_list]
    grant_sql = [s for s in all_sql if "GRANT ALL PRIVILEGES" in s]
    assert len(grant_sql) >= 1


# ── create_schema_if_not_exists ──────────────────────────────────────

@patch("src.clone_catalog.execute_sql")
def test_create_schema(mock_sql):
    create_schema_if_not_exists(MagicMock(), "wh", "cat", "schema1")
    sql = mock_sql.call_args[0][2]
    assert "CREATE SCHEMA IF NOT EXISTS" in sql
    assert "`cat`.`schema1`" in sql


@patch("src.clone_catalog.execute_sql")
def test_create_schema_dry_run(mock_sql):
    mock_sql.return_value = []
    create_schema_if_not_exists(MagicMock(), "wh", "cat", "schema1", dry_run=True)
    sql = mock_sql.call_args[0][2]
    assert "CREATE SCHEMA" in sql


# ── _build_summary ───────────────────────────────────────────────────

def test_build_summary_aggregates():
    results = [
        {
            "schema": "s1",
            "tables": {"success": 5, "failed": 1, "skipped": 2},
            "views": {"success": 2, "failed": 0, "skipped": 0},
            "functions": {"success": 1, "failed": 0, "skipped": 0},
            "volumes": {"success": 0, "failed": 0, "skipped": 0},
            "duration_seconds": 10.5,
        },
        {
            "schema": "s2",
            "tables": {"success": 3, "failed": 0, "skipped": 1},
            "views": {"success": 1, "failed": 1, "skipped": 0},
            "functions": {"success": 0, "failed": 0, "skipped": 0},
            "volumes": {"success": 1, "failed": 0, "skipped": 0},
            "duration_seconds": 8.2,
        },
    ]
    summary = _build_summary(results)
    assert summary["schemas_processed"] == 2
    assert summary["tables"]["success"] == 8
    assert summary["tables"]["failed"] == 1
    assert summary["tables"]["skipped"] == 3
    assert summary["views"]["success"] == 3
    assert summary["views"]["failed"] == 1
    assert summary["volumes"]["success"] == 1


def test_build_summary_with_errors():
    results = [
        {"schema": "s1", "error": "connection timeout"},
        {
            "schema": "s2",
            "tables": {"success": 1, "failed": 0, "skipped": 0},
            "views": {"success": 0, "failed": 0, "skipped": 0},
            "functions": {"success": 0, "failed": 0, "skipped": 0},
            "volumes": {"success": 0, "failed": 0, "skipped": 0},
        },
    ]
    summary = _build_summary(results)
    assert len(summary["errors"]) == 1
    assert "connection timeout" in summary["errors"][0]


# ── process_schema ───────────────────────────────────────────────────

@patch("src.clone_catalog.run_post_schema_hooks")
@patch("src.clone_catalog.clone_volumes_in_schema")
@patch("src.clone_catalog.clone_functions_in_schema")
@patch("src.clone_catalog.clone_views_in_schema")
@patch("src.clone_catalog.clone_tables_in_schema")
@patch("src.clone_catalog.copy_schema_tags")
@patch("src.clone_catalog.update_ownership")
@patch("src.clone_catalog.copy_schema_permissions")
@patch("src.clone_catalog.create_schema_if_not_exists")
def test_process_schema_calls_all_engines(
    mock_create, mock_perms, mock_owner, mock_tags,
    mock_tables, mock_views, mock_funcs, mock_vols, mock_hooks,
):
    mock_tables.return_value = {"success": 2, "failed": 0, "skipped": 0}
    mock_views.return_value = {"success": 1, "failed": 0, "skipped": 0}
    mock_funcs.return_value = {"success": 0, "failed": 0, "skipped": 0}
    mock_vols.return_value = {"success": 0, "failed": 0, "skipped": 0}

    config = {
        "source_catalog": "src",
        "destination_catalog": "dst",
        "sql_warehouse_id": "wh",
        "clone_type": "DEEP",
        "load_type": "FULL",
        "exclude_tables": [],
        "dry_run": False,
        "copy_permissions": True,
        "copy_ownership": True,
        "copy_tags": True,
        "copy_properties": False,
        "copy_security": False,
        "copy_constraints": False,
        "copy_comments": False,
    }

    result = process_schema(MagicMock(), config, "sales")
    assert result["schema"] == "sales"
    assert result["tables"]["success"] == 2
    assert result["views"]["success"] == 1
    mock_create.assert_called_once()
    mock_tables.assert_called_once()
    mock_views.assert_called_once()
    mock_funcs.assert_called_once()
    mock_vols.assert_called_once()
