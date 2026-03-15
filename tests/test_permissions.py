from unittest.mock import MagicMock, patch, call

from databricks.sdk.service.catalog import SecurableType

from src.permissions import (
    copy_catalog_permissions,
    copy_schema_permissions,
    copy_table_permissions,
    copy_volume_permissions,
    copy_function_permissions,
    update_ownership,
)


# ── SQL-based grant copying ──────────────────────────────────────────

@patch("src.permissions.execute_sql")
def test_copy_catalog_permissions_via_sql(mock_sql):
    mock_sql.side_effect = [
        # SHOW GRANTS result
        [
            {"Principal": "analysts", "ActionType": "SELECT"},
            {"Principal": "admins", "ActionType": "ALL_PRIVILEGES"},
        ],
        [],  # GRANT 1
        [],  # GRANT 2
    ]
    copy_catalog_permissions(MagicMock(), "src_cat", "dst_cat", warehouse_id="wh")
    assert mock_sql.call_count == 3
    show_sql = mock_sql.call_args_list[0][0][2]
    assert "SHOW GRANTS ON CATALOG" in show_sql
    grant1 = mock_sql.call_args_list[1][0][2]
    assert "GRANT SELECT" in grant1
    assert "dst_cat" in grant1


@patch("src.permissions.execute_sql")
def test_copy_schema_permissions_via_sql(mock_sql):
    mock_sql.side_effect = [
        [{"Principal": "team", "ActionType": "USE_SCHEMA"}],
        [],
    ]
    copy_schema_permissions(MagicMock(), "src", "dst", "sales", warehouse_id="wh")
    show_sql = mock_sql.call_args_list[0][0][2]
    assert "SHOW GRANTS ON SCHEMA" in show_sql
    assert "src" in show_sql
    grant_sql = mock_sql.call_args_list[1][0][2]
    assert "GRANT USE_SCHEMA" in grant_sql
    assert "dst" in grant_sql


@patch("src.permissions.execute_sql")
def test_copy_table_permissions_via_sql(mock_sql):
    mock_sql.side_effect = [
        [{"Principal": "user@test.com", "ActionType": "SELECT"}],
        [],
    ]
    copy_table_permissions(MagicMock(), "src", "dst", "schema1", "table1", warehouse_id="wh")
    show_sql = mock_sql.call_args_list[0][0][2]
    assert "SHOW GRANTS ON TABLE" in show_sql
    grant_sql = mock_sql.call_args_list[1][0][2]
    assert "GRANT SELECT ON TABLE" in grant_sql
    assert "dst" in grant_sql


@patch("src.permissions.execute_sql")
def test_copy_permissions_no_grants(mock_sql):
    mock_sql.return_value = []
    copy_catalog_permissions(MagicMock(), "src", "dst", warehouse_id="wh")
    assert mock_sql.call_count == 1  # Only SHOW GRANTS, no GRANT calls


@patch("src.permissions.execute_sql")
def test_copy_permissions_skips_internal(mock_sql):
    mock_sql.side_effect = [
        [{"Principal": "system", "ActionType": "INHERITED_FROM"}],
    ]
    copy_catalog_permissions(MagicMock(), "src", "dst", warehouse_id="wh")
    assert mock_sql.call_count == 1  # Only SHOW GRANTS, skips INHERITED_FROM


@patch("src.permissions.execute_sql")
def test_copy_permissions_handles_error(mock_sql):
    mock_sql.side_effect = Exception("connection failed")
    # Should not raise
    copy_catalog_permissions(MagicMock(), "src", "dst", warehouse_id="wh")


@patch("src.permissions.execute_sql")
def test_copy_volume_permissions(mock_sql):
    mock_sql.side_effect = [
        [{"Principal": "team", "ActionType": "READ_VOLUME"}],
        [],
    ]
    copy_volume_permissions(MagicMock(), "src", "dst", "schema1", "vol1", warehouse_id="wh")
    show_sql = mock_sql.call_args_list[0][0][2]
    assert "SHOW GRANTS ON VOLUME" in show_sql


@patch("src.permissions.execute_sql")
def test_copy_function_permissions(mock_sql):
    mock_sql.side_effect = [
        [{"Principal": "team", "ActionType": "EXECUTE"}],
        [],
    ]
    copy_function_permissions(MagicMock(), "src", "dst", "schema1", "func1", warehouse_id="wh")
    show_sql = mock_sql.call_args_list[0][0][2]
    assert "SHOW GRANTS ON FUNCTION" in show_sql


# ── SQL-based with spark executor (no warehouse_id) ──────────────────

@patch("src.permissions._has_sql_executor", return_value=True)
@patch("src.permissions.execute_sql")
def test_copy_permissions_with_spark_executor(mock_sql, mock_has_exec):
    mock_sql.side_effect = [
        [{"Principal": "team", "ActionType": "SELECT"}],
        [],
    ]
    copy_table_permissions(MagicMock(), "src", "dst", "s", "t")
    assert mock_sql.call_count == 2


# ── update_ownership ─────────────────────────────────────────────────

def test_update_ownership_catalog():
    client = MagicMock()
    client.catalogs.get.return_value = MagicMock(owner="user@test.com")
    update_ownership(client, SecurableType.CATALOG, "src_cat", "dst_cat")
    client.catalogs.update.assert_called_once_with("dst_cat", owner="user@test.com")


def test_update_ownership_schema():
    client = MagicMock()
    client.schemas.get.return_value = MagicMock(owner="user@test.com")
    update_ownership(client, SecurableType.SCHEMA, "src.schema1", "dst.schema1")
    client.schemas.update.assert_called_once_with("dst.schema1", owner="user@test.com")


def test_update_ownership_table():
    client = MagicMock()
    client.tables.get.return_value = MagicMock(owner="user@test.com")
    update_ownership(client, SecurableType.TABLE, "src.s.t", "dst.s.t")
    client.tables.update.assert_called_once_with("dst.s.t", owner="user@test.com")


def test_update_ownership_skips_system_user():
    client = MagicMock()
    client.catalogs.get.return_value = MagicMock(owner="System user")
    update_ownership(client, SecurableType.CATALOG, "src_cat", "dst_cat")
    client.catalogs.update.assert_not_called()


def test_update_ownership_skips_system():
    client = MagicMock()
    client.schemas.get.return_value = MagicMock(owner="System")
    update_ownership(client, SecurableType.SCHEMA, "src.s", "dst.s")
    client.schemas.update.assert_not_called()


def test_update_ownership_handles_error():
    client = MagicMock()
    client.catalogs.get.side_effect = Exception("not found")
    # Should not raise
    update_ownership(client, SecurableType.CATALOG, "src", "dst")
