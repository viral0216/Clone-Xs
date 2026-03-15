from unittest.mock import MagicMock, patch

from src.preflight import (
    check_catalog_access,
    check_connectivity,
    check_env_vars,
    check_warehouse,
)


def test_check_connectivity_success():
    client = MagicMock()
    client.catalogs.list.return_value = [MagicMock(), MagicMock()]
    result = check_connectivity(client)
    assert result["status"] == "OK"
    assert "2 catalogs" in result["detail"]


def test_check_connectivity_failure():
    client = MagicMock()
    client.catalogs.list.side_effect = Exception("connection refused")
    result = check_connectivity(client)
    assert result["status"] == "FAIL"
    assert "connection refused" in result["detail"]


def test_check_warehouse_running():
    client = MagicMock()
    wh = MagicMock()
    wh.name = "test-wh"
    wh.state.value = "RUNNING"
    client.warehouses.get.return_value = wh
    result = check_warehouse(client, "wh-123")
    assert result["status"] == "OK"


def test_check_warehouse_stopped():
    client = MagicMock()
    wh = MagicMock()
    wh.name = "test-wh"
    wh.state.value = "STOPPED"
    client.warehouses.get.return_value = wh
    result = check_warehouse(client, "wh-123")
    assert result["status"] == "WARN"


@patch("src.preflight.execute_sql")
def test_check_catalog_access_success(mock_sql):
    mock_sql.return_value = [{"schema_name": "public"}]
    result = check_catalog_access(MagicMock(), "wh-123", "my_catalog")
    assert result["status"] == "OK"


@patch("src.preflight.execute_sql")
def test_check_catalog_access_failure(mock_sql):
    mock_sql.side_effect = Exception("CATALOG_DOES_NOT_EXIST")
    result = check_catalog_access(MagicMock(), "wh-123", "missing_catalog")
    assert result["status"] == "FAIL"


@patch.dict("os.environ", {"DATABRICKS_HOST": "https://test.cloud.databricks.com", "DATABRICKS_TOKEN": "tok"})
def test_check_env_vars_present():
    result = check_env_vars()
    assert result["status"] == "OK"


@patch.dict("os.environ", {}, clear=True)
def test_check_env_vars_missing():
    result = check_env_vars()
    assert result["status"] == "WARN"
    assert "DATABRICKS_HOST" in result["detail"]
