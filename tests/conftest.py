"""Shared test fixtures for Clone-Xs API tests.

Provides a FastAPI TestClient with mocked Databricks dependencies
so router tests can call endpoints without a real workspace connection.
"""

import pytest
from unittest.mock import MagicMock

# Skip all API tests if FastAPI / httpx not installed
pytest.importorskip("fastapi")
pytest.importorskip("httpx")


@pytest.fixture()
def mock_workspace_client():
    """A MagicMock of databricks.sdk.WorkspaceClient with common sub-mocks."""
    client = MagicMock()

    # current_user.me()
    me = MagicMock()
    me.user_name = "test@example.com"
    me.display_name = "Test User"
    client.current_user.me.return_value = me

    # catalogs.list()
    cat = MagicMock()
    cat.name = "test_catalog"
    cat.catalog_type = "MANAGED_CATALOG"
    cat.owner = "test@example.com"
    cat.comment = "Test catalog"
    client.catalogs.list.return_value = [cat]

    # warehouses.list()
    wh = MagicMock()
    wh.id = "abc123"
    wh.name = "Test Warehouse"
    wh.state = "RUNNING"
    wh.cluster_size = "Small"
    wh.auto_stop_mins = 10
    wh.creator_name = "test@example.com"
    client.warehouses.list.return_value = [wh]

    # schemas.list()
    schema = MagicMock()
    schema.name = "default"
    schema.catalog_name = "test_catalog"
    client.schemas.list.return_value = [schema]

    # tables.list()
    table = MagicMock()
    table.full_name = "test_catalog.default.test_table"
    table.name = "test_table"
    table.table_type = "MANAGED"
    table.data_source_format = "DELTA"
    client.tables.list.return_value = [table]

    # metastores.current()
    ms = MagicMock()
    ms.metastore_id = "ms-123"
    ms.name = "test-metastore"
    ms.region = "eastus"
    ms.owner = "admin@example.com"
    ms.cloud = "azure"
    client.metastores.current.return_value = ms

    # jobs.list_runs()
    client.jobs.list_runs.return_value = MagicMock(runs=[])

    # clusters.list()
    client.clusters.list.return_value = []

    # pipelines.list_pipelines()
    client.pipelines.list_pipelines.return_value = MagicMock(statuses=[])

    # query_history.list()
    client.query_history.list.return_value = MagicMock(res=[])

    # alerts
    client.alerts_v2 = MagicMock()
    client.alerts_v2.list_alerts.return_value = []

    # workspace config
    client.config = MagicMock()
    client.config.host = "https://test.azuredatabricks.net"

    return client


@pytest.fixture()
def mock_app_config():
    """Returns a config dict matching load_config() shape."""
    return {
        "source_catalog": "source_cat",
        "destination_catalog": "dest_cat",
        "sql_warehouse_id": "wh-123",
        "clone_type": "SHALLOW",
        "load_type": "FULL",
        "max_workers": 4,
        "copy_permissions": True,
        "copy_tags": True,
        "exclude_schemas": ["information_schema"],
        "exclude_tables": [],
        "audit_trail": {"enabled": False},
    }


@pytest.fixture()
def app(mock_workspace_client, mock_app_config):
    """Create FastAPI app with dependency overrides for testing."""
    from api.main import app as _app
    from api.dependencies import get_db_client, get_app_config, get_rest_client
    from api.queue.job_manager import JobManager

    jm = JobManager(max_concurrent=1)
    _app.state.job_manager = jm

    async def _override_client():
        return mock_workspace_client

    async def _override_config(config_path="config/clone_config.yaml", profile=None):
        return mock_app_config

    async def _override_rest(client=None):
        return MagicMock()

    _app.dependency_overrides[get_db_client] = _override_client
    _app.dependency_overrides[get_app_config] = _override_config
    _app.dependency_overrides[get_rest_client] = _override_rest

    yield _app

    _app.dependency_overrides.clear()


@pytest.fixture()
def client(app):
    """HTTPX TestClient wrapping the FastAPI app."""
    from fastapi.testclient import TestClient
    return TestClient(app, raise_server_exceptions=False)
