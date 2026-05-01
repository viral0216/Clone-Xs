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
def mock_target_workspace_client():
    """A MagicMock of a TARGET WorkspaceClient for cross-workspace clone tests.

    Distinct from `mock_workspace_client` (source) so cross-workspace tests
    can verify the orchestrator uses the right client at the right step. The
    target metastore id differs from the source's so the same-metastore
    preflight passes by default — tests can override metastore IDs to
    exercise the preflight rejection path.
    """
    client = MagicMock()

    # current_user.me() — used by /target/whoami and "Logged in as" UI
    me = MagicMock()
    me.user_name = "target-user@example.com"
    me.display_name = "Target User"
    client.current_user.me.return_value = me

    # catalogs.list() — used by /target/catalogs and the destination dropdown
    target_cat = MagicMock()
    target_cat.name = "existing_target_catalog"
    target_cat.catalog_type = "MANAGED_CATALOG"
    client.catalogs.list.return_value = [target_cat]

    # warehouses.list() / get() — used by /target/warehouses and /target/validate
    target_wh = MagicMock()
    target_wh.id = "target-wh-456"
    target_wh.name = "Target Warehouse"
    target_wh.state = "RUNNING"
    target_wh.cluster_size = "Small"
    client.warehouses.list.return_value = [target_wh]
    client.warehouses.get.return_value = target_wh

    # metastores.summary() — Clone-Xs reads global_metastore_id from this for
    # the recipient USING ID. Different from source so same-metastore
    # preflight passes by default.
    target_ms = MagicMock()
    target_ms.global_metastore_id = "azure:westeurope:target-metastore-uuid"
    target_ms.metastore_id = "target-metastore-uuid"
    target_ms.name = "target-metastore"
    target_ms.region = "westeurope"
    target_ms.cloud = "azure"
    client.metastores.summary.return_value = target_ms
    client.metastores.current.return_value = target_ms

    # Empty by default — tests override when exercising provider lookup
    client.providers = MagicMock()
    client.providers.list.return_value = []

    client.config = MagicMock()
    client.config.host = "https://target.azuredatabricks.net"

    return client


@pytest.fixture()
def mock_cross_workspace_setup(mock_workspace_client, mock_target_workspace_client):
    """Wire source + target clients, distinct metastores, default TargetWorkspace.

    Returns a dict the test can mutate:
      - source_client / target_client (MagicMocks)
      - source_metastore_id / target_metastore_id (strings used as USING IDs)
      - target_workspace (dict matching the TargetWorkspace pydantic model)

    Patches `src.target_workspace.build_target_client` so the orchestrator
    receives `target_client` regardless of the credentials it's handed.
    Tests using this fixture should `monkeypatch` further as needed (e.g.
    add recipients to source_client.recipients.list to exercise reuse).
    """
    # Distinct metastores so same-metastore preflight passes by default.
    # Tests exercising the rejection path can overwrite these to be equal.
    source_metastore_id = "azure:uksouth:source-metastore-uuid"
    target_metastore_id = "azure:westeurope:target-metastore-uuid"

    # Source side: metastores.summary returns the SOURCE metastore id.
    source_ms = MagicMock()
    source_ms.global_metastore_id = source_metastore_id
    source_ms.metastore_id = "source-metastore-uuid"
    source_ms.region = "uksouth"
    source_ms.cloud = "azure"
    mock_workspace_client.metastores.summary.return_value = source_ms

    # Target side: already configured in mock_target_workspace_client, but
    # ensure consistency with the explicit target_metastore_id above.
    target_ms = mock_target_workspace_client.metastores.summary.return_value
    target_ms.global_metastore_id = target_metastore_id

    # Default empty recipients list on source — reuse-tests append to this
    mock_workspace_client.recipients = MagicMock()
    mock_workspace_client.recipients.list.return_value = []

    # Default TargetWorkspace payload matching the pydantic model
    target_workspace = {
        "host": "https://target.azuredatabricks.net",
        "auth_method": "pat",
        "token": "dapi-test-token",
        "warehouse_id": "target-wh-456",
        "data_sync_mode": "snapshot_once",
    }

    return {
        "source_client": mock_workspace_client,
        "target_client": mock_target_workspace_client,
        "source_metastore_id": source_metastore_id,
        "target_metastore_id": target_metastore_id,
        "target_workspace": target_workspace,
    }


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
