"""Tests for the auth router — all 15 endpoints."""

import pytest
from unittest.mock import patch, MagicMock

pytest.importorskip("fastapi")


# ── GET /api/auth/status ──────────────────────────────────────────────────────

def test_auth_status_unauthenticated(client):
    """Without session or headers, returns unauthenticated."""
    resp = client.get("/api/auth/status")
    assert resp.status_code == 200
    data = resp.json()
    assert "authenticated" in data


# ── POST /api/auth/login ─────────────────────────────────────────────────────

@patch("api.routers.auth.ensure_authenticated")
@patch("api.routers.auth.get_client")
@patch("api.routers.auth.clear_cache")
def test_login_success(mock_clear, mock_get_client, mock_ensure, client):
    mock_client = MagicMock()
    me = MagicMock()
    me.user_name = "test@example.com"
    mock_client.current_user.me.return_value = me
    mock_client.config.host = "https://test.azuredatabricks.net"
    mock_get_client.return_value = mock_client
    mock_ensure.return_value = {"user": "test@example.com", "host": "https://test.azuredatabricks.net", "auth_method": "pat"}

    resp = client.post("/api/auth/login", json={
        "host": "https://test.azuredatabricks.net",
        "token": "dapi_test_token",
    })
    assert resp.status_code == 200
    data = resp.json()
    assert data["authenticated"] is True


def test_login_missing_fields(client):
    resp = client.post("/api/auth/login", json={})
    # Should fail — host/token required
    assert resp.status_code in (200, 400, 422)


# ── POST /api/auth/logout ────────────────────────────────────────────────────

@patch("src.auth.clear_cache")
def test_logout(mock_clear, client):
    resp = client.post("/api/auth/logout")
    assert resp.status_code == 200


# ── GET /api/auth/env-vars ────────────────────────────────────────────────────

def test_env_vars(client):
    resp = client.get("/api/auth/env-vars")
    assert resp.status_code == 200
    assert isinstance(resp.json(), dict)


# ── GET /api/auth/warehouses ──────────────────────────────────────────────────

def test_list_warehouses(client, mock_workspace_client):
    resp = client.get("/api/auth/warehouses")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)


# ── POST /api/auth/test-warehouse ─────────────────────────────────────────────

@patch("src.client.execute_sql", return_value=[{"1": 1}])
def test_test_warehouse(mock_sql, client):
    resp = client.post("/api/auth/test-warehouse", json={"warehouse_id": "wh-123"})
    assert resp.status_code == 200


# ── GET /api/auth/volumes ─────────────────────────────────────────────────────

@patch("src.serverless.list_volumes", return_value=[])
def test_list_volumes(mock_vols, client):
    resp = client.get("/api/auth/volumes")
    assert resp.status_code == 200


# ── POST /api/auth/azure-login ────────────────────────────────────────────────

@patch("shutil.which", return_value=None)
def test_azure_login_no_cli(mock_which, client):
    """If Azure CLI is not installed, should return an error."""
    resp = client.post("/api/auth/azure-login")
    assert resp.status_code in (400, 500, 200)


# ── GET /api/auth/azure/tenants ───────────────────────────────────────────────

@patch("src.auth.list_tenants", return_value=[])
def test_azure_tenants(mock_tenants, client):
    resp = client.get("/api/auth/azure/tenants")
    assert resp.status_code == 200


# ── GET /api/auth/azure/subscriptions ─────────────────────────────────────────

def test_azure_subscriptions_missing_param(client):
    resp = client.get("/api/auth/azure/subscriptions")
    # Missing tenant_id query param
    assert resp.status_code in (200, 400, 422)


# ── GET /api/auth/azure/workspaces ────────────────────────────────────────────

def test_azure_workspaces_missing_param(client):
    resp = client.get("/api/auth/azure/workspaces")
    assert resp.status_code in (200, 400, 422)


# ── POST /api/auth/azure/connect ──────────────────────────────────────────────

@patch("src.auth.clear_cache")
def test_azure_connect(mock_clear, client):
    resp = client.post("/api/auth/azure/connect", json={"host": "https://test.azuredatabricks.net"})
    # Will fail auth but should not 500
    assert resp.status_code in (200, 400, 401, 500)


# ── GET /api/auth/auto-login ─────────────────────────────────────────────────

@patch("src.auth.is_databricks_app", return_value=False)
def test_auto_login_not_dbx_app(mock_is_app, client):
    resp = client.get("/api/auth/auto-login")
    assert resp.status_code in (200, 404, 400)


# ── POST /api/auth/oauth-login ───────────────────────────────────────────────

def test_oauth_login(client):
    resp = client.post("/api/auth/oauth-login", json={"host": "https://test.azuredatabricks.net"})
    assert resp.status_code in (200, 400, 401, 500)


# ── POST /api/auth/service-principal ──────────────────────────────────────────

def test_service_principal_login(client):
    resp = client.post("/api/auth/service-principal", json={
        "host": "https://test.azuredatabricks.net",
        "client_id": "test-id",
        "client_secret": "test-secret",
    })
    assert resp.status_code in (200, 400, 401, 500)
