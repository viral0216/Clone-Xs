"""Tests for the config router — 8 endpoints."""

import pytest
from unittest.mock import patch

pytest.importorskip("fastapi")


# ── GET /api/config ───────────────────────────────────────────────────────────

def test_get_config(client):
    resp = client.get("/api/config")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, dict)
    assert "source_catalog" in data


# ── PUT /api/config ───────────────────────────────────────────────────────────

@patch("api.routers.config.yaml")
@patch("builtins.open")
def test_update_config(mock_open, mock_yaml, client, mock_app_config):
    resp = client.put("/api/config", json=mock_app_config)
    assert resp.status_code in (200, 422)


# ── POST /api/config/diff ─────────────────────────────────────────────────────

def test_config_diff(client):
    resp = client.post("/api/config/diff", json={
        "config_a": {"key": "a"},
        "config_b": {"key": "b"},
    })
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, dict)


# ── POST /api/config/audit ────────────────────────────────────────────────────

@patch("builtins.open")
@patch("api.routers.config.yaml")
def test_save_audit_settings(mock_yaml, mock_open, client):
    mock_yaml.safe_load.return_value = {}
    resp = client.post("/api/config/audit", json={"enabled": True, "catalog": "audit_cat"})
    assert resp.status_code in (200, 422)


# ── PATCH /api/config/warehouse ───────────────────────────────────────────────

@patch("builtins.open")
@patch("api.routers.config.yaml")
def test_set_warehouse(mock_yaml, mock_open, client):
    mock_yaml.safe_load.return_value = {}
    resp = client.patch("/api/config/warehouse", json={"warehouse_id": "wh-123"})
    assert resp.status_code in (200, 422)


# ── PATCH /api/config/performance ─────────────────────────────────────────────

@patch("builtins.open")
@patch("api.routers.config.yaml")
def test_set_performance(mock_yaml, mock_open, client):
    mock_yaml.safe_load.return_value = {}
    resp = client.patch("/api/config/performance", json={"max_workers": 8})
    assert resp.status_code in (200, 422)


# ── PATCH /api/config/pricing ─────────────────────────────────────────────────

@patch("builtins.open")
@patch("api.routers.config.yaml")
def test_set_pricing(mock_yaml, mock_open, client):
    mock_yaml.safe_load.return_value = {}
    resp = client.patch("/api/config/pricing", json={"dbu_price": 0.22})
    assert resp.status_code in (200, 422)


# ── GET /api/config/profiles ──────────────────────────────────────────────────

@patch("api.routers.config.yaml")
@patch("builtins.open")
def test_list_profiles(mock_open, mock_yaml, client):
    mock_yaml.safe_load.return_value = {"profiles": {"default": {}}}
    resp = client.get("/api/config/profiles")
    assert resp.status_code == 200
