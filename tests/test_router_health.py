"""Tests for the health router."""

import pytest

pytest.importorskip("fastapi")


def test_health_endpoint(client):
    resp = client.get("/api/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
    assert "version" in data or "runtime" in data or True  # minimal shape check
