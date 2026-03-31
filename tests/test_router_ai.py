"""Tests for the ai router."""

import pytest
from unittest.mock import patch, MagicMock

pytest.importorskip("fastapi")


def _mock_unavailable_service():
    svc = MagicMock()
    svc.available = False
    return svc


def test_ai_status(client):
    with patch("api.routers.ai._get_service", return_value=_mock_unavailable_service()):
        resp = client.get("/api/ai/status")
    assert resp.status_code == 200
    data = resp.json()
    assert "available" in data


def test_ai_summarize(client):
    with patch("api.routers.ai._get_service", return_value=_mock_unavailable_service()):
        resp = client.post("/api/ai/summarize", json={
            "context_type": "dashboard",
            "data": {"key": "value"},
        })
    assert resp.status_code in (200, 422)


def test_ai_clone_builder(client):
    with patch("api.routers.ai._get_service", return_value=_mock_unavailable_service()):
        resp = client.post("/api/ai/clone-builder", json={
            "query": "clone catalog A to catalog B",
        })
    assert resp.status_code in (200, 422)


def test_ai_dq_suggestions(client):
    with patch("api.routers.ai._get_service", return_value=_mock_unavailable_service()):
        resp = client.post("/api/ai/dq-suggestions", json={
            "profiling_results": {"columns": []},
        })
    assert resp.status_code in (200, 422)


def test_ai_pii_remediation(client):
    with patch("api.routers.ai._get_service", return_value=_mock_unavailable_service()):
        resp = client.post("/api/ai/pii-remediation", json={
            "scan_results": {"findings": []},
        })
    assert resp.status_code in (200, 422)
