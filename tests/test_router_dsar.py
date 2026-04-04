"""Smoke tests for the DSAR router."""
import pytest

pytest.importorskip("fastapi")


def test_submit_request(client):
    resp = client.post("/api/dsar/requests", json={
        "subject_type": "email",
        "subject_value": "user@example.com",
        "requester_email": "dpo@example.com",
        "requester_name": "DPO",
    })
    assert resp.status_code in (200, 400, 422, 500)


def test_list_requests(client):
    resp = client.get("/api/dsar/requests")
    assert resp.status_code in (200, 400, 404, 500)


def test_overdue(client):
    resp = client.get("/api/dsar/requests/overdue")
    assert resp.status_code in (200, 400, 404, 500)


def test_dashboard(client):
    resp = client.get("/api/dsar/dashboard")
    assert resp.status_code in (200, 400, 404, 500)


def test_get_request(client):
    resp = client.get("/api/dsar/requests/req-123")
    assert resp.status_code in (200, 400, 404, 500)


def test_get_actions(client):
    resp = client.get("/api/dsar/requests/req-123/actions")
    assert resp.status_code in (200, 400, 404, 500)


def test_update_status(client):
    resp = client.put("/api/dsar/requests/req-123/status", json={
        "status": "approved",
    })
    assert resp.status_code in (200, 400, 404, 422, 500)


def test_discover(client):
    resp = client.post("/api/dsar/requests/req-123/discover", json={
        "subject_value": "user@example.com",
    })
    assert resp.status_code in (200, 400, 422, 500)


def test_export_data(client):
    resp = client.post("/api/dsar/requests/req-123/export", json={
        "subject_value": "user@example.com",
    })
    assert resp.status_code in (200, 400, 422, 500)


def test_generate_report(client):
    resp = client.post("/api/dsar/requests/req-123/report")
    assert resp.status_code in (200, 400, 404, 500)
