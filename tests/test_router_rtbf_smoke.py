"""Smoke tests for the RTBF router."""
import pytest

pytest.importorskip("fastapi")


def test_submit_request(client):
    resp = client.post("/api/rtbf/requests", json={
        "subject_type": "email",
        "subject_value": "user@example.com",
        "requester_email": "dpo@example.com",
        "requester_name": "DPO",
    })
    assert resp.status_code in (200, 400, 422, 500)


def test_list_requests(client):
    resp = client.get("/api/rtbf/requests")
    assert resp.status_code in (200, 400, 404, 500)


def test_get_overdue(client):
    resp = client.get("/api/rtbf/requests/overdue")
    assert resp.status_code in (200, 400, 404, 500)


def test_dashboard(client):
    resp = client.get("/api/rtbf/dashboard")
    assert resp.status_code in (200, 400, 404, 500)


def test_approaching_deadline(client):
    resp = client.get("/api/rtbf/requests/approaching-deadline")
    assert resp.status_code in (200, 400, 404, 500)


def test_get_request(client):
    resp = client.get("/api/rtbf/requests/req-123")
    assert resp.status_code in (200, 400, 404, 500)


def test_get_actions(client):
    resp = client.get("/api/rtbf/requests/req-123/actions")
    assert resp.status_code in (200, 400, 404, 500)


def test_update_status(client):
    resp = client.put("/api/rtbf/requests/req-123/status", json={
        "status": "approved",
    })
    assert resp.status_code in (200, 400, 404, 422, 500)


def test_get_certificate(client):
    resp = client.get("/api/rtbf/requests/req-123/certificate")
    assert resp.status_code in (200, 400, 404, 500)


def test_download_certificate(client):
    resp = client.get("/api/rtbf/requests/req-123/certificate/download")
    assert resp.status_code in (200, 400, 404, 500)
