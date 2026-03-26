"""Tests for the clone router — 4 endpoints."""

import pytest

pytest.importorskip("fastapi")


def test_list_jobs(client):
    resp = client.get("/api/clone/jobs")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)


def test_start_clone(client):
    resp = client.post("/api/clone", json={
        "source_catalog": "src_cat",
        "destination_catalog": "dst_cat",
    })
    assert resp.status_code == 200
    data = resp.json()
    assert "job_id" in data


def test_get_nonexistent_job(client):
    resp = client.get("/api/clone/nonexistent-id")
    assert resp.status_code in (200, 404)


def test_cancel_nonexistent_job(client):
    resp = client.delete("/api/clone/nonexistent-id")
    assert resp.status_code in (200, 404)


def test_start_and_get_job(client):
    """Start a job then fetch its status."""
    start = client.post("/api/clone", json={
        "source_catalog": "src",
        "destination_catalog": "dst",
    })
    assert start.status_code == 200
    job_id = start.json().get("job_id")
    if job_id:
        status = client.get(f"/api/clone/{job_id}")
        assert status.status_code == 200
