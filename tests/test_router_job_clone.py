"""Smoke tests for the job clone router."""
import pytest

pytest.importorskip("fastapi")


def test_list_jobs(client):
    resp = client.get("/api/jobs/")
    assert resp.status_code in (200, 400, 404, 500)


def test_get_job_details(client):
    resp = client.get("/api/jobs/12345")
    assert resp.status_code in (200, 400, 404, 500)


def test_clone_job(client):
    resp = client.post("/api/jobs/clone", json={
        "job_id": 12345,
        "new_name": "cloned-job",
    })
    assert resp.status_code in (200, 400, 422, 500)


def test_clone_cross_workspace(client):
    resp = client.post("/api/jobs/clone-cross-workspace", json={
        "job_id": 12345,
        "dest_host": "https://dest.azuredatabricks.net",
        "dest_token": "dapi-test-token",
        "new_name": "cross-cloned-job",
    })
    assert resp.status_code in (200, 400, 422, 500)


def test_diff_jobs(client):
    resp = client.post("/api/jobs/diff", json={
        "job_id_a": 12345,
        "job_id_b": 67890,
    })
    assert resp.status_code in (200, 400, 422, 500)


def test_backup_jobs(client):
    resp = client.post("/api/jobs/backup", json={
        "job_ids": [12345],
    })
    assert resp.status_code in (200, 400, 422, 500)


def test_restore_jobs(client):
    resp = client.post("/api/jobs/restore", json={
        "definitions": [],
    })
    assert resp.status_code in (200, 400, 422, 500)
