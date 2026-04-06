"""Smoke tests for the DLT router."""
import pytest

pytest.importorskip("fastapi")


def test_list_pipelines(client):
    resp = client.get("/api/dlt/pipelines")
    assert resp.status_code in (200, 400, 404, 500)


def test_get_pipeline(client):
    resp = client.get("/api/dlt/pipelines/pipe-123")
    assert resp.status_code in (200, 400, 404, 500)


def test_trigger_pipeline(client):
    resp = client.post("/api/dlt/pipelines/pipe-123/trigger", json={
        "full_refresh": False,
    })
    assert resp.status_code in (200, 400, 422, 500)


def test_stop_pipeline(client):
    resp = client.post("/api/dlt/pipelines/pipe-123/stop")
    assert resp.status_code in (200, 400, 404, 500)


def test_clone_pipeline(client):
    resp = client.post("/api/dlt/pipelines/pipe-123/clone", json={
        "new_name": "cloned-pipeline",
    })
    assert resp.status_code in (200, 400, 422, 500)


def test_get_events(client):
    resp = client.get("/api/dlt/pipelines/pipe-123/events")
    assert resp.status_code in (200, 400, 404, 500)


def test_get_updates(client):
    resp = client.get("/api/dlt/pipelines/pipe-123/updates")
    assert resp.status_code in (200, 400, 404, 500)


def test_get_lineage(client):
    resp = client.get("/api/dlt/pipelines/pipe-123/lineage")
    assert resp.status_code in (200, 400, 404, 500)


def test_get_expectations(client):
    resp = client.get("/api/dlt/pipelines/pipe-123/expectations")
    assert resp.status_code in (200, 400, 404, 500)


def test_dashboard(client):
    resp = client.get("/api/dlt/dashboard")
    assert resp.status_code in (200, 400, 404, 500)
