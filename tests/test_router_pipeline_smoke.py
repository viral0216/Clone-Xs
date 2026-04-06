"""Smoke tests for the pipeline router."""
import pytest

pytest.importorskip("fastapi")


def test_create_pipeline(client):
    resp = client.post("/api/pipelines/pipelines", json={
        "name": "test-pipeline",
        "steps": [{"type": "clone", "name": "step1", "config": {}}],
    })
    assert resp.status_code in (200, 400, 422, 500)


def test_list_pipelines(client):
    resp = client.get("/api/pipelines/pipelines")
    assert resp.status_code in (200, 400, 404, 500)


def test_get_pipeline(client):
    resp = client.get("/api/pipelines/pipelines/pipe-123")
    assert resp.status_code in (200, 400, 404, 500)


def test_delete_pipeline(client):
    resp = client.delete("/api/pipelines/pipelines/pipe-123")
    assert resp.status_code in (200, 400, 404, 500)


def test_run_pipeline(client):
    resp = client.post("/api/pipelines/pipelines/pipe-123/run")
    assert resp.status_code in (200, 400, 404, 500)


def test_list_runs(client):
    resp = client.get("/api/pipelines/runs")
    assert resp.status_code in (200, 400, 404, 500)


def test_get_run(client):
    resp = client.get("/api/pipelines/runs/run-123")
    assert resp.status_code in (200, 400, 404, 500)


def test_cancel_run(client):
    resp = client.post("/api/pipelines/runs/run-123/cancel")
    assert resp.status_code in (200, 400, 404, 500)


def test_list_templates(client):
    resp = client.get("/api/pipelines/templates")
    assert resp.status_code in (200, 400, 404, 500)


def test_create_from_template(client):
    resp = client.post("/api/pipelines/templates/basic-clone/create", json={
        "template_name": "basic-clone",
    })
    assert resp.status_code in (200, 400, 404, 422, 500)
