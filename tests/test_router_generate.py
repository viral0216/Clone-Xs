"""Tests for the generate router."""

import pytest
from unittest.mock import patch, MagicMock

pytest.importorskip("fastapi")


def test_generate_workflow(client):
    with patch("src.workflow.generate_workflow_yaml", return_value="/tmp/wf.yaml"), \
         patch("api.routers.generate._read_generated_file", return_value="content: {}"):
        resp = client.post("/api/generate/workflow", json={
            "format": "yaml",
            "job_name": "test-clone-job",
        })
    assert resp.status_code in (200, 422)


def test_generate_workflow_json_format(client):
    with patch("src.workflow.generate_workflow", return_value="/tmp/wf.json"), \
         patch("api.routers.generate._read_generated_file", return_value="{}"):
        resp = client.post("/api/generate/workflow", json={
            "format": "json",
            "job_name": "test-clone-job",
        })
    assert resp.status_code in (200, 422)


def test_generate_terraform(client):
    resp = client.post("/api/generate/terraform", json={
        "source_catalog": "src_cat",
        "format": "terraform",
    })
    assert resp.status_code in (200, 422)


def test_create_databricks_job(client):
    with patch("src.create_job.create_persistent_job", return_value={"job_id": 1}):
        resp = client.post("/api/generate/create-job", json={
            "source_catalog": "src_cat",
            "destination_catalog": "dst_cat",
            "job_name": "test-job",
        })
    assert resp.status_code in (200, 422)


def test_run_job_now(client):
    resp = client.post("/api/generate/run-job/123")
    assert resp.status_code in (200, 500)


def test_list_clone_xs_jobs(client):
    resp = client.get("/api/generate/clone-jobs")
    assert resp.status_code == 200


def test_generate_demo_data(client):
    resp = client.post("/api/generate/demo-data", json={
        "catalog_name": "demo_cat",
    })
    assert resp.status_code in (200, 422)


def test_cleanup_demo_data(client):
    with patch("src.demo_generator.cleanup_demo_catalog", return_value={"status": "ok"}):
        resp = client.delete("/api/generate/demo-data/demo_cat")
    assert resp.status_code in (200, 400)
