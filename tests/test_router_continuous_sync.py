"""Tests for the continuous-sync router — plan + executor lifecycle endpoints.

The plan endpoint was already there from v0.11.0 (preview-only). Feature 6
added: POST /start, GET /streams, GET /streams/{id}, POST /streams/{id}/stop,
POST /streams/{id}/restart. These tests verify routing + 404 + error paths;
the runner's behaviour is exhaustively tested in tests/test_continuous_sync_runner.py.
"""

import pytest

pytest.importorskip("fastapi")

from src.continuous_sync_runner import _reset_registry_for_tests


@pytest.fixture(autouse=True)
def _clean_registry():
    """Stream registry is process-level — clean between tests."""
    _reset_registry_for_tests()
    yield
    _reset_registry_for_tests()


def test_plan_returns_streaming_spec(client):
    """The original preview endpoint still works — backwards-compat check."""
    resp = client.post("/api/continuous-sync/plan", json={
        "source_catalog": "src_cat",
        "destination_catalog": "dst_cat",
        "tables": ["bronze.events"],
        "trigger_ms": 30_000,
    })
    assert resp.status_code == 200
    assert resp.json()["status"] == "preview_only"


def test_plan_rejects_no_tables_no_schema(client):
    """Plan generation requires either `tables` (explicit) or `schema_name`
    (all-in-schema). Neither → 400."""
    resp = client.post("/api/continuous-sync/plan", json={
        "source_catalog": "src_cat",
        "destination_catalog": "dst_cat",
    })
    assert resp.status_code == 400


def test_start_returns_record_with_run_id(client, mock_workspace_client):
    """POST /start submits the streaming job and registers a stream record.
    The mock returns run_id=42; the response carries that, plus status=starting."""
    submit_response = mock_workspace_client.jobs.submit.return_value
    submit_response.run_id = 42

    resp = client.post("/api/continuous-sync/start", json={
        "source_catalog": "src_cat",
        "destination_catalog": "dst_cat",
        "tables": ["bronze.events"],
    })
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "starting"
    assert body["run_id"] == 42
    assert body["source_catalog"] == "src_cat"
    assert body["stream_id"].startswith("sync-")


def test_start_invalid_plan_returns_400(client):
    """Plan-generation errors (no tables, no schema) propagate as 400 so
    callers can correct + retry."""
    resp = client.post("/api/continuous-sync/start", json={
        "source_catalog": "src_cat",
        "destination_catalog": "dst_cat",
    })
    assert resp.status_code == 400


def test_streams_list_after_start(client, mock_workspace_client):
    """After POST /start, GET /streams should include the new record."""
    submit = mock_workspace_client.jobs.submit.return_value
    submit.run_id = 7
    client.post("/api/continuous-sync/start", json={
        "source_catalog": "src_cat",
        "destination_catalog": "dst_cat",
        "tables": ["bronze.events"],
    })

    resp = client.get("/api/continuous-sync/streams")
    assert resp.status_code == 200
    streams = resp.json()
    assert len(streams) == 1
    assert streams[0]["run_id"] == 7


def test_get_stream_unknown_returns_404(client):
    resp = client.get("/api/continuous-sync/streams/no-such-stream")
    assert resp.status_code == 404


def test_stop_unknown_returns_404(client):
    resp = client.post("/api/continuous-sync/streams/no-such-stream/stop")
    assert resp.status_code == 404


def test_restart_unknown_returns_404(client):
    resp = client.post("/api/continuous-sync/streams/no-such-stream/restart")
    assert resp.status_code == 404


def test_stop_stream_marks_stopped(client, mock_workspace_client):
    """Full lifecycle: start, then stop. Final status reads `stopped`."""
    submit = mock_workspace_client.jobs.submit.return_value
    submit.run_id = 1234
    started = client.post("/api/continuous-sync/start", json={
        "source_catalog": "src_cat",
        "destination_catalog": "dst_cat",
        "tables": ["bronze.events"],
    }).json()
    stream_id = started["stream_id"]

    resp = client.post(f"/api/continuous-sync/streams/{stream_id}/stop")
    assert resp.status_code == 200
    assert resp.json()["status"] == "stopped"
    mock_workspace_client.jobs.cancel_run.assert_called_once_with(run_id=1234)
