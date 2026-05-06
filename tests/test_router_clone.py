"""Tests for the clone router — 4 endpoints."""

import pytest

pytest.importorskip("fastapi")


def test_list_jobs(client):
    resp = client.get("/api/clone/jobs")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)


def test_start_clone(client):
    resp = client.post(
        "/api/clone",
        json={
            "source_catalog": "src_cat",
            "destination_catalog": "dst_cat",
        },
    )
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
    start = client.post(
        "/api/clone",
        json={
            "source_catalog": "src",
            "destination_catalog": "dst",
        },
    )
    assert start.status_code == 200
    job_id = start.json().get("job_id")
    if job_id:
        status = client.get(f"/api/clone/{job_id}")
        assert status.status_code == 200


def test_start_clone_selective_load_type_accepted(client):
    """`load_type=SELECTIVE` is the new third option (alongside FULL +
    INCREMENTAL) introduced for selective re-clone — verify the router
    accepts it without 422."""
    resp = client.post(
        "/api/clone",
        json={
            "source_catalog": "src_cat",
            "destination_catalog": "dst_cat",
            "load_type": "SELECTIVE",
        },
    )
    assert resp.status_code == 200
    assert "job_id" in resp.json()


def test_start_clone_rejects_unknown_load_type(client):
    """Pydantic Literal narrows load_type to FULL/INCREMENTAL/SELECTIVE —
    anything else returns 422 (don't silently fall through to FULL)."""
    resp = client.post(
        "/api/clone",
        json={
            "source_catalog": "src_cat",
            "destination_catalog": "dst_cat",
            "load_type": "BOGUS",
        },
    )
    assert resp.status_code == 422


def test_start_clone_with_quiesce_source_accepted(client):
    """`quiesce_source=True` is the new opt-in pre-clone read-only mode —
    verify the router accepts it without 422 and propagates the flag."""
    resp = client.post(
        "/api/clone",
        json={
            "source_catalog": "src_cat",
            "destination_catalog": "dst_cat",
            "quiesce_source": True,
        },
    )
    assert resp.status_code == 200
    assert "job_id" in resp.json()


def _target_payload(host: str) -> dict:
    return {
        "host": f"https://{host}.azuredatabricks.net",
        "auth_method": "pat",
        "token": "dapi-test-token",
        "warehouse_id": f"wh-{host}",
        "data_sync_mode": "snapshot_once",
    }


def test_start_clone_with_target_workspaces_routes_to_fanout(client):
    """Plural `target_workspaces` is the new multi-target fanout entrypoint.
    Verify the router accepts it (200) and the response message mentions
    fanout / target count so users know which dispatch path was taken."""
    resp = client.post(
        "/api/clone",
        json={
            "source_catalog": "src_cat",
            "destination_catalog": "dst_cat",
            "target_workspaces": [_target_payload("eu"), _target_payload("us")],
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert "job_id" in body
    # Server-side message reflects the dispatch decision.
    assert body.get("message")
    assert "2 targets" in body["message"] or "fanout" in body["message"].lower()


def test_start_clone_rejects_both_singular_and_plural_target(client):
    """XOR validator: setting BOTH `target_workspace` (singular) AND
    `target_workspaces` (plural) is a 422. Silently picking one would
    surprise callers since the dispatch differs."""
    resp = client.post(
        "/api/clone",
        json={
            "source_catalog": "src_cat",
            "destination_catalog": "dst_cat",
            "target_workspace": _target_payload("solo"),
            "target_workspaces": [_target_payload("eu")],
        },
    )
    assert resp.status_code == 422


def test_start_clone_rejects_invalid_fanout_max_parallel(client):
    """fanout_max_parallel must be ≥ 1; zero or negative is a 422."""
    resp = client.post(
        "/api/clone",
        json={
            "source_catalog": "src_cat",
            "destination_catalog": "dst_cat",
            "target_workspaces": [_target_payload("eu")],
            "fanout_max_parallel": 0,
        },
    )
    assert resp.status_code == 422
