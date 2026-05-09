"""Tests for api/routers/demo_code.py — round-trip + validation.

Mirrors tests/test_router_demo_logs.py and tests/test_router_demo_knowledge.py.
No 503 missing-deps path because Code has no optional Python deps.
"""

from __future__ import annotations

from unittest.mock import patch


# ── GET /demo-code/types ──────────────────────────────────────────


def test_get_types_returns_registry_with_available_true(client):
    resp = client.get("/api/generate/demo-code/types")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["available"] is True
    assert body["unavailable_reason"] is None
    assert isinstance(body["types"], list)
    assert len(body["types"]) == 3
    type_ids = {t["type"] for t in body["types"]}
    assert type_ids == {"python_repo", "js_repo", "java_repo"}
    for t in body["types"]:
        for key in ("type", "category", "label", "extension", "language"):
            assert key in t


# ── POST /demo-code/preview ───────────────────────────────────────


def test_preview_returns_repos_files_and_bytes(client):
    resp = client.post(
        "/api/generate/demo-code/preview",
        json={
            "types": ["python_repo", "js_repo"],
            "counts": {"python_repo": 3, "js_repo": 2},
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["total_repos"] == 5
    assert body["total_files"] > 0
    assert body["total_bytes"] > 0


def test_preview_with_empty_types_returns_zero_totals(client):
    resp = client.post(
        "/api/generate/demo-code/preview",
        json={"types": [], "counts": {}},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["total_repos"] == 0
    assert body["total_files"] == 0


# ── POST /demo-code (submit) ──────────────────────────────────────


def _valid_submit_payload(**overrides) -> dict:
    base = {
        "catalog": "demo",
        "schema": "iot",
        "volume": "demo_unstructured",
        "destination": "volume_with_catalog",
        "types": ["python_repo"],
        "counts": {"python_repo": 2},
        "industry": "healthcare",
        "warehouse_id": "wh-1",
    }
    base.update(overrides)
    return base


def test_submit_returns_job_id(client):
    with patch("src.demo_code.generate_code") as mock_gen:
        mock_gen.return_value = {"status": "completed", "files_written": 60}
        resp = client.post("/api/generate/demo-code", json=_valid_submit_payload())
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert "job_id" in body
    assert body["status"] == "queued"


def test_submit_rejects_dotted_catalog(client):
    resp = client.post(
        "/api/generate/demo-code",
        json=_valid_submit_payload(catalog="demo.iot"),
    )
    assert resp.status_code == 422
    detail = str(resp.json()["detail"]).lower()
    assert "single unity catalog identifier" in detail


def test_submit_rejects_unknown_code_type(client):
    resp = client.post(
        "/api/generate/demo-code",
        json=_valid_submit_payload(types=["go_repo"]),
    )
    assert resp.status_code == 422


def test_submit_rejects_volume_destinations_without_volume(client):
    payload = _valid_submit_payload()
    del payload["volume"]
    resp = client.post("/api/generate/demo-code", json=payload)
    assert resp.status_code == 422
    detail = str(resp.json()["detail"]).lower()
    assert "volume" in detail


def test_submit_allows_direct_table_without_volume(client):
    payload = _valid_submit_payload(destination="direct_table")
    del payload["volume"]
    with patch("src.demo_code.generate_code") as mock_gen:
        mock_gen.return_value = {"status": "completed"}
        resp = client.post("/api/generate/demo-code", json=payload)
    assert resp.status_code == 200, resp.text


def test_submit_rejects_counts_referencing_unselected_type(client):
    resp = client.post(
        "/api/generate/demo-code",
        json=_valid_submit_payload(
            types=["python_repo"],
            counts={"python_repo": 1, "js_repo": 5},  # js_repo NOT in types
        ),
    )
    assert resp.status_code == 422


def test_submit_caps_per_type_count_at_50(client):
    """Code cap is 50 repos (vs Logs at 1000 files, Knowledge at
    10000 files) — each repo is ~30 files, so 50 = 1500 files."""
    resp = client.post(
        "/api/generate/demo-code",
        json=_valid_submit_payload(counts={"python_repo": 100}),
    )
    assert resp.status_code == 422
    assert "50" in str(resp.json()["detail"])


def test_submit_returns_400_when_no_warehouse_anywhere(client):
    from api.dependencies import get_app_config
    from api.main import app

    app.dependency_overrides[get_app_config] = lambda: {}
    try:
        payload = _valid_submit_payload()
        del payload["warehouse_id"]
        resp = client.post("/api/generate/demo-code", json=payload)
    finally:
        app.dependency_overrides.pop(get_app_config, None)
    assert resp.status_code == 400
    assert "warehouse_id" in str(resp.json()["detail"]).lower()
