"""Tests for api/routers/demo_logs.py — round-trip + validation.

Mirrors tests/test_router_demo_knowledge.py. Like Knowledge, Logs
has no missing-deps path (no optional Python deps), so the 503 test
from the Documents/Media routers isn't needed here.
"""

from __future__ import annotations

from unittest.mock import patch


# ── GET /demo-logs/types ──────────────────────────────────────────


def test_get_types_returns_registry_with_available_true(client):
    """Logs has no optional deps so `available` is always True. Pin
    this so a future refactor that wires in a missing-deps gate by
    mistake trips this test."""
    resp = client.get("/api/generate/demo-logs/types")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["available"] is True
    assert body["unavailable_reason"] is None
    assert isinstance(body["types"], list)
    assert len(body["types"]) == 4
    type_ids = {t["type"] for t in body["types"]}
    assert type_ids == {"nginx_access", "app_json", "syslog", "otel_trace"}
    for t in body["types"]:
        for key in ("type", "category", "label", "extension"):
            assert key in t


# ── POST /demo-logs/preview ───────────────────────────────────────


def test_preview_returns_files_lines_and_bytes(client):
    resp = client.post(
        "/api/generate/demo-logs/preview",
        json={
            "types": ["nginx_access", "app_json"],
            "counts": {"nginx_access": 3, "app_json": 2},
            "lines_per_file": 1000,
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["total_files"] == 5
    assert body["total_lines"] == 5000
    assert body["total_bytes"] > 0


def test_preview_with_empty_types_returns_zero_totals(client):
    resp = client.post(
        "/api/generate/demo-logs/preview",
        json={"types": [], "counts": {}, "lines_per_file": 1000},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["total_files"] == 0
    assert body["total_lines"] == 0
    assert body["total_bytes"] == 0


def test_preview_scales_with_lines_per_file(client):
    """Doubling lines_per_file doubles total_lines + total_bytes —
    pin so a UI bug that forgets to send the field surfaces here."""
    base = client.post(
        "/api/generate/demo-logs/preview",
        json={"types": ["nginx_access"], "counts": {"nginx_access": 2}, "lines_per_file": 1000},
    ).json()
    doubled = client.post(
        "/api/generate/demo-logs/preview",
        json={"types": ["nginx_access"], "counts": {"nginx_access": 2}, "lines_per_file": 2000},
    ).json()
    assert doubled["total_lines"] == 2 * base["total_lines"]
    assert doubled["total_bytes"] == 2 * base["total_bytes"]


# ── POST /demo-logs (submit) ──────────────────────────────────────


def _valid_submit_payload(**overrides) -> dict:
    base = {
        "catalog": "demo",
        "schema": "iot",
        "volume": "demo_unstructured",
        "destination": "volume_with_catalog",
        "types": ["nginx_access"],
        "counts": {"nginx_access": 3},
        "industry": "healthcare",
        "lines_per_file": 100,
        "days_back": 7,
        "warehouse_id": "wh-1",
    }
    base.update(overrides)
    return base


def test_submit_returns_job_id(client):
    """Logs has no missing-dep path — submit always reaches the
    JobManager when the payload is valid."""
    with patch("src.demo_logs.generate_logs") as mock_gen:
        mock_gen.return_value = {"status": "completed", "files_written": 3}
        resp = client.post("/api/generate/demo-logs", json=_valid_submit_payload())
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert "job_id" in body
    assert body["status"] == "queued"


def test_submit_rejects_dotted_catalog(client):
    resp = client.post(
        "/api/generate/demo-logs",
        json=_valid_submit_payload(catalog="demo.iot"),
    )
    assert resp.status_code == 422
    detail = str(resp.json()["detail"]).lower()
    assert "single unity catalog identifier" in detail


def test_submit_rejects_unknown_log_type(client):
    resp = client.post(
        "/api/generate/demo-logs",
        json=_valid_submit_payload(types=["not_a_real_type"]),
    )
    assert resp.status_code == 422


def test_submit_rejects_volume_destinations_without_volume(client):
    payload = _valid_submit_payload()
    del payload["volume"]
    resp = client.post("/api/generate/demo-logs", json=payload)
    assert resp.status_code == 422
    detail = str(resp.json()["detail"]).lower()
    assert "volume" in detail


def test_submit_allows_direct_table_without_volume(client):
    payload = _valid_submit_payload(destination="direct_table")
    del payload["volume"]
    with patch("src.demo_logs.generate_logs") as mock_gen:
        mock_gen.return_value = {"status": "completed"}
        resp = client.post("/api/generate/demo-logs", json=payload)
    assert resp.status_code == 200, resp.text


def test_submit_rejects_counts_referencing_unselected_type(client):
    resp = client.post(
        "/api/generate/demo-logs",
        json=_valid_submit_payload(
            types=["nginx_access"],
            counts={"nginx_access": 1, "app_json": 5},  # app_json NOT in types
        ),
    )
    assert resp.status_code == 422


def test_submit_caps_per_type_count_at_1000(client):
    """Logs cap is 1000 (lower than Documents/Knowledge at 10000)
    because each "count" is a number of files and each file holds
    1000+ lines."""
    resp = client.post(
        "/api/generate/demo-logs",
        json=_valid_submit_payload(counts={"nginx_access": 999_999}),
    )
    assert resp.status_code == 422
    assert "1000" in str(resp.json()["detail"])


def test_submit_caps_lines_per_file_at_100000(client):
    resp = client.post(
        "/api/generate/demo-logs",
        json=_valid_submit_payload(lines_per_file=200_000),
    )
    assert resp.status_code == 422


def test_submit_caps_days_back_at_365(client):
    resp = client.post(
        "/api/generate/demo-logs",
        json=_valid_submit_payload(days_back=400),
    )
    assert resp.status_code == 422


def test_submit_returns_400_when_no_warehouse_anywhere(client):
    from api.dependencies import get_app_config
    from api.main import app

    app.dependency_overrides[get_app_config] = lambda: {}
    try:
        payload = _valid_submit_payload()
        del payload["warehouse_id"]
        resp = client.post("/api/generate/demo-logs", json=payload)
    finally:
        app.dependency_overrides.pop(get_app_config, None)
    assert resp.status_code == 400
    assert "warehouse_id" in str(resp.json()["detail"]).lower()
