"""Tests for api/routers/demo_knowledge.py — round-trip + validation.

Mirrors tests/test_router_demo_documents.py and
tests/test_router_demo_media.py. Knowledge has no missing-deps path
(no optional Python deps), so the 503 test from the other modules
isn't needed here.
"""

from __future__ import annotations

from unittest.mock import patch


# ── GET /demo-knowledge/types ─────────────────────────────────────


def test_get_types_returns_registry_with_available_true(client):
    """Knowledge has no optional deps so `available` is always True.
    Pin this so a future refactor that wires in a missing-deps gate
    by mistake trips this test."""
    resp = client.get("/api/generate/demo-knowledge/types")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["available"] is True
    assert body["unavailable_reason"] is None
    assert isinstance(body["types"], list)
    assert len(body["types"]) == 3
    type_ids = {t["type"] for t in body["types"]}
    assert type_ids == {"wiki_article", "qa_pair", "chat_thread"}
    for t in body["types"]:
        for key in ("type", "category", "label", "extension"):
            assert key in t


# ── POST /demo-knowledge/preview ──────────────────────────────────


def test_preview_round_trips_counts_to_total_files(client):
    resp = client.post(
        "/api/generate/demo-knowledge/preview",
        json={"types": ["wiki_article", "qa_pair"], "counts": {"wiki_article": 5, "qa_pair": 10}},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["total_files"] == 15
    assert body["total_bytes"] > 0


def test_preview_with_empty_types_returns_zero_totals(client):
    resp = client.post(
        "/api/generate/demo-knowledge/preview",
        json={"types": [], "counts": {}},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["total_files"] == 0
    assert body["total_bytes"] == 0


# ── POST /demo-knowledge (submit) ─────────────────────────────────


def _valid_submit_payload(**overrides) -> dict:
    base = {
        "catalog": "demo",
        "schema": "iot",
        "volume": "demo_unstructured",
        "destination": "volume_with_catalog",
        "types": ["wiki_article"],
        "counts": {"wiki_article": 3},
        "industry": "healthcare",
        "warehouse_id": "wh-1",
    }
    base.update(overrides)
    return base


def test_submit_returns_job_id(client):
    """Knowledge has no missing-dep path — submit always reaches the
    JobManager when the payload is valid."""
    with patch("src.demo_knowledge.generate_knowledge") as mock_gen:
        mock_gen.return_value = {"status": "completed", "files_written": 3}
        resp = client.post("/api/generate/demo-knowledge", json=_valid_submit_payload())
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert "job_id" in body
    assert body["status"] == "queued"


def test_submit_rejects_dotted_catalog(client):
    resp = client.post(
        "/api/generate/demo-knowledge",
        json=_valid_submit_payload(catalog="demo.iot"),
    )
    assert resp.status_code == 422
    detail = str(resp.json()["detail"]).lower()
    assert "single unity catalog identifier" in detail


def test_submit_rejects_unknown_knowledge_type(client):
    resp = client.post(
        "/api/generate/demo-knowledge",
        json=_valid_submit_payload(types=["not_a_real_type"]),
    )
    assert resp.status_code == 422


def test_submit_rejects_volume_destinations_without_volume(client):
    payload = _valid_submit_payload()
    del payload["volume"]
    resp = client.post("/api/generate/demo-knowledge", json=payload)
    assert resp.status_code == 422
    detail = str(resp.json()["detail"]).lower()
    assert "volume" in detail


def test_submit_allows_direct_table_without_volume(client):
    payload = _valid_submit_payload(destination="direct_table")
    del payload["volume"]
    with patch("src.demo_knowledge.generate_knowledge") as mock_gen:
        mock_gen.return_value = {"status": "completed"}
        resp = client.post("/api/generate/demo-knowledge", json=payload)
    assert resp.status_code == 200, resp.text


def test_submit_rejects_counts_referencing_unselected_type(client):
    resp = client.post(
        "/api/generate/demo-knowledge",
        json=_valid_submit_payload(
            types=["wiki_article"],
            counts={"wiki_article": 1, "qa_pair": 5},  # qa_pair NOT in types
        ),
    )
    assert resp.status_code == 422


def test_submit_caps_per_type_count_at_10000(client):
    """Knowledge cap matches Documents (10000) — generation is fast
    so the higher cap is fine."""
    resp = client.post(
        "/api/generate/demo-knowledge",
        json=_valid_submit_payload(counts={"wiki_article": 999_999}),
    )
    assert resp.status_code == 422
    assert "10000" in str(resp.json()["detail"])


def test_submit_returns_400_when_no_warehouse_anywhere(client):
    from api.dependencies import get_app_config
    from api.main import app

    app.dependency_overrides[get_app_config] = lambda: {}
    try:
        payload = _valid_submit_payload()
        del payload["warehouse_id"]
        resp = client.post("/api/generate/demo-knowledge", json=payload)
    finally:
        app.dependency_overrides.pop(get_app_config, None)
    assert resp.status_code == 400
    assert "warehouse_id" in str(resp.json()["detail"]).lower()
