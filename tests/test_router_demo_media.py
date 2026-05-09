"""Tests for api/routers/demo_media.py — round-trip + validation.

Mirrors tests/test_router_demo_documents.py with media-specific
additions: the `ffmpeg_available` flag on /types, and the lower
per-type cap (5000 vs 10000).
"""

from __future__ import annotations

from unittest.mock import patch


# ── GET /demo-media/types ─────────────────────────────────────────


def test_get_types_returns_registry_with_dual_availability_flags(client):
    """Two distinct availability signals — Pillow and ffmpeg — so the
    UI can grey out video_clip independently of the rest."""
    resp = client.get("/api/generate/demo-media/types")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert isinstance(body["available"], bool)
    assert isinstance(body["ffmpeg_available"], bool)
    assert isinstance(body["types"], list)
    if body["available"]:
        assert len(body["types"]) > 0
        for t in body["types"]:
            for key in ("type", "category", "label", "extension"):
                assert key in t
        # video_clip type must exist in the registry regardless of
        # ffmpeg presence — UI greys it out, doesn't drop it.
        type_ids = {t["type"] for t in body["types"]}
        assert "video_clip" in type_ids


# ── POST /demo-media/preview ──────────────────────────────────────


def test_preview_round_trips_counts_to_total_files(client):
    resp = client.post(
        "/api/generate/demo-media/preview",
        json={
            "types": ["img_xray", "audio_voicemail"],
            "counts": {"img_xray": 5, "audio_voicemail": 10},
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["total_files"] == 15
    assert body["total_bytes"] > 0


def test_preview_with_empty_types_returns_zero_totals(client):
    resp = client.post(
        "/api/generate/demo-media/preview",
        json={"types": [], "counts": {}},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["total_files"] == 0
    assert body["total_bytes"] == 0


# ── POST /demo-media (submit) ─────────────────────────────────────


def _valid_submit_payload(**overrides) -> dict:
    base = {
        "catalog": "demo",
        "schema": "iot",
        "volume": "demo_unstructured",
        "destination": "volume_with_catalog",
        "types": ["img_xray"],
        "counts": {"img_xray": 3},
        "industry": "healthcare",
        "warehouse_id": "wh-1",
    }
    base.update(overrides)
    return base


def test_submit_returns_job_id_when_deps_available(client):
    from src.demo_media import MEDIA_AVAILABLE

    if not MEDIA_AVAILABLE:
        import pytest

        pytest.skip("[media] extra not installed in this venv")

    with patch("src.demo_media.generate_media") as mock_gen:
        mock_gen.return_value = {"status": "completed", "files_written": 3}
        resp = client.post("/api/generate/demo-media", json=_valid_submit_payload())
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert "job_id" in body
    assert body["status"] == "queued"


def test_submit_returns_503_with_install_hint_when_deps_missing(client):
    with patch("src.demo_media.is_available", return_value=(False, "deps missing")):
        resp = client.post("/api/generate/demo-media", json=_valid_submit_payload())
    assert resp.status_code == 503
    detail = resp.json()["detail"]
    assert detail["error"] == "dependencies_missing"
    assert detail["extra"] == "media"
    assert "pip install clone-xs[media]" in detail["install_command"]


def test_submit_rejects_dotted_catalog(client):
    resp = client.post(
        "/api/generate/demo-media",
        json=_valid_submit_payload(catalog="demo.iot"),
    )
    assert resp.status_code == 422
    detail = str(resp.json()["detail"]).lower()
    assert "single unity catalog identifier" in detail


def test_submit_rejects_unknown_media_type(client):
    resp = client.post(
        "/api/generate/demo-media",
        json=_valid_submit_payload(types=["not_a_real_type"]),
    )
    assert resp.status_code == 422


def test_submit_rejects_volume_destinations_without_volume(client):
    payload = _valid_submit_payload()
    del payload["volume"]
    resp = client.post("/api/generate/demo-media", json=payload)
    assert resp.status_code == 422
    detail = str(resp.json()["detail"]).lower()
    assert "volume" in detail


def test_submit_allows_direct_table_without_volume(client):
    from src.demo_media import MEDIA_AVAILABLE

    if not MEDIA_AVAILABLE:
        import pytest

        pytest.skip("[media] extra not installed in this venv")

    payload = _valid_submit_payload(destination="direct_table")
    del payload["volume"]
    with patch("src.demo_media.generate_media") as mock_gen:
        mock_gen.return_value = {"status": "completed"}
        resp = client.post("/api/generate/demo-media", json=payload)
    assert resp.status_code == 200, resp.text


def test_submit_rejects_counts_referencing_unselected_type(client):
    resp = client.post(
        "/api/generate/demo-media",
        json=_valid_submit_payload(
            types=["img_xray"],
            counts={"img_xray": 1, "video_clip": 5},  # video_clip NOT in types
        ),
    )
    assert resp.status_code == 422


def test_submit_caps_per_type_count_at_5000(client):
    """Media's per-type cap is 5000 (lower than Documents' 10000)
    because video_clip generation is ~500 ms per file."""
    resp = client.post(
        "/api/generate/demo-media",
        json=_valid_submit_payload(counts={"img_xray": 999_999}),
    )
    assert resp.status_code == 422
    assert "5000" in str(resp.json()["detail"])


def test_submit_returns_400_when_no_warehouse_anywhere(client):
    from src.demo_media import MEDIA_AVAILABLE

    if not MEDIA_AVAILABLE:
        import pytest

        pytest.skip("[media] extra not installed in this venv")

    from api.dependencies import get_app_config
    from api.main import app

    app.dependency_overrides[get_app_config] = lambda: {}
    try:
        payload = _valid_submit_payload()
        del payload["warehouse_id"]
        resp = client.post("/api/generate/demo-media", json=payload)
    finally:
        app.dependency_overrides.pop(get_app_config, None)
    assert resp.status_code == 400
    assert "warehouse_id" in str(resp.json()["detail"]).lower()
