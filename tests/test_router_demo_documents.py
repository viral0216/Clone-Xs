"""Tests for api/routers/demo_documents.py — round-trip + validation.

The router is intentionally thin (validation + JobManager dispatch),
so most coverage is on the request model. The orchestrator itself
is covered by tests/test_demo_documents.py.
"""

from __future__ import annotations

from unittest.mock import patch


# ── GET /demo-documents/types ─────────────────────────────────────


def test_get_types_returns_registry_with_availability_flag(client):
    """The UI calls this on mount to render the checkbox grid + the
    install hint. Must always return 200 with `available` + `types`,
    even when the deps aren't installed (so the UI can show the hint
    instead of a broken page)."""
    resp = client.get("/api/generate/demo-documents/types")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert isinstance(body["available"], bool)
    assert isinstance(body["types"], list)
    if body["available"]:
        assert len(body["types"]) > 0
        # Each entry has the four expected keys.
        for t in body["types"]:
            for key in ("type", "category", "label", "extension"):
                assert key in t, f"types entry missing key: {key}"


# ── POST /demo-documents/preview ──────────────────────────────────


def test_preview_round_trips_counts_to_total_files(client):
    """Preview is pure arithmetic — must return immediately with the
    summed file count. UI calls this on every form change."""
    resp = client.post(
        "/api/generate/demo-documents/preview",
        json={"types": ["pdf_claim", "eml_message"], "counts": {"pdf_claim": 5, "eml_message": 10}},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["total_files"] == 15
    assert body["total_bytes"] > 0
    assert len(body["per_type"]) == 2


def test_preview_with_empty_types_returns_zero_totals(client):
    """Empty form → preview returns 0/0/0 cleanly. The UI relies on
    this to clear the estimate tile when the operator unchecks
    everything."""
    resp = client.post(
        "/api/generate/demo-documents/preview",
        json={"types": [], "counts": {}},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["total_files"] == 0
    assert body["total_bytes"] == 0


# ── POST /demo-documents (submit) ──────────────────────────────────


def _valid_submit_payload(**overrides) -> dict:
    base = {
        "catalog": "demo",
        "schema": "iot",
        "volume": "demo_unstructured",
        "destination": "volume_with_catalog",
        "types": ["eml_message"],
        "counts": {"eml_message": 3},
        "industry": "healthcare",
        "warehouse_id": "wh-1",
    }
    base.update(overrides)
    return base


def test_submit_returns_job_id_when_deps_available(client):
    """Happy path — when the [documents] extra is installed, submit
    returns 200 with a job_id the UI can poll."""
    # The dep is installed in the dev venv (we install it explicitly
    # for the test suite); skip the test if not.
    from src.demo_documents import DOCUMENTS_AVAILABLE

    if not DOCUMENTS_AVAILABLE:
        import pytest

        pytest.skip("[documents] extra not installed in this venv")

    with patch("src.demo_documents.generate_documents") as mock_gen:
        mock_gen.return_value = {"status": "completed", "files_written": 3}
        resp = client.post("/api/generate/demo-documents", json=_valid_submit_payload())
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert "job_id" in body
    assert body["status"] == "queued"


def test_submit_returns_503_with_install_hint_when_deps_missing(client):
    """When the [documents] extra is NOT installed, the endpoint must
    return a structured 503 with the install command embedded so the
    UI can render a calm banner instead of a generic error toast."""
    with patch("src.demo_documents.is_available", return_value=(False, "deps missing")):
        resp = client.post("/api/generate/demo-documents", json=_valid_submit_payload())
    assert resp.status_code == 503
    detail = resp.json()["detail"]
    assert detail["error"] == "dependencies_missing"
    assert detail["extra"] == "documents"
    assert "pip install clone-xs[documents]" in detail["install_command"]


def test_submit_rejects_dotted_catalog(client):
    """Single most common operator mistake — pasting a multi-part FQN
    prefix into the catalog field. Reject up-front with 422 + a clear
    message naming the offending field."""
    resp = client.post(
        "/api/generate/demo-documents",
        json=_valid_submit_payload(catalog="demo.iot"),
    )
    assert resp.status_code == 422
    detail = str(resp.json()["detail"]).lower()
    assert "single unity catalog identifier" in detail
    assert "catalog" in detail


def test_submit_rejects_unknown_document_type(client):
    """Unknown type IDs are rejected by the Pydantic Literal at
    validation time — never reach the orchestrator."""
    resp = client.post(
        "/api/generate/demo-documents",
        json=_valid_submit_payload(types=["not_a_real_type"]),
    )
    assert resp.status_code == 422


def test_submit_rejects_volume_destinations_without_volume(client):
    """`volume` and `volume_with_catalog` need a Volume. Missing
    Volume → 422 with a clear message instead of a confusing UC
    error mid-job."""
    payload = _valid_submit_payload()
    del payload["volume"]
    resp = client.post("/api/generate/demo-documents", json=payload)
    assert resp.status_code == 422
    detail = str(resp.json()["detail"]).lower()
    assert "volume" in detail


def test_submit_allows_direct_table_without_volume(client):
    """`direct_table` doesn't write to a Volume, so omitting it is
    valid. Pin this so a future tightening of the validator doesn't
    accidentally require it everywhere."""
    from src.demo_documents import DOCUMENTS_AVAILABLE

    if not DOCUMENTS_AVAILABLE:
        import pytest

        pytest.skip("[documents] extra not installed in this venv")

    payload = _valid_submit_payload(destination="direct_table")
    del payload["volume"]
    with patch("src.demo_documents.generate_documents") as mock_gen:
        mock_gen.return_value = {"status": "completed"}
        resp = client.post("/api/generate/demo-documents", json=payload)
    assert resp.status_code == 200, resp.text


def test_submit_rejects_counts_referencing_unselected_type(client):
    """`counts` referencing a type not in `types` is an operator
    mistake (typo or stale form state). Reject up-front."""
    resp = client.post(
        "/api/generate/demo-documents",
        json=_valid_submit_payload(
            types=["eml_message"],
            counts={"eml_message": 1, "pdf_claim": 5},  # pdf_claim NOT in types
        ),
    )
    assert resp.status_code == 422


def test_submit_caps_per_type_count_at_10000(client):
    """Per-type count is capped at 10000 to prevent runaway demo
    runs that fill the warehouse's compute / storage budget."""
    resp = client.post(
        "/api/generate/demo-documents",
        json=_valid_submit_payload(counts={"eml_message": 999_999}),
    )
    assert resp.status_code == 422
    assert "10000" in str(resp.json()["detail"])


def test_submit_returns_400_when_no_warehouse_anywhere(client):
    """No warehouse_id in the request AND no default in app config →
    400 with a clear message. Mirrors every other endpoint's contract."""
    from src.demo_documents import DOCUMENTS_AVAILABLE

    if not DOCUMENTS_AVAILABLE:
        import pytest

        pytest.skip("[documents] extra not installed in this venv")

    from api.dependencies import get_app_config
    from api.main import app

    app.dependency_overrides[get_app_config] = lambda: {}
    try:
        payload = _valid_submit_payload()
        del payload["warehouse_id"]
        resp = client.post("/api/generate/demo-documents", json=payload)
    finally:
        app.dependency_overrides.pop(get_app_config, None)
    assert resp.status_code == 400
    assert "warehouse_id" in str(resp.json()["detail"]).lower()
