"""Tests for src/demo_media.py.

Mirrors tests/test_demo_documents.py — per-type bytes verification,
preview math, orchestrator destination dispatch, ffmpeg-missing
graceful degradation.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.demo_media import (
    MEDIA_AVAILABLE,
    MEDIA_TYPES,
    _build_summary,
    _sql_str,
    is_available,
    preview_media,
)

pytestmark_if_unavailable = pytest.mark.skipif(
    not MEDIA_AVAILABLE,
    reason="media generation deps not installed (pip install clone-xs[media])",
)


def _split_uploads(upload_mock, *, sidecar: bool) -> list:
    """Partition `client.files.upload` calls into binary-media uploads
    vs `.txt` sidecar uploads. Phase B.5 introduced sidecars so the
    raw call_count doubled for every successfully generated file."""
    out = []
    for call in upload_mock.call_args_list:
        path = call.kwargs.get("file_path") or (call.args[0] if call.args else "")
        is_sidecar = str(path).endswith(".txt")
        if is_sidecar == sidecar:
            out.append(call)
    return out


# ── Registry ──────────────────────────────────────────────────────


def test_registry_contains_expected_types_and_categories():
    """The 5 v1 media types should cover Image / Audio / Video. Pin the
    registry shape so a future PR that drops a category trips this
    test instead of silently breaking the UI's checkbox grid."""
    expected = {"img_xray", "img_scan", "img_photo", "audio_voicemail", "video_clip"}
    assert set(MEDIA_TYPES) == expected
    categories = {t["category"] for t in MEDIA_TYPES.values()}
    assert categories == {"Image", "Audio", "Video"}


def test_is_available_reports_a_reason_when_unavailable():
    available, reason = is_available()
    if available:
        assert reason is None
    else:
        assert reason is not None
        assert "pip install" in reason


# ── Per-type generators (file-magic + metadata) ───────────────────


@pytestmark_if_unavailable
@pytest.mark.parametrize(
    "type_id,magic_or_subseq,magic_offset",
    [
        ("img_xray", b"\x89PNG", 0),
        ("img_scan", b"\x89PNG", 0),
        ("img_photo", b"\x89PNG", 0),
        ("audio_voicemail", b"RIFF", 0),
        # MP4 files start with a 4-byte length prefix, then 'ftyp'.
        ("video_clip", b"ftyp", 4),
    ],
)
def test_each_generator_emits_correct_file_magic(type_id, magic_or_subseq, magic_offset):
    """Per-type generators must emit bytes with the right file-magic
    prefix. video_clip is skipped automatically when ffmpeg isn't on
    PATH so this test runs cleanly on dev machines without the binary."""
    from faker import Faker
    from src import demo_media

    if type_id == "video_clip":
        ok, _ = demo_media._ffmpeg_available()
        if not ok:
            pytest.skip("ffmpeg not on PATH — video_clip generator can't run")

    fkr = Faker()
    fkr.seed_instance(42)
    fn_name = MEDIA_TYPES[type_id]["gen_fn"]
    fn = getattr(demo_media, fn_name)
    file_bytes, meta = fn("healthcare", fkr, None)
    assert isinstance(file_bytes, bytes)
    assert len(file_bytes) > 100, (
        f"{type_id} produced suspiciously small output ({len(file_bytes)} bytes)"
    )
    # MP4 magic lives at offset 4; PNG / RIFF at offset 0.
    assert file_bytes[magic_offset : magic_offset + len(magic_or_subseq)] == magic_or_subseq, (
        f"{type_id} bytes missing magic {magic_or_subseq!r} at offset {magic_offset}; "
        f"first 16 bytes: {file_bytes[:16]!r}"
    )
    assert isinstance(meta, dict)
    assert len(meta) > 0


@pytestmark_if_unavailable
def test_audio_voicemail_metadata_includes_transcript_and_caller():
    """Pin the schema of audio_voicemail metadata. RAG / NLP demos
    pair the WAV bytes with the transcript text — both must be present."""
    from faker import Faker
    from src.demo_media import _gen_audio_voicemail

    fkr = Faker()
    fkr.seed_instance(42)
    _, meta = _gen_audio_voicemail("healthcare", fkr, None)
    for key in (
        "caller_name",
        "caller_phone",
        "callee_name",
        "transcript",
        "duration_ms",
        "sample_rate_hz",
    ):
        assert key in meta, f"audio_voicemail metadata missing required key: {key}"
    assert meta["duration_ms"] > 0
    assert meta["sample_rate_hz"] == 22050
    assert len(meta["transcript"]) > 20


@pytestmark_if_unavailable
def test_img_xray_metadata_carries_view_and_age():
    """X-ray metadata feeds the catalog table's content_summary, so
    view + patient_age must be present for `WHERE view = 'AP chest'`
    queries to work in demos."""
    from faker import Faker
    from src.demo_media import _gen_img_xray

    fkr = Faker()
    fkr.seed_instance(42)
    _, meta = _gen_img_xray("healthcare", fkr, None)
    assert meta["modality"] == "X-ray"
    assert meta["view"] in ("AP chest", "PA chest", "Lateral chest")
    assert 18 <= meta["patient_age_years"] <= 88
    assert "synthetic" in meta["format"].lower()


# ── Preview arithmetic ────────────────────────────────────────────


def test_preview_returns_total_files_and_bytes():
    out = preview_media(
        {
            "types": ["img_xray", "audio_voicemail"],
            "counts": {"img_xray": 5, "audio_voicemail": 10},
        }
    )
    assert out["total_files"] == 15
    assert out["total_bytes"] > 0
    assert len(out["per_type"]) == 2
    assert out["unknown_types"] == []


def test_preview_flags_unknown_types_without_failing():
    out = preview_media(
        {
            "types": ["img_xray", "not_a_real_type"],
            "counts": {"img_xray": 3, "not_a_real_type": 5},
        }
    )
    assert out["total_files"] == 3
    assert "not_a_real_type" in out["unknown_types"]


def test_preview_defaults_count_to_5_for_listed_types_without_explicit_count():
    """Media defaults to 5 per type (lower than Documents' 10) because
    media generation is slower per file."""
    out = preview_media({"types": ["img_xray"], "counts": {}})
    assert out["total_files"] == 5


# ── Helpers ───────────────────────────────────────────────────────


def test_sql_str_doubles_single_quotes():
    assert _sql_str("Bob's voicemail") == "'Bob''s voicemail'"
    assert _sql_str(None) == "NULL"
    assert _sql_str("clean") == "'clean'"


def test_build_summary_falls_back_to_type_id_for_unknown():
    assert _build_summary("future_type", {}) == "future_type"


@pytestmark_if_unavailable
def test_build_summary_uses_type_specific_fields():
    summary = _build_summary(
        "audio_voicemail",
        {
            "caller_name": "Alice",
            "callee_name": "Bob",
        },
    )
    assert "Alice" in summary
    assert "Bob" in summary


# ── Orchestrator (destination dispatch) ───────────────────────────


@pytestmark_if_unavailable
@patch("src.demo_media.execute_sql")
def test_generate_media_volume_with_catalog_creates_catalog_table(mock_sql):
    """`destination=volume_with_catalog` creates the catalog table +
    uploads files + INSERTs metadata rows."""
    from src.demo_media import generate_media

    client = MagicMock()
    client.files.upload = MagicMock()
    config = {
        "catalog": "demo",
        "schema": "iot",
        "volume": "demo_unstructured",
        "destination": "volume_with_catalog",
        "types": ["img_xray"],
        "counts": {"img_xray": 3},
        "industry": "healthcare",
    }
    progress: dict = {}
    result = generate_media(client, "wh-1", config, progress=progress)

    # 3 media files + 3 .txt sidecars (one per media file) = 6 uploads.
    file_uploads = _split_uploads(client.files.upload, sidecar=False)
    sidecar_uploads = _split_uploads(client.files.upload, sidecar=True)
    assert len(file_uploads) == 3
    assert len(sidecar_uploads) == 3
    sqls = [c.args[2] for c in mock_sql.call_args_list]
    assert any("CREATE VOLUME IF NOT EXISTS" in s for s in sqls)
    assert any("CREATE OR REPLACE TABLE" in s and "demo_media_catalog" in s for s in sqls)
    assert any("INSERT INTO" in s for s in sqls)

    assert result["status"] == "completed"
    assert result["destination"] == "volume_with_catalog"
    assert result["files_written"] == 3
    assert result["table_fqn"] == "demo.iot.demo_media_catalog"
    assert progress["per_type"]["img_xray"] == 3


@pytestmark_if_unavailable
@patch("src.demo_media.execute_sql")
def test_generate_media_volume_only_skips_catalog_table(mock_sql):
    from src.demo_media import generate_media

    client = MagicMock()
    client.files.upload = MagicMock()
    config = {
        "catalog": "demo",
        "schema": "iot",
        "volume": "demo_unstructured",
        "destination": "volume",
        "types": ["img_xray"],
        "counts": {"img_xray": 2},
        "industry": "healthcare",
    }
    result = generate_media(client, "wh-1", config)
    # 2 media files + 2 .txt sidecars = 4 uploads.
    assert len(_split_uploads(client.files.upload, sidecar=False)) == 2
    assert len(_split_uploads(client.files.upload, sidecar=True)) == 2
    sqls = [c.args[2] for c in mock_sql.call_args_list]
    assert any("CREATE VOLUME IF NOT EXISTS" in s for s in sqls)
    assert not any("CREATE OR REPLACE TABLE" in s for s in sqls)
    assert not any("INSERT INTO" in s for s in sqls)
    assert result["table_fqn"] is None


@pytestmark_if_unavailable
@patch("src.demo_media.execute_sql")
def test_generate_media_direct_table_uses_inline_bytes(mock_sql):
    from src.demo_media import generate_media

    client = MagicMock()
    client.files.upload = MagicMock()
    config = {
        "catalog": "demo",
        "schema": "iot",
        "destination": "direct_table",
        "types": ["img_xray"],
        "counts": {"img_xray": 2},
        "industry": "healthcare",
    }
    result = generate_media(client, "wh-1", config)
    assert client.files.upload.call_count == 0
    sqls = [c.args[2] for c in mock_sql.call_args_list]
    assert not any("CREATE VOLUME" in s for s in sqls)
    assert any("CREATE OR REPLACE TABLE" in s and "demo_media" in s for s in sqls)
    insert_sqls = [s for s in sqls if "INSERT INTO" in s]
    assert len(insert_sqls) == 2
    assert all("unhex(" in s for s in insert_sqls)
    assert result["table_fqn"] == "demo.iot.demo_media"


@pytestmark_if_unavailable
def test_generate_media_rejects_unknown_destination():
    from src.demo_media import generate_media

    with pytest.raises(ValueError, match="Unknown destination"):
        generate_media(
            MagicMock(),
            "wh-1",
            {
                "catalog": "demo",
                "schema": "iot",
                "volume": "v",
                "destination": "made_up",
                "types": ["img_xray"],
            },
        )


@pytestmark_if_unavailable
def test_generate_media_rejects_empty_types():
    from src.demo_media import generate_media

    with pytest.raises(ValueError, match="at least one"):
        generate_media(
            MagicMock(),
            "wh-1",
            {
                "catalog": "demo",
                "schema": "iot",
                "volume": "v",
                "destination": "volume",
                "types": [],
            },
        )


@pytestmark_if_unavailable
def test_generate_media_rejects_unknown_type_in_request():
    from src.demo_media import generate_media

    with pytest.raises(ValueError, match="Unknown media types"):
        generate_media(
            MagicMock(),
            "wh-1",
            {
                "catalog": "demo",
                "schema": "iot",
                "volume": "v",
                "destination": "volume",
                "types": ["not_a_type"],
            },
        )


# ── ffmpeg-missing graceful degradation ───────────────────────────


@pytestmark_if_unavailable
@patch("src.demo_media._ffmpeg_available", return_value=(False, "ffmpeg not installed (test mock)"))
@patch("src.demo_media.execute_sql")
def test_generate_media_skips_video_clip_cleanly_when_ffmpeg_missing(mock_sql, _mock_ffmpeg):
    """When ffmpeg isn't on PATH, video_clip should be skipped with a
    clear per_type_failures entry — but other types in the same job
    must continue to completion. This is the failure-mode the user
    sees most often (Pillow installed, ffmpeg missing on macOS without
    Homebrew)."""
    from src.demo_media import generate_media

    client = MagicMock()
    client.files.upload = MagicMock()
    config = {
        "catalog": "demo",
        "schema": "iot",
        "volume": "v",
        "destination": "volume",
        "types": ["img_xray", "video_clip"],
        "counts": {"img_xray": 2, "video_clip": 5},
        "industry": "healthcare",
    }
    result = generate_media(client, "wh-1", config)

    # img_xray completed all 2; video_clip was skipped entirely.
    assert result["per_type"]["img_xray"] == 2
    assert result["per_type"]["video_clip"] == 0
    assert "video_clip" in result["per_type_failures"]
    assert "ffmpeg" in result["per_type_failures"]["video_clip"].lower()
    # The 2 img_xray uploads happened (+ 2 sidecars) — the ffmpeg miss
    # didn't block.
    assert len(_split_uploads(client.files.upload, sidecar=False)) == 2
    assert len(_split_uploads(client.files.upload, sidecar=True)) == 2


@pytestmark_if_unavailable
@patch("src.demo_media.execute_sql")
def test_generate_media_stop_check_aborts_loop(mock_sql):
    from src.demo_media import generate_media

    client = MagicMock()
    client.files.upload = MagicMock()
    state = {"calls": 0}

    def stop():
        state["calls"] += 1
        return state["calls"] > 1

    config = {
        "catalog": "demo",
        "schema": "iot",
        "volume": "v",
        "destination": "volume",
        "types": ["img_xray"],
        "counts": {"img_xray": 100},
        "industry": "healthcare",
    }
    result = generate_media(client, "wh-1", config, stop_check=stop)
    assert client.files.upload.call_count < 100
    assert result["files_written"] < 100
