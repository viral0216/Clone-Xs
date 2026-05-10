"""Unit tests for `src.demo_capture` — Live Capture orchestrator.

Mocks the Databricks SDK + execute_sql so we can verify:
  * Volume + table create idempotency (CREATE IF NOT EXISTS, not REPLACE)
  * Per-frame upload path (file_path layout, BINARY embedding)
  * AI captioning gate (off by default, on when realistic_content=True)
  * `list_recent` SELECT shape + degrades to empty list on missing table

The tests deliberately avoid the FastAPI router layer — the multipart
plumbing there is covered by manual smoke tests against a real
warehouse. The orchestrator is the integration risk.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.demo_capture import (
    handle_frame,
    init_capture_target,
    list_recent,
)


def _make_client() -> MagicMock:
    client = MagicMock()
    client.files.upload = MagicMock()
    return client


# ── /init equivalent ──────────────────────────────────────────────


@patch("src.demo_capture.execute_sql")
def test_init_creates_volume_and_table_if_not_exists(mock_sql):
    """Calling init twice in a row issues idempotent CREATE IF NOT
    EXISTS statements. Critical because the synthetic-tab pattern
    uses CREATE OR REPLACE which would discard prior captures."""
    out = init_capture_target(
        _make_client(),
        "wh-1",
        {"catalog": "demo", "schema": "iot", "volume": "v"},
    )
    sqls = [c.args[2] for c in mock_sql.call_args_list]
    assert any("CREATE VOLUME IF NOT EXISTS demo.iot.v" in s for s in sqls)
    assert any("CREATE TABLE IF NOT EXISTS demo.iot.demo_capture_catalog" in s for s in sqls)
    # Schema includes the user's explicit ask: file_path AND content BINARY.
    create_sql = next(s for s in sqls if "CREATE TABLE IF NOT EXISTS" in s)
    assert "file_path" in create_sql
    assert "content          BINARY" in create_sql or "content BINARY" in create_sql
    assert out["table_fqn"] == "demo.iot.demo_capture_catalog"
    assert out["volume_path"] == "/Volumes/demo/iot/v/capture"


@patch("src.demo_capture.execute_sql")
def test_init_honours_custom_table_name(mock_sql):
    out = init_capture_target(
        _make_client(),
        "wh-1",
        {
            "catalog": "demo",
            "schema": "iot",
            "volume": "v",
            "table_name": "team_alpha_capture",
        },
    )
    sqls = [c.args[2] for c in mock_sql.call_args_list]
    assert any("demo.iot.team_alpha_capture" in s for s in sqls)
    assert out["table_fqn"] == "demo.iot.team_alpha_capture"


# ── /frame equivalent ─────────────────────────────────────────────


@patch("src.demo_capture.execute_sql")
def test_handle_frame_uploads_to_volume_and_inserts_row(mock_sql):
    """Photo path: bytes go to the Volume AND the row carries inline
    BINARY via unhex('<hex>'). The user explicitly asked for both."""
    client = _make_client()
    file_bytes = b"\xff\xd8\xff\xe0fake-jpeg" + b"X" * 100  # 109 bytes total
    row = handle_frame(
        client,
        "wh-1",
        file_bytes=file_bytes,
        capture_type="photo",
        mime_type="image/jpeg",
        width=640,
        height=480,
        duration_ms=None,
        industry="healthcare",
        config={"catalog": "demo", "schema": "iot", "volume": "v"},
    )

    # Volume upload happened with the right shape.
    assert client.files.upload.call_count == 1
    upload_path = client.files.upload.call_args.kwargs.get("file_path")
    assert upload_path is not None
    assert upload_path.startswith("/Volumes/demo/iot/v/capture/photo/")
    assert upload_path.endswith(".jpg")

    # INSERT carries the bytes inline as BINARY.
    insert_sqls = [c.args[2] for c in mock_sql.call_args_list if "INSERT INTO" in c.args[2]]
    assert len(insert_sqls) == 1
    insert_sql = insert_sqls[0]
    assert f"unhex('{file_bytes.hex()}')" in insert_sql
    # Row dimensions land as integers, not strings.
    assert "640" in insert_sql
    assert "480" in insert_sql
    # Photo: duration_ms is NULL (not 0).
    assert "NULL" in insert_sql

    # Returned row matches what the UI will render.
    assert row["capture_type"] == "photo"
    assert row["size_bytes"] == len(file_bytes)
    assert row["file_path"] == upload_path
    assert row["table_fqn"] == "demo.iot.demo_capture_catalog"
    assert row["caption"]  # fallback caption when AI is off


@patch("src.demo_capture.execute_sql")
def test_handle_frame_video_chunk_carries_duration(mock_sql):
    """Video path: webm extension by default, duration_ms surfaces."""
    client = _make_client()
    row = handle_frame(
        client,
        "wh-1",
        file_bytes=b"\x1a\x45\xdf\xa3fake-webm" * 50,
        capture_type="video",
        mime_type="video/webm",
        width=1280,
        height=720,
        duration_ms=5_000,
        industry="financial",
        config={"catalog": "demo", "schema": "iot", "volume": "v"},
    )
    assert row["capture_type"] == "video"
    assert row["duration_ms"] == 5_000
    assert row["file_path"].endswith(".webm")
    insert_sql = next(c.args[2] for c in mock_sql.call_args_list if "INSERT INTO" in c.args[2])
    assert "5000" in insert_sql


@patch("src.demo_capture.execute_sql")
def test_handle_frame_falls_back_to_jpg_when_mime_unknown(mock_sql):
    client = _make_client()
    row = handle_frame(
        client,
        "wh-1",
        file_bytes=b"x" * 100,
        capture_type="photo",
        mime_type=None,
        width=None,
        height=None,
        duration_ms=None,
        industry="retail",
        config={"catalog": "demo", "schema": "iot", "volume": "v"},
    )
    assert row["file_extension"] == "jpg"


@patch("src.demo_capture.execute_sql")
def test_handle_frame_rejects_empty_bytes(mock_sql):
    import pytest

    with pytest.raises(ValueError, match="empty"):
        handle_frame(
            _make_client(),
            "wh-1",
            file_bytes=b"",
            capture_type="photo",
            mime_type="image/jpeg",
            width=640,
            height=480,
            duration_ms=None,
            industry="healthcare",
            config={"catalog": "demo", "schema": "iot", "volume": "v"},
        )


@patch("src.demo_capture.execute_sql")
def test_handle_frame_rejects_unknown_capture_type(mock_sql):
    import pytest

    with pytest.raises(ValueError, match="capture_type"):
        handle_frame(
            _make_client(),
            "wh-1",
            file_bytes=b"x" * 10,
            capture_type="audio",  # not photo/video
            mime_type="audio/webm",
            width=None,
            height=None,
            duration_ms=None,
            industry="healthcare",
            config={"catalog": "demo", "schema": "iot", "volume": "v"},
        )


# ── AI captioning gate ────────────────────────────────────────────


@patch("src.demo_capture.execute_sql")
def test_handle_frame_skips_ai_when_realistic_content_off(mock_sql):
    """build_drafter() returns None when realistic_content is False —
    `_maybe_ai` should fall back to the templated caption without
    touching the LLM service."""
    with patch("src.ai_drafter.build_drafter") as mock_drafter:
        mock_drafter.return_value = None
        row = handle_frame(
            _make_client(),
            "wh-1",
            file_bytes=b"x" * 100,
            capture_type="photo",
            mime_type="image/jpeg",
            width=640,
            height=480,
            duration_ms=None,
            industry="healthcare",
            config={
                "catalog": "demo",
                "schema": "iot",
                "volume": "v",
                "realistic_content": False,
            },
        )
    # Fallback caption format: "Live photo from healthcare demo workstation at HH:MM"
    assert "Live photo" in row["caption"]
    assert "healthcare" in row["caption"]


# ── /recent equivalent ────────────────────────────────────────────


@patch("src.demo_capture.execute_sql")
def test_list_recent_returns_rows_sorted_by_captured_at(mock_sql):
    mock_sql.return_value = [
        {
            "capture_id": "abc",
            "capture_type": "photo",
            "file_path": "/Volumes/demo/iot/v/capture/photo/2026-05-10/photo_abc.jpg",
            "file_extension": "jpg",
            "size_bytes": 12345,
            "width": 640,
            "height": 480,
            "duration_ms": None,
            "mime_type": "image/jpeg",
            "industry": "healthcare",
            "caption": "Test caption",
            "alt_text": "Test alt",
            "captured_at": "2026-05-10T14:23:00",
        }
    ]
    rows = list_recent(_make_client(), "wh-1", catalog="demo", schema="iot", limit=5)
    assert len(rows) == 1
    assert rows[0]["capture_id"] == "abc"
    sql = mock_sql.call_args.args[2]
    assert "ORDER BY captured_at DESC" in sql
    assert "LIMIT 5" in sql
    # The recent SELECT must NOT pull the inline BINARY content — the
    # response stays small even with thousands of captures.
    assert "content," not in sql
    assert "content " not in sql.split("FROM")[0]


@patch("src.demo_capture.execute_sql")
def test_list_recent_returns_empty_on_missing_table(mock_sql):
    mock_sql.side_effect = Exception("Table or view not found: demo.iot.demo_capture_catalog")
    rows = list_recent(_make_client(), "wh-1", catalog="demo", schema="iot")
    assert rows == []


@patch("src.demo_capture.execute_sql")
def test_list_recent_filters_by_session_id(mock_sql):
    """When session_id is passed, the SELECT carries a WHERE clause so
    concurrent users don't see each other's captures."""
    mock_sql.return_value = []
    list_recent(
        _make_client(),
        "wh-1",
        catalog="demo",
        schema="iot",
        session_id="session-abc-123",
    )
    sql = mock_sql.call_args.args[2]
    assert "WHERE session_id = 'session-abc-123'" in sql
    assert "ORDER BY captured_at DESC" in sql


# ── #3 session_id + submitted_by ──────────────────────────────────


@patch("src.demo_capture.execute_sql")
def test_handle_frame_persists_session_id_and_user(mock_sql):
    """session_id from the UI + submitted_by resolved from current_user
    both land in the row."""
    client = _make_client()
    me = MagicMock()
    me.user_name = "data_engineering_0216@yahoo.com"
    me.display_name = "Viral Patel"
    client.current_user.me.return_value = me

    row = handle_frame(
        client,
        "wh-1",
        file_bytes=b"\xff\xd8\xff" + b"x" * 50,
        capture_type="photo",
        mime_type="image/jpeg",
        width=640,
        height=480,
        duration_ms=None,
        industry="healthcare",
        config={"catalog": "demo", "schema": "iot", "volume": "v"},
        session_id="tab-xyz",
    )
    insert_sql = next(c.args[2] for c in mock_sql.call_args_list if "INSERT INTO" in c.args[2])
    assert "'tab-xyz'" in insert_sql
    assert "'data_engineering_0216@yahoo.com'" in insert_sql
    assert row["session_id"] == "tab-xyz"
    assert row["submitted_by"] == "data_engineering_0216@yahoo.com"


@patch("src.demo_capture.execute_sql")
def test_handle_frame_degrades_when_current_user_fails(mock_sql):
    """If current_user.me() raises (auth scope, network), the capture
    still lands but submitted_by is NULL — never block a capture on
    user lookup."""
    client = _make_client()
    client.current_user.me.side_effect = Exception("Unauthorized")
    row = handle_frame(
        client,
        "wh-1",
        file_bytes=b"x" * 50,
        capture_type="photo",
        mime_type="image/jpeg",
        width=640,
        height=480,
        duration_ms=None,
        industry="healthcare",
        config={"catalog": "demo", "schema": "iot", "volume": "v"},
    )
    assert row["submitted_by"] is None
    insert_sql = next(c.args[2] for c in mock_sql.call_args_list if "INSERT INTO" in c.args[2])
    # submitted_by literal renders as NULL when no email could be resolved
    assert "NULL" in insert_sql


# ── #1 image-grounded consolidated metadata ──────────────────────


_AI_JSON_RESPONSE = (
    '{"caption": "Operator at workstation",'
    ' "alt_text": "Photo of a person at a desk with monitors",'
    ' "summary": "An operator works at a healthcare workstation in afternoon light. '
    'Two monitors display patient charts. The room appears clean and focused.",'
    ' "tags": ["operator", "workstation", "monitors", "healthcare", "indoor"],'
    ' "detected_text": "PATIENT 042",'
    ' "scene_category": "office"}'
)


@patch("src.demo_capture.execute_sql")
def test_handle_frame_passes_image_bytes_for_photo_caption(mock_sql):
    """Photo + JPEG mime + AI on → the multimodal call gets the bytes
    (vision input). Consolidated JSON response populates all six AI
    fields in one call."""
    captured: dict = {}

    def fake_draft(prompt, fallback, max_tokens=200, *, image_bytes=None, image_mime=None):
        captured["image_bytes"] = image_bytes
        captured["image_mime"] = image_mime
        captured["prompt"] = prompt
        return _AI_JSON_RESPONSE

    drafter = MagicMock()
    drafter.draft.side_effect = fake_draft

    with patch("src.demo_capture.build_drafter", return_value=drafter):
        row = handle_frame(
            _make_client(),
            "wh-1",
            file_bytes=b"\xff\xd8\xff\xe0fakejpeg",
            capture_type="photo",
            mime_type="image/jpeg",
            width=640,
            height=480,
            duration_ms=None,
            industry="healthcare",
            config={
                "catalog": "demo",
                "schema": "iot",
                "volume": "v",
                "realistic_content": True,
            },
        )
    # Bytes were forwarded to the vision endpoint.
    assert captured["image_bytes"] == b"\xff\xd8\xff\xe0fakejpeg"
    assert captured["image_mime"] == "image/jpeg"
    # Exactly one consolidated AI call — not six per-field calls.
    assert drafter.draft.call_count == 1
    # All six fields parsed from the JSON response.
    assert row["caption"] == "Operator at workstation"
    assert row["alt_text"] == "Photo of a person at a desk with monitors"
    assert "operator works at a healthcare workstation" in row["summary"]
    assert row["tags"] == "operator,workstation,monitors,healthcare,indoor"
    assert row["detected_text"] == "PATIENT 042"
    assert row["scene_category"] == "office"


@patch("src.demo_capture.execute_sql")
def test_handle_frame_does_not_send_image_bytes_for_video_chunk(mock_sql):
    """Video chunks (webm) are NOT sent as image input — Llama 4
    accepts images, not video. Visual-only fields (detected_text,
    scene_category) are blanked because the model didn't see the
    image."""
    captured: dict = {}

    def fake_draft(prompt, fallback, max_tokens=200, *, image_bytes=None, image_mime=None):
        captured["image_bytes"] = image_bytes
        # Even if the model hallucinates visual fields, the post-processor
        # drops them on the text-grounded path.
        return (
            '{"caption": "trader at desk", "alt_text": "trader at desk in suit",'
            ' "summary": "A trading floor scene with monitors and a person.",'
            ' "tags": ["trading", "floor", "monitors"],'
            ' "detected_text": "BLOOMBERG", "scene_category": "office"}'
        )

    drafter = MagicMock()
    drafter.draft.side_effect = fake_draft

    with patch("src.demo_capture.build_drafter", return_value=drafter):
        row = handle_frame(
            _make_client(),
            "wh-1",
            file_bytes=b"\x1a\x45\xdf\xa3fake-webm" * 20,
            capture_type="video",
            mime_type="video/webm",
            width=1280,
            height=720,
            duration_ms=5_000,
            industry="financial",
            config={
                "catalog": "demo",
                "schema": "iot",
                "volume": "v",
                "realistic_content": True,
            },
        )
    # Video chunk → image_bytes must be None (we don't send video to
    # the vision endpoint).
    assert captured["image_bytes"] is None
    # Visual-only fields are blanked / forced to "unknown" on the
    # text-grounded path so SQL aggregates aren't polluted.
    assert row["detected_text"] == ""
    assert row["scene_category"] == "unknown"
    # The non-visual fields can come from the JSON.
    assert row["caption"] == "trader at desk"
    assert row["tags"] == "trading,floor,monitors"


@patch("src.demo_capture.execute_sql")
def test_handle_frame_falls_back_when_ai_returns_malformed_json(mock_sql):
    """If the model returns prose / broken JSON, every AI field falls
    back to the templated string — no crash, row still inserts."""
    drafter = MagicMock()
    drafter.draft.return_value = "Here is the description: a person at a desk."

    with patch("src.demo_capture.build_drafter", return_value=drafter):
        row = handle_frame(
            _make_client(),
            "wh-1",
            file_bytes=b"\xff\xd8\xff\xe0fakejpeg",
            capture_type="photo",
            mime_type="image/jpeg",
            width=640,
            height=480,
            duration_ms=None,
            industry="retail",
            config={
                "catalog": "demo",
                "schema": "iot",
                "volume": "v",
                "realistic_content": True,
            },
        )
    # Templated fallback caption format.
    assert "Live photo" in row["caption"]
    assert "retail" in row["caption"]
    # Tags and scene_category land on their templated defaults.
    assert row["tags"] == "retail,photo,webcam,demo"
    assert row["scene_category"] == "unknown"


@patch("src.demo_capture.execute_sql")
def test_strict_mode_prompt_omits_industry_and_demographic_priming(mock_sql):
    """Strict mode (default) must NOT include the industry name or
    demographic-priming language in the image-grounded prompt. This
    is the fix for the 'man at desk in healthcare → labelled nurse'
    bug. The industry still lands on the row column for filtering."""
    captured: dict = {}

    def fake_draft(prompt, fallback, max_tokens=200, *, image_bytes=None, image_mime=None):
        captured["prompt"] = prompt
        return _AI_JSON_RESPONSE

    drafter = MagicMock()
    drafter.draft.side_effect = fake_draft

    with patch("src.demo_capture.build_drafter", return_value=drafter):
        handle_frame(
            _make_client(),
            "wh-1",
            file_bytes=b"\xff\xd8\xff\xe0fakejpeg",
            capture_type="photo",
            mime_type="image/jpeg",
            width=640,
            height=480,
            duration_ms=None,
            industry="healthcare",
            config={
                "catalog": "demo",
                "schema": "iot",
                "volume": "v",
                "realistic_content": True,
            },
            # description_style defaults to "strict"
        )
    p = captured["prompt"]
    # Industry name must not appear in the strict image-grounded prompt.
    assert "healthcare" not in p
    # Demographic-prohibition language must be present.
    assert "gender" in p
    assert "profession" in p
    assert "'a person'" in p


@patch("src.demo_capture.execute_sql")
def test_permissive_mode_prompt_includes_industry_priming(mock_sql):
    """Permissive mode re-enables industry priming and lifts the
    demographic prohibition. Caller has accepted the bias risk."""
    captured: dict = {}

    def fake_draft(prompt, fallback, max_tokens=200, *, image_bytes=None, image_mime=None):
        captured["prompt"] = prompt
        return _AI_JSON_RESPONSE

    drafter = MagicMock()
    drafter.draft.side_effect = fake_draft

    with patch("src.demo_capture.build_drafter", return_value=drafter):
        handle_frame(
            _make_client(),
            "wh-1",
            file_bytes=b"\xff\xd8\xff\xe0fakejpeg",
            capture_type="photo",
            mime_type="image/jpeg",
            width=640,
            height=480,
            duration_ms=None,
            industry="healthcare",
            config={
                "catalog": "demo",
                "schema": "iot",
                "volume": "v",
                "realistic_content": True,
            },
            description_style="permissive",
        )
    p = captured["prompt"]
    # Industry priming is back on.
    assert "healthcare" in p
    # No "Do not infer gender" prohibition.
    assert "Do not infer or assert" not in p


@patch("src.demo_capture.execute_sql")
def test_handle_frame_clamps_unknown_description_style_to_strict(mock_sql):
    """Defence-in-depth: an unknown style string from the wire (typo,
    enum drift) falls back to strict — never silently re-enables the
    bias-prone permissive prompt."""
    captured: dict = {}

    def fake_draft(prompt, fallback, max_tokens=200, *, image_bytes=None, image_mime=None):
        captured["prompt"] = prompt
        return _AI_JSON_RESPONSE

    drafter = MagicMock()
    drafter.draft.side_effect = fake_draft

    with patch("src.demo_capture.build_drafter", return_value=drafter):
        handle_frame(
            _make_client(),
            "wh-1",
            file_bytes=b"\xff\xd8\xff\xe0fakejpeg",
            capture_type="photo",
            mime_type="image/jpeg",
            width=640,
            height=480,
            duration_ms=None,
            industry="healthcare",
            config={
                "catalog": "demo",
                "schema": "iot",
                "volume": "v",
                "realistic_content": True,
            },
            description_style="bogus_value",
        )
    p = captured["prompt"]
    # Strict prompt landed: no industry, demographic prohibition present.
    assert "healthcare" not in p
    assert "gender" in p


@patch("src.demo_capture.execute_sql")
def test_handle_frame_persists_new_ai_fields_to_insert(mock_sql):
    """The INSERT carries every AI-derived column, not just caption +
    alt_text. Schema additions must be wired through to SQL or rows
    will land with NULL in the new columns."""
    drafter = MagicMock()
    drafter.draft.return_value = _AI_JSON_RESPONSE

    with patch("src.demo_capture.build_drafter", return_value=drafter):
        handle_frame(
            _make_client(),
            "wh-1",
            file_bytes=b"\xff\xd8\xff\xe0fakejpeg",
            capture_type="photo",
            mime_type="image/jpeg",
            width=640,
            height=480,
            duration_ms=None,
            industry="healthcare",
            config={
                "catalog": "demo",
                "schema": "iot",
                "volume": "v",
                "realistic_content": True,
            },
        )
    insert_sql = next(c.args[2] for c in mock_sql.call_args_list if "INSERT INTO" in c.args[2])
    # Column list mentions every new AI field.
    assert "summary" in insert_sql
    assert "tags" in insert_sql
    assert "detected_text" in insert_sql
    assert "scene_category" in insert_sql
    # Values are the parsed JSON, not the templated fallbacks.
    assert "'Operator at workstation'" in insert_sql
    assert "'office'" in insert_sql
    assert "'PATIENT 042'" in insert_sql
