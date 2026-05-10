"""Live webcam capture → UC Volume + Delta catalog with inline BINARY.

Pairs with `src.demo_media` but inverts the data flow: instead of a
synthetic generator on the server building bytes from Pillow / ffmpeg,
the bytes arrive from the user's browser webcam (one HTTP multipart
request per snapshot or video chunk). Each capture is processed
synchronously — uploaded to the Volume and INSERTed into a single
indexed catalog table with the bytes embedded inline as a `BINARY`
column.

Why a single combined-shape table:

  - Every capture row has both ``file_path`` (Volume pointer for
    browsable / downloadable bytes) AND ``content BINARY`` (inline
    bytes for SQL-only RAG demos that don't want to round-trip the
    Volume). The user explicitly asked for both.
  - The table is created with ``CREATE TABLE IF NOT EXISTS`` rather
    than ``CREATE OR REPLACE`` (the synthetic tabs use the latter)
    because captures accumulate across browser sessions; recreating
    on every request would discard prior captures.

Architecture is deliberately simpler than `src.demo_media`:

  - No JobManager / no progress polling — each ``handle_frame()`` call
    is one synchronous unit of work that the router invokes directly
    from the request handler.
  - No batching — rows go in one at a time so the user sees them in
    the table as soon as the upload returns.
  - AI captioning reuses the shared `_maybe_ai` helper from
    ``src.ai_drafter``; Phase 1 is text-grounded (metadata only),
    image-grounded captioning is Phase 2.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from databricks.sdk import WorkspaceClient

from src.ai_drafter import build_drafter, maybe_ai_json as _maybe_ai_json
from src.client import execute_sql

logger = logging.getLogger(__name__)


# Default extensions per (capture_type, mime_type) — used when the
# browser doesn't send a recognised mime, or for the volume path
# suffix. Kept narrow on purpose: photos are JPEG (browser canvas
# default) and videos are WebM (MediaRecorder default on Chrome /
# Firefox); MP4 is the Safari fallback.
_DEFAULT_EXTENSIONS: dict[str, str] = {
    "image/jpeg": "jpg",
    "image/jpg": "jpg",
    "image/png": "png",
    "image/webp": "webp",
    "video/webm": "webm",
    "video/mp4": "mp4",
    "audio/webm": "webm",
}


def _ext_for(mime_type: str | None, capture_type: str) -> str:
    if mime_type and mime_type.lower() in _DEFAULT_EXTENSIONS:
        return _DEFAULT_EXTENSIONS[mime_type.lower()]
    # Sensible fall-backs when the browser sends a bare or unknown mime.
    return "webm" if capture_type == "video" else "jpg"


def _sql_str(s: str | None) -> str:
    """Single-quote escape for inline INSERT VALUES."""
    if s is None:
        return "NULL"
    return "'" + s.replace("'", "''") + "'"


def _ensure_volume(client: WorkspaceClient, warehouse_id: str, vol_fqn: str) -> None:
    """Idempotent volume create. Same shape as the synthetic tabs."""
    execute_sql(client, warehouse_id, f"CREATE VOLUME IF NOT EXISTS {vol_fqn}")


def _ensure_capture_table(
    client: WorkspaceClient,
    warehouse_id: str,
    table_fqn: str,
) -> None:
    """Create-IF-NOT-EXISTS the capture catalog table.

    Schema combines the synthetic Media tab's volume_with_catalog row
    shape with the direct_table inline-bytes column. Every capture row
    has both ``file_path`` and ``content BINARY``.
    """
    sql = f"""
    CREATE TABLE IF NOT EXISTS {table_fqn} (
        capture_id       STRING,
        capture_type     STRING,
        file_path        STRING,
        file_extension   STRING,
        size_bytes       BIGINT,
        width            INT,
        height           INT,
        duration_ms      BIGINT,
        mime_type        STRING,
        industry         STRING,
        caption          STRING,
        alt_text         STRING,
        summary          STRING,
        tags             STRING,
        detected_text    STRING,
        scene_category   STRING,
        content_full     STRING,
        captured_at      TIMESTAMP,
        session_id       STRING,
        submitted_by     STRING,
        content          BINARY,
        metadata_json    STRING
    ) USING delta
    """
    execute_sql(client, warehouse_id, sql)
    # Best-effort additive migration for tables created before the
    # newer columns existed. ALTER ADD COLUMN IF NOT EXISTS is a no-op
    # when the column is already present, and never fails on a fresh
    # table because the CREATE above carries every column. We swallow
    # exceptions so first-call failure against an old table doesn't
    # block new captures.
    for col in (
        "session_id STRING",
        "submitted_by STRING",
        "summary STRING",
        "tags STRING",
        "detected_text STRING",
        "scene_category STRING",
    ):
        try:
            execute_sql(
                client,
                warehouse_id,
                f"ALTER TABLE {table_fqn} ADD COLUMN IF NOT EXISTS {col}",
            )
        except Exception as e:
            # Bumped from debug to warning so a genuine migration
            # failure (e.g. ALTER not supported on this warehouse,
            # permission denied) shows up in the API log instead of
            # silently leading to "column not found" on the INSERT.
            logger.warning("ALTER ADD COLUMN %s on %s failed: %s", col, table_fqn, e)


def init_capture_target(
    client: WorkspaceClient,
    warehouse_id: str,
    config: dict,
) -> dict:
    """Idempotent: ensure the volume and catalog table exist. Called
    from the UI when the user opens the Live Capture tab so the first
    `/frame` upload doesn't pay the create-volume + create-table cost.

    Returns ``{"volume_path": ..., "table_fqn": ...}`` so the UI can
    surface where captures will land before recording starts.
    """
    catalog = config["catalog"]
    schema = config["schema"]
    volume = config.get("volume") or "demo_unstructured"
    table_name = (config.get("table_name") or "").strip() or "demo_capture_catalog"

    vol_fqn = f"{catalog}.{schema}.{volume}"
    _ensure_volume(client, warehouse_id, vol_fqn)

    table_fqn = f"{catalog}.{schema}.{table_name}"
    _ensure_capture_table(client, warehouse_id, table_fqn)

    volume_path = f"/Volumes/{catalog}/{schema}/{volume}/capture"
    return {"volume_path": volume_path, "table_fqn": table_fqn}


_IMAGE_MIMES_FOR_VISION = ("image/jpeg", "image/jpg", "image/png", "image/webp")


def _is_image_describable(capture_type: str, mime_type: str | None) -> bool:
    """Only photo-shaped captures with an inline-image mime can be
    sent to the multimodal endpoint. Video chunks (webm / mp4) are
    out of scope — Llama 4 Maverick accepts images, not video."""
    if capture_type != "photo":
        return False
    if not mime_type:
        return False
    return mime_type.lower() in _IMAGE_MIMES_FOR_VISION


def _metadata_fallbacks(capture_type: str, industry: str, dims: str, ts: str) -> dict[str, str]:
    """Templated values used when AI mode is off or the call fails."""
    return {
        "caption": f"Live {capture_type} from {industry} demo workstation at {ts}",
        "alt_text": f"Webcam {capture_type} ({dims}) for {industry} demo, captured at {ts}",
        "summary": (
            f"A live {capture_type} captured in a {industry} setting at {ts}. "
            f"Frame size {dims}; visual content not analyzed (AI mode off)."
        ),
        "tags": f"{industry},{capture_type},webcam,demo",
        "detected_text": "",
        "scene_category": "unknown",
    }


DESCRIPTION_STYLES: tuple[str, ...] = ("strict", "permissive")


def _strict_image_prompt() -> str:
    """Industry-neutral, demographics-neutral prompt. Vision models
    are unreliable at gender / age / ethnicity / occupation from a
    single low-res webcam frame, and asserting those traits in
    alt-text is poor accessibility practice. The model is constrained
    to describe only directly-observable features."""
    return (
        "Describe ONLY what is actually visible in this image. Do not "
        "infer or assert any of: gender, age, ethnicity, profession, "
        "role, name, mood, intent, location, or industry. If a person "
        "is visible, refer to them as 'a person' (or 'two people', "
        "etc.) and describe only directly-observable features — "
        "clothing colour and type, hair colour and length, eyewear, "
        "posture, and the action they appear to be performing. Do "
        "not guess what they do for a living. Return ONLY a JSON "
        "object with these exact keys (no markdown, no prose, no "
        "code fences):\n"
        "{\n"
        '  "caption": "1 sentence stating what is visible, max 14 '
        'words. No demographic or profession claims",\n'
        '  "alt_text": "1 sentence accessibility description of '
        "visible content, max 18 words. Use 'a person' not 'a man' "
        "or 'a woman'\",\n"
        '  "summary": "2-3 sentences describing visible elements, '
        "lighting, objects, and the action being performed — no "
        'demographic or occupational claims",\n'
        '  "tags": ["5 to 8 single-word visual keywords for objects, '
        "colours, or settings actually visible — no profession or "
        'demographic words"],\n'
        '  "detected_text": "any text visible in the image (signs, '
        'screens, whiteboards), or empty string if none readable",\n'
        '  "scene_category": "1-2 word category for the visible '
        "setting like office, lab, outdoor, vehicle, kitchen, "
        'bedroom, etc"\n'
        "}"
    )


def _permissive_image_prompt(industry: str) -> str:
    """Vivid-description prompt. The model is allowed to describe
    visible gender presentation, apparent profession, and industry
    context when the scene supports it. Still grounded in what is
    visible — the model is told to say so when it is unsure rather
    than confabulate. Caller has accepted the bias risk by selecting
    Permissive mode."""
    return (
        f"Describe what is visible in this webcam image. The scene is "
        f"from a {industry} setting; if the visible content supports "
        f"that context (people, objects, setting consistent with "
        f"{industry}), you may reflect it in your description. You "
        f"may describe a person's apparent gender presentation, "
        f"clothing, posture, and what they appear to be doing. Where "
        f"you are uncertain, prefer neutral phrasing ('a person') over "
        f"a confident guess. Do not invent objects or text that are "
        f"not visible. Return ONLY a JSON object with these exact "
        f"keys (no markdown, no prose, no code fences):\n"
        f"{{\n"
        f'  "caption": "1 sentence describing what is visible, max '
        f'14 words",\n'
        f'  "alt_text": "1 sentence accessibility description, max '
        f'18 words",\n'
        f'  "summary": "2-3 sentences with visible scene context, '
        f'objects, lighting, and the action taking place",\n'
        f'  "tags": ["5 to 8 single-word visual keywords"],\n'
        f'  "detected_text": "any text visible in the image (signs, '
        f'screens, whiteboards), or empty string if none readable",\n'
        f'  "scene_category": "1-2 word category for the visible '
        f"setting like office, lab, outdoor, vehicle, kitchen, "
        f'bedroom, etc"\n'
        f"}}"
    )


def _metadata_prompt(
    *,
    image_grounded: bool,
    capture_type: str,
    industry: str,
    dims: str,
    ts: str,
    description_style: str,
) -> str:
    """Build the JSON-instructing prompt.

    Image-grounded path supports two styles:

      * ``strict`` (default) — industry-neutral, demographics-neutral.
        Forces the model to describe only what is directly visible.
        Best for accessibility / data-quality demos and for avoiding
        gender / occupation misidentification.
      * ``permissive`` — vivid description. Allows industry priming
        and apparent demographic / occupation language. Caller has
        accepted the bias risk.

    Text-grounded path (no image bytes — videos / unsupported mimes)
    always uses industry, because the model has nothing else to go on.
    """
    if image_grounded:
        if description_style == "permissive":
            return _permissive_image_prompt(industry)
        return _strict_image_prompt()
    return (
        f"Generate plausible metadata for a webcam {capture_type} "
        f"({dims}) captured in a {industry} setting at {ts}. The image "
        f"itself is not available. Return ONLY a JSON object with these "
        f"exact keys (no markdown, no prose, no code fences):\n"
        f"{{\n"
        f'  "caption": "1 sentence plausible caption, max 14 words",\n'
        f'  "alt_text": "1 sentence accessibility description, max 18 words",\n'
        f'  "summary": "2-3 sentences describing a plausible scene for this industry",\n'
        f'  "tags": ["5 to 8 single-word keywords likely to apply"]\n'
        f"}}"
    )


def _coerce_tags(raw: Any, fallback: str) -> str:
    """Normalise the model's tags output to a comma-joined string.
    The model may return a list (preferred), a comma string, or
    something unexpected; coerce or fall back."""
    if isinstance(raw, list):
        joined = ",".join(str(t).strip() for t in raw if str(t).strip())
    elif isinstance(raw, str):
        joined = ",".join(p.strip() for p in raw.split(",") if p.strip())
    else:
        joined = ""
    return joined or fallback


def _draft_image_metadata(
    ai_client: Any | None,
    *,
    capture_type: str,
    industry: str,
    width: int | None,
    height: int | None,
    captured_at: datetime,
    image_bytes: bytes | None = None,
    image_mime: str | None = None,
    description_style: str = "strict",
) -> dict[str, str]:
    """Six AI-derived fields per capture in **one** multimodal call.

    Returns a dict with keys: ``caption``, ``alt_text``, ``summary``,
    ``tags``, ``detected_text``, ``scene_category``. ``tags`` is
    comma-joined from the model's array; the rest are plain strings.

    When ``image_bytes`` is supplied AND the capture is a still image
    AND an AI backend is wired, the bytes are sent inline as a base64
    data URL so the model can ground all six fields in the actual scene.
    Otherwise we send a metadata-only prompt and the visual-only fields
    (``detected_text``, ``scene_category``) get sensible empty defaults.

    Every field falls back to a templated string when ``ai_client`` is
    None (no AI backend configured) or the JSON call fails to parse.
    """
    dims = f"{width}x{height}" if width and height else "unknown size"
    ts = captured_at.strftime("%H:%M")
    fallbacks = _metadata_fallbacks(capture_type, industry, dims, ts)
    image_grounded = image_bytes is not None and _is_image_describable(capture_type, image_mime)

    parsed = _maybe_ai_json(
        ai_client,
        prompt=_metadata_prompt(
            image_grounded=image_grounded,
            capture_type=capture_type,
            industry=industry,
            dims=dims,
            ts=ts,
            description_style=description_style,
        ),
        fallback_dict=fallbacks,
        max_tokens=400,
        image_bytes=image_bytes if image_grounded else None,
        image_mime=image_mime if image_grounded else None,
    )

    # Visual-only fields are blanked when the model didn't see the
    # image, so SQL aggregates aren't polluted with hallucinated values.
    if image_grounded:
        detected_text = str(parsed.get("detected_text") or "").strip()
        scene_category = str(parsed.get("scene_category") or "unknown").strip() or "unknown"
    else:
        detected_text = ""
        scene_category = "unknown"

    return {
        "caption": str(parsed.get("caption") or fallbacks["caption"]).strip(),
        "alt_text": str(parsed.get("alt_text") or fallbacks["alt_text"]).strip(),
        "summary": str(parsed.get("summary") or fallbacks["summary"]).strip(),
        "tags": _coerce_tags(parsed.get("tags"), fallbacks["tags"]),
        "detected_text": detected_text,
        "scene_category": scene_category,
    }


def _resolve_user(client: WorkspaceClient) -> str | None:
    """Best-effort: pull the caller's email from the workspace client.

    Used to populate ``submitted_by`` on each capture row. We never
    block a capture on this — if the SDK call fails for any reason
    (network, auth, missing scope) the row just lands with NULL
    submitted_by.
    """
    try:
        me = client.current_user.me()
        return me.user_name or me.display_name or None
    except Exception as e:  # pragma: no cover — defensive
        logger.debug("current_user.me() failed: %s", e)
        return None


def handle_frame(
    client: WorkspaceClient,
    warehouse_id: str,
    *,
    file_bytes: bytes,
    capture_type: str,
    mime_type: str | None,
    width: int | None,
    height: int | None,
    duration_ms: int | None,
    industry: str,
    config: dict,
    session_id: str | None = None,
    description_style: str = "strict",
) -> dict:
    """Process one captured frame: upload to Volume + INSERT row.

    Returns the row that was written so the UI can append it to the
    live "recent" strip without a follow-up SELECT.

    ``config`` carries ``catalog``, ``schema``, ``volume``, optional
    ``table_name``, ``realistic_content``, ``ai_endpoint_name``, and
    ``ai_token_budget`` — same shape the synthetic tabs use, kept
    consistent so the AI adapter wiring is identical.
    """
    if capture_type not in ("photo", "video"):
        raise ValueError(f"capture_type must be 'photo' or 'video', got {capture_type!r}")
    if not file_bytes:
        raise ValueError("file_bytes is empty — nothing to upload")

    catalog = config["catalog"]
    schema = config["schema"]
    volume = config.get("volume") or "demo_unstructured"
    table_name = (config.get("table_name") or "").strip() or "demo_capture_catalog"
    table_fqn = f"{catalog}.{schema}.{table_name}"

    captured_at = datetime.now(timezone.utc)
    capture_id = uuid.uuid4().hex
    ext = _ext_for(mime_type, capture_type)
    day_str = captured_at.strftime("%Y-%m-%d")
    file_name = f"{capture_type}_{capture_id}.{ext}"
    file_path = f"/Volumes/{catalog}/{schema}/{volume}/capture/{capture_type}/{day_str}/{file_name}"

    import io  # narrow import — only needed when actually streaming bytes

    # Ensure volume + table on every call. Both are IF-NOT-EXISTS so
    # cost is a single metadata RPC after the first call. Cheaper than
    # gating on a per-process boolean and avoids races across uvicorn
    # workers.
    _ensure_volume(client, warehouse_id, f"{catalog}.{schema}.{volume}")
    _ensure_capture_table(client, warehouse_id, table_fqn)

    # Volume upload — synchronous for the live-capture flow. The user
    # is staring at the preview; per-frame latency is what they feel.
    client.files.upload(
        file_path=file_path,
        contents=io.BytesIO(file_bytes),
        overwrite=True,
    )

    # AI metadata — opt-in via realistic_content. build_drafter returns
    # None when the gate is off / no backend, and _draft_image_metadata
    # degrades to templated fallbacks in that case. One consolidated
    # multimodal call returns all six AI-derived fields as a JSON blob.
    ai_client = build_drafter(config, sdk_client=client)
    style = description_style if description_style in DESCRIPTION_STYLES else "strict"
    meta = _draft_image_metadata(
        ai_client,
        capture_type=capture_type,
        industry=industry,
        width=width,
        height=height,
        captured_at=captured_at,
        # Pass the bytes for image-grounded analysis when the model can
        # use them (still images only; video chunks bypass).
        image_bytes=file_bytes,
        image_mime=mime_type,
        description_style=style,
    )
    caption = meta["caption"]
    alt_text = meta["alt_text"]
    summary = meta["summary"]
    tags = meta["tags"]
    detected_text = meta["detected_text"]
    scene_category = meta["scene_category"]
    # content_full = the queryable text projection (same convention as
    # the other tabs' content_full columns). Captures have no body
    # text of their own, so we surface summary + caption + alt_text +
    # detected_text — enough for `WHERE content_full LIKE '%...'` RAG
    # demos that should now also hit OCR'd content.
    content_full = "\n\n".join(p for p in (summary, caption, alt_text, detected_text) if p)

    metadata_json = json.dumps(
        {
            "capture_id": capture_id,
            "capture_type": capture_type,
            "mime_type": mime_type,
            "width": width,
            "height": height,
            "duration_ms": duration_ms,
            "industry": industry,
            "captured_at": captured_at.isoformat(timespec="seconds"),
        },
        default=str,
    )

    duration_literal = "NULL" if duration_ms is None else str(int(duration_ms))
    width_literal = "NULL" if width is None else str(int(width))
    height_literal = "NULL" if height is None else str(int(height))

    submitted_by = _resolve_user(client)
    session_id_clean = (session_id or "").strip() or None

    sql = (
        f"INSERT INTO {table_fqn} "
        f"(capture_id, capture_type, file_path, file_extension, size_bytes, "
        f"width, height, duration_ms, mime_type, industry, caption, alt_text, "
        f"summary, tags, detected_text, scene_category, "
        f"content_full, captured_at, session_id, submitted_by, content, "
        f"metadata_json) VALUES ("
        f"{_sql_str(capture_id)}, "
        f"{_sql_str(capture_type)}, "
        f"{_sql_str(file_path)}, "
        f"{_sql_str(ext)}, "
        f"{len(file_bytes)}, "
        f"{width_literal}, "
        f"{height_literal}, "
        f"{duration_literal}, "
        f"{_sql_str(mime_type)}, "
        f"{_sql_str(industry)}, "
        f"{_sql_str(caption)}, "
        f"{_sql_str(alt_text)}, "
        f"{_sql_str(summary)}, "
        f"{_sql_str(tags)}, "
        f"{_sql_str(detected_text)}, "
        f"{_sql_str(scene_category)}, "
        f"{_sql_str(content_full)}, "
        f"current_timestamp(), "
        f"{_sql_str(session_id_clean)}, "
        f"{_sql_str(submitted_by)}, "
        f"unhex('{file_bytes.hex()}'), "
        f"{_sql_str(metadata_json)})"
    )
    execute_sql(client, warehouse_id, sql)

    return {
        "capture_id": capture_id,
        "capture_type": capture_type,
        "file_path": file_path,
        "file_extension": ext,
        "size_bytes": len(file_bytes),
        "width": width,
        "height": height,
        "duration_ms": duration_ms,
        "mime_type": mime_type,
        "industry": industry,
        "caption": caption,
        "alt_text": alt_text,
        "summary": summary,
        "tags": tags,
        "detected_text": detected_text,
        "scene_category": scene_category,
        "captured_at": captured_at.isoformat(timespec="seconds"),
        "session_id": session_id_clean,
        "submitted_by": submitted_by,
        "table_fqn": table_fqn,
    }


def list_recent(
    client: WorkspaceClient,
    warehouse_id: str,
    *,
    catalog: str,
    schema: str,
    table_name: str | None = None,
    limit: int = 20,
    session_id: str | None = None,
) -> list[dict]:
    """Return the N most-recent captures for the live UI strip.

    SELECTs only the metadata columns — never the inline ``content``
    BINARY — so the response stays small even when the table has
    grown to thousands of rows.

    When ``session_id`` is provided, only rows from that session are
    returned. Useful for the per-tab Recent strip so the UI doesn't
    show captures from other concurrent sessions.
    """
    table = (table_name or "").strip() or "demo_capture_catalog"
    table_fqn = f"{catalog}.{schema}.{table}"
    n = max(1, min(int(limit), 200))
    where = ""
    if session_id and session_id.strip():
        where = f"WHERE session_id = {_sql_str(session_id.strip())} "
    sql = (
        f"SELECT capture_id, capture_type, file_path, file_extension, "
        f"size_bytes, width, height, duration_ms, mime_type, industry, "
        f"caption, alt_text, summary, tags, detected_text, scene_category, "
        f"captured_at, session_id, submitted_by "
        f"FROM {table_fqn} {where}ORDER BY captured_at DESC LIMIT {n}"
    )
    try:
        rows = execute_sql(client, warehouse_id, sql)
    except Exception as e:
        # Table may not exist yet (first-time use) — degrade quietly.
        logger.info("list_recent: empty / missing table %s (%s)", table_fqn, e)
        return []
    return rows
