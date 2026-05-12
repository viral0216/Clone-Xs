"""POST /api/capture — live webcam → Volume + Delta with inline BINARY.

Distinct from `/api/generate/demo-*` (the synthetic-data endpoints):

  - **No JobManager**. Each request is one synchronous unit of work
    that the request handler completes before returning. The user is
    looking at a live preview in the browser; per-request latency is
    what they feel, and there is no fixed-end "job" to poll.
  - **Multipart upload** for the frame endpoint — the bytes come from
    `MediaRecorder` / `<canvas>.toBlob()` in the browser and are
    attached as a file part rather than base64'd in JSON.

Three endpoints:

  - ``POST /api/capture/init``    — idempotent volume + table create.
  - ``POST /api/capture/frame``   — multipart: blob + form fields →
                                    Volume upload + INSERT row.
  - ``GET  /api/capture/recent``  — recent metadata rows for the live
                                    UI (no inline BINARY in payload).
"""

from __future__ import annotations

import logging
from typing import Any, Optional

from fastapi import APIRouter, Depends, File, Form, Header, HTTPException, UploadFile

from api.dependencies import get_app_config, get_db_client
from api.models.demo_capture import (
    DemoCaptureInitRequest,
    DemoCaptureInitResponse,
    DemoCaptureRecentResponse,
    DemoCaptureRow,
)

logger = logging.getLogger(__name__)

router = APIRouter()


def _resolve_warehouse_id(req_warehouse_id: str | None, app_config: dict) -> str:
    warehouse_id = req_warehouse_id or app_config.get("sql_warehouse_id", "")
    if not warehouse_id:
        raise HTTPException(
            status_code=400,
            detail=(
                "warehouse_id is required (request body or default config). "
                "Live Capture creates a UC Volume and Delta table via SQL."
            ),
        )
    return warehouse_id


@router.post(
    "/capture/init",
    response_model=DemoCaptureInitResponse,
    summary="Idempotent volume + capture-table create",
)
async def init_target(
    req: DemoCaptureInitRequest,
    client=Depends(get_db_client),
    app_config=Depends(get_app_config),
) -> DemoCaptureInitResponse:
    from src.demo_capture import init_capture_target

    warehouse_id = _resolve_warehouse_id(req.warehouse_id, app_config)
    config = {
        "catalog": req.catalog,
        "schema": req.schema_name,
        "volume": req.volume,
        "table_name": req.table_name,
    }
    out = init_capture_target(client, warehouse_id, config)
    return DemoCaptureInitResponse(**out)


@router.post(
    "/capture/frame",
    response_model=DemoCaptureRow,
    summary="Upload one captured frame (photo or video chunk) to Volume + INSERT row",
)
async def upload_frame(
    file: UploadFile = File(..., description="Captured photo blob or video chunk"),
    capture_type: str = Form(..., description="'photo' or 'video'"),
    catalog: str = Form(...),
    schema_: str = Form(..., alias="schema"),
    volume: str | None = Form(default=None),
    table_name: str | None = Form(default=None),
    industry: str = Form(default="healthcare"),
    realistic_content: bool = Form(default=False),
    ai_token_budget: int = Form(default=50_000),
    width: int | None = Form(default=None),
    height: int | None = Form(default=None),
    duration_ms: int | None = Form(default=None),
    mime_type: str | None = Form(default=None),
    session_id: str | None = Form(default=None),
    description_style: str = Form(
        default="strict",
        description=(
            "AI prompt style: 'strict' (industry-neutral, no demographic / "
            "profession claims — accessibility-friendly default) or "
            "'permissive' (vivid descriptions including apparent gender "
            "/ profession when the scene supports it; caller accepts "
            "the bias risk)."
        ),
    ),
    warehouse_id: str | None = Form(default=None),
    client=Depends(get_db_client),
    app_config=Depends(get_app_config),
    x_databricks_model: Optional[str] = Header(None, alias="X-Databricks-Model"),
) -> DemoCaptureRow:
    """Synchronous: read blob → upload to Volume → INSERT row → return.

    The `mime_type` form field is independent of the upload's
    Content-Type header so the UI can normalise (e.g. send
    ``video/webm`` even when the browser tags it ``video/webm;codecs=vp9``).
    """
    from src.demo_capture import handle_frame

    warehouse_id_resolved = _resolve_warehouse_id(warehouse_id, app_config)
    file_bytes = await file.read()
    if not file_bytes:
        raise HTTPException(status_code=400, detail="Uploaded blob is empty")

    config: dict[str, Any] = {
        "catalog": catalog,
        "schema": schema_,
        "volume": volume,
        "table_name": table_name,
        "realistic_content": realistic_content,
        "ai_token_budget": ai_token_budget,
        "ai_endpoint_name": x_databricks_model,
    }

    # Effective mime type: prefer the form-provided one, fall back to
    # the multipart Content-Type the browser tagged on the blob.
    effective_mime = mime_type or file.content_type

    try:
        row = handle_frame(
            client,
            warehouse_id_resolved,
            file_bytes=file_bytes,
            capture_type=capture_type,
            mime_type=effective_mime,
            width=width,
            height=height,
            duration_ms=duration_ms,
            industry=industry,
            config=config,
            session_id=session_id,
            description_style=description_style,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    logger.info(
        "Capture %s landed at %s (%d bytes)",
        row["capture_id"],
        row["file_path"],
        row["size_bytes"],
    )
    return DemoCaptureRow(**row)


@router.get(
    "/capture/recent",
    response_model=DemoCaptureRecentResponse,
    summary="List the N most-recent captures (metadata only — no inline BINARY)",
)
async def recent(
    catalog: str,
    schema: str,
    table_name: str | None = None,
    limit: int = 20,
    session_id: str | None = None,
    warehouse_id: str | None = None,
    client=Depends(get_db_client),
    app_config=Depends(get_app_config),
) -> DemoCaptureRecentResponse:
    from src.demo_capture import list_recent

    warehouse_id_resolved = _resolve_warehouse_id(warehouse_id, app_config)
    rows = list_recent(
        client,
        warehouse_id_resolved,
        catalog=catalog,
        schema=schema,
        table_name=table_name,
        limit=limit,
        session_id=session_id,
    )

    table = (table_name or "").strip() or "demo_capture_catalog"
    table_fqn = f"{catalog}.{schema}.{table}"

    # `execute_sql` returns dict rows with column names as keys; map
    # to the response schema, coercing types where SQL gave us numeric
    # strings.
    parsed: list[DemoCaptureRow] = []
    for r in rows:
        captured_at = r.get("captured_at")
        if captured_at is not None and not isinstance(captured_at, str):
            captured_at = captured_at.isoformat(timespec="seconds")
        parsed.append(
            DemoCaptureRow(
                capture_id=r.get("capture_id") or "",
                capture_type=r.get("capture_type") or "photo",
                file_path=r.get("file_path") or "",
                file_extension=r.get("file_extension") or "",
                size_bytes=int(r.get("size_bytes") or 0),
                width=r.get("width"),
                height=r.get("height"),
                duration_ms=r.get("duration_ms"),
                mime_type=r.get("mime_type"),
                industry=r.get("industry") or "",
                caption=r.get("caption"),
                alt_text=r.get("alt_text"),
                summary=r.get("summary"),
                tags=r.get("tags"),
                detected_text=r.get("detected_text"),
                scene_category=r.get("scene_category"),
                captured_at=captured_at or "",
                session_id=r.get("session_id"),
                submitted_by=r.get("submitted_by"),
                table_fqn=table_fqn,
            )
        )

    return DemoCaptureRecentResponse(rows=parsed, table_fqn=table_fqn)
