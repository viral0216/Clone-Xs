"""POST /api/generate/demo-media — kick off a media-corpus generation.

Mirrors api/routers/demo_documents.py — same three endpoints, same
JobManager dispatch shape. The new field is `ffmpeg_available` on
the GET /types response so the UI can grey out the video_clip
checkbox when ffmpeg isn't on PATH.
"""

import logging
from typing import Any, Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Request

from api.dependencies import get_app_config, get_db_client
from api.models.demo_media import (
    DemoMediaPerTypePreview,
    DemoMediaPreviewRequest,
    DemoMediaPreviewResponse,
    DemoMediaRequest,
    DemoMediaSubmitResponse,
    DemoMediaTypeInfo,
    DemoMediaTypesResponse,
)

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get(
    "/demo-media/types",
    response_model=DemoMediaTypesResponse,
    summary="List the registered media types + dep + ffmpeg availability",
)
async def get_media_types() -> DemoMediaTypesResponse:
    """Return the media type registry + Pillow + ffmpeg availability.

    Two distinct availability signals:
      - `available` / `unavailable_reason`: Pillow (the [media] extra)
      - `ffmpeg_available` / `ffmpeg_unavailable_reason`: ffmpeg system
        binary, only required by the video_clip type

    The UI renders both — install hint at the top when Pillow is
    missing (everything off), per-type grey-out for video_clip when
    only ffmpeg is missing.
    """
    try:
        from src.demo_media import MEDIA_TYPES, _ffmpeg_available, is_available

        available, reason = is_available()
        ffmpeg_ok, ffmpeg_reason = _ffmpeg_available()
        types = [
            DemoMediaTypeInfo(
                type=type_id,
                category=info["category"],
                label=info["label"],
                extension=info["extension"],
            )
            for type_id, info in MEDIA_TYPES.items()
        ]
        return DemoMediaTypesResponse(
            types=types,
            available=available,
            unavailable_reason=reason,
            ffmpeg_available=ffmpeg_ok,
            ffmpeg_unavailable_reason=ffmpeg_reason,
        )
    except Exception as e:
        logger.error(f"Could not import demo_media registry: {e}")
        return DemoMediaTypesResponse(
            types=[],
            available=False,
            unavailable_reason=f"Internal error loading registry: {e}",
            ffmpeg_available=False,
            ffmpeg_unavailable_reason="Could not probe ffmpeg",
        )


@router.post(
    "/demo-media/preview",
    response_model=DemoMediaPreviewResponse,
    summary="Estimate file count, total size, and duration without hitting the warehouse",
)
async def preview(req: DemoMediaPreviewRequest) -> DemoMediaPreviewResponse:
    """Pure arithmetic — no warehouse round-trip. Returns immediately."""
    from src.demo_media import preview_media

    out = preview_media({"types": list(req.types), "counts": dict(req.counts)})
    return DemoMediaPreviewResponse(
        per_type=[DemoMediaPerTypePreview(**p) for p in out["per_type"]],
        total_files=out["total_files"],
        total_bytes=out["total_bytes"],
        estimated_seconds=out["estimated_seconds"],
        unknown_types=out["unknown_types"],
    )


@router.post(
    "/demo-media",
    response_model=DemoMediaSubmitResponse,
    summary="Submit a media-generation job",
)
async def submit(
    req: DemoMediaRequest,
    request: Request,
    client=Depends(get_db_client),
    app_config=Depends(get_app_config),
    x_databricks_model: Optional[str] = Header(None, alias="X-Databricks-Model"),
) -> DemoMediaSubmitResponse:
    """Submit a media-generation job. Returns 503 when [media] extra
    is missing. Per-type ffmpeg availability is enforced inside the
    orchestrator (so the operator can include video_clip in their
    request and get a clean per-cell skip rather than a 503 for the
    whole batch)."""
    from src.demo_media import is_available

    available, reason = is_available()
    if not available:
        raise HTTPException(
            status_code=503,
            detail={
                "error": "dependencies_missing",
                "extra": "media",
                "install_command": "pip install clone-xs[media]",
                "reason": reason,
            },
        )

    warehouse_id = req.warehouse_id or app_config.get("sql_warehouse_id", "")
    if not warehouse_id:
        raise HTTPException(
            status_code=400,
            detail=(
                "warehouse_id is required (request body or default config). "
                "Media generation auto-creates the destination Volume "
                "via SQL, which needs a SQL warehouse."
            ),
        )

    config: dict[str, Any] = {
        "catalog": req.catalog,
        "schema": req.schema_name,
        "volume": req.volume,
        "table_name": req.table_name,
        "destination": req.destination,
        "types": list(req.types),
        "counts": dict(req.counts),
        "industry": req.industry,
        "realistic_content": req.realistic_content,
        "ai_token_budget": req.ai_token_budget,
        # X-Databricks-Model is set by ui/src/lib/api-client.ts from
        # localStorage.dbx_model (the model picked in Settings).
        "ai_endpoint_name": x_databricks_model,
        "faker_locale": req.faker_locale,
        "faker_seed": req.faker_seed,
        "sql_warehouse_id": warehouse_id,
    }

    jm = request.app.state.job_manager
    job_id = await jm.submit_job("demo-media", config, client)
    logger.info(
        f"Submitted demo-media job {job_id} "
        f"(types={list(req.types)}, destination={req.destination}, "
        f"ai_mode={req.realistic_content}, "
        f"ai_endpoint={x_databricks_model or 'anthropic'})"
    )
    return DemoMediaSubmitResponse(job_id=job_id, status="queued")
