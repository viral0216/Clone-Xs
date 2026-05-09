"""POST /api/generate/demo-logs — kick off a log-corpus generation.

Mirrors api/routers/demo_knowledge.py — same three routes, same
JobManager dispatch. No 503 missing-deps path because Logs has no
optional Python deps.
"""

import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request

from api.dependencies import get_app_config, get_db_client
from api.models.demo_logs import (
    DemoLogsPerTypePreview,
    DemoLogsPreviewRequest,
    DemoLogsPreviewResponse,
    DemoLogsRequest,
    DemoLogsSubmitResponse,
    DemoLogsTypeInfo,
    DemoLogsTypesResponse,
)

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get(
    "/demo-logs/types",
    response_model=DemoLogsTypesResponse,
    summary="List the registered log types (no optional deps to gate on)",
)
async def get_log_types() -> DemoLogsTypesResponse:
    try:
        from src.demo_logs import LOG_TYPES, is_available

        available, reason = is_available()
        types = [
            DemoLogsTypeInfo(
                type=type_id,
                category=info["category"],
                label=info["label"],
                extension=info["extension"],
            )
            for type_id, info in LOG_TYPES.items()
        ]
        return DemoLogsTypesResponse(
            types=types,
            available=available,
            unavailable_reason=reason,
        )
    except Exception as e:
        logger.error(f"Could not import demo_logs registry: {e}")
        return DemoLogsTypesResponse(
            types=[],
            available=False,
            unavailable_reason=f"Internal error loading registry: {e}",
        )


@router.post(
    "/demo-logs/preview",
    response_model=DemoLogsPreviewResponse,
    summary="Estimate file count, total lines, total size, and duration",
)
async def preview(req: DemoLogsPreviewRequest) -> DemoLogsPreviewResponse:
    from src.demo_logs import preview_logs

    out = preview_logs(
        {
            "types": list(req.types),
            "counts": dict(req.counts),
            "lines_per_file": req.lines_per_file,
        }
    )
    return DemoLogsPreviewResponse(
        per_type=[DemoLogsPerTypePreview(**p) for p in out["per_type"]],
        total_files=out["total_files"],
        total_lines=out["total_lines"],
        total_bytes=out["total_bytes"],
        estimated_seconds=out["estimated_seconds"],
        unknown_types=out["unknown_types"],
    )


@router.post(
    "/demo-logs",
    response_model=DemoLogsSubmitResponse,
    summary="Submit a log-corpus generation job",
)
async def submit(
    req: DemoLogsRequest,
    request: Request,
    client=Depends(get_db_client),
    app_config=Depends(get_app_config),
) -> DemoLogsSubmitResponse:
    warehouse_id = req.warehouse_id or app_config.get("sql_warehouse_id", "")
    if not warehouse_id:
        raise HTTPException(
            status_code=400,
            detail=(
                "warehouse_id is required (request body or default config). "
                "Logs generation auto-creates the destination Volume via "
                "SQL, which needs a SQL warehouse."
            ),
        )

    config: dict[str, Any] = {
        "catalog": req.catalog,
        "schema": req.schema_name,
        "volume": req.volume,
        "destination": req.destination,
        "types": list(req.types),
        "counts": dict(req.counts),
        "industry": req.industry,
        "lines_per_file": req.lines_per_file,
        "days_back": req.days_back,
        "faker_locale": req.faker_locale,
        "faker_seed": req.faker_seed,
        "sql_warehouse_id": warehouse_id,
    }

    jm = request.app.state.job_manager
    job_id = await jm.submit_job("demo-logs", config, client)
    logger.info(
        f"Submitted demo-logs job {job_id} "
        f"(types={list(req.types)}, destination={req.destination}, "
        f"lines_per_file={req.lines_per_file})"
    )
    return DemoLogsSubmitResponse(job_id=job_id, status="queued")
