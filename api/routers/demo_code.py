"""POST /api/generate/demo-code — kick off a code-corpus generation.

Mirrors api/routers/demo_logs.py and api/routers/demo_knowledge.py.
No 503 missing-deps path because Code has no optional Python deps.
"""

import logging
from typing import Any, Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Request

from api.dependencies import get_app_config, get_db_client
from api.models.demo_code import (
    DemoCodePerTypePreview,
    DemoCodePreviewRequest,
    DemoCodePreviewResponse,
    DemoCodeRequest,
    DemoCodeSubmitResponse,
    DemoCodeTypeInfo,
    DemoCodeTypesResponse,
)

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get(
    "/demo-code/types",
    response_model=DemoCodeTypesResponse,
    summary="List the registered code types (no optional deps to gate on)",
)
async def get_code_types() -> DemoCodeTypesResponse:
    try:
        from src.demo_code import CODE_TYPES, is_available

        available, reason = is_available()
        types = [
            DemoCodeTypeInfo(
                type=type_id,
                category=info["category"],
                label=info["label"],
                extension=info["extension"],
                language=info["language"],
            )
            for type_id, info in CODE_TYPES.items()
        ]
        return DemoCodeTypesResponse(
            types=types,
            available=available,
            unavailable_reason=reason,
        )
    except Exception as e:
        logger.error(f"Could not import demo_code registry: {e}")
        return DemoCodeTypesResponse(
            types=[],
            available=False,
            unavailable_reason=f"Internal error loading registry: {e}",
        )


@router.post(
    "/demo-code/preview",
    response_model=DemoCodePreviewResponse,
    summary="Estimate repo / file count, total size, and duration",
)
async def preview(req: DemoCodePreviewRequest) -> DemoCodePreviewResponse:
    from src.demo_code import preview_code

    out = preview_code({"types": list(req.types), "counts": dict(req.counts)})
    return DemoCodePreviewResponse(
        per_type=[DemoCodePerTypePreview(**p) for p in out["per_type"]],
        total_repos=out["total_repos"],
        total_files=out["total_files"],
        total_bytes=out["total_bytes"],
        estimated_seconds=out["estimated_seconds"],
        unknown_types=out["unknown_types"],
    )


@router.post(
    "/demo-code",
    response_model=DemoCodeSubmitResponse,
    summary="Submit a code-corpus generation job",
)
async def submit(
    req: DemoCodeRequest,
    request: Request,
    client=Depends(get_db_client),
    app_config=Depends(get_app_config),
    x_databricks_model: Optional[str] = Header(None, alias="X-Databricks-Model"),
) -> DemoCodeSubmitResponse:
    warehouse_id = req.warehouse_id or app_config.get("sql_warehouse_id", "")
    if not warehouse_id:
        raise HTTPException(
            status_code=400,
            detail=(
                "warehouse_id is required (request body or default config). "
                "Code generation auto-creates the destination Volume via "
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
    job_id = await jm.submit_job("demo-code", config, client)
    logger.info(
        f"Submitted demo-code job {job_id} "
        f"(types={list(req.types)}, destination={req.destination}, "
        f"ai_mode={req.realistic_content}, "
        f"ai_endpoint={x_databricks_model or 'anthropic'})"
    )
    return DemoCodeSubmitResponse(job_id=job_id, status="queued")
