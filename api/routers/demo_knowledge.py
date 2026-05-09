"""POST /api/generate/demo-knowledge — kick off a knowledge-base
corpus generation.

Mirrors api/routers/demo_documents.py and api/routers/demo_media.py.
The endpoint is shape-identical to the others — same three routes,
same JobManager dispatch — except `available` on /types is always
True because Knowledge has no optional Python deps.
"""

import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request

from api.dependencies import get_app_config, get_db_client
from api.models.demo_knowledge import (
    DemoKnowledgePerTypePreview,
    DemoKnowledgePreviewRequest,
    DemoKnowledgePreviewResponse,
    DemoKnowledgeRequest,
    DemoKnowledgeSubmitResponse,
    DemoKnowledgeTypeInfo,
    DemoKnowledgeTypesResponse,
)

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get(
    "/demo-knowledge/types",
    response_model=DemoKnowledgeTypesResponse,
    summary="List the registered knowledge types (no optional deps to gate on)",
)
async def get_knowledge_types() -> DemoKnowledgeTypesResponse:
    """Return the type registry. Knowledge has no optional Python
    deps so `available` is always True; the field is kept for shape-
    uniformity with the documents and media endpoints."""
    try:
        from src.demo_knowledge import KNOWLEDGE_TYPES, is_available

        available, reason = is_available()
        types = [
            DemoKnowledgeTypeInfo(
                type=type_id,
                category=info["category"],
                label=info["label"],
                extension=info["extension"],
            )
            for type_id, info in KNOWLEDGE_TYPES.items()
        ]
        return DemoKnowledgeTypesResponse(
            types=types,
            available=available,
            unavailable_reason=reason,
        )
    except Exception as e:
        logger.error(f"Could not import demo_knowledge registry: {e}")
        return DemoKnowledgeTypesResponse(
            types=[],
            available=False,
            unavailable_reason=f"Internal error loading registry: {e}",
        )


@router.post(
    "/demo-knowledge/preview",
    response_model=DemoKnowledgePreviewResponse,
    summary="Estimate file count, total size, and duration without hitting the warehouse",
)
async def preview(req: DemoKnowledgePreviewRequest) -> DemoKnowledgePreviewResponse:
    from src.demo_knowledge import preview_knowledge

    out = preview_knowledge({"types": list(req.types), "counts": dict(req.counts)})
    return DemoKnowledgePreviewResponse(
        per_type=[DemoKnowledgePerTypePreview(**p) for p in out["per_type"]],
        total_files=out["total_files"],
        total_bytes=out["total_bytes"],
        estimated_seconds=out["estimated_seconds"],
        unknown_types=out["unknown_types"],
    )


@router.post(
    "/demo-knowledge",
    response_model=DemoKnowledgeSubmitResponse,
    summary="Submit a knowledge-corpus generation job",
)
async def submit(
    req: DemoKnowledgeRequest,
    request: Request,
    client=Depends(get_db_client),
    app_config=Depends(get_app_config),
) -> DemoKnowledgeSubmitResponse:
    """Submit a knowledge-corpus generation job.

    No 503 missing-deps path because Knowledge has no optional
    deps. The endpoint matches the shape of demo-documents /
    demo-media so the UI's per-tab component can stay
    parameterised on the category.
    """
    warehouse_id = req.warehouse_id or app_config.get("sql_warehouse_id", "")
    if not warehouse_id:
        raise HTTPException(
            status_code=400,
            detail=(
                "warehouse_id is required (request body or default config). "
                "Knowledge generation auto-creates the destination Volume "
                "via SQL, which needs a SQL warehouse."
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
        "faker_locale": req.faker_locale,
        "faker_seed": req.faker_seed,
        "sql_warehouse_id": warehouse_id,
    }

    jm = request.app.state.job_manager
    job_id = await jm.submit_job("demo-knowledge", config, client)
    logger.info(
        f"Submitted demo-knowledge job {job_id} "
        f"(types={list(req.types)}, destination={req.destination})"
    )
    return DemoKnowledgeSubmitResponse(job_id=job_id, status="queued")
