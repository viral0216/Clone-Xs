"""POST /api/generate/demo-documents — kick off a document-corpus generation.

Mirrors the convert / streaming routers in shape:

  - POST /demo-documents          → submit a job, return {job_id}
  - POST /demo-documents/preview  → pure-arithmetic estimate, no warehouse
  - GET  /demo-documents/types    → registry inventory for the UI

Job submission goes through the existing JobManager (same one that
runs the structured-batch and streaming-emit jobs); operators poll
`GET /api/clone/{job_id}` for live progress. The progress dict is
populated per-file by `src.demo_documents.generate_documents`.

The deps are lazy-imported. If the `[documents]` optional extra
isn't installed, the type-list endpoint surfaces `available=false`
and the submit endpoint returns 503 with an install hint — neither
crashes the API server at import time.
"""

import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request

from api.dependencies import get_app_config, get_db_client
from api.models.demo_documents import (
    DemoDocumentsPerTypePreview,
    DemoDocumentsPreviewRequest,
    DemoDocumentsPreviewResponse,
    DemoDocumentsRequest,
    DemoDocumentsSubmitResponse,
    DemoDocumentsTypeInfo,
    DemoDocumentsTypesResponse,
)

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get(
    "/demo-documents/types",
    response_model=DemoDocumentsTypesResponse,
    summary="List the registered document types + dep availability",
)
async def get_document_types() -> DemoDocumentsTypesResponse:
    """Return the registered document types + whether the optional
    `[documents]` extra is installed.

    The UI calls this on mount to render the checkbox grid + show
    the install hint when deps are missing — without the deps
    installed we can still surface the type list (it's defined in
    the models module) but the user can't actually run a job until
    they install.
    """
    # Lazy import — the models module is dep-free, so we can import
    # the registry from src/demo_documents only after the availability
    # probe to avoid noise on systems without the extra.
    try:
        from src.demo_documents import DOCUMENT_TYPES, is_available

        available, reason = is_available()
        types = [
            DemoDocumentsTypeInfo(
                type=type_id,
                category=info["category"],
                label=info["label"],
                extension=info["extension"],
            )
            for type_id, info in DOCUMENT_TYPES.items()
        ]
        return DemoDocumentsTypesResponse(
            types=types,
            available=available,
            unavailable_reason=reason,
        )
    except Exception as e:
        # Defensive — even the registry import shouldn't fail, but
        # if it does, return a clean empty list rather than crashing.
        logger.error(f"Could not import demo_documents registry: {e}")
        return DemoDocumentsTypesResponse(
            types=[],
            available=False,
            unavailable_reason=f"Internal error loading registry: {e}",
        )


@router.post(
    "/demo-documents/preview",
    response_model=DemoDocumentsPreviewResponse,
    summary="Estimate file count, total size, and duration without hitting the warehouse",
)
async def preview(req: DemoDocumentsPreviewRequest) -> DemoDocumentsPreviewResponse:
    """Pure arithmetic on average-bytes-per-type × counts. Returns
    in microseconds; the UI calls this on every form change."""
    from src.demo_documents import preview_documents

    out = preview_documents({"types": list(req.types), "counts": dict(req.counts)})
    return DemoDocumentsPreviewResponse(
        per_type=[DemoDocumentsPerTypePreview(**p) for p in out["per_type"]],
        total_files=out["total_files"],
        total_bytes=out["total_bytes"],
        estimated_seconds=out["estimated_seconds"],
        unknown_types=out["unknown_types"],
    )


@router.post(
    "/demo-documents",
    response_model=DemoDocumentsSubmitResponse,
    summary="Submit a document-generation job",
)
async def submit(
    req: DemoDocumentsRequest,
    request: Request,
    client=Depends(get_db_client),
    app_config=Depends(get_app_config),
) -> DemoDocumentsSubmitResponse:
    """Submit a document-corpus generation job to the JobManager.

    The work runs in the background; the operator polls
    `GET /api/clone/{job_id}` for progress + final summary. Returns
    503 if the `[documents]` extra is not installed.
    """
    # Availability gate — return a structured 503 with the install
    # hint so the UI can render a calm banner instead of a generic
    # error toast.
    from src.demo_documents import is_available

    available, reason = is_available()
    if not available:
        raise HTTPException(
            status_code=503,
            detail={
                "error": "dependencies_missing",
                "extra": "documents",
                "install_command": "pip install clone-xs[documents]",
                "reason": reason,
            },
        )

    warehouse_id = req.warehouse_id or app_config.get("sql_warehouse_id", "")
    if not warehouse_id:
        raise HTTPException(
            status_code=400,
            detail=(
                "warehouse_id is required (request body or default config). "
                "Document generation auto-creates the destination Volume "
                "via SQL, which needs a SQL warehouse."
            ),
        )

    # Build the job config from the request. The JobManager passes
    # this dict through to the per-job-type branch in `_run_job`,
    # which calls `src.demo_documents.generate_documents`.
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
        # JobManager reads this for per-job warehouse routing — same
        # convention every other job type uses.
        "sql_warehouse_id": warehouse_id,
    }

    jm = request.app.state.job_manager
    job_id = await jm.submit_job("demo-documents", config, client)
    logger.info(
        f"Submitted demo-documents job {job_id} "
        f"(types={list(req.types)}, destination={req.destination})"
    )
    return DemoDocumentsSubmitResponse(job_id=job_id, status="queued")
