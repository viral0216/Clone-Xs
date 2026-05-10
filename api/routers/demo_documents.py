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
from typing import Any, Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Request

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
    summary="List document types visible for the chosen industry + dep availability",
)
async def get_document_types(industry: str | None = None) -> DemoDocumentsTypesResponse:
    """Return the document types visible for ``industry`` (or all
    types when ``industry`` is omitted) + whether the ``[documents]``
    extra is installed.

    Each entry's ``label`` is already resolved for the requested
    industry — e.g. ``pdf_contract`` returns ``"Lease agreement"``
    for ``real_estate`` and ``"Loan agreement"`` for ``financial``.
    Types with no matching industry entry (and no ``"*"`` default in
    their registry block) are filtered out, so the picker only shows
    documents that make sense for the chosen vertical.

    The UI calls this on mount AND on every industry change so the
    checkbox grid + labels stay in sync with the form.
    """
    # Lazy import — the models module is dep-free, so we can import
    # the registry from src/demo_documents only after the availability
    # probe to avoid noise on systems without the extra.
    try:
        from src.demo_documents import (
            DOCUMENT_TYPES,
            is_available,
            label_for,
            types_for_industry,
        )

        available, reason = is_available()
        if industry:
            entries = types_for_industry(industry)
            types = [
                DemoDocumentsTypeInfo(
                    type=e["id"],
                    category=e["category"],
                    label=e["label"],
                    extension=e["extension"],
                )
                for e in entries
            ]
        else:
            # Industry omitted — return every registered type with its
            # legacy ``label`` so callers that haven't yet upgraded to
            # pass an industry param keep working.
            types = [
                DemoDocumentsTypeInfo(
                    type=type_id,
                    category=info["category"],
                    label=label_for(type_id, ""),
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
    x_databricks_model: Optional[str] = Header(None, alias="X-Databricks-Model"),
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
        "table_name": req.table_name,
        "destination": req.destination,
        "types": list(req.types),
        "counts": dict(req.counts),
        "industry": req.industry,
        "realistic_content": req.realistic_content,
        "ai_token_budget": req.ai_token_budget,
        # AI endpoint name is forwarded from the X-Databricks-Model
        # header — same pattern api/routers/ai.py uses. The header is
        # set automatically by the UI's api-client from
        # localStorage.dbx_model whenever the user has picked a
        # Databricks Model Serving endpoint in Settings.
        "ai_endpoint_name": x_databricks_model,
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
        f"(types={list(req.types)}, destination={req.destination}, "
        f"ai_mode={req.realistic_content}, "
        f"ai_endpoint={x_databricks_model or 'anthropic'})"
    )
    return DemoDocumentsSubmitResponse(job_id=job_id, status="queued")
