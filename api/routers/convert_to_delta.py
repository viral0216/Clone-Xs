"""POST /api/convert-to-delta — in-place format conversion (#13).

Distinct from /api/clone because:

  * destructive on source (no destination FQN),
  * synchronous response (no job queue) — typical workloads are a handful
    of tables and operators want immediate feedback before they make the
    second decision (e.g. re-clone the now-Delta source to a lower env),
  * confirmation gate on the request model.

Endpoint returns 400 if the request isn't dry-run and confirm_destructive
is False; 422 on validation; 500 on conversion failure; 200 on success
(even when individual tables in the batch fail — the response body has
per-table ``status`` so partial success is observable).
"""

import logging

from fastapi import APIRouter, Depends, HTTPException

from api.dependencies import get_app_config, get_db_client
from api.models.convert_to_delta import (
    ConvertResultResponse,
    ConvertSummaryResponse,
    ConvertToDeltaRequest,
)
from src.convert_to_delta import (
    ConvertToDeltaError,
    convert_tables_to_delta,
)

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("", response_model=ConvertSummaryResponse)
def post_convert_to_delta(
    req: ConvertToDeltaRequest,
    client=Depends(get_db_client),
    app_config=Depends(get_app_config),
) -> ConvertSummaryResponse:
    """Convert one or more UC tables in-place from Parquet/Iceberg to Delta.

    The Pydantic ``@model_validator`` on the request already enforces the
    confirm-or-dry-run gate, so by the time we get here either the user
    has acknowledged the destructive nature or this is a preview run.
    """
    warehouse_id = req.warehouse_id or app_config.get("sql_warehouse_id", "")
    if not warehouse_id:
        raise HTTPException(
            status_code=400,
            detail=(
                "warehouse_id is required (request body or default config). "
                "CONVERT TO DELTA needs a SQL warehouse to execute the DDL."
            ),
        )

    targets = [(t.fqn, t.source_format) for t in req.targets]
    try:
        summary = convert_tables_to_delta(
            client,
            warehouse_id,
            targets,
            confirm_destructive=req.confirm_destructive,
            dry_run=req.dry_run,
        )
    except ConvertToDeltaError as e:
        # Refusal (confirm_destructive missing) → 400, callers can reflect
        # the message back into the UI confirmation dialog without parsing.
        raise HTTPException(status_code=400, detail=str(e)) from e

    return ConvertSummaryResponse(
        total=summary.total,
        converted=summary.converted,
        failed=summary.failed,
        skipped=summary.skipped,
        results=[
            ConvertResultResponse(
                fqn=r.fqn,
                source_format=r.source_format,
                status=r.status,
                duration_ms=r.duration_ms,
                error=r.error,
            )
            for r in summary.results
        ],
    )
