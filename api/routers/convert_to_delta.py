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
import uuid
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException

from api.dependencies import get_app_config, get_db_client
from api.models.convert_to_delta import (
    ConvertResultResponse,
    ConvertSummaryResponse,
    ConvertToDeltaRequest,
)
from src.audit_trail import ensure_convert_audit_table, log_convert_result
from src.convert_to_delta import (
    ConvertResult,
    ConvertToDeltaError,
    convert_tables_to_delta,
)

logger = logging.getLogger(__name__)

router = APIRouter()


def _build_audit_callback(
    client,
    warehouse_id: str,
    config: dict,
    *,
    operation_id: str,
    dry_run: bool,
):
    """Construct the per-target audit callback for one batch.

    Closes over the API-layer context (client, warehouse, config,
    operation_id) so ``src/convert_to_delta.py`` stays free of audit
    plumbing — that module just calls the callback if it has one.

    Audit init is best-effort: if creating the audit table fails (perms,
    transient warehouse error), we log and return ``None`` so the
    conversion continues without audit. Same posture as the clone path,
    which doesn't fail a clone because the audit row didn't write.
    """
    try:
        ensure_convert_audit_table(client, warehouse_id, config)
    except Exception as e:
        logger.warning(f"Convert audit table init failed; running without audit: {e}")
        return None

    def _callback(
        result: ConvertResult,
        started_at: datetime,
        completed_at: datetime,
    ) -> None:
        log_convert_result(
            client,
            warehouse_id,
            config,
            operation_id=operation_id,
            fqn_target=result.fqn,
            source_format=result.source_format,
            status=result.status,
            started_at=started_at,
            completed_at=completed_at,
            duration_ms=result.duration_ms,
            dry_run=dry_run,
            error_message=result.error,
        )

    return _callback


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

    operation_id = str(uuid.uuid4())
    audit_callback = _build_audit_callback(
        client,
        warehouse_id,
        app_config,
        operation_id=operation_id,
        dry_run=req.dry_run,
    )

    targets = [(t.fqn, t.source_format) for t in req.targets]
    try:
        summary = convert_tables_to_delta(
            client,
            warehouse_id,
            targets,
            confirm_destructive=req.confirm_destructive,
            dry_run=req.dry_run,
            audit_callback=audit_callback,
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
