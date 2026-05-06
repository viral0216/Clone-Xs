"""REST surface for the approval workflow.

The actual approval state machine lives in `src/approval.py` (file-backed:
JSON files under `approval_requests/`). These endpoints expose it to the
clone-approvals UI so reviewers can list pending requests and approve /
deny them without dropping into the CLI.

The approve / deny endpoints try to identify the reviewer via the
Databricks SDK (`client.current_user.me()`); if that call fails (e.g.
unauthenticated dev environment) we fall back to "ui" so the request still
records SOMEONE made the decision and the audit trail isn't blank.
"""

from dataclasses import asdict

from fastapi import APIRouter, Depends, HTTPException

from api.dependencies import get_db_client
from api.models.approval import (
    ApprovalActionResponse,
    ApprovalRequestModel,
    DenyApprovalBody,
)

router = APIRouter()


def _identify_reviewer(client) -> str:
    """Return the current user identifier for audit trail, with fallback."""
    try:
        me = client.current_user.me()
        return me.user_name or me.display_name or "ui"
    except Exception:
        return "ui"


@router.get("/pending", response_model=list[ApprovalRequestModel])
async def list_pending() -> list[ApprovalRequestModel]:
    """List all pending approval requests."""
    from src.approval import list_pending_requests

    requests = list_pending_requests()
    return [ApprovalRequestModel(**asdict(r)) for r in requests]


@router.get("/{request_id}", response_model=ApprovalRequestModel)
async def get_request(request_id: str) -> ApprovalRequestModel:
    """Fetch one approval request by id (works for any status)."""
    from src.approval import check_approval_status

    req = check_approval_status(request_id)
    if not req:
        raise HTTPException(status_code=404, detail="Approval request not found")
    return ApprovalRequestModel(**asdict(req))


@router.post("/{request_id}/approve", response_model=ApprovalActionResponse)
async def approve(
    request_id: str,
    client=Depends(get_db_client),
) -> ApprovalActionResponse:
    """Approve a pending request. Idempotent on terminal states."""
    from src.approval import approve_request, check_approval_status

    existing = check_approval_status(request_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Approval request not found")
    if existing.status != "pending":
        return ApprovalActionResponse(
            success=False,
            request_id=request_id,
            status=existing.status,
            decided_by=existing.approved_by or existing.denied_by,
            decided_at=existing.decided_at,
            detail=f"Request already {existing.status}",
        )

    approved_by = _identify_reviewer(client)
    ok = approve_request(request_id, approved_by=approved_by)
    if not ok:
        raise HTTPException(status_code=409, detail="Approval failed (race)")

    after = check_approval_status(request_id)
    return ApprovalActionResponse(
        success=True,
        request_id=request_id,
        status="approved",
        decided_by=approved_by,
        decided_at=after.decided_at if after else None,
    )


@router.post("/{request_id}/deny", response_model=ApprovalActionResponse)
async def deny(
    request_id: str,
    body: DenyApprovalBody | None = None,
    client=Depends(get_db_client),
) -> ApprovalActionResponse:
    """Deny a pending request. Reason is recorded for audit."""
    from src.approval import check_approval_status, deny_request

    existing = check_approval_status(request_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Approval request not found")
    if existing.status != "pending":
        return ApprovalActionResponse(
            success=False,
            request_id=request_id,
            status=existing.status,
            decided_by=existing.approved_by or existing.denied_by,
            decided_at=existing.decided_at,
            detail=f"Request already {existing.status}",
        )

    denied_by = _identify_reviewer(client)
    reason = (body.reason if body else "") or ""
    ok = deny_request(request_id, denied_by=denied_by, reason=reason)
    if not ok:
        raise HTTPException(status_code=409, detail="Deny failed (race)")

    after = check_approval_status(request_id)
    return ApprovalActionResponse(
        success=True,
        request_id=request_id,
        status="denied",
        decided_by=denied_by,
        decided_at=after.decided_at if after else None,
        detail=reason or None,
    )
