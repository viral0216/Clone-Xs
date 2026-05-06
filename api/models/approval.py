"""Approval request / response models."""

from pydantic import BaseModel


class ApprovalRequestModel(BaseModel):
    """Pending or decided approval request — the on-disk shape from src/approval.py."""

    request_id: str
    source_catalog: str
    dest_catalog: str
    clone_type: str
    requested_by: str
    requested_at: str
    status: str
    approved_by: str | None = None
    denied_by: str | None = None
    decided_at: str | None = None
    deny_reason: str = ""
    timeout_hours: int = 24
    config_summary: dict | None = None


class DenyApprovalBody(BaseModel):
    """Optional reason supplied with a deny — surfaces in the audit trail."""

    reason: str = ""


class ApprovalActionResponse(BaseModel):
    """Result of approve / deny."""

    success: bool
    request_id: str
    status: str  # "approved" | "denied" | "not_found" | "already_decided"
    decided_by: str | None = None
    decided_at: str | None = None
    detail: str | None = None
