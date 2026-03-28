"""RTBF (Right to Be Forgotten) request/response models."""

from typing import Literal

from pydantic import BaseModel


class RTBFSubmitRequest(BaseModel):
    subject_type: Literal["email", "customer_id", "ssn", "phone", "name", "national_id", "passport", "credit_card", "custom"] = "email"
    subject_value: str
    subject_column: str | None = None
    requester_email: str
    requester_name: str
    legal_basis: str = "GDPR Article 17(1)(a) - Consent withdrawn"
    strategy: Literal["delete", "anonymize", "pseudonymize"] = "delete"
    scope_catalogs: list[str] = []
    grace_period_days: int = 0
    notes: str | None = None
    warehouse_id: str | None = None


class RTBFRequestResponse(BaseModel):
    request_id: str
    status: str
    deadline: str
    message: str | None = None


class RTBFStatusUpdate(BaseModel):
    status: Literal["approved", "on_hold", "cancelled"]
    reason: str | None = None


class RTBFExecuteRequest(BaseModel):
    subject_value: str
    strategy: Literal["delete", "anonymize", "pseudonymize"] | None = None
    dry_run: bool = False
    warehouse_id: str | None = None


class RTBFVacuumRequest(BaseModel):
    retention_hours: int | None = None
    warehouse_id: str | None = None


class RTBFVerifyRequest(BaseModel):
    subject_value: str
    warehouse_id: str | None = None


class RTBFCertificateRequest(BaseModel):
    output_dir: str | None = None
    warehouse_id: str | None = None


class RTBFJobResponse(BaseModel):
    job_id: str
    status: str
    message: str | None = None
