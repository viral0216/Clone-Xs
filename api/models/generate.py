"""Generate request models."""

from typing import Literal

from pydantic import BaseModel


class WorkflowRequest(BaseModel):
    format: Literal["json", "yaml"] = "json"
    output_path: str | None = None
    job_name: str | None = None
    cluster_id: str | None = None
    schedule: str | None = None
    notification_email: str | None = None


class TerraformRequest(BaseModel):
    source_catalog: str
    warehouse_id: str | None = None
    format: Literal["terraform", "pulumi"] = "terraform"
    output_path: str | None = None
    exclude_schemas: list[str] = ["information_schema", "default"]


class CreateJobRequest(BaseModel):
    source_catalog: str
    destination_catalog: str
    job_name: str | None = None
    volume: str | None = None
    schedule: str | None = None
    timezone: str = "UTC"
    notification_emails: list[str] = []
    max_retries: int = 0
    timeout: int = 7200
    tags: dict[str, str] = {}
    update_job_id: int | None = None
