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
    # Clone configuration
    clone_type: Literal["DEEP", "SHALLOW"] = "DEEP"
    load_type: Literal["FULL", "INCREMENTAL"] = "FULL"
    max_workers: int = 4
    parallel_tables: int = 1
    max_parallel_queries: int = 10
    max_rps: float = 0
    # Copy options
    copy_permissions: bool = True
    copy_ownership: bool = True
    copy_tags: bool = True
    copy_properties: bool = True
    copy_security: bool = True
    copy_constraints: bool = True
    copy_comments: bool = True
    # Features
    enable_rollback: bool = False
    validate_after_clone: bool = False
    validate_checksum: bool = False
    force_reclone: bool = False
    show_progress: bool = True
    # Filtering
    exclude_schemas: list[str] = ["information_schema", "default"]
    include_schemas: list[str] = []
    include_tables_regex: str = ""
    exclude_tables_regex: str = ""
    order_by_size: str = ""
    # Time travel
    as_of_timestamp: str = ""
    as_of_version: str = ""
