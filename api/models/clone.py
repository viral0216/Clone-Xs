"""Clone request/response models."""

from typing import Literal

from pydantic import BaseModel


class CloneRequest(BaseModel):
    source_catalog: str
    destination_catalog: str
    warehouse_id: str | None = None
    clone_type: Literal["DEEP", "SHALLOW"] = "DEEP"
    load_type: Literal["FULL", "INCREMENTAL"] = "FULL"
    dry_run: bool = False
    max_workers: int = 4
    parallel_tables: int = 1
    include_schemas: list[str] = []
    exclude_schemas: list[str] = ["information_schema", "default"]
    include_tables_regex: str | None = None
    exclude_tables_regex: str | None = None
    copy_permissions: bool = True
    copy_ownership: bool = True
    copy_tags: bool = True
    copy_properties: bool = True
    copy_security: bool = True
    copy_constraints: bool = True
    copy_comments: bool = True
    enable_rollback: bool = True
    validate_after_clone: bool = False
    validate_checksum: bool = False
    order_by_size: Literal["asc", "desc"] | None = None
    max_rps: float = 0
    as_of_timestamp: str | None = None
    as_of_version: int | None = None
    location: str | None = None
    profile: str | None = None
    serverless: bool = False
    volume: str | None = None
    force_reclone: bool = False
    schema_only: bool = False


class CloneJobResponse(BaseModel):
    job_id: str
    status: str
    message: str | None = None


class CloneJobStatus(BaseModel):
    job_id: str
    status: str
    source_catalog: str | None = None
    destination_catalog: str | None = None
    clone_type: str | None = None
    progress: dict | None = None
    result: dict | None = None
    error: str | None = None
    run_url: str | None = None
    logs: list[str] = []
    created_at: str | None = None
    started_at: str | None = None
    completed_at: str | None = None
