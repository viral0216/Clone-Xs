"""Management request models."""

from pydantic import BaseModel


class RollbackRequest(BaseModel):
    log_file: str
    warehouse_id: str | None = None
    drop_catalog: bool = False


class PreflightRequest(BaseModel):
    source_catalog: str
    destination_catalog: str
    warehouse_id: str | None = None
    check_write: bool = True


class PIIScanRequest(BaseModel):
    source_catalog: str
    warehouse_id: str | None = None
    exclude_schemas: list[str] = ["information_schema", "default"]
    sample_data: bool = False
    max_workers: int = 4
    pii_config: dict | None = None
    read_uc_tags: bool = False
    save_history: bool = False


class PIITagRequest(BaseModel):
    source_catalog: str
    scan_id: str | None = None
    warehouse_id: str | None = None
    tag_prefix: str = "pii"
    min_confidence: float = 0.7
    dry_run: bool = True


class PIIRemediationRequest(BaseModel):
    catalog: str
    schema_name: str
    table_name: str
    column_name: str
    pii_type: str
    status: str  # detected, reviewed, masked, accepted, false_positive
    notes: str = ""


class SyncRequest(BaseModel):
    source_catalog: str
    destination_catalog: str
    warehouse_id: str | None = None
    exclude_schemas: list[str] = ["information_schema", "default"]
    dry_run: bool = False
    drop_extra: bool = False
