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


class SyncRequest(BaseModel):
    source_catalog: str
    destination_catalog: str
    warehouse_id: str | None = None
    exclude_schemas: list[str] = ["information_schema", "default"]
    dry_run: bool = False
    drop_extra: bool = False
