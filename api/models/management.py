"""Management request models."""

from pydantic import BaseModel, model_validator


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
    """PII scan request — single OR multi catalog.

    Single mode (default): pass `source_catalog: str`. Routes to
    `src.pii_detection.scan_catalog_for_pii`.

    Multi mode: pass `source_catalogs: list[str]`. Routes to
    `src.pii_multi.scan_catalogs_for_pii_multi`, which fans the scan
    out across the listed catalogs in parallel and returns a merged
    response with each detection stamped with its owning `catalog`.
    """

    # Optional in multi mode. The validator below requires at least
    # one of `source_catalog` / `source_catalogs` to be set.
    source_catalog: str = ""
    source_catalogs: list[str] | None = None
    warehouse_id: str | None = None
    exclude_schemas: list[str] = ["information_schema", "default"]
    sample_data: bool = False
    max_workers: int = 4
    pii_config: dict | None = None
    read_uc_tags: bool = False
    save_history: bool = False
    schema_filter: list[str] | None = None
    table_filter: str | None = None

    @model_validator(mode="after")
    def _at_least_one_catalog(self) -> "PIIScanRequest":
        if not self.source_catalog and not self.source_catalogs:
            from api.models.analysis import _NEITHER_CATALOG_MSG

            raise ValueError(_NEITHER_CATALOG_MSG)
        return self


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


class ScheduleRequest(BaseModel):
    name: str
    source_catalog: str
    destination_catalog: str
    cron: str
    template: str | None = None
    clone_type: str = "DEEP"


class RbacPolicyRequest(BaseModel):
    principals: list[str]
    allowed_sources: list[str] = [".*"]
    allowed_destinations: list[str] = [".*"]
    allowed_operations: list[str] = ["*"]
    deny: bool = False
