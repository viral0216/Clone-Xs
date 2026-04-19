"""Clone request/response models."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class ObjectRef(BaseModel):
    """A single UC object selected for partial-scope clone.

    Used in ``CloneRequest.include_objects`` when the user picks specific
    objects in the UI Scope Picker (rather than cloning the whole catalog).
    Wire JSON uses ``schema`` — the Python attribute is ``schema_name`` to
    avoid shadowing ``BaseModel.schema()``.
    """

    model_config = ConfigDict(populate_by_name=True)

    schema_name: str = Field(alias="schema", serialization_alias="schema")
    name: str
    type: Literal["table", "view", "function", "volume"] = "table"


class TargetWorkspace(BaseModel):
    """Credentials for a target workspace in cross-workspace/cross-cloud migration.

    When set on a CloneRequest, the clone job switches to the cross-workspace
    orchestrator: source workspace creates a Delta Share, target workspace
    consumes it via DEEP CLONE so data physically lands in the target cloud.
    """

    host: str
    auth_method: Literal["pat", "service_principal", "profile"] = "pat"
    token: str | None = None
    client_id: str | None = None
    client_secret: str | None = None
    profile: str | None = None
    warehouse_id: str
    keep_share: bool = False  # keep Delta Share after migration for audit/debug

    @model_validator(mode="after")
    def _creds_present(self) -> "TargetWorkspace":
        host = (self.host or "").strip()
        if not host or not (host.startswith("http://") or host.startswith("https://")):
            raise ValueError("target host must be a full https:// URL")
        if self.auth_method == "pat" and not self.token:
            raise ValueError("target token is required for PAT auth")
        if self.auth_method == "service_principal" and not (self.client_id and self.client_secret):
            raise ValueError("target client_id and client_secret are required for service_principal auth")
        if self.auth_method == "profile" and not self.profile:
            raise ValueError("target profile name is required for profile auth")
        if not (self.warehouse_id or "").strip():
            raise ValueError("target warehouse_id is required (DDL + DEEP CLONE run on target)")
        return self


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
    include_objects: list[ObjectRef] | None = None
    target_workspace: TargetWorkspace | None = None

    @model_validator(mode="after")
    def _different_catalogs(self) -> "CloneRequest":
        # Same-catalog name is fine when the target is a different workspace.
        if self.target_workspace is not None:
            return self
        if self.source_catalog and self.source_catalog == self.destination_catalog:
            raise ValueError("source_catalog and destination_catalog must differ")
        return self


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
