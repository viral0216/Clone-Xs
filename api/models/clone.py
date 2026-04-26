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
    # How to handle re-runs of an already-cloned table.
    #   snapshot_once = CREATE TABLE IF NOT EXISTS ... DEEP CLONE  (no-op on existing tables)
    #   incremental   = CREATE OR REPLACE TABLE ... DEEP CLONE     (mirror source updates)
    #   force_full    = DROP + CREATE                              (full re-clone every run)
    # incremental and force_full overwrite any target-side writes to cloned tables.
    data_sync_mode: Literal["snapshot_once", "incremental", "force_full"] = "snapshot_once"
    # Delta Sharing refuses to share tables that have column masks or row filters.
    # When True, Clone-Xs will:
    #   1. Inventory mask/filter functions on each source table before adding to share
    #   2. Drop them on source so the table can be added to the share
    #   3. After the clone completes, re-apply the same masks/filters on the target
    #   4. For data_sync_mode in (snapshot_once, force_full): also restore on source
    #      For data_sync_mode=incremental: leave source masks dropped (otherwise
    #      ongoing share reads would fail on Databricks-side; logs a warning)
    auto_handle_masks: bool = False
    # When True, drop the deterministic share/recipient/shared-catalog at end of run.
    # Default False: deterministic objects are designed to persist between runs so
    # subsequent re-clones reuse them (true incremental sync). Set True for one-shot
    # migrations where you don't intend to re-run.
    cleanup_after_clone: bool = False
    # When True, re-runs also `ALTER SHARE … REMOVE TABLE` for tables that are in
    # the share but no longer exist in the source. Default False because pruning
    # is destructive on the share side.
    prune_share_extras: bool = False

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


class TargetWorkspaceConnect(BaseModel):
    """Same auth fields as TargetWorkspace, but without warehouse_id.

    Used by /api/target/warehouses to discover warehouses *before* the user
    has picked one — they can't supply a warehouse_id at that point.
    """

    host: str
    auth_method: Literal["pat", "service_principal", "profile"] = "pat"
    token: str | None = None
    client_id: str | None = None
    client_secret: str | None = None
    profile: str | None = None

    @model_validator(mode="after")
    def _creds_present(self) -> "TargetWorkspaceConnect":
        host = (self.host or "").strip()
        if not host or not (host.startswith("http://") or host.startswith("https://")):
            raise ValueError("target host must be a full https:// URL")
        if self.auth_method == "pat" and not self.token:
            raise ValueError("target token is required for PAT auth")
        if self.auth_method == "service_principal" and not (self.client_id and self.client_secret):
            raise ValueError("target client_id and client_secret are required for service_principal auth")
        if self.auth_method == "profile" and not self.profile:
            raise ValueError("target profile name is required for profile auth")
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
    # Optional `TBLPROPERTIES (...)` overrides emitted on every per-table
    # CLONE statement (e.g. {"delta.logRetentionDuration": "3650 days"} for
    # archival tables). Setting via ALTER TABLE post-clone is too late for
    # retention windows because the first commit has already happened.
    clone_tbl_properties: dict[str, str] | None = None
    # Cross-workspace object-type toggles. Effective only when target_workspace
    # is set; same-workspace clone_catalog.py does not read these.
    clone_views: bool = True
    clone_functions: bool = True
    clone_volumes: bool = True
    # Per-file cap (MB) for managed-volume file copy via Databricks Files API.
    # Files larger than this are skipped with a warning. Effective only for
    # cross-workspace migrations.
    volume_max_file_mb: int = 500
    # Runtime guardrails (None = no limit)
    max_duration_min: int | None = None
    max_tables: int | None = None
    # Named snapshot to clone from. When set, the orchestrator resolves the
    # snapshot's captured per-table Delta version and issues DEEP CLONE …
    # VERSION AS OF … statements instead of cloning current state.
    source_snapshot_id: str | None = None

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
