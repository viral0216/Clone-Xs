"""Analysis request/response models."""

from typing import Literal

from pydantic import BaseModel, Field, model_validator

# Shared validator message — all "single OR multi" request models reject
# requests where neither `source_catalog` nor `source_catalogs` is set.
_NEITHER_CATALOG_MSG = (
    "either `source_catalog` (single) or `source_catalogs` "
    "(list) must be provided"
)


class CatalogRequest(BaseModel):
    """Base request for operations on a single catalog."""
    source_catalog: str
    warehouse_id: str | None = None
    exclude_schemas: list[str] = ["information_schema", "default"]
    # When true, the `/stats` endpoint serves the fast path (one bulk
    # information_schema query, ~1-3s for any catalog size). Default
    # false keeps the existing detailed path (per-table COUNT(*) and
    # DESCRIBE DETAIL — exact row counts, num_files, last_modified —
    # but 30-90s on a 500-table catalog). The Catalog Explorer page
    # passes fast=true so the page loads instantly; the "Detailed"
    # toggle drops back to fast=false for users who need the extra
    # fields. Other endpoints inheriting from CatalogRequest ignore
    # this field — it's only consulted by the /stats route.
    fast: bool = False


class StatsRequest(CatalogRequest):
    """Request for the `/stats` endpoint specifically — supports either
    a single catalog (inherited `source_catalog`) or multiple catalogs
    via `source_catalogs`. When `source_catalogs` is non-empty the
    route fans out the per-catalog stats query in parallel and merges
    the results; each `tables[]` row carries its owning catalog. Other
    endpoints (search, estimate, storage-metrics, profile) keep using
    the bare `CatalogRequest` so their single-catalog contract is
    unchanged."""

    # `source_catalog` becomes optional ONLY on this request — multi
    # callers may pass `source_catalogs` and skip it. The validator
    # below requires at least one of the two.
    source_catalog: str = ""
    source_catalogs: list[str] | None = None

    @model_validator(mode="after")
    def _at_least_one_catalog(self) -> "StatsRequest":
        if not self.source_catalog and not self.source_catalogs:
            raise ValueError(_NEITHER_CATALOG_MSG)
        return self


class CatalogPairRequest(BaseModel):
    """Base request for operations comparing two catalogs."""
    source_catalog: str
    destination_catalog: str
    warehouse_id: str | None = None
    exclude_schemas: list[str] = ["information_schema", "default"]


class SchemaDriftRequest(CatalogPairRequest):
    """Request for schema drift detection with optional schema/table filtering."""
    model_config = {"populate_by_name": True}
    schema_name: str | None = Field(None, alias="schema")
    table: str | None = None


class ValidateRequest(CatalogPairRequest):
    use_checksum: bool = False
    max_workers: int = 4


class SearchRequest(CatalogRequest):
    """Search request — single OR multi catalog.

    Single: `source_catalog` set, `source_catalogs` empty/None — routes
    to `src.search.search_tables`.
    Multi: `source_catalogs` non-empty — routes to
    `src.search_multi.search_tables_multi`. The merged response stamps
    each match with its owning `catalog`.
    """
    # Optional in multi mode. The validator below requires at least one of
    # `source_catalog` / `source_catalogs` to be set.
    source_catalog: str = ""
    source_catalogs: list[str] | None = None
    pattern: str
    search_columns: bool = False

    @model_validator(mode="after")
    def _at_least_one_catalog(self) -> "SearchRequest":
        if not self.source_catalog and not self.source_catalogs:
            raise ValueError(_NEITHER_CATALOG_MSG)
        return self


class ProfileRequest(CatalogRequest):
    model_config = {"populate_by_name": True}
    schema_name: str | None = Field(None, alias="schema")
    max_workers: int = 4
    output_path: str | None = None


class EstimateRequest(CatalogRequest):
    price_per_gb: float = 0.023
    include_schemas: list[str] | None = None
    # Optional destination catalog. When set AND it already exists on the
    # workspace, the estimate response includes a `selective` block showing
    # what a SELECTIVE re-clone (drifted tables only) would cost vs a full
    # clone — drives the side-by-side comparison tile on the /clone preview.
    destination_catalog: str | None = None


class StorageMetricsRequest(CatalogRequest):
    schema_filter: str | None = None
    table_filter: str | None = None
    deep_analyze: bool = False  # When True, runs ANALYZE TABLE (expensive); default uses DESCRIBE DETAIL (fast)


class PermissionsAuditRequest(CatalogRequest):
    """Permissions audit request — find risky GRANTs in a catalog.

    Optional `pii_intersection: true` runs `scan_catalog_for_pii`
    inline and overlays the result, so findings on PII-bearing tables
    escalate one risk level (HIGH → CRITICAL when a public group
    holds SELECT on a PII table). Skipping the overlay is faster but
    surfaces only the structural risk.
    """
    # Inherits source_catalog from CatalogRequest (required, single).
    pii_intersection: bool = False


class StaleScanRequest(CatalogRequest):
    """Stale & orphan detection request — single OR multi catalog.

    Single mode (default): pass `source_catalog: str`. Routes to
    `src.stale_detection.detect_stale_tables`.

    Multi mode: pass `source_catalogs: list[str]`. Routes to
    `src.stale_detection_multi.detect_stale_tables_multi`. Each finding
    is stamped with its owning `catalog`; per-catalog rollups live
    under `per_catalog`.
    """
    # Optional in multi mode. The validator below requires at least one
    # of `source_catalog` / `source_catalogs` to be set.
    source_catalog: str = ""
    source_catalogs: list[str] | None = None
    # `system.access.audit` retention is ~90 days on most workspaces;
    # values above 90 silently behave like 90.
    days_threshold: int = Field(default=90, ge=1, le=365)
    # Skip tables created/altered within this window — they haven't had
    # time to accumulate read activity and would always classify as
    # stale otherwise (false positives).
    min_age_days: int = Field(default=7, ge=0)
    # De-noise filter: drop findings smaller than this size in bytes
    # (only applies when `has_stats=True`; tables without stats always
    # surface so the user can run OPTIMIZE).
    min_size_bytes: int = Field(default=0, ge=0)
    # Opt-in DESCRIBE DETAIL enrichment for small-files detection.
    # Adds 1-3s per scan but flags tables that would benefit from
    # OPTIMIZE for compaction (Delta best practice: file sizes
    # 128 MB – 1 GB; we flag avg < 64 MB with ≥ 50 files).
    check_small_files: bool = False

    @model_validator(mode="after")
    def _at_least_one_catalog(self) -> "StaleScanRequest":
        if not self.source_catalog and not self.source_catalogs:
            raise ValueError(_NEITHER_CATALOG_MSG)
        return self


class TableMaintenanceRequest(BaseModel):
    """Request to run OPTIMIZE or VACUUM on selected tables."""
    source_catalog: str
    warehouse_id: str | None = None
    tables: list[dict] | None = None  # [{"schema": "x", "table": "y"}]
    schema_filter: str | None = None
    retention_hours: int = 168  # VACUUM only
    dry_run: bool = False


class TableProfileRequest(BaseModel):
    """Request for deep-profiling a single table."""
    table_fqn: str
    warehouse_id: str | None = None
    sample_limit: int = 0
    top_n: int = 10
    histogram_bins: int = 20


class ResultsProfileRequest(BaseModel):
    """Request for deep-profiling arbitrary SQL query results."""
    sql: str
    warehouse_id: str | None = None
    top_n: int = 10
    histogram_bins: int = 20


class ExportRequest(CatalogRequest):
    format: Literal["csv", "json"] = "csv"
    output_path: str | None = None


class SnapshotRequest(CatalogRequest):
    output_path: str | None = None
