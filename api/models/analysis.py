"""Analysis request/response models."""

from typing import Literal

from pydantic import BaseModel, Field


class CatalogRequest(BaseModel):
    """Base request for operations on a single catalog."""
    source_catalog: str
    warehouse_id: str | None = None
    exclude_schemas: list[str] = ["information_schema", "default"]


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
    pattern: str
    search_columns: bool = False


class ProfileRequest(CatalogRequest):
    model_config = {"populate_by_name": True}
    schema_name: str | None = Field(None, alias="schema")
    max_workers: int = 4
    output_path: str | None = None


class EstimateRequest(CatalogRequest):
    price_per_gb: float = 0.023
    include_schemas: list[str] | None = None


class StorageMetricsRequest(CatalogRequest):
    schema_filter: str | None = None
    table_filter: str | None = None
    deep_analyze: bool = False  # When True, runs ANALYZE TABLE (expensive); default uses DESCRIBE DETAIL (fast)


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
