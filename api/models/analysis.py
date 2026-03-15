"""Analysis request/response models."""

from typing import Literal

from pydantic import BaseModel


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


class ValidateRequest(CatalogPairRequest):
    use_checksum: bool = False
    max_workers: int = 4


class SearchRequest(CatalogRequest):
    pattern: str
    search_columns: bool = False


class ProfileRequest(CatalogRequest):
    max_workers: int = 4
    output_path: str | None = None


class EstimateRequest(CatalogRequest):
    price_per_gb: float = 0.023
    include_schemas: list[str] | None = None


class ExportRequest(CatalogRequest):
    format: Literal["csv", "json"] = "csv"
    output_path: str | None = None


class SnapshotRequest(CatalogRequest):
    output_path: str | None = None
