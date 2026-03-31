"""Lakehouse Monitor request/response models."""

from pydantic import BaseModel


class MonitorListRequest(BaseModel):
    """Request to list quality monitors in a catalog."""
    source_catalog: str
    warehouse_id: str | None = None
    schema_filter: str | None = None


class MonitorCloneRequest(BaseModel):
    """Request to clone quality monitors between catalogs."""
    source_catalog: str
    destination_catalog: str
    warehouse_id: str | None = None
    schema_filter: str | None = None
    dry_run: bool = False


class MonitorCompareRequest(BaseModel):
    """Request to compare monitor metrics between source and destination."""
    source_table: str
    destination_table: str
    warehouse_id: str | None = None
