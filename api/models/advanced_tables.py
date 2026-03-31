"""Advanced table types request/response models."""

from pydantic import BaseModel


class AdvancedTablesListRequest(BaseModel):
    """Request to list advanced table types in a catalog."""
    source_catalog: str
    warehouse_id: str | None = None
    schema_filter: str | None = None


class AdvancedTablesCloneRequest(BaseModel):
    """Request to clone advanced table types between catalogs."""
    source_catalog: str
    destination_catalog: str
    warehouse_id: str | None = None
    schema_filter: str | None = None
    include_materialized_views: bool = True
    include_streaming_tables: bool = True
    include_online_tables: bool = True
    dry_run: bool = False
