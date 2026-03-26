"""Federation request/response models."""

from pydantic import BaseModel


class ForeignTablesRequest(BaseModel):
    """Request to list tables in a foreign catalog."""
    catalog: str
    warehouse_id: str | None = None
    schema_filter: str | None = None


class ConnectionCloneRequest(BaseModel):
    """Request to clone a connection."""
    connection_name: str
    new_name: str | None = None
    credentials: dict | None = None
    dry_run: bool = False


class MigrateRequest(BaseModel):
    """Request to migrate a foreign table to managed Delta."""
    foreign_fqn: str
    dest_fqn: str
    warehouse_id: str | None = None
    dry_run: bool = False
