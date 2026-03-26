"""ML Assets request/response models."""

from typing import Literal

from pydantic import BaseModel, Field


class MLAssetListRequest(BaseModel):
    """Request to list ML assets in a catalog."""
    source_catalog: str
    warehouse_id: str | None = None
    schemas: list[str] = []


class MLAssetCloneRequest(BaseModel):
    """Request to clone ML assets between catalogs."""
    source_catalog: str
    destination_catalog: str
    warehouse_id: str | None = None
    schemas: list[str] = []
    include_models: bool = True
    include_feature_tables: bool = True
    include_vector_indexes: bool = True
    include_serving_endpoints: bool = False
    clone_type: str = "DEEP"
    copy_versions: bool = True
    dry_run: bool = False
    max_workers: int = 4


class ServingEndpointImportRequest(BaseModel):
    """Request to import a serving endpoint config."""
    config: dict
    dest_catalog: str | None = None
    source_catalog: str | None = None
    name_suffix: str = ""
    dry_run: bool = False
