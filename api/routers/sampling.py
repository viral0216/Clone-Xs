"""Sampling endpoints — preview and compare table data."""

from fastapi import APIRouter, Depends

from api.dependencies import get_db_client, get_app_config
from api.routers.deps import get_warehouse_id
from pydantic import BaseModel

router = APIRouter()


class SampleRequest(BaseModel):
    catalog: str
    schema_name: str
    table_name: str
    warehouse_id: str | None = None
    limit: int = 10


class CompareSampleRequest(BaseModel):
    source_catalog: str
    destination_catalog: str
    schema_name: str
    table_name: str
    warehouse_id: str | None = None
    limit: int = 5
    order_by: str | None = None


@router.post("/sample")
async def sample_table(req: SampleRequest, client=Depends(get_db_client)):
    """Get sample rows from a table."""
    from src.sampling import sample_table
    config = await get_app_config()
    wid = req.warehouse_id or get_warehouse_id(config)
    rows = sample_table(client, wid, req.catalog, req.schema_name, req.table_name, req.limit)
    return {"catalog": req.catalog, "schema": req.schema_name, "table": req.table_name, "rows": rows}


@router.post("/sample/compare")
async def compare_samples(req: CompareSampleRequest, client=Depends(get_db_client)):
    """Compare sample rows between source and destination tables."""
    from src.sampling import compare_samples
    config = await get_app_config()
    wid = req.warehouse_id or get_warehouse_id(config)
    result = compare_samples(
        client, wid, req.source_catalog, req.destination_catalog,
        req.schema_name, req.table_name, req.limit, req.order_by,
    )
    return result
