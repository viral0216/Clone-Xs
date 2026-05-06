"""DQ Coverage Map & Gap Analysis API endpoints."""

from typing import Optional
from fastapi import APIRouter, Depends, Query
from api.dependencies import get_db_client, get_app_config

router = APIRouter()


@router.get("/{catalog}", summary="Get coverage map for a catalog")
async def get_coverage(catalog: str, client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.coverage_map import get_coverage

    return get_coverage(catalog, client, wid, config)


@router.get("/{catalog}/summary", summary="Get coverage summary for a catalog")
async def get_coverage_summary(catalog: str, client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.coverage_map import get_coverage_summary

    return get_coverage_summary(catalog, client, wid, config)


@router.get("/{catalog}/gaps", summary="Get uncovered tables ranked by priority")
async def get_gaps(catalog: str, client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.coverage_map import get_gaps

    return get_gaps(catalog, client, wid, config)


@router.post("/{catalog}/compute", summary="Compute coverage snapshot for a catalog")
async def compute_coverage(
    catalog: str,
    schema_filter: Optional[str] = Query(default=None),
    client=Depends(get_db_client),
):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.coverage_map import compute_coverage

    return compute_coverage(catalog, schema_filter, client, wid, config)
