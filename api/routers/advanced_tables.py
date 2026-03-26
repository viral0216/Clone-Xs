"""Advanced table types endpoints: materialized views, streaming tables, online tables."""

from fastapi import APIRouter, Depends

from api.dependencies import get_db_client, get_app_config
from api.models.advanced_tables import AdvancedTablesCloneRequest, AdvancedTablesListRequest

router = APIRouter()


@router.post("/list", summary="List advanced table types in a catalog")
async def list_advanced(req: AdvancedTablesListRequest, client=Depends(get_db_client)):
    """List materialized views, streaming tables, and online tables in a catalog."""
    from src.clone_advanced_tables import list_all_advanced_tables
    config = await get_app_config()
    wid = req.warehouse_id or config.get("sql_warehouse_id", "")
    return list_all_advanced_tables(client, wid, req.source_catalog, req.schema_filter)


@router.post("/clone", summary="Clone advanced table types between catalogs")
async def clone_advanced(req: AdvancedTablesCloneRequest, client=Depends(get_db_client)):
    """Clone materialized views and online tables from source to destination.

    Streaming tables are exported as definitions (require DLT pipeline for creation).
    """
    from src.clone_advanced_tables import clone_all_advanced_tables
    config = await get_app_config()
    wid = req.warehouse_id or config.get("sql_warehouse_id", "")
    return clone_all_advanced_tables(
        client, wid, req.source_catalog, req.destination_catalog,
        schema=req.schema_filter,
        include_mvs=req.include_materialized_views,
        include_streaming=req.include_streaming_tables,
        include_online=req.include_online_tables,
        dry_run=req.dry_run,
    )
