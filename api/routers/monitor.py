"""Monitor endpoints + WebSocket."""

from fastapi import APIRouter, Depends

from api.dependencies import get_db_client, get_app_config
from api.models.analysis import CatalogPairRequest

router = APIRouter()


@router.post("/monitor")
async def monitor_once(
    req: CatalogPairRequest,
    client=Depends(get_db_client),
):
    """Run a single monitoring check."""
    from src.monitor import monitor_once
    config = await get_app_config()
    wid = req.warehouse_id or config["sql_warehouse_id"]
    result = monitor_once(
        client, wid, req.source_catalog, req.destination_catalog,
        config.get("exclude_schemas", []),
        check_drift=getattr(req, "check_drift", True),
        check_counts=getattr(req, "check_counts", False),
    )
    return result
