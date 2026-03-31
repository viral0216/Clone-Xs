"""Monitor endpoints + WebSocket."""

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from api.dependencies import get_db_client, get_app_config

router = APIRouter()


class MonitorRequest(BaseModel):
    source_catalog: str
    destination_catalog: str
    warehouse_id: str | None = None
    exclude_schemas: list[str] = ["information_schema", "default"]
    check_drift: bool = True
    check_counts: bool = False


@router.post("/monitor")
async def monitor_once(
    req: MonitorRequest,
    client=Depends(get_db_client),
):
    """Run a single monitoring check."""
    from src.monitor import monitor_once
    config = await get_app_config()
    wid = req.warehouse_id or config["sql_warehouse_id"]
    result = monitor_once(
        client, wid, req.source_catalog, req.destination_catalog,
        config.get("exclude_schemas", req.exclude_schemas),
        check_drift=req.check_drift,
        check_counts=req.check_counts,
    )
    return result
