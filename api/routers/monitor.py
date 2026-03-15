"""Monitor endpoints + WebSocket."""

from fastapi import APIRouter, Depends, WebSocket, WebSocketDisconnect

from api.dependencies import get_db_client, get_app_config

router = APIRouter()


@router.post("/monitor")
async def monitor_once(
    source_catalog: str,
    destination_catalog: str,
    warehouse_id: str | None = None,
    check_drift: bool = True,
    check_counts: bool = False,
    client=Depends(get_db_client),
):
    """Run a single monitoring check."""
    from src.monitor import monitor_once
    config = await get_app_config()
    wid = warehouse_id or config["sql_warehouse_id"]
    result = monitor_once(
        client, wid, source_catalog, destination_catalog,
        config.get("exclude_schemas", []),
        check_drift=check_drift, check_counts=check_counts,
    )
    return result
