"""Cross-Table Anomaly Correlation API endpoints."""

from fastapi import APIRouter, Depends, Query
from api.dependencies import get_db_client, get_app_config

router = APIRouter()


@router.get("/groups", summary="Get recent anomaly correlation groups")
async def get_correlation_groups(client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.anomaly_correlation import get_correlation_groups
    return get_correlation_groups(client, wid, config)


@router.get("/groups/{group_id}", summary="Get detail for a correlation group")
async def get_correlation_detail(group_id: str, client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.anomaly_correlation import get_correlation_detail
    return get_correlation_detail(group_id, client, wid, config)


@router.post("/correlate", summary="Run anomaly correlation analysis")
async def correlate(
    time_window_minutes: int = Query(default=120, ge=10),
    client=Depends(get_db_client),
):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.anomaly_correlation import correlate_anomalies
    return correlate_anomalies(time_window_minutes, client, wid, config)


@router.get("/root-causes", summary="Get top root cause tables")
async def get_root_causes(client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.anomaly_correlation import get_root_causes
    return get_root_causes(client, wid, config)
