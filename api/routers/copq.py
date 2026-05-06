"""Cost of Poor Data Quality (COPQ) API endpoints."""

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from api.dependencies import get_db_client, get_app_config

router = APIRouter()


class COPQConfigRequest(BaseModel):
    hourly_engineer_cost: float = 75.0
    per_rerun_cost: float = 25.0
    sla_breach_penalty: float = 500.0
    downstream_disruption_cost: float = 100.0
    avg_responders_per_incident: int = 2


@router.get("/summary", summary="Get COPQ summary with breakdown")
async def get_copq_summary(days: int = Query(default=30, ge=1), client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.copq import get_copq_summary

    return get_copq_summary(days, client, wid, config)


@router.get("/by-table", summary="Get COPQ ranked by table")
async def get_copq_by_table(days: int = Query(default=30, ge=1), client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.copq import get_copq_by_table

    return get_copq_by_table(days, client, wid, config)


@router.get("/trends", summary="Get weekly COPQ trends")
async def get_copq_trends(days: int = Query(default=90, ge=7), client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.copq import get_copq_trends

    return get_copq_trends(days, client, wid, config)


@router.get("/config", summary="Get COPQ cost assumptions")
async def get_copq_config(client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.copq import get_copq_config

    return get_copq_config(client, wid, config)


@router.put("/config", summary="Update COPQ cost assumptions")
async def update_copq_config(req: COPQConfigRequest, client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.copq import update_copq_config

    return update_copq_config(req.model_dump(), client, wid, config)


@router.post("/compute", summary="Auto-compute COPQ events from DQ failures")
async def compute_copq(client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.copq import compute_copq_from_dq

    return compute_copq_from_dq(client, wid, config)
