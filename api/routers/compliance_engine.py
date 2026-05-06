"""Regulatory Compliance Automation API endpoints."""

from fastapi import APIRouter, Depends
from api.dependencies import get_db_client, get_app_config

router = APIRouter()


@router.get("/frameworks", summary="List supported frameworks with scores")
async def get_frameworks(client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.compliance_engine import get_frameworks

    return get_frameworks(client, wid, config)


@router.post("/frameworks/{framework_name}/assess", summary="Run compliance assessment")
async def assess_framework(framework_name: str, client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.compliance_engine import collect_evidence

    return collect_evidence(framework_name, client, wid, config)


@router.get("/frameworks/{framework_name}/gaps", summary="Get compliance gaps")
async def get_gaps(framework_name: str, client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.compliance_engine import get_gaps

    return get_gaps(framework_name, client, wid, config)


@router.get("/frameworks/{framework_name}/trend", summary="Get compliance score trend")
async def get_score_trend(framework_name: str, client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.compliance_engine import get_score_trend

    return get_score_trend(framework_name, client, wid, config)


@router.get("/controls", summary="Get all framework control definitions")
async def get_controls():
    from src.compliance_engine import FRAMEWORK_CONTROLS

    return FRAMEWORK_CONTROLS
