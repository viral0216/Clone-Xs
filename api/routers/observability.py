"""Data Observability API — unified health scoring and issue tracking."""

from fastapi import APIRouter, Depends, Query

from api.dependencies import get_db_client, get_app_config

router = APIRouter()


def _get_service(client, config):
    from src.observability import ObservabilityService
    wid = config.get("sql_warehouse_id", "")
    return ObservabilityService(client, wid, config=config)


@router.get("/dashboard")
async def get_dashboard(client=Depends(get_db_client)):
    """Full observability dashboard: health score, summary, top issues, category breakdown."""
    config = await get_app_config()
    return _get_service(client, config).get_dashboard()


@router.get("/health-score")
async def get_health_score(client=Depends(get_db_client)):
    """Composite health score (0-100)."""
    config = await get_app_config()
    score = _get_service(client, config).get_health_score()
    return {"health_score": score}


@router.get("/issues")
async def get_issues(limit: int = Query(10, ge=1, le=100), client=Depends(get_db_client)):
    """Top issues across all observability categories."""
    config = await get_app_config()
    return _get_service(client, config).get_top_issues(limit=limit)


@router.get("/trends/{metric}")
async def get_trends(
    metric: str, days: int = Query(30, ge=1, le=365), client=Depends(get_db_client),
):
    """Time-series trend data for sparklines. Metric: freshness, sla, dq."""
    config = await get_app_config()
    return _get_service(client, config).get_trend_data(metric, days=days)


@router.get("/category-health")
async def get_category_health(client=Depends(get_db_client)):
    """Per-category health breakdown with weights."""
    config = await get_app_config()
    return _get_service(client, config).get_category_breakdown()
