"""Trust Score API endpoints."""

from typing import Optional
from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from api.dependencies import get_db_client, get_app_config

router = APIRouter()


class UpdateWeightsRequest(BaseModel):
    dq: float = 0.30
    freshness: float = 0.25
    anomaly: float = 0.15
    schema_stability: float = 0.10
    pii: float = 0.10
    lineage: float = 0.10


@router.get("/scores/{catalog}", summary="Get trust scores for a catalog")
async def get_trust_scores(catalog: str, client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.trust_score import get_trust_scores

    return get_trust_scores(catalog, client, wid, config)


@router.get("/scores/{catalog}/{schema}/{table}", summary="Get trust score for a specific table")
async def get_table_trust_score(
    catalog: str, schema: str, table: str, client=Depends(get_db_client)
):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    table_fqn = f"{catalog}.{schema}.{table}"
    from src.trust_score import compute_trust_score

    return compute_trust_score(table_fqn, client, wid, config)


@router.get("/scores/{catalog}/{schema}/{table}/history", summary="Trust score trend for a table")
async def get_trust_score_history(
    catalog: str, schema: str, table: str, client=Depends(get_db_client)
):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.trust_score import get_trust_score_history

    return get_trust_score_history(f"{catalog}.{schema}.{table}", client, wid, config)


@router.post("/compute/{catalog}", summary="Compute trust scores for all tables in a catalog")
async def compute_catalog_scores(
    catalog: str,
    schema_filter: Optional[str] = Query(default=None),
    client=Depends(get_db_client),
):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.trust_score import compute_trust_scores_for_catalog

    return compute_trust_scores_for_catalog(catalog, schema_filter, client, wid, config)


@router.get("/config", summary="Get trust score dimension weights")
async def get_weights(client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.trust_score import get_weights

    return get_weights(client, wid, config)


@router.put("/config", summary="Update trust score dimension weights")
async def update_weights(req: UpdateWeightsRequest, client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.trust_score import update_weights

    return update_weights(req.model_dump(), client=client, warehouse_id=wid, config=config)
