"""Data Environment Manager API endpoints."""

from typing import Optional
from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from api.dependencies import get_db_client, get_app_config

router = APIRouter()


class CreateEnvironmentRequest(BaseModel):
    name: str
    source_catalog: str
    tables: list = []
    masking_profile: str = "none"
    ttl_hours: int = 72
    cost_budget: float = 100.0
    clone_type: str = "SHALLOW"
    access_grants: list = []


class CreateTemplateRequest(BaseModel):
    name: str
    description: str = ""
    config: dict = {}


@router.get("/", summary="List environments")
async def list_environments(
    status: Optional[str] = None,
    client=Depends(get_db_client),
):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.environment_manager import list_environments
    return list_environments(status, client, wid, config)


@router.post("/", summary="Create ephemeral environment")
async def create_environment(req: CreateEnvironmentRequest, client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.environment_manager import create_environment
    return create_environment(**req.model_dump(), client=client, warehouse_id=wid, config=config)


@router.get("/{env_id}", summary="Get environment details")
async def get_environment(env_id: str, client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.environment_manager import get_environment
    return get_environment(env_id, client, wid, config)


@router.post("/{env_id}/extend", summary="Extend environment TTL")
async def extend_environment(
    env_id: str,
    additional_hours: int = Query(default=24, ge=1),
    client=Depends(get_db_client),
):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.environment_manager import extend_environment
    return extend_environment(env_id, additional_hours, client, wid, config)


@router.delete("/{env_id}", summary="Destroy environment")
async def destroy_environment(env_id: str, client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.environment_manager import destroy_environment
    return destroy_environment(env_id, client, wid, config)


@router.post("/cleanup", summary="Clean up expired environments")
async def cleanup(client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.environment_manager import cleanup_expired
    return cleanup_expired(client, wid, config)


# ─── Templates ──────────────────────────────────────────────────────────

@router.get("/templates/list", summary="List environment templates")
async def list_templates(client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.environment_manager import list_templates
    return list_templates(client, wid, config)


@router.post("/templates", summary="Create environment template")
async def create_template(req: CreateTemplateRequest, client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.environment_manager import create_template
    return create_template(req.name, req.description, req.config, client=client, warehouse_id=wid, config=config)


@router.delete("/templates/{template_id}", summary="Delete template")
async def delete_template(template_id: str, client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.environment_manager import delete_template
    delete_template(template_id, client, wid, config)
    return {"status": "deleted"}
