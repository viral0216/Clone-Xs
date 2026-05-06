"""Automated Remediation Playbooks API endpoints."""

from typing import Optional
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from api.dependencies import get_db_client, get_app_config

router = APIRouter()


class CreatePlaybookRequest(BaseModel):
    name: str
    description: str = ""
    trigger_type: str
    trigger_config: dict = {}
    conditions: list = []
    actions: list = []
    max_executions_per_hour: int = 5


class UpdatePlaybookRequest(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    trigger_type: Optional[str] = None
    trigger_config: Optional[dict] = None
    conditions: Optional[list] = None
    actions: Optional[list] = None
    enabled: Optional[bool] = None
    max_executions_per_hour: Optional[int] = None


@router.get("/", summary="List all playbooks")
async def list_playbooks(client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.playbooks import list_playbooks

    return list_playbooks(client, wid, config)


@router.post("/", summary="Create a playbook")
async def create_playbook(req: CreatePlaybookRequest, client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.playbooks import create_playbook

    return create_playbook(
        req.name,
        req.trigger_type,
        req.trigger_config,
        req.conditions,
        req.actions,
        req.description,
        req.max_executions_per_hour,
        client=client,
        warehouse_id=wid,
        config=config,
    )


@router.get("/templates", summary="Get playbook templates")
async def get_templates():
    from src.playbooks import get_templates

    return get_templates()


@router.get("/{playbook_id}", summary="Get a playbook")
async def get_playbook(playbook_id: str, client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.playbooks import get_playbook

    return get_playbook(playbook_id, client, wid, config)


@router.put("/{playbook_id}", summary="Update a playbook")
async def update_playbook(
    playbook_id: str, req: UpdatePlaybookRequest, client=Depends(get_db_client)
):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.playbooks import update_playbook

    updates = {k: v for k, v in req.model_dump().items() if v is not None}
    return update_playbook(playbook_id, updates, client, wid, config)


@router.delete("/{playbook_id}", summary="Delete a playbook")
async def delete_playbook(playbook_id: str, client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.playbooks import delete_playbook

    delete_playbook(playbook_id, client, wid, config)
    return {"status": "deleted"}


@router.post("/{playbook_id}/execute", summary="Execute a playbook")
async def execute_playbook(playbook_id: str, client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.playbooks import execute_playbook

    return execute_playbook(playbook_id, client=client, warehouse_id=wid, config=config)


@router.get("/{playbook_id}/history", summary="Get playbook execution history")
async def get_execution_history(playbook_id: str, client=Depends(get_db_client)):
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    from src.playbooks import get_execution_history

    return get_execution_history(playbook_id, client, wid, config)
