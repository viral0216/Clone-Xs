"""Dependency analysis endpoints — view/function dependency graphs and creation order."""

from fastapi import APIRouter, Depends

from api.dependencies import get_db_client, get_app_config
from pydantic import BaseModel

router = APIRouter()


class DepsRequest(BaseModel):
    catalog: str
    schema_name: str
    warehouse_id: str | None = None


@router.post("/dependencies/views")
async def view_dependencies(req: DepsRequest, client=Depends(get_db_client)):
    """Get view dependency graph for a schema."""
    from src.dependencies import get_view_dependencies
    config = await get_app_config()
    wid = req.warehouse_id or config["sql_warehouse_id"]
    deps = get_view_dependencies(client, wid, req.catalog, req.schema_name)
    return {"catalog": req.catalog, "schema": req.schema_name, "dependencies": deps}


@router.post("/dependencies/functions")
async def function_dependencies(req: DepsRequest, client=Depends(get_db_client)):
    """Get function dependency graph for a schema."""
    from src.dependencies import get_function_dependencies
    config = await get_app_config()
    wid = req.warehouse_id or config["sql_warehouse_id"]
    deps = get_function_dependencies(client, wid, req.catalog, req.schema_name)
    return {"catalog": req.catalog, "schema": req.schema_name, "dependencies": deps}


@router.post("/dependencies/order")
async def creation_order(req: DepsRequest, client=Depends(get_db_client)):
    """Get topologically sorted creation order for views."""
    from src.dependencies import get_ordered_views
    config = await get_app_config()
    wid = req.warehouse_id or config["sql_warehouse_id"]
    order = get_ordered_views(client, wid, req.catalog, req.schema_name)
    return {"catalog": req.catalog, "schema": req.schema_name, "creation_order": order}
