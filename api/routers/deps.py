"""Dependency analysis endpoints — view/function dependency graphs and creation order."""

import re

from fastapi import APIRouter, Depends, HTTPException

from api.dependencies import get_db_client, get_app_config
from src.client import execute_sql
from pydantic import BaseModel

router = APIRouter()

_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _validate_catalog(catalog: str) -> str:
    """Validate catalog name is a safe SQL identifier."""
    if not _IDENTIFIER_RE.match(catalog):
        raise HTTPException(status_code=400, detail=f"Invalid catalog name: {catalog}")
    return catalog


@router.get("/functions/{catalog}")
async def list_functions(catalog: str, client=Depends(get_db_client)):
    catalog = _validate_catalog(catalog)
    """List all user-defined functions across all schemas in a catalog."""
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    try:
        rows = execute_sql(client, wid, f"""
            SELECT routine_catalog, routine_schema, routine_name, routine_type,
                   data_type, routine_definition
            FROM {catalog}.information_schema.routines
            WHERE routine_type = 'FUNCTION'
            AND routine_schema NOT IN ('information_schema', '__internal')
            ORDER BY routine_schema, routine_name
        """)
        return [
            {
                "name": r.get("routine_name", ""),
                "schema": r.get("routine_schema", ""),
                "full_name": f"{catalog}.{r.get('routine_schema', '')}.{r.get('routine_name', '')}",
                "data_type": r.get("data_type", ""),
                "definition": (r.get("routine_definition", "") or "")[:200],
            }
            for r in rows
        ]
    except Exception:
        return []


@router.get("/views/{catalog}")
async def list_views(catalog: str, client=Depends(get_db_client)):
    """List all views across all schemas in a catalog."""
    catalog = _validate_catalog(catalog)
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    try:
        rows = execute_sql(client, wid, f"""
            SELECT table_catalog, table_schema, table_name, view_definition
            FROM {catalog}.information_schema.tables
            WHERE table_type = 'VIEW'
            AND table_schema NOT IN ('information_schema', '__internal')
            ORDER BY table_schema, table_name
        """)
        return [
            {
                "name": r.get("table_name", ""),
                "schema": r.get("table_schema", ""),
                "full_name": f"{catalog}.{r.get('table_schema', '')}.{r.get('table_name', '')}",
                "definition": (r.get("view_definition", "") or "")[:200],
            }
            for r in rows
        ]
    except Exception:
        return []


class DepsRequest(BaseModel):
    catalog: str
    schema_name: str
    warehouse_id: str | None = None


@router.post("/dependencies/views")
async def view_dependencies(req: DepsRequest, client=Depends(get_db_client)):
    """Get view dependency graph for a schema."""
    try:
        from src.dependencies import get_view_dependencies
        config = await get_app_config()
        wid = req.warehouse_id or config["sql_warehouse_id"]
        deps = get_view_dependencies(client, wid, req.catalog, req.schema_name)
        return {"catalog": req.catalog, "schema": req.schema_name, "dependencies": deps}
    except Exception as e:
        return {"catalog": req.catalog, "schema": req.schema_name, "dependencies": [], "error": str(e)}


@router.post("/dependencies/functions")
async def function_dependencies(req: DepsRequest, client=Depends(get_db_client)):
    """Get function dependency graph for a schema."""
    try:
        from src.dependencies import get_function_dependencies
        config = await get_app_config()
        wid = req.warehouse_id or config["sql_warehouse_id"]
        deps = get_function_dependencies(client, wid, req.catalog, req.schema_name)
        return {"catalog": req.catalog, "schema": req.schema_name, "dependencies": deps}
    except Exception as e:
        return {"catalog": req.catalog, "schema": req.schema_name, "dependencies": [], "error": str(e)}


@router.post("/dependencies/order")
async def creation_order(req: DepsRequest, client=Depends(get_db_client)):
    """Get topologically sorted creation order for views."""
    try:
        from src.dependencies import get_ordered_views
        config = await get_app_config()
        wid = req.warehouse_id or config["sql_warehouse_id"]
        order = get_ordered_views(client, wid, req.catalog, req.schema_name)
        return {"catalog": req.catalog, "schema": req.schema_name, "creation_order": order}
    except Exception as e:
        return {"catalog": req.catalog, "schema": req.schema_name, "creation_order": [], "error": str(e)}
