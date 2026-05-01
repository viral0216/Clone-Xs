"""Dependency analysis endpoints — view/function dependency graphs and creation order."""

import re

from fastapi import APIRouter, Depends, HTTPException

from api.dependencies import get_db_client, get_app_config
from src.client import execute_sql
from pydantic import BaseModel

router = APIRouter()


class FunctionsMultiRequest(BaseModel):
    """Multi-catalog UDF listing — fans the per-catalog query out.

    The single-catalog `GET /functions/{catalog}` is unchanged; this is
    a sibling for callers (e.g. the Catalog Explorer's Multi mode) that
    need to merge UDFs across N catalogs in one round-trip.
    """
    catalogs: list[str]
    warehouse_id: str | None = None

_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def get_warehouse_id(config: dict) -> str:
    """Safely get the SQL warehouse ID from config."""
    return config.get("sql_warehouse_id", "")


def _validate_catalog(catalog: str) -> str:
    """Validate catalog name is a safe SQL identifier."""
    if not _IDENTIFIER_RE.match(catalog):
        raise HTTPException(status_code=400, detail=f"Invalid catalog name: {catalog}")
    return catalog


@router.get("/functions/{catalog}")
async def list_functions(catalog: str, client=Depends(get_db_client)):
    """List all user-defined functions across all schemas in a catalog."""
    catalog = _validate_catalog(catalog)
    from src.functions_listing import list_functions_for_catalog
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    try:
        return list_functions_for_catalog(client, wid, catalog)
    except Exception:
        return []


@router.post("/functions/multi", summary="List UDFs across multiple catalogs")
async def list_functions_multi_endpoint(
    req: FunctionsMultiRequest, client=Depends(get_db_client),
):
    """List UDFs across multiple catalogs.

    Fans the per-catalog query out across N catalogs in parallel, stamps
    each row with its owning `catalog`, and surfaces per-catalog errors
    instead of aborting on the first failure (mirrors `/stats` multi).
    """
    if not req.catalogs:
        raise HTTPException(status_code=400, detail="`catalogs` must be non-empty")
    for c in req.catalogs:
        _validate_catalog(c)
    from src.functions_listing import list_functions_multi
    config = await get_app_config()
    wid = req.warehouse_id or config.get("sql_warehouse_id", "")
    return list_functions_multi(client, wid, req.catalogs)


@router.get("/views/{catalog}")
async def list_views(catalog: str, client=Depends(get_db_client)):
    """List all views across all schemas in a catalog."""
    catalog = _validate_catalog(catalog)
    config = await get_app_config()
    wid = config.get("sql_warehouse_id", "")
    try:
        # `view_definition` lives on `information_schema.views`, NOT
        # on `information_schema.tables` (that one only has table-level
        # metadata: type, comment, created, last_altered, …). Querying
        # tables for view_definition fails with UNRESOLVED_COLUMN on
        # any UC catalog. Use the dedicated `views` view instead.
        rows = execute_sql(client, wid, f"""
            SELECT table_catalog, table_schema, table_name, view_definition
            FROM {catalog}.information_schema.views
            WHERE table_schema NOT IN ('information_schema', '__internal')
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
        wid = req.warehouse_id or get_warehouse_id(config)
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
        wid = req.warehouse_id or get_warehouse_id(config)
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
        wid = req.warehouse_id or get_warehouse_id(config)
        order = get_ordered_views(client, wid, req.catalog, req.schema_name)
        return {"catalog": req.catalog, "schema": req.schema_name, "creation_order": order}
    except Exception as e:
        return {"catalog": req.catalog, "schema": req.schema_name, "creation_order": [], "error": str(e)}
