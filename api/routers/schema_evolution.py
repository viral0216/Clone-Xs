"""Schema evolution — detect + apply additive schema changes without re-cloning.

Wraps the existing ``src/schema_evolution.py`` functions in REST endpoints
so the UI can show detected drift and one-click ALTER changes through.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from api.dependencies import get_app_config, get_db_client

router = APIRouter()


def _wh(config: dict) -> str:
    wid = (config.get("sql_warehouse_id") or "").strip()
    if not wid:
        raise HTTPException(400, "sql_warehouse_id is not configured")
    return wid


class DetectRequest(BaseModel):
    source_catalog: str
    destination_catalog: str
    schema_name: str
    table_name: str


class ApplyRequest(BaseModel):
    destination_catalog: str
    schema_name: str
    table_name: str
    changes: dict
    dry_run: bool = True
    drop_removed: bool = False


class EvolveCatalogRequest(BaseModel):
    source_catalog: str
    destination_catalog: str
    exclude_schemas: list[str] | None = None
    dry_run: bool = True
    drop_removed: bool = False
    max_workers: int = 4


@router.post("/detect")
async def detect(
    req: DetectRequest,
    client=Depends(get_db_client),
    config=Depends(get_app_config),
):
    """Compare source and destination schemas for one table.

    Returns added_columns, removed_columns, changed_columns, and is_compatible flag.
    """
    from src.schema_evolution import detect_schema_changes

    wid = _wh(config)
    try:
        return detect_schema_changes(
            client, wid, req.source_catalog, req.destination_catalog,
            req.schema_name, req.table_name,
        )
    except Exception as e:
        raise HTTPException(500, f"Detect failed: {e}")


@router.post("/apply")
async def apply(
    req: ApplyRequest,
    client=Depends(get_db_client),
    config=Depends(get_app_config),
):
    """Apply the detected changes as ALTER TABLE statements.

    Pass `dry_run: true` (default) to preview the SQL without executing.
    """
    from src.schema_evolution import apply_schema_evolution

    wid = _wh(config)
    try:
        return apply_schema_evolution(
            client, wid, req.destination_catalog, req.schema_name,
            req.table_name, req.changes,
            dry_run=req.dry_run, drop_removed=req.drop_removed,
        )
    except Exception as e:
        raise HTTPException(500, f"Apply failed: {e}")


@router.post("/evolve-catalog")
async def evolve_catalog(
    req: EvolveCatalogRequest,
    client=Depends(get_db_client),
    config=Depends(get_app_config),
):
    """Detect + apply schema evolution across every table in a catalog.

    Returns a per-table summary. Pass `dry_run: true` for a preview.
    """
    from src.schema_evolution import evolve_catalog_schema

    wid = _wh(config)
    try:
        return evolve_catalog_schema(
            client, wid, req.source_catalog, req.destination_catalog,
            exclude_schemas=req.exclude_schemas,
            dry_run=req.dry_run, drop_removed=req.drop_removed,
            max_workers=req.max_workers,
        )
    except Exception as e:
        raise HTTPException(500, f"Evolve failed: {e}")
