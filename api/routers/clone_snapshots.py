"""Clone Snapshots — named fork points for point-in-time clones.

Distinct from ``api/routers/analysis.py``'s metadata snapshot (schema-DDL
capture for diffing) — this stores per-table Delta versions and a captured
timestamp so the clone orchestrator can replay a catalog's state at the
point the snapshot was taken.
"""

from __future__ import annotations

from fastapi import APIRouter, Body, Depends, HTTPException
from pydantic import BaseModel

from api.dependencies import get_app_config, get_db_client

router = APIRouter()


def _wh(config: dict) -> str:
    wid = (config.get("sql_warehouse_id") or "").strip()
    if not wid:
        raise HTTPException(
            status_code=400,
            detail="sql_warehouse_id is not configured — snapshots need a warehouse for DESCRIBE DETAIL.",
        )
    return wid


class SnapshotCreateRequest(BaseModel):
    source_catalog: str
    name: str
    description: str | None = None
    exclude_schemas: list[str] | None = None


@router.post("")
async def create(
    req: SnapshotCreateRequest,
    client=Depends(get_db_client),
    config=Depends(get_app_config),
):
    """Capture a named snapshot of a catalog's current Delta-version state."""
    from src.clone_snapshots import create_snapshot as _create

    wid = _wh(config)
    # Best-effort "created_by" from the client's host auth (SDK auth chain).
    created_by = ""
    try:
        me = client.current_user.me()
        created_by = getattr(me, "user_name", "") or getattr(me, "display_name", "") or ""
    except Exception:
        pass

    try:
        row = _create(
            client, wid, config,
            source_catalog=req.source_catalog,
            name=req.name,
            description=req.description,
            created_by=created_by,
            exclude_schemas=req.exclude_schemas,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Snapshot create failed: {e}")
    # Strip the heavy tables_json from the response; callers hit GET /{id} for detail.
    row.pop("tables_json", None)
    return row


@router.get("")
async def list_all(
    source_catalog: str | None = None,
    client=Depends(get_db_client),
    config=Depends(get_app_config),
):
    """List snapshots, newest first. Optional `?source_catalog=` filter."""
    from src.clone_snapshots import list_snapshots as _list

    wid = _wh(config)
    try:
        return _list(client, wid, config, source_catalog=source_catalog)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/{snapshot_id}")
async def get_one(
    snapshot_id: str,
    client=Depends(get_db_client),
    config=Depends(get_app_config),
):
    """Return one snapshot including the per-table version + size list."""
    from src.clone_snapshots import get_snapshot as _get

    wid = _wh(config)
    try:
        snap = _get(client, wid, config, snapshot_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    if not snap:
        raise HTTPException(status_code=404, detail="Snapshot not found")
    snap.pop("tables_json", None)
    return snap


@router.delete("/{snapshot_id}")
async def delete_one(
    snapshot_id: str,
    client=Depends(get_db_client),
    config=Depends(get_app_config),
):
    """Remove a snapshot row. Idempotent."""
    from src.clone_snapshots import delete_snapshot as _delete

    wid = _wh(config)
    try:
        _delete(client, wid, config, snapshot_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"snapshot_id": snapshot_id, "deleted": True}
