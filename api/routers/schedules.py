"""Clone / Sync schedules — cron-backed recurring jobs.

Thin wrapper around ``src/scheduler.py``. Schedules are persisted to a JSON
file; when a client is supplied the scheduler also creates a Databricks Job
so the cron runs against the workspace even when Clone-Xs is offline.
"""

from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from api.dependencies import get_app_config, get_db_client

router = APIRouter()


class ScheduleCreateRequest(BaseModel):
    name: str
    source_catalog: str
    destination_catalog: str
    cron: str
    clone_type: Literal["DEEP", "SHALLOW"] = "DEEP"
    template: str | None = None
    job_type: Literal["clone", "sync", "incremental_sync"] = "sync"
    # Optional extras that flow into the Databricks Job config
    schema_name: str | None = None
    sync_mode: str | None = None
    dry_run: bool = False
    drop_extra: bool = False


@router.get("")
async def list_all():
    """Return all saved schedules (both active + paused) with computed next_run."""
    from src.scheduler import list_schedules

    try:
        return list_schedules()
    except Exception as e:
        raise HTTPException(500, f"list_schedules failed: {e}")


@router.post("")
async def create(
    req: ScheduleCreateRequest,
    client=Depends(get_db_client),
    config=Depends(get_app_config),
):
    """Create a schedule. When the Databricks client is available, also
    creates a matching Databricks Job so the cron fires workspace-side."""
    from src.scheduler import create_schedule

    # Pass through the sync-specific fields to the Databricks Job payload.
    job_config = dict(config or {})
    for key in ("schema_name", "sync_mode", "dry_run", "drop_extra"):
        val = getattr(req, key)
        if val is not None:
            job_config[key] = val
    job_config["job_type"] = req.job_type

    try:
        return create_schedule(
            name=req.name,
            source_catalog=req.source_catalog,
            destination_catalog=req.destination_catalog,
            cron=req.cron,
            clone_type=req.clone_type,
            template=req.template,
            config=job_config,
            client=client,
        )
    except Exception as e:
        raise HTTPException(500, f"create_schedule failed: {e}")


@router.post("/{schedule_id}/pause")
async def pause(schedule_id: str):
    """Pause a schedule by ID — clears next_run; cron no longer fires."""
    from src.scheduler import pause_schedule

    result = pause_schedule(schedule_id)
    if result is None:
        raise HTTPException(404, "Schedule not found")
    return result


@router.post("/{schedule_id}/resume")
async def resume(schedule_id: str):
    """Resume a paused schedule."""
    from src.scheduler import resume_schedule

    result = resume_schedule(schedule_id)
    if result is None:
        raise HTTPException(404, "Schedule not found")
    return result


@router.delete("/{schedule_id}")
async def delete_one(schedule_id: str):
    """Delete a schedule. Idempotent — returns ok=True whether or not it existed."""
    from src.scheduler import delete_schedule

    return {"deleted": delete_schedule(schedule_id), "schedule_id": schedule_id}
