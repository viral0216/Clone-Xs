"""Continuous sync endpoints — plan + executor lifecycle.

Plan generation (`POST /plan`) was the v0.11.0 preview-only surface.
Feature 6 (executor) added `POST /start`, `GET /streams`, `GET /streams/{id}`,
`POST /streams/{id}/stop`, and `POST /streams/{id}/restart` — backed by
`src/continuous_sync_runner.py`'s in-process registry.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from api.dependencies import get_db_client

router = APIRouter()


class PlanRequest(BaseModel):
    source_catalog: str
    destination_catalog: str
    tables: list[str] | None = None
    schema_name: str | None = None
    trigger_ms: int = 30_000
    checkpoint_root: str | None = None


class StartRequest(BaseModel):
    """Submit a continuous-sync stream to Databricks. Same shape as PlanRequest;
    the executor calls `build_streaming_plan` internally and submits the
    resulting job spec."""
    source_catalog: str
    destination_catalog: str
    tables: list[str] | None = None
    schema_name: str | None = None
    trigger_ms: int = 30_000
    checkpoint_root: str | None = None


@router.post("/plan")
async def plan(req: PlanRequest, _=Depends(get_db_client)):
    """Build a continuous-sync streaming plan. Returns the plan JSON.

    Used for preview / download. To actually submit, use POST /start.
    """
    from src.continuous_sync import build_streaming_plan

    try:
        return build_streaming_plan(
            source_catalog=req.source_catalog,
            destination_catalog=req.destination_catalog,
            tables=req.tables,
            schema=req.schema_name,
            trigger_ms=req.trigger_ms,
            checkpoint_root=req.checkpoint_root,
        )
    except ValueError as e:
        raise HTTPException(400, str(e))
    except Exception as e:
        raise HTTPException(500, f"Plan generation failed: {e}")


@router.post("/start", summary="Start a continuous sync stream")
async def start(req: StartRequest, client=Depends(get_db_client)):
    """Submit a continuous-sync streaming job to Databricks and register it
    in the runner's process-local stream registry.

    Returns the StreamRecord. If the underlying SDK submit raised, the
    record's `status` will be `failed` and `error` carries the message —
    the endpoint still returns 200 so the UI can render the failure
    consistently with run-time failures (4xx would force the client to
    branch on transport vs. business state).
    """
    from src.continuous_sync_runner import _record_to_dict, start_stream

    try:
        record = start_stream(
            client,
            source_catalog=req.source_catalog,
            destination_catalog=req.destination_catalog,
            tables=req.tables,
            schema=req.schema_name,
            trigger_ms=req.trigger_ms,
            checkpoint_root=req.checkpoint_root,
        )
        return _record_to_dict(record)
    except ValueError as e:
        # Plan-generation argument errors (e.g. neither tables nor schema set)
        # surface as 400 so callers can correct + retry.
        raise HTTPException(400, str(e))


@router.get("/streams", summary="List all continuous sync streams")
async def list_streams(refresh: bool = False, client=Depends(get_db_client)):
    """Return every registered continuous sync stream.

    `refresh=true` polls Databricks for each stream's current state — only
    pass it on detail views or when the user clicks "Refresh", since it's
    one SDK call per stream. Default `false` returns cached state for fast
    list rendering.
    """
    from src.continuous_sync_runner import list_streams as _list

    return _list(client=client if refresh else None, refresh=refresh)


@router.get("/streams/{stream_id}", summary="Get one continuous sync stream")
async def get_stream(stream_id: str, client=Depends(get_db_client)):
    """Detail view for one stream. Always polls Databricks for fresh state
    (detail view = expectation of fresh data)."""
    from src.continuous_sync_runner import _record_to_dict, refresh_stream_status

    try:
        record = refresh_stream_status(client, stream_id)
    except KeyError:
        raise HTTPException(404, f"unknown stream_id: {stream_id}")
    return _record_to_dict(record)


@router.post("/streams/{stream_id}/stop", summary="Stop a continuous sync stream")
async def stop_stream(stream_id: str, client=Depends(get_db_client)):
    """Cancel the underlying Databricks run. Idempotent — calling stop on
    an already-stopped stream returns 200 with the current record."""
    from src.continuous_sync_runner import _record_to_dict
    from src.continuous_sync_runner import stop_stream as _stop

    try:
        record = _stop(client, stream_id)
    except KeyError:
        raise HTTPException(404, f"unknown stream_id: {stream_id}")
    return _record_to_dict(record)


@router.post("/streams/{stream_id}/restart", summary="Restart a continuous sync stream")
async def restart_stream(stream_id: str, client=Depends(get_db_client)):
    """Cancel + resubmit with the same parameters. Used when the stream
    has crashed or hit a schema-drift error that requires a clean restart.
    Returns the new StreamRecord (same `stream_id`, new `run_id`)."""
    from src.continuous_sync_runner import _record_to_dict
    from src.continuous_sync_runner import restart_stream as _restart

    try:
        record = _restart(client, stream_id)
    except KeyError:
        raise HTTPException(404, f"unknown stream_id: {stream_id}")
    return _record_to_dict(record)
