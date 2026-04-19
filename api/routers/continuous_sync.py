"""Continuous sync — PREVIEW endpoints.

Generates a plan for a future Structured Streaming execution engine. In
v0.11.0 these endpoints return the plan only; actual submission is planned
for v0.12.0.
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


@router.post("/plan")
async def plan(req: PlanRequest, _=Depends(get_db_client)):
    """Build a continuous-sync streaming plan. Returns the plan JSON.

    The plan is runnable once submitted to a scheduler; Clone-Xs does not
    auto-submit in v0.11.0.
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
