"""Streaming / MV data-clone generator endpoint — PREVIEW in v0.11.0."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from api.dependencies import get_db_client

router = APIRouter()


class GenerateRequest(BaseModel):
    source_catalog: str
    destination_catalog: str
    schema_name: str
    advanced_tables: list[dict]
    target_schema: str | None = None
    pipeline_name: str | None = None


@router.post("/generate")
async def generate(req: GenerateRequest, _=Depends(get_db_client)):
    """Generate a DLT pipeline spec + notebook SQL that will materialize
    MV / streaming-table data on the destination catalog."""
    from src.streaming_clone_generator import generate_dlt_pipeline_spec

    try:
        return generate_dlt_pipeline_spec(
            source_catalog=req.source_catalog,
            destination_catalog=req.destination_catalog,
            schema=req.schema_name,
            advanced_tables=req.advanced_tables,
            target_schema=req.target_schema,
            pipeline_name=req.pipeline_name,
        )
    except ValueError as e:
        raise HTTPException(400, str(e))
    except Exception as e:
        raise HTTPException(500, f"Generation failed: {e}")
