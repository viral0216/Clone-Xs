"""Pydantic request / response models for /api/capture (Live Capture tab).

Distinct from the synthetic `/api/generate/demo-*` models:

  - The frame upload endpoint accepts ``multipart/form-data`` (so it
    isn't represented as a Pydantic body model — see
    `api.routers.demo_capture.upload_frame`).
  - This module covers the JSON ``/init`` and the ``/recent`` response
    only.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

CaptureType = Literal["photo", "video"]


class DemoCaptureInitRequest(BaseModel):
    """Idempotent init: ensure the volume + capture table exist.

    Called from the UI when the Live Capture tab mounts so the first
    `/frame` upload doesn't pay the volume / table create cost.
    """

    catalog: str = Field(..., description="UC catalog the table / Volume lives in")
    schema_name: str = Field(
        ...,
        alias="schema",
        description="UC schema the table / Volume lives in",
    )
    volume: str | None = Field(
        default=None,
        description=(
            "Volume name (within `<catalog>.<schema>`). Defaults to "
            "`demo_unstructured`. Created with CREATE VOLUME IF NOT EXISTS."
        ),
    )
    table_name: str | None = Field(
        default=None,
        description=(
            "Custom table name (within `<catalog>.<schema>`). Defaults to "
            "`demo_capture_catalog`. Created with CREATE TABLE IF NOT "
            "EXISTS — captures accumulate across browser sessions."
        ),
    )
    warehouse_id: str | None = Field(
        default=None,
        description=(
            "SQL warehouse ID. Falls back to the workspace default "
            "configured in app config when omitted."
        ),
    )


class DemoCaptureInitResponse(BaseModel):
    volume_path: str
    table_fqn: str


class DemoCaptureRow(BaseModel):
    """One captured frame as it lives in the catalog table.

    Returned by `POST /api/capture/frame` (the row that was just
    inserted) and by `GET /api/capture/recent` (the N most-recent
    rows). Never carries the inline ``content BINARY`` bytes — those
    can be huge and the UI doesn't need them.
    """

    capture_id: str
    capture_type: CaptureType
    file_path: str
    file_extension: str
    size_bytes: int
    width: int | None = None
    height: int | None = None
    duration_ms: int | None = None
    mime_type: str | None = None
    industry: str
    caption: str | None = None
    alt_text: str | None = None
    summary: str | None = None
    tags: str | None = None
    detected_text: str | None = None
    scene_category: str | None = None
    captured_at: str  # ISO timestamp string from the orchestrator
    session_id: str | None = None
    submitted_by: str | None = None
    table_fqn: str | None = None


class DemoCaptureRecentResponse(BaseModel):
    rows: list[DemoCaptureRow]
    table_fqn: str
