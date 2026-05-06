"""GET /api/files/list — enumerate file objects under a Volume path.

Used by the convert UI's post-export "N files written" indicator. The
convert path writes files via ``INSERT OVERWRITE DIRECTORY`` into a
Volume sub-path; once the convert returns "converted", the UI fires
this endpoint to count + sum what landed.

Why a dedicated endpoint instead of returning the count from the
convert response: the warehouse doesn't surface a row-count for
``INSERT OVERWRITE DIRECTORY`` in a structured way (the SQL command
returns 0 rows by design). Listing the Volume after the fact is the
honest signal — "the export claims success but here are zero files"
is exactly the failure mode the indicator is meant to catch.
"""

import logging

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from api.dependencies import get_db_client
from src.volume_files import FileEntry, list_files, total_size_bytes

logger = logging.getLogger(__name__)

router = APIRouter()


class FileEntryResponse(BaseModel):
    """One file in the listing — flat shape so the UI can render
    without translating between snake_case fields."""

    path: str
    size_bytes: int
    modification_time: int | None = None


class ListFilesResponse(BaseModel):
    """Aggregate response. ``count`` and ``total_size_bytes`` mirror the
    chip the UI renders so the frontend doesn't have to re-aggregate
    (and so they can't drift from the per-row sizes if pagination ever
    lands)."""

    path: str
    count: int
    total_size_bytes: int
    files: list[FileEntryResponse]


@router.get("/files/list", response_model=ListFilesResponse)
def list_volume_files(
    path: str = Query(
        ...,
        description=(
            "Absolute Volume URI to walk recursively. Must begin with "
            "/Volumes/<catalog>/<schema>/<volume>[/<sub-path>]"
        ),
    ),
    client=Depends(get_db_client),
) -> ListFilesResponse:
    """Walk every file under ``path`` and return their paths + sizes.

    Reads-only; safe to call repeatedly. The convert UI fires this
    once per export-shaped cart row after the convert returns
    "converted" so the operator sees how many files actually landed.

    Returns 400 when ``path`` doesn't start with ``/Volumes/`` —
    callers occasionally paste DBFS or workspace paths and the SDK's
    error for those is opaque. Empty Volumes return 200 with
    ``count=0`` (the convert "succeeded" but produced no files —
    surface that to the operator rather than swallow it).
    """
    if not path.startswith("/Volumes/"):
        raise HTTPException(
            status_code=400,
            detail=(
                f"path {path!r} must start with /Volumes/<catalog>/<schema>/<volume>. "
                f"DBFS / workspace paths are not supported by this endpoint."
            ),
        )
    entries: list[FileEntry] = list_files(client, path)
    return ListFilesResponse(
        path=path,
        count=len(entries),
        total_size_bytes=total_size_bytes(entries),
        files=[
            FileEntryResponse(
                path=e.path,
                size_bytes=e.size_bytes,
                modification_time=e.modification_time,
            )
            for e in entries
        ],
    )
