"""Request/response models for the convert-to-delta endpoint (#13).

Why this model is shaped differently from CloneRequest: a clone has source
and destination; CONVERT TO DELTA has only a single FQN that mutates
in-place. Folding it into CloneRequest with a nullable destination would
sneak the destructive semantic past readers. Keep it explicit.
"""

from typing import Literal

from pydantic import BaseModel, Field, model_validator


class ConvertTargetRef(BaseModel):
    """A single UC table to convert. ``source_format`` is what UC currently
    reports for the table; we don't re-detect server-side because callers
    typically pre-screen via the catalog explorer."""

    fqn: str = Field(..., description="3-part fully qualified name, e.g. catalog.schema.table")
    source_format: Literal["PARQUET", "ICEBERG", "DELTA"] = "ICEBERG"


class ConvertToDeltaRequest(BaseModel):
    """Submit a CONVERT TO DELTA job.

    The request is *intentionally* destructive. Server refuses unless
    ``confirm_destructive`` is True — the UI must surface this clearly
    (typed-name confirmation recommended) before flipping the flag.
    Dry-run bypasses the gate so previews are safe by default.
    """

    targets: list[ConvertTargetRef] = Field(..., min_length=1)
    warehouse_id: str | None = None
    # Caller's explicit acknowledgement that this is destructive on source.
    # Without this flag (and without dry_run), the server returns 400.
    # Plumbed through to convert_to_delta.convert_tables_to_delta which
    # applies the same gate as a defence in depth.
    confirm_destructive: bool = False
    dry_run: bool = False

    @model_validator(mode="after")
    def _confirmed_or_dry_run(self) -> "ConvertToDeltaRequest":
        if not self.dry_run and not self.confirm_destructive:
            raise ValueError(
                "convert-to-delta is destructive on source — set "
                "`confirm_destructive: true` explicitly, or set `dry_run: true` "
                "to preview the SQL without executing"
            )
        return self


class ConvertResultResponse(BaseModel):
    """Per-table outcome — flattened from src.convert_to_delta.ConvertResult."""

    fqn: str
    source_format: str
    status: Literal["converted", "failed", "skipped"]
    duration_ms: int
    error: str | None = None


class ConvertSummaryResponse(BaseModel):
    """Aggregate response from POST /api/convert-to-delta."""

    total: int
    converted: int
    failed: int
    skipped: int
    results: list[ConvertResultResponse]
