"""Pydantic request / response models for /api/generate/demo-media.

Mirrors api/models/demo_documents.py — same destination radio, same
validators, same response shape — so the UI can rely on identical
behaviour across the unstructured-generator family.
"""

from __future__ import annotations

import re
from typing import Literal

from pydantic import BaseModel, Field, model_validator

_UC_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# Operator-facing media type IDs. Must match
# `src.demo_media.MEDIA_TYPES` keys exactly.
MediaTypeID = Literal[
    "img_xray",
    "img_scan",
    "img_photo",
    "audio_voicemail",
    "video_clip",
]

MediaDestination = Literal["volume", "volume_with_catalog", "direct_table"]


class DemoMediaRequest(BaseModel):
    """Run the Media generator and emit a corpus into either a UC
    Volume, a Volume + per-tab catalog table, or a direct (inline-bytes)
    Delta table.

    Note on `direct_table` for video_clip: video files can exceed
    Delta's ~16 MB row size cap on a busy run. The orchestrator does
    NOT split or truncate today (v2 work) — operators picking video
    + direct_table should keep counts low or pick volume_with_catalog
    for video-heavy demos.
    """

    catalog: str = Field(..., description="UC catalog the output table / Volume lives in")
    schema_name: str = Field(
        ...,
        alias="schema",
        description="UC schema the output table / Volume lives in",
    )
    volume: str | None = Field(
        default=None,
        description=(
            "Volume name (within `<catalog>.<schema>`). Required when "
            "`destination` is 'volume' or 'volume_with_catalog'; "
            "ignored when 'direct_table' is picked."
        ),
    )
    table_name: str | None = Field(
        default=None,
        description=(
            "Custom table name (within `<catalog>.<schema>`). Defaults to "
            "`demo_media_catalog` (volume_with_catalog) or `demo_media` "
            "(direct_table). Ignored when destination is 'volume'."
        ),
    )
    destination: MediaDestination = Field(
        default="volume_with_catalog",
        description=(
            "Where bytes land. 'volume' = files only. 'volume_with_catalog' "
            "= files + a Delta table indexing them. 'direct_table' = bytes "
            "inline in a Delta table (no Volume writes)."
        ),
    )
    types: list[MediaTypeID] = Field(
        ...,
        min_length=1,
        description="Media types to generate. Pydantic Literal rejects unknown IDs.",
    )
    counts: dict[str, int] = Field(
        default_factory=dict,
        description=(
            "Per-type count. Defaults to 5 per type if a type appears in "
            "`types` but not here. Counts capped at 5000 per type — "
            "lower than Documents because media generation (especially "
            "video_clip) is much slower per file."
        ),
    )
    industry: Literal[
        "healthcare",
        "financial",
        "retail",
        "telecom",
        "manufacturing",
        "energy",
        "education",
        "real_estate",
        "logistics",
        "insurance",
    ] = Field(
        default="healthcare",
        description="Industry context (currently only used by metadata; v2 will drive image / audio variations).",
    )
    realistic_content: bool = Field(
        default=False,
        description=(
            "When True, narrative content (e.g. audio_voicemail transcripts) "
            "is drafted by the AI client (slower, requires either a Databricks "
            "Model Serving endpoint picked in Settings or ANTHROPIC_API_KEY). "
            "Image and video types ignore the flag — their content is "
            "structural, not narrative."
        ),
    )
    ai_token_budget: int = Field(
        default=50_000,
        ge=0,
        le=10_000_000,
        description=(
            "Approximate per-job ceiling on AI tokens. When the budget is "
            "hit, remaining draft calls fall back to templates. Default "
            "50,000 ≈ ~$0.50 on Sonnet at typical max_tokens settings. "
            "Ignored when realistic_content is False; set to 0 to disable "
            "AI entirely even when realistic_content=True."
        ),
    )
    faker_locale: str = Field(default="en_US")
    faker_seed: int | None = Field(default=None)
    warehouse_id: str | None = Field(default=None)

    model_config = {"populate_by_name": True}

    @model_validator(mode="after")
    def _identifiers_are_single_part(self) -> "DemoMediaRequest":
        problems: list[str] = []
        for field_name, value in (
            ("catalog", self.catalog),
            ("schema", self.schema_name),
            ("volume", self.volume),
        ):
            if value is None:
                continue
            if not _UC_IDENTIFIER_RE.match(value):
                problems.append(
                    f"{field_name}={value!r} is not a single Unity Catalog "
                    f"identifier (must start with a letter or underscore "
                    f"and contain only letters, digits, and underscores)."
                )
        if problems:
            raise ValueError(" ".join(problems))
        return self

    @model_validator(mode="after")
    def _volume_required_for_volume_destinations(self) -> "DemoMediaRequest":
        if (
            self.destination in ("volume", "volume_with_catalog")
            and not (self.volume or "").strip()
        ):
            raise ValueError(
                f"`volume` is required when destination={self.destination!r}. "
                f"Pass a Volume name or switch destination to 'direct_table'."
            )
        return self

    @model_validator(mode="after")
    def _counts_keys_must_be_in_types(self) -> "DemoMediaRequest":
        extras = sorted(set(self.counts.keys()) - set(self.types))
        if extras:
            raise ValueError(
                f"counts references types not in `types`: {extras}. "
                f"Remove them or add them to `types`."
            )
        for type_id, n in self.counts.items():
            if n < 0:
                raise ValueError(f"counts[{type_id!r}] = {n} must be ≥ 0.")
            # Lower cap than Documents because video_clip generation
            # is ~500 ms per file — even 5000 is a long-running job.
            if n > 5_000:
                raise ValueError(
                    f"counts[{type_id!r}] = {n} exceeds the per-type cap of "
                    f"5000. Media generation (especially video_clip at "
                    f"~500ms/file) is much slower than Documents — "
                    f"split into multiple smaller runs."
                )
        return self


class DemoMediaPreviewRequest(BaseModel):
    types: list[MediaTypeID] = Field(default_factory=list)
    counts: dict[str, int] = Field(default_factory=dict)


class DemoMediaPerTypePreview(BaseModel):
    type: str
    category: str
    label: str
    count: int
    estimated_bytes: int
    estimated_seconds: float


class DemoMediaPreviewResponse(BaseModel):
    per_type: list[DemoMediaPerTypePreview]
    total_files: int
    total_bytes: int
    estimated_seconds: float
    unknown_types: list[str] = Field(default_factory=list)


class DemoMediaSubmitResponse(BaseModel):
    job_id: str
    status: str = "queued"


class DemoMediaTypeInfo(BaseModel):
    type: str
    category: str
    label: str
    extension: str


class DemoMediaTypesResponse(BaseModel):
    types: list[DemoMediaTypeInfo]
    available: bool
    unavailable_reason: str | None = None
    # Distinct flag for ffmpeg — Pillow can be installed without
    # ffmpeg, in which case images/audio work but video_clip doesn't.
    # The UI uses this to grey out the video_clip checkbox + show
    # an inline install hint.
    ffmpeg_available: bool = True
    ffmpeg_unavailable_reason: str | None = None
