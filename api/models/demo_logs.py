"""Pydantic request / response models for /api/generate/demo-logs.

Mirrors api/models/demo_knowledge.py — same destination radio, same
validators, same response shape — so the per-tab UI component can
stay parameterised on the category.

Logs has no optional Python deps (pure stdlib + Faker) so the
`available` flag is always True; the field is kept for shape-
uniformity with the other unstructured endpoints.

Two extras specific to Logs vs the other tabs:
  - `lines_per_file` (default 1000) — each "count" is a number of
    *files*; lines_per_file controls the per-file density.
  - `days_back` (default 7) — files are spread across the last N UTC
    days so the corpus has a multi-day shape.
"""

from __future__ import annotations

import re
from typing import Literal

from pydantic import BaseModel, Field, model_validator

_UC_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

LogTypeID = Literal["nginx_access", "app_json", "syslog", "otel_trace"]
LogDestination = Literal["volume", "volume_with_catalog", "direct_table"]


class DemoLogsRequest(BaseModel):
    """Run the Logs generator and emit a corpus into either a UC
    Volume, a Volume + per-file catalog table, or a direct (one-row-
    per-line) Delta table.

    Per-line direct-table is the natural shape for log analytics —
    operators query
    ``SELECT count(*) FROM demo_logs WHERE level = 'ERROR'``, not
    file-level aggregates.
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
            "`demo_logs_catalog` (volume_with_catalog) or `demo_logs` "
            "(direct_table). Ignored when destination is 'volume'."
        ),
    )
    destination: LogDestination = Field(
        default="volume_with_catalog",
        description=(
            "Where bytes land. 'volume' = files only. 'volume_with_catalog' "
            "= files + a per-file Delta catalog table. 'direct_table' = one "
            "row per LOG LINE in a Delta table (no Volume writes)."
        ),
    )
    types: list[LogTypeID] = Field(
        ...,
        min_length=1,
        description="Log types to generate. Pydantic Literal rejects unknown IDs.",
    )
    counts: dict[str, int] = Field(
        default_factory=dict,
        description=(
            "Per-type FILE count. Defaults to 5 per type if a type appears "
            "in `types` but not here. Capped at 1000 files per type — the "
            "cap is lower than Documents/Knowledge because each file is "
            "1000+ lines, so 1000 files = 1M log lines per type."
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
        description=(
            "Industry context drives the service-name pool and (for "
            "nginx_access) the URL path templates so the corpus reads as "
            "plausible-looking observability data for the picked industry."
        ),
    )
    lines_per_file: int = Field(
        default=1000,
        ge=1,
        le=100_000,
        description=(
            "Lines per file. Default 1000. Capped at 100000 — beyond that "
            "individual files become unwieldy for direct-table per-line "
            "inserts (10K files × 100K lines × multiple INSERT VALUES "
            "round-trips = warehouse hot-spot)."
        ),
    )
    days_back: int = Field(
        default=7,
        ge=1,
        le=365,
        description=(
            "Files are distributed across the last N UTC days. Default 7. "
            "The day appears in the file path "
            "(`/logs/<type>/<service>/<day>/...`) so operators can "
            "exercise day-partitioned reads without bespoke setup."
        ),
    )
    faker_locale: str = Field(default="en_US")
    faker_seed: int | None = Field(default=None)
    warehouse_id: str | None = Field(default=None)

    model_config = {"populate_by_name": True}

    @model_validator(mode="after")
    def _identifiers_are_single_part(self) -> "DemoLogsRequest":
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
    def _volume_required_for_volume_destinations(self) -> "DemoLogsRequest":
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
    def _counts_keys_must_be_in_types(self) -> "DemoLogsRequest":
        extras = sorted(set(self.counts.keys()) - set(self.types))
        if extras:
            raise ValueError(
                f"counts references types not in `types`: {extras}. "
                f"Remove them or add them to `types`."
            )
        for type_id, n in self.counts.items():
            if n < 0:
                raise ValueError(f"counts[{type_id!r}] = {n} must be ≥ 0.")
            if n > 1_000:
                raise ValueError(
                    f"counts[{type_id!r}] = {n} exceeds the per-type cap of "
                    f"1000 files. With the default 1000 lines/file that's "
                    f"already 1M log lines per type. Split into smaller runs."
                )
        return self


class DemoLogsPreviewRequest(BaseModel):
    types: list[LogTypeID] = Field(default_factory=list)
    counts: dict[str, int] = Field(default_factory=dict)
    lines_per_file: int = Field(default=1000, ge=1, le=100_000)


class DemoLogsPerTypePreview(BaseModel):
    type: str
    category: str
    label: str
    count: int
    line_count: int
    estimated_bytes: int
    estimated_seconds: float


class DemoLogsPreviewResponse(BaseModel):
    per_type: list[DemoLogsPerTypePreview]
    total_files: int
    total_lines: int
    total_bytes: int
    estimated_seconds: float
    unknown_types: list[str] = Field(default_factory=list)


class DemoLogsSubmitResponse(BaseModel):
    job_id: str
    status: str = "queued"


class DemoLogsTypeInfo(BaseModel):
    type: str
    category: str
    label: str
    extension: str


class DemoLogsTypesResponse(BaseModel):
    types: list[DemoLogsTypeInfo]
    available: bool = True
    unavailable_reason: str | None = None
