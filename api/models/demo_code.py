"""Pydantic request / response models for /api/generate/demo-code.

Mirrors api/models/demo_knowledge.py — same destination radio +
validators. Code has no optional Python deps so `available` is
always True.

Distinct from the other unstructured tabs:
  - Each "count" is a number of REPOS, not files. Each repo holds
    ~25-35 source files. Per-type cap is 50 repos to keep the total
    file count manageable (50 × 30 = 1500 files per type).
  - direct_table is one row per FILE with `content STRING` (source
    code is text-shaped, fits comfortably in a STRING column for any
    realistic file size).
"""

from __future__ import annotations

import re
from typing import Literal

from pydantic import BaseModel, Field, model_validator

_UC_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

CodeTypeID = Literal["python_repo", "js_repo", "java_repo"]
CodeDestination = Literal["volume", "volume_with_catalog", "direct_table"]


class DemoCodeRequest(BaseModel):
    """Run the Code generator and emit synthetic source-code repos
    into either a UC Volume, a Volume + per-file catalog table, or a
    direct table with `content STRING` per file.
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
            "`demo_code_catalog` (volume_with_catalog) or `demo_code` "
            "(direct_table). Ignored when destination is 'volume'."
        ),
    )
    destination: CodeDestination = Field(
        default="volume_with_catalog",
        description=(
            "Where bytes land. 'volume' = files only. 'volume_with_catalog' "
            "= files + a per-file Delta catalog table. 'direct_table' = one "
            "row per source file with `content STRING` inline."
        ),
    )
    types: list[CodeTypeID] = Field(
        ...,
        min_length=1,
        description="Code types to generate (one type = one language).",
    )
    counts: dict[str, int] = Field(
        default_factory=dict,
        description=(
            "Per-type REPO count. Defaults to 3 per type if a type appears "
            "in `types` but not here. Each repo is ~25-35 source files. "
            "Capped at 50 repos per type — that's already 1500 files."
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
            "Industry context drives repo naming (e.g. 'payments-service-...' "
            "for financial, 'patient-portal-...' for healthcare) so the "
            "corpus has plausible naming for the picked industry."
        ),
    )
    realistic_content: bool = Field(
        default=False,
        description=(
            "When True, function/method bodies are AI-drafted (slower, "
            "requires either a Databricks Model Serving endpoint picked in "
            "Settings or ANTHROPIC_API_KEY). When off, bodies are templated "
            "with Faker-generated identifiers."
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
    def _identifiers_are_single_part(self) -> "DemoCodeRequest":
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
    def _volume_required_for_volume_destinations(self) -> "DemoCodeRequest":
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
    def _counts_keys_must_be_in_types(self) -> "DemoCodeRequest":
        extras = sorted(set(self.counts.keys()) - set(self.types))
        if extras:
            raise ValueError(
                f"counts references types not in `types`: {extras}. "
                f"Remove them or add them to `types`."
            )
        for type_id, n in self.counts.items():
            if n < 0:
                raise ValueError(f"counts[{type_id!r}] = {n} must be ≥ 0.")
            if n > 50:
                raise ValueError(
                    f"counts[{type_id!r}] = {n} exceeds the per-type cap of "
                    f"50 repos. Each repo is ~30 files, so 50 = 1500 files."
                )
        return self


class DemoCodePreviewRequest(BaseModel):
    types: list[CodeTypeID] = Field(default_factory=list)
    counts: dict[str, int] = Field(default_factory=dict)


class DemoCodePerTypePreview(BaseModel):
    type: str
    category: str
    label: str
    count: int
    file_count: int
    estimated_bytes: int
    estimated_seconds: float


class DemoCodePreviewResponse(BaseModel):
    per_type: list[DemoCodePerTypePreview]
    total_repos: int
    total_files: int
    total_bytes: int
    estimated_seconds: float
    unknown_types: list[str] = Field(default_factory=list)


class DemoCodeSubmitResponse(BaseModel):
    job_id: str
    status: str = "queued"


class DemoCodeTypeInfo(BaseModel):
    type: str
    category: str
    label: str
    extension: str
    language: str


class DemoCodeTypesResponse(BaseModel):
    types: list[DemoCodeTypeInfo]
    available: bool = True
    unavailable_reason: str | None = None
