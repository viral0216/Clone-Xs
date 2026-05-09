"""Pydantic request / response models for /api/generate/demo-knowledge.

Mirrors api/models/demo_documents.py and api/models/demo_media.py —
same destination radio, same validators, same response shape — so
the UI's per-tab component can stay parameterised on the category.

Distinct from the other two: no missing-deps path because Knowledge
has no optional Python deps (markdown is plain text, JSON is stdlib).
The `available` flag on /types is always True; the field is kept for
shape-uniformity with the other two endpoints.
"""

from __future__ import annotations

import re
from typing import Literal

from pydantic import BaseModel, Field, model_validator

_UC_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

KnowledgeTypeID = Literal["wiki_article", "qa_pair", "chat_thread"]
KnowledgeDestination = Literal["volume", "volume_with_catalog", "direct_table"]


class DemoKnowledgeRequest(BaseModel):
    """Run the Knowledge generator and emit a corpus into either a UC
    Volume, a Volume + per-tab catalog table, or a direct (inline-text)
    Delta table.

    Knowledge content is text-shaped, so the direct-table variant
    uses a `content STRING` column (vs Documents/Media which use
    `BINARY`). This means demos can do
    `SELECT * FROM demo_knowledge WHERE content LIKE '%billing%'`
    without any decoding step.
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
    destination: KnowledgeDestination = Field(
        default="volume_with_catalog",
        description=(
            "Where bytes land. 'volume' = files only. 'volume_with_catalog' "
            "= files + a Delta table indexing them. 'direct_table' = text "
            "content inline in a Delta STRING column (no Volume writes)."
        ),
    )
    types: list[KnowledgeTypeID] = Field(
        ...,
        min_length=1,
        description="Knowledge types to generate. Pydantic Literal rejects unknown IDs.",
    )
    counts: dict[str, int] = Field(
        default_factory=dict,
        description=(
            "Per-type count. Defaults to 5 per type if a type appears in "
            "`types` but not here. Counts capped at 10000 per type — "
            "knowledge generation is fast (no heavy deps), so the cap "
            "matches Documents."
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
            "Industry context drives topic selection (per-industry topic "
            "list of 10–20 entries) so the corpus has a coherent IA — "
            "RAG demos that filter by topic actually have something to "
            "filter on."
        ),
    )
    realistic_content: bool = Field(
        default=False,
        description=(
            "When True, wiki article body sections and Q&A answers are "
            "AI-drafted (slower, requires API key). chat_thread ignores "
            "the flag — Faker sentences are fine for short messages."
        ),
    )
    faker_locale: str = Field(default="en_US")
    faker_seed: int | None = Field(default=None)
    warehouse_id: str | None = Field(default=None)

    model_config = {"populate_by_name": True}

    @model_validator(mode="after")
    def _identifiers_are_single_part(self) -> "DemoKnowledgeRequest":
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
    def _volume_required_for_volume_destinations(self) -> "DemoKnowledgeRequest":
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
    def _counts_keys_must_be_in_types(self) -> "DemoKnowledgeRequest":
        extras = sorted(set(self.counts.keys()) - set(self.types))
        if extras:
            raise ValueError(
                f"counts references types not in `types`: {extras}. "
                f"Remove them or add them to `types`."
            )
        for type_id, n in self.counts.items():
            if n < 0:
                raise ValueError(f"counts[{type_id!r}] = {n} must be ≥ 0.")
            if n > 10_000:
                raise ValueError(
                    f"counts[{type_id!r}] = {n} exceeds the per-type cap of "
                    f"10000. Split into multiple smaller runs."
                )
        return self


class DemoKnowledgePreviewRequest(BaseModel):
    types: list[KnowledgeTypeID] = Field(default_factory=list)
    counts: dict[str, int] = Field(default_factory=dict)


class DemoKnowledgePerTypePreview(BaseModel):
    type: str
    category: str
    label: str
    count: int
    estimated_bytes: int
    estimated_seconds: float


class DemoKnowledgePreviewResponse(BaseModel):
    per_type: list[DemoKnowledgePerTypePreview]
    total_files: int
    total_bytes: int
    estimated_seconds: float
    unknown_types: list[str] = Field(default_factory=list)


class DemoKnowledgeSubmitResponse(BaseModel):
    job_id: str
    status: str = "queued"


class DemoKnowledgeTypeInfo(BaseModel):
    type: str
    category: str
    label: str
    extension: str


class DemoKnowledgeTypesResponse(BaseModel):
    types: list[DemoKnowledgeTypeInfo]
    available: bool = True
    unavailable_reason: str | None = None
