"""Pydantic request / response models for the /api/generate/demo-documents endpoint.

Mirrors the shape of the streaming-emit and convert-format requests so
the UI can rely on the same Pydantic-validation behaviour across the
unstructured-generator family of endpoints (Documents, Media, Knowledge,
Logs, Code in v1).
"""

from __future__ import annotations

import re
from typing import Literal

from pydantic import BaseModel, Field, model_validator


# Single-segment UC identifier: letters/digits/underscores, must start
# with a letter or underscore. Mirrors Databricks' identifier rules so
# the operator catches a bad name before it reaches the warehouse.
_UC_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


# Operator-facing IDs. Must match `src.demo_documents.DOCUMENT_TYPES`
# keys exactly — the router validates this at request-validation time
# via the model below.
_DOCUMENT_TYPE_IDS = (
    "pdf_claim",
    "pdf_invoice",
    "pdf_contract",
    "docx_letter",
    "docx_report",
    "pptx_deck",
    "xlsx_budget",
    "xlsx_inventory",
    "eml_message",
)

DocumentTypeID = Literal[
    "pdf_claim",
    "pdf_invoice",
    "pdf_contract",
    "docx_letter",
    "docx_report",
    "pptx_deck",
    "xlsx_budget",
    "xlsx_inventory",
    "eml_message",
]

DocumentDestination = Literal["volume", "volume_with_catalog", "direct_table"]


class DemoDocumentsRequest(BaseModel):
    """Run the Documents generator and emit a corpus into either a UC
    Volume, a Volume + per-tab catalog table, or a direct (inline-bytes)
    Delta table.

    The destination radio mirrors the streaming module's same-named
    enum; the orchestrator branches on it once at start and the rest
    of the loop is shape-identical regardless of where the bytes land.
    """

    # ── Destination ─────────────────────────────────────────────────
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
    destination: DocumentDestination = Field(
        default="volume_with_catalog",
        description=(
            "Where bytes land. 'volume' = files only. 'volume_with_catalog' "
            "= files + a Delta table indexing them. 'direct_table' = bytes "
            "inline in a Delta table (no Volume writes)."
        ),
    )

    # ── What to generate ────────────────────────────────────────────
    types: list[DocumentTypeID] = Field(
        ...,
        min_length=1,
        description="Document types to generate. Pydantic Literal rejects unknown IDs.",
    )
    counts: dict[str, int] = Field(
        default_factory=dict,
        description=(
            "Per-type count. Defaults to 10 per type if a type appears in "
            "`types` but not here. Counts capped at 10000 per type to "
            "prevent runaway demos."
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
        description="Industry context drives template selection within each generator.",
    )
    realistic_content: bool = Field(
        default=False,
        description=(
            "When True, document body text is drafted by the AI client "
            "(slower, requires API key). When False, templated text with "
            "Faker substitutions is used. Spreadsheets ignore the flag."
        ),
    )

    # ── Faker reproducibility ───────────────────────────────────────
    faker_locale: str = Field(default="en_US")
    faker_seed: int | None = Field(
        default=None,
        description="When set, all Faker calls use this seed → reproducible names / IDs / dates across runs.",
    )

    # ── Job execution ───────────────────────────────────────────────
    warehouse_id: str | None = Field(
        default=None,
        description="SQL warehouse to use. Falls back to clone_config.yaml default.",
    )

    model_config = {"populate_by_name": True}

    # ── Validators ──────────────────────────────────────────────────

    @model_validator(mode="after")
    def _identifiers_are_single_part(self) -> "DemoDocumentsRequest":
        """Reject dotted identifiers in catalog / schema / volume.
        Same rule as the smoke-test endpoint — the most common
        operator mistake is pasting a multi-part FQN prefix into the
        catalog field."""
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
    def _volume_required_for_volume_destinations(self) -> "DemoDocumentsRequest":
        """`volume` and `volume_with_catalog` need a Volume name;
        `direct_table` doesn't (and ignores it). Catch missing
        Volume up-front so the operator gets a clean 422 instead of
        a confusing UC error mid-job."""
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
    def _counts_keys_must_be_in_types(self) -> "DemoDocumentsRequest":
        """Reject `counts` referencing a type not in `types` — that's
        an operator mistake (typo, stale form state) and we'd rather
        flag it than silently drop the count."""
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


class DemoDocumentsPreviewRequest(BaseModel):
    """Pure-arithmetic preview — no warehouse round-trip."""

    types: list[DocumentTypeID] = Field(default_factory=list)
    counts: dict[str, int] = Field(default_factory=dict)


class DemoDocumentsPerTypePreview(BaseModel):
    type: str
    category: str
    label: str
    count: int
    estimated_bytes: int
    estimated_seconds: float


class DemoDocumentsPreviewResponse(BaseModel):
    per_type: list[DemoDocumentsPerTypePreview]
    total_files: int
    total_bytes: int
    estimated_seconds: float
    unknown_types: list[str] = Field(default_factory=list)


class DemoDocumentsSubmitResponse(BaseModel):
    """Returned immediately after the job is queued. Operator polls
    `GET /api/clone/{job_id}` for live progress (same shape used by
    the streaming and convert paths)."""

    job_id: str
    status: str = "queued"


class DemoDocumentsTypeInfo(BaseModel):
    """Metadata for one registered document type — surfaced by GET so
    the UI can render the checkbox grid without hardcoding the list."""

    type: str
    category: str
    label: str
    extension: str


class DemoDocumentsTypesResponse(BaseModel):
    types: list[DemoDocumentsTypeInfo]
    available: bool
    unavailable_reason: str | None = None
