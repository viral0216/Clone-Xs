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
    typically pre-screen via the catalog explorer.

    ``target_format`` (added in D1 of #9 N×N converter) defaults to
    ``"DELTA"`` so old clients sending no field get the previous
    behaviour. Only pairs in ``src.convert_to_delta.SUPPORTED_PAIRS``
    actually execute — others are rejected at request-validation time
    with a 422 referencing the unsupported pair. ``"HUDI"`` is
    accepted by the model so the UI can render it disabled with a
    "needs Job-cluster runtime" tooltip, but the validator rejects
    every Hudi pair until D3 lands.
    """

    fqn: str = Field(..., description="3-part fully qualified name, e.g. catalog.schema.table")
    source_format: Literal["PARQUET", "ICEBERG", "DELTA", "AVRO", "ORC", "JSON", "HUDI"] = "ICEBERG"
    target_format: Literal["DELTA", "ICEBERG", "PARQUET", "AVRO", "ORC", "JSON", "HUDI"] = "DELTA"
    # Required when target_format is one of PARQUET / AVRO / ORC / JSON
    # (the "export-shaped" formats — UC managed tables can't be these,
    # so the converter writes files to a Volume instead of rewriting
    # the table in place). Format:
    # ``/Volumes/<catalog>/<schema>/<volume>[/<sub-path>]``.
    # Ignored for DELTA / ICEBERG targets, which stay in-place at
    # ``fqn``.
    destination_path: str | None = Field(
        default=None,
        description=(
            "Volume path for export-shaped targets. Required when "
            "target_format ∈ {PARQUET, AVRO, ORC, JSON}. Format: "
            "/Volumes/<catalog>/<schema>/<volume>[/<sub-path>]"
        ),
    )


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
    # D2 — Delta→Iceberg has two physical paths:
    #   - UniForm (default, no data movement, table stays Delta)
    #   - Physical CTAS (real Iceberg table, loses Delta history)
    # iceberg_physical=True picks the second. Only meaningful for any
    # row whose target_format is ICEBERG; ignored for other targets.
    iceberg_physical: bool = False
    # D2 — for temp+rename CTAS pairs (any → ICEBERG/PARQUET via CTAS),
    # rename the source aside as `{fqn}_pre_convert_<utc>` instead of
    # dropping it. Default True so the conversion is reversible — the
    # operator can rename the backup back if they need to roll back.
    # Set False to drop the source (non-recoverable).
    keep_backup: bool = True
    # CTAS strategies replace the underlying table, so the new table at
    # the original FQN starts with no GRANTs and is owned by the caller.
    # When True (default), the orchestrator captures `SHOW GRANTS` +
    # owner before the plan and replays both after — the new table looks
    # identical to the source from a permissions standpoint. Best-effort
    # per-grant: a partial-permission caller still gets the grants they
    # can apply. No-op for non-CTAS strategies (convert_to_delta,
    # uniform — same physical table, grants preserved automatically).
    copy_permissions: bool = True

    @model_validator(mode="after")
    def _confirmed_or_dry_run(self) -> "ConvertToDeltaRequest":
        if not self.dry_run and not self.confirm_destructive:
            raise ValueError(
                "convert-to-delta is destructive on source — set "
                "`confirm_destructive: true` explicitly, or set `dry_run: true` "
                "to preview the SQL without executing"
            )
        return self

    @model_validator(mode="after")
    def _export_targets_require_destination_path(self) -> "ConvertToDeltaRequest":
        """Reject export-shaped targets that have no Volume path set.

        UC managed tables can't be PARQUET / AVRO / ORC / JSON, so the
        converter for those formats writes files to a Volume rather
        than rewriting the table in place. The path must be set
        explicitly — auto-deriving from the source FQN would silently
        require a Volume to exist with a magic name, which fails
        opaquely. The UI is expected to pre-populate the field with a
        sensible default the operator can edit.
        """
        export_formats = {"PARQUET", "AVRO", "ORC", "JSON"}
        missing: list[str] = []
        for t in self.targets:
            if t.target_format.upper() not in export_formats:
                continue
            path = (t.destination_path or "").strip()
            if not path:
                missing.append(f"{t.fqn} (target {t.target_format})")
                continue
            if not path.startswith("/Volumes/"):
                missing.append(
                    f"{t.fqn} (destination_path {path!r} must start with "
                    f"/Volumes/<catalog>/<schema>/<volume>)"
                )
        if missing:
            raise ValueError(
                "Export-shaped targets (PARQUET / AVRO / ORC / JSON) "
                "require a Volume `destination_path` — UC managed tables "
                "can't be these formats, so the converter writes files "
                "to a Volume instead. Offending targets: " + ", ".join(missing)
            )
        return self

    @model_validator(mode="after")
    def _all_pairs_supported(self) -> "ConvertToDeltaRequest":
        """Reject targets whose (source_format, target_format) pair is
        not yet executable. D1 ships only the two CONVERT TO DELTA cells;
        every other pair surfaces here as a 422 with a structured
        message that names the offending pair so the UI can render it
        inline rather than as a generic toast.

        Imported lazily to avoid a circular dependency between the
        models package and the orchestrator module.
        """
        from src.convert_to_delta import is_pair_supported

        unsupported: list[str] = []
        for t in self.targets:
            # Skip identity pairs (already-target rows) — the orchestrator
            # short-circuits these with a benign "skipped" result.
            if t.source_format.upper() == t.target_format.upper():
                continue
            if not is_pair_supported(t.source_format, t.target_format):
                unsupported.append(f"{t.fqn} ({t.source_format}→{t.target_format})")
        if unsupported:
            raise ValueError(
                "Some target pairs are not yet supported in this release. "
                "Hudi conversions are gated on a future runtime decision; "
                "other format pairs land in D2. Offending targets: " + ", ".join(unsupported)
            )
        return self


class ConvertResultResponse(BaseModel):
    """Per-table outcome — flattened from src.convert_to_delta.ConvertResult.

    ``destination_format`` defaults to ``"DELTA"`` so callers parsing
    historic responses (where the field didn't exist) keep the right
    semantic — those operations were always-Delta-target by design.

    ``strategy_used`` (D2) names the physical path the orchestrator
    picked. Multiple strategies can produce the same destination
    format with different physical outcomes; the canonical case is
    Delta→Iceberg, where ``"uniform"`` leaves data files alone but
    ``"ctas_iceberg"`` replaces them. Empty for skipped rows.
    """

    fqn: str
    source_format: str
    destination_format: str = "DELTA"
    strategy_used: str = ""
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


class ConvertHistoryRow(BaseModel):
    """One row from the convert_operations audit table.

    Mirrors the shape `ensure_convert_audit_table` defines so the
    response can be parsed straight from the warehouse query result
    without bespoke mapping. Datetime fields are returned as strings
    (UTC, ``YYYY-MM-DD HH:MM:SS``) — Pydantic will coerce them to
    ``datetime`` if the consumer types this model that way.
    """

    operation_id: str
    fqn: str
    source_format: str
    destination_format: str = "DELTA"
    strategy_used: str = ""
    status: Literal["converted", "failed", "skipped"]
    started_at: str | None = None
    completed_at: str | None = None
    duration_ms: int | None = None
    user_name: str | None = None
    host: str | None = None
    dry_run: bool | None = None
    trigger: str | None = None
    error_message: str | None = None
    recorded_at: str | None = None


class ConvertHistoryResponse(BaseModel):
    """Response from GET /api/convert-to-delta/history.

    Wrapped in a top-level object (rather than a bare list) so we can
    add summary fields later — total count, scanned date range,
    earliest/latest timestamps — without breaking the wire format.
    """

    rows: list[ConvertHistoryRow]
    count: int
