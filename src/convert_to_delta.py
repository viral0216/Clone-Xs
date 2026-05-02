"""In-place CONVERT TO DELTA — Backlog item #13.

This is *not* a clone. It mutates the source table from Iceberg / Parquet
to Delta in-place, leaving the same FQN. Different feature shape from
`POST /clone`:

  - **Destructive on source.** No destination — the source itself becomes
    Delta. Callers must confirm explicitly (``confirm_destructive=True``)
    or the request is refused.
  - **Source must be quiesced.** Concurrent writes during the conversion
    can corrupt the resulting Delta log. We don't *enforce* this with
    grant changes (the existing quiesce path is clone-specific and would
    over-fire here) but we surface it in the docstring + error messages.
  - **Inventoried, not parallel.** Conversion is per-table; we run them
    sequentially so a partial failure leaves an obvious "everything
    before this point converted, everything after still on source format"
    state instead of an interleaved mess.

Why a new module instead of folding into ``clone_iceberg.py``: the clone
modules treat source-vs-destination as fundamental. A ``CONVERT TO DELTA``
has only a single FQN. Trying to share types/functions would require
nullable destination fields throughout the audit trail and confuse the
callers. Keep them separate; share nothing.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Literal

from databricks.sdk import WorkspaceClient

from src.client import execute_sql

logger = logging.getLogger(__name__)


# Per-table audit callback shape. Caller supplies a closure that captures
# `client`, `warehouse_id`, `config`, and `operation_id` so this module
# stays free of audit-table coupling. Called once per target after the
# conversion finishes (whether converted, failed, or skipped).
ConvertAuditCallback = Callable[
    ["ConvertResult", datetime, datetime],  # result, started_at, completed_at
    None,
]


# Source formats Databricks ``CONVERT TO DELTA`` accepts. (Parquet works
# too; Iceberg requires DBR 13.3+ and the table must be UC-registered.)
SUPPORTED_SOURCE_FORMATS: frozenset[str] = frozenset({"PARQUET", "ICEBERG"})


class ConvertToDeltaError(RuntimeError):
    """Raised when a CONVERT TO DELTA request is refused or fails.

    Distinct exception type so the API layer can map to specific HTTP
    status codes (refusal → 400, source not found → 404, conversion error
    → 500) without string-matching the generic RuntimeError message.
    """


@dataclass
class ConvertResult:
    """Per-table outcome from a convert-to-delta operation.

    Mirrors the shape of TableResult in ``clone_cross_workspace`` so the
    audit / report layer can render both side-by-side without bespoke code.
    """

    fqn: str
    source_format: str
    status: Literal["converted", "failed", "skipped"]
    duration_ms: int = 0
    error: str | None = None


@dataclass
class ConvertSummary:
    """Aggregate results of a convert-to-delta job."""

    total: int = 0
    converted: int = 0
    failed: int = 0
    skipped: int = 0
    results: list[ConvertResult] = field(default_factory=list)


def _qualify(fqn: str) -> str:
    """Wrap each part of a 3-part FQN in backticks if not already quoted.

    Defends against schema/table names that need quoting (reserved words,
    hyphens). Idempotent — already-quoted parts pass through.
    """
    parts = fqn.split(".")
    return ".".join(p if p.startswith("`") else f"`{p}`" for p in parts)


def convert_table_to_delta(
    client: WorkspaceClient,
    warehouse_id: str,
    fqn: str,
    source_format: str,
    *,
    dry_run: bool = False,
    audit_callback: ConvertAuditCallback | None = None,
) -> ConvertResult:
    """Run ``CONVERT TO DELTA`` on a single UC-registered table.

    Args:
        fqn: 3-part identifier, e.g. ``edp_dev.bronze.events_iceberg``.
        source_format: Upper-cased ``data_source_format`` from the source
            inventory. Anything outside ``SUPPORTED_SOURCE_FORMATS`` is
            skipped (Delta tables are obvious no-ops; managed-views aren't
            convertible).
        dry_run: When True, log the SQL and return a ``"skipped"`` result
            with status reason in ``error``. Intended for the wizard's
            preview step.

    Returns:
        A ``ConvertResult`` describing the outcome. Never raises for
        per-table failures — the orchestrator continues with the next
        table. Exceptions propagate only for caller-bug situations
        (missing warehouse, bad client).
    """
    fmt = (source_format or "").upper()
    started_at = datetime.now(timezone.utc)

    # Single-exit helper. Builds the result, fires the audit callback if
    # the caller supplied one, then hands the result back. Defined inline
    # so it closes over ``fqn`` / ``fmt`` / ``started_at`` and the
    # callback. Audit failures are swallowed (warned) — the conversion
    # already succeeded; an audit-write failure shouldn't fail the call.
    def _finish(status: str, error: str | None = None) -> ConvertResult:
        completed_at = datetime.now(timezone.utc)
        duration_ms = int((completed_at - started_at).total_seconds() * 1000)
        result = ConvertResult(
            fqn=fqn,
            source_format=fmt,
            status=status,  # type: ignore[arg-type]  # narrow to Literal at call sites
            error=error,
            duration_ms=duration_ms,
        )
        if audit_callback is not None:
            try:
                audit_callback(result, started_at, completed_at)
            except Exception as audit_e:
                logger.warning(f"convert audit callback failed for {fqn}: {audit_e}")
        return result

    if fmt == "DELTA":
        logger.info(f"  Skipping {fqn} — already Delta")
        return _finish("skipped", "already Delta")

    if fmt not in SUPPORTED_SOURCE_FORMATS:
        logger.warning(f"  Skipping {fqn} — unsupported source format {fmt}")
        return _finish("skipped", f"unsupported source format {fmt}")

    qualified = _qualify(fqn)
    sql = f"CONVERT TO DELTA {qualified}"
    if dry_run:
        logger.info(f"[DRY RUN] {sql}")
        return _finish("skipped", "dry-run")

    try:
        execute_sql(client, warehouse_id, sql)
        logger.info(f"  ✓ Converted {fqn} ({fmt} → DELTA)")
        return _finish("converted")
    except Exception as e:
        logger.error(f"  ✗ Convert failed for {fqn}: {e}")
        return _finish("failed", str(e))


def convert_tables_to_delta(
    client: WorkspaceClient,
    warehouse_id: str,
    targets: list[tuple[str, str]],
    *,
    confirm_destructive: bool,
    dry_run: bool = False,
    audit_callback: ConvertAuditCallback | None = None,
) -> ConvertSummary:
    """Run CONVERT TO DELTA across a list of (fqn, source_format) tuples.

    Refuses unconditionally if ``confirm_destructive`` is False — this is
    a safety gate, not a validation rule. The API layer must collect a
    typed-name confirmation (or similar) from the user before passing
    True. Dry-run bypasses the gate so wizard previews can run safely.

    Tables are processed sequentially. Parallelism would complicate
    failure handling (CONVERT TO DELTA writes data files; an interrupted
    conversion leaves an inconsistent state) and the workload is
    typically small (handfuls of tables, not thousands).

    audit_callback (optional): fires once per target after that target's
    conversion finishes. Receives ``(result, started_at, completed_at)``
    so callers writing to an audit Delta table have the timestamps as
    native ``datetime`` values without recomputing from duration_ms.
    """
    if not dry_run and not confirm_destructive:
        raise ConvertToDeltaError(
            "convert_to_delta is destructive — set confirm_destructive=True "
            "explicitly. The source table will be rewritten in-place; the "
            "Delta target replaces it at the same FQN."
        )

    summary = ConvertSummary(total=len(targets))
    for fqn, fmt in targets:
        result = convert_table_to_delta(
            client,
            warehouse_id,
            fqn,
            fmt,
            dry_run=dry_run,
            audit_callback=audit_callback,
        )
        summary.results.append(result)
        if result.status == "converted":
            summary.converted += 1
        elif result.status == "failed":
            summary.failed += 1
        else:
            summary.skipped += 1

    logger.info(
        "Convert-to-delta summary: %d total, %d converted, %d failed, %d skipped",
        summary.total,
        summary.converted,
        summary.failed,
        summary.skipped,
    )
    return summary
