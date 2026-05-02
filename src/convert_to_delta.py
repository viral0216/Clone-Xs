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
import time
from dataclasses import dataclass, field
from typing import Literal

from databricks.sdk import WorkspaceClient

from src.client import execute_sql

logger = logging.getLogger(__name__)


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
    started_at = time.time()

    if fmt == "DELTA":
        logger.info(f"  Skipping {fqn} — already Delta")
        return ConvertResult(
            fqn=fqn,
            source_format=fmt,
            status="skipped",
            error="already Delta",
            duration_ms=int((time.time() - started_at) * 1000),
        )

    if fmt not in SUPPORTED_SOURCE_FORMATS:
        logger.warning(f"  Skipping {fqn} — unsupported source format {fmt}")
        return ConvertResult(
            fqn=fqn,
            source_format=fmt,
            status="skipped",
            error=f"unsupported source format {fmt}",
            duration_ms=int((time.time() - started_at) * 1000),
        )

    qualified = _qualify(fqn)
    sql = f"CONVERT TO DELTA {qualified}"
    if dry_run:
        logger.info(f"[DRY RUN] {sql}")
        return ConvertResult(
            fqn=fqn,
            source_format=fmt,
            status="skipped",
            error="dry-run",
            duration_ms=int((time.time() - started_at) * 1000),
        )

    try:
        execute_sql(client, warehouse_id, sql)
        logger.info(f"  ✓ Converted {fqn} ({fmt} → DELTA)")
        return ConvertResult(
            fqn=fqn,
            source_format=fmt,
            status="converted",
            duration_ms=int((time.time() - started_at) * 1000),
        )
    except Exception as e:
        logger.error(f"  ✗ Convert failed for {fqn}: {e}")
        return ConvertResult(
            fqn=fqn,
            source_format=fmt,
            status="failed",
            error=str(e),
            duration_ms=int((time.time() - started_at) * 1000),
        )


def convert_tables_to_delta(
    client: WorkspaceClient,
    warehouse_id: str,
    targets: list[tuple[str, str]],
    *,
    confirm_destructive: bool,
    dry_run: bool = False,
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
    """
    if not dry_run and not confirm_destructive:
        raise ConvertToDeltaError(
            "convert_to_delta is destructive — set confirm_destructive=True "
            "explicitly. The source table will be rewritten in-place; the "
            "Delta target replaces it at the same FQN."
        )

    summary = ConvertSummary(total=len(targets))
    for fqn, fmt in targets:
        result = convert_table_to_delta(client, warehouse_id, fqn, fmt, dry_run=dry_run)
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
