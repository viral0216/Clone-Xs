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
# Kept for back-compat — the new pair-aware logic uses SUPPORTED_PAIRS.
SUPPORTED_SOURCE_FORMATS: frozenset[str] = frozenset({"PARQUET", "ICEBERG"})


# Format pairs the converter knows how to execute. Each pair maps to one
# SQL strategy (see _dispatch_strategy). D1 ships only the two
# pre-existing CONVERT TO DELTA cells; D2 adds the four CTAS cells
# (Delta→Iceberg, Parquet→Iceberg, Delta→Parquet, Iceberg→Parquet) once
# format_strategies.py lands.
SUPPORTED_PAIRS: frozenset[tuple[str, str]] = frozenset(
    {
        ("PARQUET", "DELTA"),
        ("ICEBERG", "DELTA"),
    }
)


# All format names the API surface accepts. Hudi is included so the UI
# can render it disabled-with-tooltip, but it never appears in
# SUPPORTED_PAIRS until the D3 runtime sponsorship lands.
KNOWN_FORMATS: frozenset[str] = frozenset({"DELTA", "ICEBERG", "PARQUET", "HUDI"})


def is_pair_supported(source_format: str, target_format: str) -> bool:
    """True iff the converter has a SQL strategy for this (source, target) pair.

    Used by the API request-validator to return a structured 422 instead
    of letting the request reach the orchestrator only to be rejected
    later. ``source_format`` is normalised to upper-case via the SDK
    boundary in ``src/client.py:_normalize_format`` so we don't need to
    case-fold here defensively, but we do anyway — callers in tests
    sometimes pass lower-case values.
    """
    return (source_format.upper(), target_format.upper()) in SUPPORTED_PAIRS


class ConvertToDeltaError(RuntimeError):
    """Raised when a CONVERT TO DELTA request is refused or fails.

    Distinct exception type so the API layer can map to specific HTTP
    status codes (refusal → 400, source not found → 404, conversion error
    → 500) without string-matching the generic RuntimeError message.
    """


@dataclass
class ConvertResult:
    """Per-table outcome from a convert-format operation.

    Mirrors the shape of TableResult in ``clone_cross_workspace`` so the
    audit / report layer can render both side-by-side without bespoke code.

    ``destination_format`` defaults to ``"DELTA"`` so older callers (and
    audit rows recorded before D1) keep their semantics. The strategy
    dispatch fills it explicitly for new pairs.
    """

    fqn: str
    source_format: str
    status: Literal["converted", "failed", "skipped"]
    duration_ms: int = 0
    error: str | None = None
    destination_format: str = "DELTA"


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


def _dispatch_strategy(source_format: str, target_format: str, qualified: str) -> str | None:
    """Return the SQL string for a given (source, target) pair, or None.

    None means the pair is not supported in this phase. The orchestrator
    converts that into a ``"skipped"`` result with a not-yet-supported
    error reason. D1 only handles the two CONVERT TO DELTA cells;
    additional pairs (Delta→Iceberg, Parquet→Iceberg, *→Parquet) land
    in D2 once ``src/format_strategies.py`` extracts the CTAS+UniForm
    primitives from ``src/clone_tables.py``.
    """
    src = source_format.upper()
    tgt = target_format.upper()

    if (src, tgt) == ("PARQUET", "DELTA"):
        return f"CONVERT TO DELTA {qualified}"
    if (src, tgt) == ("ICEBERG", "DELTA"):
        return f"CONVERT TO DELTA {qualified}"
    return None


def convert_table_format(
    client: WorkspaceClient,
    warehouse_id: str,
    fqn: str,
    source_format: str,
    *,
    target_format: str = "DELTA",
    dry_run: bool = False,
    audit_callback: ConvertAuditCallback | None = None,
) -> ConvertResult:
    """Convert a single UC-registered table from one format to another.

    Args:
        fqn: 3-part identifier, e.g. ``edp_dev.bronze.events_iceberg``.
        source_format: Upper-cased ``data_source_format`` from the source
            inventory. Defaults to DELTA / ICEBERG / PARQUET handling;
            unsupported formats skip with a clear reason.
        target_format: Destination format. Defaults to ``"DELTA"`` so old
            callers behave unchanged. Other targets (ICEBERG, PARQUET,
            HUDI) are accepted by the API surface but only cells in
            ``SUPPORTED_PAIRS`` actually execute — the rest skip with
            ``"not yet supported"``.
        dry_run: When True, log the SQL and return a ``"skipped"`` result
            with status reason in ``error``. Intended for the wizard's
            preview step.

    Returns:
        A ``ConvertResult`` describing the outcome. Never raises for
        per-table failures — the orchestrator continues with the next
        table. Exceptions propagate only for caller-bug situations
        (missing warehouse, bad client).
    """
    src_fmt = (source_format or "").upper()
    tgt_fmt = (target_format or "DELTA").upper()
    started_at = datetime.now(timezone.utc)

    def _finish(status: str, error: str | None = None) -> ConvertResult:
        completed_at = datetime.now(timezone.utc)
        duration_ms = int((completed_at - started_at).total_seconds() * 1000)
        result = ConvertResult(
            fqn=fqn,
            source_format=src_fmt,
            destination_format=tgt_fmt,
            status=status,  # type: ignore[arg-type]
            error=error,
            duration_ms=duration_ms,
        )
        if audit_callback is not None:
            try:
                audit_callback(result, started_at, completed_at)
            except Exception as audit_e:
                logger.warning(f"convert audit callback failed for {fqn}: {audit_e}")
        return result

    # Identity (source == target) is always a no-op. Cheaper to short-
    # circuit here than to render a strategy that does nothing.
    if src_fmt == tgt_fmt:
        logger.info(f"  Skipping {fqn} — already {tgt_fmt}")
        return _finish("skipped", f"already {tgt_fmt}")

    qualified = _qualify(fqn)
    sql = _dispatch_strategy(src_fmt, tgt_fmt, qualified)

    if sql is None:
        # Pair not in SUPPORTED_PAIRS. The API request-validator will
        # usually catch this earlier (returning 422), but the
        # orchestrator double-checks so callers bypassing the API
        # surface (CLI, tests) get a consistent skip reason.
        msg = f"pair {src_fmt}→{tgt_fmt} not yet supported"
        logger.warning(f"  Skipping {fqn} — {msg}")
        return _finish("skipped", msg)

    if dry_run:
        logger.info(f"[DRY RUN] {sql}")
        return _finish("skipped", "dry-run")

    try:
        execute_sql(client, warehouse_id, sql)
        logger.info(f"  ✓ Converted {fqn} ({src_fmt} → {tgt_fmt})")
        return _finish("converted")
    except Exception as e:
        logger.error(f"  ✗ Convert failed for {fqn}: {e}")
        return _finish("failed", str(e))


# --- Back-compat shim --------------------------------------------------
#
# Existing callers (and tests) import ``convert_table_to_delta``. The
# function is now a thin wrapper around ``convert_table_format`` with
# ``target_format="DELTA"`` baked in. Removing it would break any
# external script / notebook that imports the symbol directly.
def convert_table_to_delta(
    client: WorkspaceClient,
    warehouse_id: str,
    fqn: str,
    source_format: str,
    *,
    dry_run: bool = False,
    audit_callback: ConvertAuditCallback | None = None,
) -> ConvertResult:
    """Back-compat shim. New code should call ``convert_table_format``."""
    return convert_table_format(
        client,
        warehouse_id,
        fqn,
        source_format,
        target_format="DELTA",
        dry_run=dry_run,
        audit_callback=audit_callback,
    )


def convert_tables_format(
    client: WorkspaceClient,
    warehouse_id: str,
    targets: list[tuple[str, str] | tuple[str, str, str]],
    *,
    confirm_destructive: bool,
    dry_run: bool = False,
    audit_callback: ConvertAuditCallback | None = None,
) -> ConvertSummary:
    """Run convert-format across a list of targets.

    ``targets`` accepts either the legacy 2-tuple ``(fqn, source_format)``
    (target defaults to DELTA) or the new 3-tuple ``(fqn, source_format,
    target_format)``. Mixed lists are fine — the per-target loop
    normalises each entry before dispatching.

    Refuses unconditionally if ``confirm_destructive`` is False — this is
    a safety gate, not a validation rule. The API layer must collect a
    typed-name confirmation (or similar) from the user before passing
    True. Dry-run bypasses the gate so wizard previews can run safely.

    Tables are processed sequentially. Parallelism would complicate
    failure handling (the SQL writes data files; an interrupted
    conversion leaves an inconsistent state) and the workload is
    typically small (handfuls of tables, not thousands).

    audit_callback (optional): fires once per target after that target's
    conversion finishes. Receives ``(result, started_at, completed_at)``
    so callers writing to an audit Delta table have the timestamps as
    native ``datetime`` values without recomputing from duration_ms.
    """
    if not dry_run and not confirm_destructive:
        raise ConvertToDeltaError(
            "convert is destructive — set confirm_destructive=True "
            "explicitly. The source table will be rewritten in-place "
            "(or replaced via CTAS+rename) at the same FQN."
        )

    summary = ConvertSummary(total=len(targets))
    for entry in targets:
        # Normalise 2-tuple → 3-tuple. Default target = DELTA so the
        # legacy contract behaves unchanged.
        if len(entry) == 2:
            fqn, src_fmt = entry
            tgt_fmt = "DELTA"
        else:
            fqn, src_fmt, tgt_fmt = entry  # type: ignore[misc]

        result = convert_table_format(
            client,
            warehouse_id,
            fqn,
            src_fmt,
            target_format=tgt_fmt,
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
        "Convert-format summary: %d total, %d converted, %d failed, %d skipped",
        summary.total,
        summary.converted,
        summary.failed,
        summary.skipped,
    )
    return summary


# --- Back-compat shim --------------------------------------------------
def convert_tables_to_delta(
    client: WorkspaceClient,
    warehouse_id: str,
    targets: list[tuple[str, str]],
    *,
    confirm_destructive: bool,
    dry_run: bool = False,
    audit_callback: ConvertAuditCallback | None = None,
) -> ConvertSummary:
    """Back-compat shim. New code should call ``convert_tables_format``."""
    return convert_tables_format(
        client,
        warehouse_id,
        targets,  # type: ignore[arg-type]
        confirm_destructive=confirm_destructive,
        dry_run=dry_run,
        audit_callback=audit_callback,
    )
