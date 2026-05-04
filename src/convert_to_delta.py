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
from src.format_compat import check_pair_compat
from src.format_strategies import (
    Plan,
    ctas_iceberg_inplace_plan,
    enable_uniform_hudi_plan,
    enable_uniform_plan,
    export_to_volume_plan,
)

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


# Format pairs the converter knows how to execute. Each pair maps to a
# Plan (see _dispatch_strategy). D2 adds the four CTAS cells on top of
# D1's CONVERT TO DELTA pair; D3 will add the four Hudi cells once a
# Job-cluster runtime sponsor is identified.
SUPPORTED_PAIRS: frozenset[tuple[str, str]] = frozenset(
    {
        # D1
        ("PARQUET", "DELTA"),
        ("ICEBERG", "DELTA"),
        # D2 — temp+rename CTAS for non-Delta targets, plus UniForm
        # metadata for Delta→Iceberg without data movement.
        ("DELTA", "ICEBERG"),
        ("PARQUET", "ICEBERG"),
        ("DELTA", "PARQUET"),
        ("ICEBERG", "PARQUET"),
        # D2.5 — AVRO + ORC sinks. Same temp+rename CTAS shape as the
        # PARQUET pairs; only the ``USING <fmt>`` clause differs.
        # Avro is the row-oriented escape hatch for streaming sinks;
        # ORC is the Hive-era columnar interop format.
        ("DELTA", "AVRO"),
        ("ICEBERG", "AVRO"),
        ("PARQUET", "AVRO"),
        ("DELTA", "ORC"),
        ("ICEBERG", "ORC"),
        ("PARQUET", "ORC"),
        # D2.6 — JSON sinks (export-shaped, for HTTP webhooks /
        # NoSQL pipelines / event consumers). Same CTAS shape.
        ("DELTA", "JSON"),
        ("ICEBERG", "JSON"),
        ("PARQUET", "JSON"),
        # D2.6 — Delta → Hudi UniForm (Beta). Sidecar metadata only,
        # no data movement. Only Delta sources are valid because
        # UniForm needs a Delta base; physical Hudi from non-Delta
        # sources still needs a Job-cluster runtime and stays gated.
        ("DELTA", "HUDI"),
    }
)


# All format names the API surface accepts. JSON, AVRO, ORC ship with
# CTAS strategies; Hudi is partially supported (Delta→Hudi UniForm
# Beta only — every other Hudi pair still needs a Job-cluster runtime).
KNOWN_FORMATS: frozenset[str] = frozenset(
    {"DELTA", "ICEBERG", "PARQUET", "AVRO", "ORC", "JSON", "HUDI"}
)


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

    ``strategy_used`` (added in D2) names which physical path the
    dispatch picked — e.g. "uniform" vs "ctas_iceberg" for the same
    Delta→Iceberg destination. Empty string for skipped/identity rows.
    """

    fqn: str
    source_format: str
    status: Literal["converted", "failed", "skipped"]
    duration_ms: int = 0
    error: str | None = None
    destination_format: str = "DELTA"
    strategy_used: str = ""


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


@dataclass
class StrategyChoice:
    """The Plan picked by `_dispatch_strategy` plus a short label that
    surfaces in the audit row's `strategy_used` column.

    Multiple strategies can produce the same destination format with
    different physical outcomes — the canonical case is Delta→Iceberg,
    where UniForm-update leaves data files alone but a CTAS-based
    physical Iceberg replaces them entirely. The `strategy` label lets
    operators tell post-hoc which path a given run took.
    """

    plan: Plan
    strategy: str  # e.g. "convert_to_delta", "uniform", "ctas_iceberg", "ctas_parquet"


def _dispatch_strategy(
    source_format: str,
    target_format: str,
    qualified: str,
    *,
    iceberg_physical: bool = False,
    keep_backup: bool = True,
    destination_path: str | None = None,
) -> StrategyChoice | None:
    """Return the Plan for a (source, target) pair, or None if unsupported.

    The orchestrator converts None into a ``"skipped"`` result with a
    not-yet-supported reason. The flags are decision points specific to
    a target format:

    - `iceberg_physical` chooses between the UniForm-update path (Delta
      target stays Delta + adds Iceberg metadata) and the temp+rename
      CTAS path (replaces the underlying table with `USING iceberg`).
      Only meaningful for Delta→Iceberg.
    - `keep_backup` controls whether the temp+rename CTAS pairs rename
      the source aside (recoverable) or drop it (non-recoverable).
      Default True everywhere.
    """
    src = source_format.upper()
    tgt = target_format.upper()

    # D1: in-place CONVERT TO DELTA. Wrap as a single-step Plan so the
    # orchestrator's plan-execution path is uniform across all cells.
    if (src, tgt) in {("PARQUET", "DELTA"), ("ICEBERG", "DELTA")}:
        plan = Plan()
        plan.add(
            f"convert {src.lower()} to delta in place",
            f"CONVERT TO DELTA {qualified}",
        )
        return StrategyChoice(plan=plan, strategy="convert_to_delta")

    # D2: Delta → UniForm-Iceberg-readable Delta. Three-step ALTER
    # chain; data files don't move. Default for Delta→Iceberg unless
    # the caller asked for physical Iceberg.
    if (src, tgt) == ("DELTA", "ICEBERG") and not iceberg_physical:
        return StrategyChoice(
            plan=enable_uniform_plan(qualified),
            strategy="uniform",
        )

    # D2: Delta → physical Iceberg via temp+rename CTAS. UC reports
    # `Data source: Iceberg`. Loses Delta history.
    if (src, tgt) == ("DELTA", "ICEBERG") and iceberg_physical:
        return StrategyChoice(
            plan=ctas_iceberg_inplace_plan(qualified, keep_backup=keep_backup),
            strategy="ctas_iceberg",
        )

    # D2: Parquet → physical Iceberg. UniForm needs Delta as a base, so
    # CTAS is the only physical path here.
    if (src, tgt) == ("PARQUET", "ICEBERG"):
        return StrategyChoice(
            plan=ctas_iceberg_inplace_plan(qualified, keep_backup=keep_backup),
            strategy="ctas_iceberg",
        )

    # D2.7: Export-shaped targets — PARQUET / AVRO / ORC / JSON. UC
    # managed tables MUST be Delta, so the previous CTAS-into-the-same-
    # FQN approach was a dead end (Databricks rejects it). The
    # converter now writes raw files to a Volume the caller picked.
    # The original table at ``qualified`` is preserved — these are
    # genuine exports, not destructive in-place rewrites.
    #
    # ``destination_path`` is required for these targets; the API
    # request validator catches missing paths with a 422 before we
    # reach this point. Defence-in-depth: if it's somehow None here
    # (e.g. a CLI caller bypassing the validator), return None and let
    # the orchestrator skip the row with a clear reason.
    export_pairs = {
        ("DELTA", "PARQUET"),
        ("ICEBERG", "PARQUET"),
        ("DELTA", "AVRO"),
        ("ICEBERG", "AVRO"),
        ("PARQUET", "AVRO"),
        ("DELTA", "ORC"),
        ("ICEBERG", "ORC"),
        ("PARQUET", "ORC"),
        ("DELTA", "JSON"),
        ("ICEBERG", "JSON"),
        ("PARQUET", "JSON"),
    }
    if (src, tgt) in export_pairs:
        if not destination_path:
            return None
        return StrategyChoice(
            plan=export_to_volume_plan(
                qualified,
                fmt=tgt.lower(),
                volume_path=destination_path,
            ),
            strategy=f"export_{tgt.lower()}",
        )

    # D2.6: Delta → Hudi UniForm (Beta). Sidecar metadata only — same
    # ALTER chain as the Iceberg UniForm path, just a different
    # CompatV* property + universalFormat enum value. Always picks the
    # UniForm path; physical-Hudi from Delta requires a Job-cluster
    # runtime and stays gated by the SUPPORTED_PAIRS set.
    if (src, tgt) == ("DELTA", "HUDI"):
        return StrategyChoice(
            plan=enable_uniform_hudi_plan(qualified),
            strategy="uniform_hudi",
        )

    return None


@dataclass
class _CapturedPermissions:
    """Snapshot of a table's GRANTs + owner taken before a CTAS run.

    The CTAS strategies replace the underlying table entirely, so the
    new table at the same FQN starts with fresh permissions (the
    creator owns it, no GRANTs). We capture the source's permissions
    up-front and replay them after the plan succeeds. Best-effort:
    if SHOW GRANTS or `client.tables.get` fails, we record what we can
    and let the replay phase do whatever applies.
    """

    grants: list[tuple[str, str]] = field(default_factory=list)  # (principal, privilege)
    owner: str | None = None


def _capture_table_permissions(
    client: WorkspaceClient, warehouse_id: str, fqn: str
) -> _CapturedPermissions:
    """Capture GRANTs and ownership for ``fqn`` before a CTAS rewrite.

    Reads:
      - SHOW GRANTS ON TABLE <fqn> — every (principal, privilege) row
      - client.tables.get(fqn).owner — the table owner

    Both reads are best-effort; failures log a warning and the
    captured snapshot just has empty fields for what couldn't be read.
    The replay step tolerates None / empty values.
    """
    captured = _CapturedPermissions()

    qualified = _qualify(fqn)
    try:
        rows = execute_sql(client, warehouse_id, f"SHOW GRANTS ON TABLE {qualified}")
    except Exception as e:
        logger.warning(
            f"Could not SHOW GRANTS on {fqn} for permission preservation "
            f"(continuing without GRANT replay): {e}"
        )
        rows = []

    skip_privileges = {"OWN", "OWNERSHIP"}
    for row in rows or []:
        principal = row.get("Principal") or row.get("principal") or ""
        privilege = row.get("ActionType") or row.get("privilege") or row.get("action_type") or ""
        if not principal or not privilege:
            continue
        if privilege.upper() in skip_privileges:
            # Ownership is a separate concept (ALTER TABLE … OWNER TO),
            # not a GRANT. Skip here; we capture owner via the SDK below.
            continue
        captured.grants.append((principal, privilege))

    try:
        info = client.tables.get(fqn)
        if info and getattr(info, "owner", None):
            captured.owner = info.owner
    except Exception as e:
        logger.warning(
            f"Could not read owner of {fqn} for permission preservation "
            f"(continuing without OWNER replay): {e}"
        )

    if captured.grants or captured.owner:
        logger.info(
            f"Captured {len(captured.grants)} grants + owner={captured.owner!r} "
            f"on {fqn} for post-CTAS replay"
        )
    return captured


def _replay_table_permissions(
    client: WorkspaceClient,
    warehouse_id: str,
    fqn: str,
    captured: _CapturedPermissions,
) -> None:
    """Replay captured GRANTs + ownership on the new table at ``fqn``.

    Per-grant try/except so a partial-permission caller still gets
    whatever grants they're allowed to apply. Failures log a warning;
    the conversion result is already success at this point.
    """
    qualified = _qualify(fqn)
    applied = 0
    for principal, privilege in captured.grants:
        try:
            execute_sql(
                client,
                warehouse_id,
                f"GRANT {privilege} ON TABLE {qualified} TO `{principal}`",
            )
            applied += 1
        except Exception as e:
            logger.warning(f"Could not replay GRANT {privilege} TO {principal} on {fqn}: {e}")

    if captured.owner:
        try:
            execute_sql(
                client,
                warehouse_id,
                f"ALTER TABLE {qualified} OWNER TO `{captured.owner}`",
            )
            logger.info(f"Restored owner={captured.owner!r} on {fqn}")
        except Exception as e:
            logger.warning(f"Could not restore owner on {fqn}: {e}")

    if applied:
        logger.info(f"Replayed {applied}/{len(captured.grants)} grants on {fqn}")


def convert_table_format(
    client: WorkspaceClient,
    warehouse_id: str,
    fqn: str,
    source_format: str,
    *,
    target_format: str = "DELTA",
    iceberg_physical: bool = False,
    keep_backup: bool = True,
    copy_permissions: bool = True,
    dry_run: bool = False,
    audit_callback: ConvertAuditCallback | None = None,
    destination_path: str | None = None,
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
        iceberg_physical: Only meaningful for Delta→Iceberg. False
            (default) picks the UniForm-update path; True picks the
            CTAS+rename path that produces a real Iceberg table.
        keep_backup: For temp+rename pairs (any → ICEBERG/PARQUET via
            CTAS), True (default) renames the source aside as
            ``{fqn}_pre_convert_<utc>`` so the conversion is reversible.
            False drops the source after rename — non-recoverable.
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

    def _finish(status: str, error: str | None = None, strategy: str = "") -> ConvertResult:
        completed_at = datetime.now(timezone.utc)
        duration_ms = int((completed_at - started_at).total_seconds() * 1000)
        result = ConvertResult(
            fqn=fqn,
            source_format=src_fmt,
            destination_format=tgt_fmt,
            status=status,  # type: ignore[arg-type]
            error=error,
            duration_ms=duration_ms,
            strategy_used=strategy,
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

    # Auto-create the destination Volume for export-shaped targets
    # (PARQUET / AVRO / ORC / JSON). Without this the operator hits
    # `UC_VOLUME_NOT_FOUND` if the Volume doesn't exist yet — same
    # root cause as the smoke endpoint had before its own auto-
    # create. Idempotent; cheap. Skip in dry-run so previewing the
    # SQL doesn't side-effect.
    _export_formats_needing_volume = {"PARQUET", "AVRO", "ORC", "JSON"}
    if not dry_run and tgt_fmt in _export_formats_needing_volume and destination_path:
        # Parse the Volume FQN out of `/Volumes/<cat>/<sch>/<vol>/...`.
        # The path format is enforced by the API request validator
        # (must start with /Volumes/) so we can rely on the prefix.
        vol_parts = destination_path.strip("/").split("/")
        if len(vol_parts) >= 4 and vol_parts[0].lower() == "volumes":
            vol_fqn = f"{vol_parts[1]}.{vol_parts[2]}.{vol_parts[3]}"
            try:
                execute_sql(
                    client,
                    warehouse_id,
                    f"CREATE VOLUME IF NOT EXISTS {vol_fqn}",
                )
            except Exception as e:
                logger.error(f"  ✗ Could not auto-create Volume {vol_fqn} for {fqn}: {e}")
                return _finish(
                    "failed",
                    (
                        f"Could not auto-create Volume {vol_fqn}: {e}. "
                        f"Most likely: the schema has no managed location "
                        f"(run ALTER SCHEMA {vol_parts[1]}.{vol_parts[2]} "
                        f"SET MANAGED LOCATION ...) or the caller lacks "
                        f"CREATE VOLUME privilege on the schema."
                    ),
                )

    # Cross-format compat preflight — refuse hidden Iceberg
    # partitioning, refuse GENERATED/IDENTITY Delta columns when
    # target can't represent them. Skip in dry-run so the operator
    # can preview the SQL even when the source has known
    # incompatibilities (some users want to see the dry-run plan
    # before deciding whether to refactor the source).
    if not dry_run:
        compat_reasons = check_pair_compat(client, warehouse_id, qualified, src_fmt, tgt_fmt)
        if compat_reasons:
            joined = "; ".join(compat_reasons)
            logger.warning(f"  Refusing {fqn} on compat preflight: {joined}")
            return _finish("skipped", f"compat preflight refused: {joined}")

    choice = _dispatch_strategy(
        src_fmt,
        tgt_fmt,
        qualified,
        iceberg_physical=iceberg_physical,
        keep_backup=keep_backup,
        destination_path=destination_path,
    )

    if choice is None:
        # Pair not in SUPPORTED_PAIRS. The API request-validator will
        # usually catch this earlier (returning 422), but the
        # orchestrator double-checks so callers bypassing the API
        # surface (CLI, tests) get a consistent skip reason.
        msg = f"pair {src_fmt}→{tgt_fmt} not yet supported"
        logger.warning(f"  Skipping {fqn} — {msg}")
        return _finish("skipped", msg)

    if dry_run:
        # Render every step so the operator sees the full multi-statement
        # plan, not just the first SQL.
        for step in choice.plan.steps:
            logger.info(f"[DRY RUN] [{step.label}] {step.sql}")
        return _finish("skipped", "dry-run", strategy=choice.strategy)

    # CTAS-based strategies create a brand-new table at the original
    # FQN, so the source's GRANTs and OWNER are reset on the
    # destination. Capture them up-front; replay after the plan
    # finishes so the new table looks identical to the source from a
    # permissions standpoint. Two early-out cases:
    #   - non-CTAS strategies (convert_to_delta, uniform) keep the
    #     same physical table — no preservation needed.
    #   - copy_permissions=False — caller explicitly opted out (e.g.
    #     they're rotating ownership intentionally as part of the
    #     conversion).
    captured_perms = (
        _capture_table_permissions(client, warehouse_id, fqn)
        if copy_permissions and choice.strategy == "ctas_iceberg"
        else None
    )

    try:
        choice.plan.execute(client, warehouse_id, dry_run=False)
        logger.info(f"  ✓ Converted {fqn} ({src_fmt} → {tgt_fmt}) via {choice.strategy}")
    except Exception as e:
        # Translate the Hudi UniForm runtime-not-supported error into
        # a friendly message. Databricks reports it as a generic
        # ``DELTA_UNKNOWN_CONFIGURATION`` for ``delta.enableHudiCompatV1``
        # — not obvious unless you know that property only exists on
        # newer DBR. Surface the actionable next step (upgrade
        # warehouse runtime) rather than the raw config-not-found.
        msg = str(e)
        if (
            choice.strategy == "uniform_hudi"
            and "DELTA_UNKNOWN_CONFIGURATION" in msg
            and "delta.enableHudiCompatV1" in msg
        ):
            msg = (
                "Hudi UniForm is not supported on this SQL warehouse runtime "
                "(`delta.enableHudiCompatV1` is unknown). Hudi UniForm requires "
                "a recent Databricks Runtime — upgrade the warehouse, or pick a "
                "different target format. Original error: " + str(e)
            )
        logger.error(f"  ✗ Convert failed for {fqn}: {msg}")
        return _finish("failed", msg, strategy=choice.strategy)

    # Replay GRANTs + OWNER after a successful CTAS conversion. Each
    # individual GRANT is best-effort — a partial-permission caller
    # (e.g. they can ALTER but not GRANT) still gets the grants that
    # would succeed. The conversion itself is already done at this
    # point; we don't fail the whole thing on a permission replay.
    if captured_perms is not None:
        _replay_table_permissions(client, warehouse_id, fqn, captured_perms)

    return _finish("converted", strategy=choice.strategy)


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
    targets: list[tuple[str, str] | tuple[str, str, str] | tuple[str, str, str, str | None]],
    *,
    confirm_destructive: bool,
    dry_run: bool = False,
    audit_callback: ConvertAuditCallback | None = None,
    iceberg_physical: bool = False,
    keep_backup: bool = True,
    copy_permissions: bool = True,
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
        # Normalise 2-/3-/4-tuple. Default target = DELTA + no
        # destination_path so the legacy contracts behave unchanged.
        # 4-tuple is the export-shaped target shape (PARQUET / AVRO /
        # ORC / JSON) — `dest_path` is the Volume URI; for in-place
        # targets it's None.
        dest_path: str | None = None
        if len(entry) == 2:
            fqn, src_fmt = entry  # type: ignore[misc]
            tgt_fmt = "DELTA"
        elif len(entry) == 3:
            fqn, src_fmt, tgt_fmt = entry  # type: ignore[misc]
        else:
            fqn, src_fmt, tgt_fmt, dest_path = entry  # type: ignore[misc]

        result = convert_table_format(
            client,
            warehouse_id,
            fqn,
            src_fmt,
            target_format=tgt_fmt,
            iceberg_physical=iceberg_physical,
            keep_backup=keep_backup,
            copy_permissions=copy_permissions,
            dry_run=dry_run,
            audit_callback=audit_callback,
            destination_path=dest_path,
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
