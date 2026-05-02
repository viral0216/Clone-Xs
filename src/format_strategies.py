"""Reusable SQL primitives for cross-format conversion (D2 of #9 N×N converter).

The clone path (`src/clone_tables.py`) and the convert path
(`src/convert_to_delta.py`) both need to:

- Enable Delta UniForm metadata so external Iceberg engines can read
  a Delta target without a copy.
- CTAS into a physical Iceberg table (UC reports `Data source: Iceberg`).
- CTAS into a Parquet table (escape hatch for downstream tools that
  insist on raw Parquet).

Before D2 each of those was inlined in clone_tables.py. Lifting them
here means clone and convert call the same code — bug fixes flow to
both paths, and the strategy registry that lands later in D2 has clean
unit-testable building blocks to compose.

Each function returns a `Plan` — an ordered list of SQL statements
plus a short label per step. `Plan.execute()` runs each step against
the warehouse with the same `execute_sql(..., dry_run=...)` call the
rest of the codebase uses; `Plan.statements()` is for the dry-run
preview (UI shows the numbered list before submit). The two entry
points keep the convert UI honest — there's no hidden multi-step DDL
that doesn't show up in dry-run.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from databricks.sdk import WorkspaceClient

from src.client import execute_sql

logger = logging.getLogger(__name__)


@dataclass
class PlanStep:
    """One SQL statement in a multi-step plan, plus a short label that
    surfaces in the dry-run preview ("disable deletion vectors", "REORG
    PURGE", "set UniForm props"). Failure messages reference the label
    so operators don't have to read the SQL to know which step blew up.
    """

    label: str
    sql: str


@dataclass
class Plan:
    """An ordered list of PlanSteps describing a complete conversion.

    Used by both the convert orchestrator and the clone path. The plan
    is built up-front (no execute-and-then-build) so the dry-run
    preview matches the live execution exactly — there's no second
    code path to drift.
    """

    steps: list[PlanStep] = field(default_factory=list)

    def add(self, label: str, sql: str) -> None:
        self.steps.append(PlanStep(label=label, sql=sql))

    def statements(self) -> list[str]:
        """Just the SQL strings, in order — for the dry-run preview."""
        return [s.sql for s in self.steps]

    def execute(
        self,
        client: WorkspaceClient,
        warehouse_id: str,
        *,
        dry_run: bool = False,
    ) -> None:
        """Run every step against the warehouse. On any failure, the
        exception is re-raised with the step's label prefixed so callers
        can render a structured "step <label> failed: <error>" instead
        of a bare SQL stack trace.
        """
        for step in self.steps:
            try:
                execute_sql(client, warehouse_id, step.sql, dry_run=dry_run)
            except Exception as e:
                # Wrap with the label, but preserve the original cause via
                # `from e` so the traceback chain stays intact.
                raise RuntimeError(f"step '{step.label}' failed: {e}") from e


# --- Delta → UniForm-Iceberg-readable Delta ---------------------------
#
# UniForm sits on top of Delta — the table stays Delta and the data
# files don't move. Three-step DDL is mandatory: Databricks rejects
# `SET enableIcebergCompatV2=true` while deletion vectors are still
# enabled, and rejects it again if any DV files exist on disk (hence
# the REORG PURGE between disable and set).
#
# Same SQL the existing clone-path UniForm block emits at
# `clone_tables.py:351-376`. Centralised here so any future change
# (e.g. adding `delta.enableIcebergCompatV3` once it ships) lands in
# both paths from one edit.


def enable_uniform_plan(dest_fqn: str) -> Plan:
    """Plan for enabling UniForm on a Delta table.

    Caller is responsible for verifying the target is actually Delta —
    UniForm on a non-Delta table is rejected by Databricks with a
    cryptic error. The convert path's preflight handles this; the
    clone path gates on `source_format.upper() == "DELTA"` before
    calling.
    """
    plan = Plan()
    plan.add(
        "disable deletion vectors",
        f"ALTER TABLE {dest_fqn} SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'false')",
    )
    plan.add(
        "purge deletion vector files",
        f"REORG TABLE {dest_fqn} APPLY (PURGE)",
    )
    plan.add(
        "enable Iceberg compat metadata",
        (
            f"ALTER TABLE {dest_fqn} SET TBLPROPERTIES ("
            f"'delta.columnMapping.mode' = 'name', "
            f"'delta.enableIcebergCompatV2' = 'true', "
            f"'delta.universalFormat.enabledFormats' = 'iceberg'"
            f")"
        ),
    )
    return plan


# --- Physical Iceberg target via CTAS ---------------------------------
#
# `CREATE TABLE … USING iceberg AS SELECT * FROM source` produces a
# real Iceberg table — UC reports `Data source: Iceberg`. Loses Delta
# history (target starts at version 0), can't be combined with
# TIMESTAMP/VERSION AS OF on the SELECT side.


def ctas_iceberg_plan(
    source_fqn: str,
    dest_fqn: str,
    *,
    where: str | None = None,
    tbl_properties: dict[str, str] | None = None,
) -> Plan:
    """Plan for materialising an Iceberg target via CTAS.

    Same SQL emitted by the existing clone-path physical-Iceberg block
    at `clone_tables.py:255-281`. Optional `where` filter and
    `tbl_properties` (rendered as a post-CTAS ALTER, since CTAS
    doesn't accept TBLPROPERTIES inline against an Iceberg target).
    """
    plan = Plan()
    select_clause = f"SELECT * FROM {source_fqn}"
    if where:
        select_clause += f" WHERE {where}"
    plan.add(
        "create iceberg table from source",
        f"CREATE TABLE IF NOT EXISTS {dest_fqn} USING iceberg AS {select_clause}",
    )
    if tbl_properties:
        # Render as `key = 'value'` pairs with single-quote escaping
        # via doubling. Same rule the clone-path's
        # `_format_tbl_properties` helper applies.
        pairs = [
            f"'{k}' = '{str(v).replace(chr(39), chr(39) * 2)}'" for k, v in tbl_properties.items()
        ]
        plan.add(
            "apply tblproperties post-ctas",
            f"ALTER TABLE {dest_fqn} SET TBLPROPERTIES ({', '.join(pairs)})",
        )
    return plan


# --- Parquet target via CTAS ------------------------------------------
#
# `CREATE TABLE … USING parquet AS SELECT * FROM source` produces a
# raw Parquet table. Loses Delta history, deletion vectors, change
# data feed, time-travel — Parquet has none of those concepts. The
# convert UI gates this behind an `acknowledge_history_loss` flag so
# users see the consequence; without that flag the request validator
# rejects the pair with a structured 422.
#
# In-place semantics require a temp+rename dance because
# CREATE OR REPLACE doesn't change the underlying USING clause on an
# existing Delta table. This is built into the plan rather than
# composed at the call site so the dry-run preview shows the full
# multi-statement sequence.


def ctas_parquet_inplace_plan(
    source_fqn: str,
    *,
    where: str | None = None,
    keep_backup: bool = True,
) -> Plan:
    """Plan for replacing `source_fqn` (a Delta or Iceberg table) with
    a Parquet table at the same FQN, via temp+rename.

    `keep_backup=True` (default) renames the source out of the way to
    `{fqn}_pre_convert_<timestamp>` so the conversion is reversible.
    `False` drops the source after the rename — non-recoverable, only
    for callers who really mean it.

    The temp name is deterministic (suffix `_convert_tmp`) for the
    same operation_id so a retry or partial failure leaves a single
    artefact for the operator to inspect rather than a cascade of
    timestamped tables.
    """
    plan = Plan()
    temp_fqn = _temp_fqn(source_fqn)
    select_clause = f"SELECT * FROM {source_fqn}"
    if where:
        select_clause += f" WHERE {where}"
    plan.add(
        "create parquet table at temp fqn",
        f"CREATE TABLE {temp_fqn} USING parquet AS {select_clause}",
    )
    if keep_backup:
        backup_fqn = _backup_fqn(source_fqn)
        plan.add(
            "rename source to backup fqn",
            f"ALTER TABLE {source_fqn} RENAME TO {backup_fqn}",
        )
    else:
        plan.add(
            "drop source table",
            f"DROP TABLE {source_fqn}",
        )
    plan.add(
        "rename temp to original fqn",
        f"ALTER TABLE {temp_fqn} RENAME TO {source_fqn}",
    )
    return plan


def ctas_iceberg_inplace_plan(
    source_fqn: str,
    *,
    where: str | None = None,
    keep_backup: bool = True,
) -> Plan:
    """Plan for replacing `source_fqn` with an Iceberg table at the
    same FQN. Same temp+rename dance as the Parquet variant; only the
    ``USING <fmt>`` clause differs.

    Use this when the user wants the converter to leave the FQN
    pointing at a real Iceberg table rather than a Delta table with
    UniForm metadata. The two paths produce different physical
    outcomes — the Convert UI lets the user pick.
    """
    plan = Plan()
    temp_fqn = _temp_fqn(source_fqn)
    select_clause = f"SELECT * FROM {source_fqn}"
    if where:
        select_clause += f" WHERE {where}"
    plan.add(
        "create iceberg table at temp fqn",
        f"CREATE TABLE {temp_fqn} USING iceberg AS {select_clause}",
    )
    if keep_backup:
        backup_fqn = _backup_fqn(source_fqn)
        plan.add(
            "rename source to backup fqn",
            f"ALTER TABLE {source_fqn} RENAME TO {backup_fqn}",
        )
    else:
        plan.add(
            "drop source table",
            f"DROP TABLE {source_fqn}",
        )
    plan.add(
        "rename temp to original fqn",
        f"ALTER TABLE {temp_fqn} RENAME TO {source_fqn}",
    )
    return plan


def _temp_fqn(source_fqn: str) -> str:
    """Build the deterministic temp-table FQN for an in-place CTAS.

    Suffix is `_convert_tmp` rather than a timestamp — deterministic
    means a retry against the same source produces the same temp name,
    so a half-finished previous run can be resumed (or cleaned up
    manually) without timestamped detritus piling up.

    The suffix is appended to the *table* segment, not the catalog or
    schema. Backticks are preserved if the caller already quoted the
    parts (which `_qualify` does by default).
    """
    parts = source_fqn.rsplit(".", 1)
    if len(parts) != 2:
        # Unqualified or single-part name — caller error, but fall back
        # to suffixing the whole thing rather than splitting incorrectly.
        return f"{source_fqn}_convert_tmp"
    prefix, last = parts
    if last.startswith("`") and last.endswith("`"):
        return f"{prefix}.`{last[1:-1]}_convert_tmp`"
    return f"{prefix}.{last}_convert_tmp"


def _backup_fqn(source_fqn: str) -> str:
    """Build the backup FQN used when `keep_backup=True` on an in-place
    CTAS. Suffix is `_pre_convert_<utc-yyyymmddHHMMSS>` so multiple
    backups from different runs don't collide.
    """
    from datetime import datetime, timezone

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    parts = source_fqn.rsplit(".", 1)
    if len(parts) != 2:
        return f"{source_fqn}_pre_convert_{stamp}"
    prefix, last = parts
    if last.startswith("`") and last.endswith("`"):
        return f"{prefix}.`{last[1:-1]}_pre_convert_{stamp}`"
    return f"{prefix}.{last}_pre_convert_{stamp}"
