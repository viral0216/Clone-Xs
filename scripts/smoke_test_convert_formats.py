#!/usr/bin/env python3
"""Live smoke test for the convert-format dispatch matrix.

Runs every (DELTA → target) cell against a real Databricks workspace
and reports per-cell pass/fail. Designed to catch the kinds of
warehouse-side rejections that unit tests can't (e.g. the Hudi-vs-
Iceberg-UniForm conflict that shipped to prod with green tests).

USAGE
-----
    python scripts/smoke_test_convert_formats.py \\
        --catalog edp_dev \\
        --schema bronze \\
        --volume clone_xs_smoke \\
        --warehouse-id <warehouse-id>

PRE-REQS
--------
    1. ``DATABRICKS_HOST`` + ``DATABRICKS_TOKEN`` in the environment
       (or any ``Config()``-compatible auth).
    2. The catalog/schema given must be writable by the caller.
    3. The Volume given must exist with WRITE FILES privilege —
       create it once with::

           CREATE VOLUME <catalog>.<schema>.<volume>;

WHAT IT TESTS
-------------
For each target format the convert API supports from a Delta source,
the script:

    1. Creates a fresh fixture Delta table (drops on retry).
    2. Calls ``convert_table_format(...)`` with the appropriate args
       (e.g. ``destination_path`` for export-shaped targets).
    3. Records the outcome — converted / failed / skipped — and
       captures the strategy label + duration for the results table.
    4. Drops the fixture so the test is idempotent.

Fixtures use scratch table names suffixed with the target format and
a UTC timestamp so concurrent runs don't collide.

WHAT IT DOES NOT TEST
---------------------
    - PARQUET / ICEBERG sources — would need pre-existing fixtures
      created out-of-band (UC managed tables can't be Parquet, and
      Managed Iceberg requires workspace-level support). Those source
      formats are exercised by the existing pytest suite with mocks.
    - Schema-evolution edge cases — every fixture has the same trivial
      ``(id BIGINT, name STRING)`` schema. Real-world tables hit more
      compat preflight rules (GENERATED columns, hidden Iceberg
      partitions); those are unit-tested separately.
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

# Make `src.` imports resolve when the script is run from the repo
# root via a venv that hasn't installed the package in dev mode.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from databricks.sdk import WorkspaceClient  # noqa: E402

from src.client import execute_sql  # noqa: E402
from src.convert_to_delta import convert_table_format  # noqa: E402


# Cells we want to validate live. Source is always DELTA — every other
# source format would need an external fixture. Each entry is the
# target format and a one-line description of what the cell exercises.
TARGET_CELLS: list[tuple[str, str]] = [
    ("DELTA", "identity skip — short-circuits without touching the warehouse"),
    ("ICEBERG", "Delta → Iceberg UniForm (sidecar metadata, no data movement)"),
    ("PARQUET", "Delta → Parquet export to Volume (INSERT OVERWRITE DIRECTORY)"),
    ("AVRO", "Delta → Avro export to Volume"),
    ("ORC", "Delta → ORC export to Volume"),
    ("JSON", "Delta → JSON export to Volume"),
    ("HUDI", "Delta → Hudi UniForm Beta (sidecar metadata)"),
]

# Targets that write files to a Volume rather than rewriting the table
# in place. Mirrors ``api/models/convert_to_delta._export_targets_*``.
EXPORT_TARGETS: frozenset[str] = frozenset({"PARQUET", "AVRO", "ORC", "JSON"})


@dataclass
class CellResult:
    target: str
    status: str  # "converted" | "failed" | "skipped"
    strategy: str = ""
    duration_ms: int = 0
    error: str = ""


@dataclass
class SmokeReport:
    cells: list[CellResult] = field(default_factory=list)
    fixtures_created: int = 0
    fixtures_dropped: int = 0


def _utc_suffix() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _fixture_fqn(catalog: str, schema: str, target: str, run_id: str) -> str:
    """Return a per-target fixture FQN. Suffixing with ``run_id`` lets
    concurrent smoke runs coexist; suffixing with ``target`` makes the
    table easy to identify in UC if cleanup is interrupted."""
    return f"{catalog}.{schema}.smoke_convert_{target.lower()}_{run_id}"


def _create_delta_fixture(client: WorkspaceClient, warehouse_id: str, fqn: str) -> None:
    """Drop-and-recreate a tiny Delta table so each cell starts from a
    known clean state. Idempotent: prior runs of the same FQN are
    cleared first."""
    execute_sql(client, warehouse_id, f"DROP TABLE IF EXISTS {fqn}")
    execute_sql(
        client,
        warehouse_id,
        f"CREATE TABLE {fqn} (id BIGINT, name STRING) USING delta",
    )
    execute_sql(
        client,
        warehouse_id,
        f"INSERT INTO {fqn} VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')",
    )


def _drop_fixture(client: WorkspaceClient, warehouse_id: str, fqn: str) -> None:
    """Best-effort cleanup. Failure is logged but doesn't fail the
    smoke test — the operator can drop the table by hand if needed."""
    try:
        execute_sql(client, warehouse_id, f"DROP TABLE IF EXISTS {fqn}")
    except Exception as e:
        print(f"  ⚠️  cleanup: could not drop {fqn}: {e}")


def _volume_path_for(catalog: str, schema: str, volume: str, target: str, run_id: str) -> str:
    """Per-target export sub-path under the operator's Volume so each
    cell writes to its own directory and we can inspect / clean them
    independently."""
    return f"/Volumes/{catalog}/{schema}/{volume}/smoke_{target.lower()}_{run_id}/"


def run_cell(
    client: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
    schema: str,
    volume: str,
    target: str,
    run_id: str,
) -> CellResult:
    """Run one (DELTA → target) cell end-to-end.

    Each cell gets its own fixture so a failure doesn't poison the
    next cell's starting state. The fixture is dropped in ``finally``
    even when convert raises.
    """
    fixture = _fixture_fqn(catalog, schema, target, run_id)
    print(f"\n→ {target}: {fixture}")

    try:
        _create_delta_fixture(client, warehouse_id, fixture)
    except Exception as e:
        return CellResult(
            target=target,
            status="failed",
            error=f"fixture create failed: {e}",
        )

    try:
        kwargs: dict = {"target_format": target}
        if target in EXPORT_TARGETS:
            kwargs["destination_path"] = _volume_path_for(catalog, schema, volume, target, run_id)

        result = convert_table_format(client, warehouse_id, fixture, "DELTA", **kwargs)
        return CellResult(
            target=target,
            status=result.status,
            strategy=result.strategy_used,
            duration_ms=result.duration_ms,
            error=result.error or "",
        )
    finally:
        _drop_fixture(client, warehouse_id, fixture)


def render_report(report: SmokeReport) -> str:
    """Pretty-print the per-cell results as a fixed-width table the
    user can paste into a chat / issue tracker without losing
    alignment."""
    lines = [
        "",
        "=" * 88,
        f"{'TARGET':<10} {'STATUS':<10} {'STRATEGY':<22} {'DURATION':>10}  DETAIL",
        "-" * 88,
    ]
    for c in report.cells:
        # Truncate detail/error to fit; full text already printed above.
        detail = (c.error or "")[:40]
        lines.append(
            f"{c.target:<10} {c.status:<10} {c.strategy:<22} {c.duration_ms:>8} ms  {detail}"
        )
    lines.append("=" * 88)
    converted = sum(1 for c in report.cells if c.status == "converted")
    failed = sum(1 for c in report.cells if c.status == "failed")
    skipped = sum(1 for c in report.cells if c.status == "skipped")
    lines.append(
        f"Total: {len(report.cells)}  ·  converted: {converted}  ·  "
        f"failed: {failed}  ·  skipped: {skipped}"
    )
    return "\n".join(lines)


def main() -> int:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--catalog", required=True, help="Target catalog for fixture tables")
    p.add_argument("--schema", required=True, help="Target schema for fixture tables")
    p.add_argument(
        "--volume",
        required=True,
        help="Existing Volume (in <catalog>.<schema>) the export-shaped cells will write into",
    )
    p.add_argument(
        "--warehouse-id",
        default=os.environ.get("DATABRICKS_WAREHOUSE_ID"),
        help="SQL warehouse ID (defaults to $DATABRICKS_WAREHOUSE_ID)",
    )
    args = p.parse_args()

    if not args.warehouse_id:
        print("ERROR: --warehouse-id required (or set $DATABRICKS_WAREHOUSE_ID)", file=sys.stderr)
        return 2

    client = WorkspaceClient()
    run_id = _utc_suffix()
    print(f"Smoke-testing convert-format dispatch — run_id={run_id}")
    print(f"  catalog/schema: {args.catalog}.{args.schema}")
    print(f"  Volume:         {args.volume}")
    print(f"  Warehouse:      {args.warehouse_id}")
    print(f"  Cells to run:   {len(TARGET_CELLS)}")

    report = SmokeReport()
    for target, _description in TARGET_CELLS:
        cell = run_cell(
            client,
            args.warehouse_id,
            args.catalog,
            args.schema,
            args.volume,
            target,
            run_id,
        )
        report.cells.append(cell)

    print(render_report(report))

    # Exit non-zero if any cell ran-but-failed. Skipped cells are NOT
    # treated as failure — identity skip is expected, and unsupported-
    # pair skips happen on workspaces missing optional features (e.g.
    # Hudi UniForm pre-DBR-15).
    return 1 if any(c.status == "failed" for c in report.cells) else 0


if __name__ == "__main__":
    sys.exit(main())
