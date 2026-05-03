"""Tests for src/format_strategies.py — D2 of #9 N×N converter.

Each plan-builder is tested at three levels:
  1. The right SQL statements appear in the right order.
  2. Optional flags (`where`, `keep_backup`, `tbl_properties`) toggle
     the right plan steps.
  3. ``Plan.execute`` raises with a labelled error when a step fails.

These primitives are the building blocks the convert orchestrator and
clone path both compose, so a regression here would cascade into both
features. Worth the unit-level coverage.
"""

from unittest.mock import MagicMock, patch

import pytest

from src.format_strategies import (
    Plan,
    PlanStep,
    ctas_avro_inplace_plan,
    ctas_iceberg_inplace_plan,
    ctas_json_inplace_plan,
    ctas_orc_inplace_plan,
    ctas_parquet_inplace_plan,
    enable_uniform_hudi_plan,
    enable_uniform_plan,
    export_to_volume_plan,
)


def test_enable_uniform_plan_emits_three_steps_in_order():
    """The Databricks IcebergCompatV2 validator demands the exact
    sequence: disable DV → REORG PURGE → SET props. Reordering or
    collapsing steps trips the validator with a cryptic error;
    asserting on the order here catches future "let me simplify this"
    refactors before they ship."""
    plan = enable_uniform_plan("`cat`.`schema`.`tbl`")
    assert len(plan.steps) == 3
    assert plan.steps[0].label == "disable deletion vectors"
    assert "delta.enableDeletionVectors" in plan.steps[0].sql
    assert plan.steps[1].label == "purge deletion vector files"
    assert "REORG TABLE" in plan.steps[1].sql
    assert plan.steps[2].label == "enable Iceberg compat metadata"
    assert "delta.universalFormat.enabledFormats" in plan.steps[2].sql


def test_ctas_iceberg_inplace_with_keep_backup_renames_aside():
    """Default `keep_backup=True` produces a 3-step plan: create at
    temp FQN, rename source to `_pre_convert_<utc>`, rename temp to
    original. Asserts each step is present so a future change that
    skips the backup step is caught."""
    plan = ctas_iceberg_inplace_plan("`cat`.`schema`.`tbl`", keep_backup=True)
    assert len(plan.steps) == 3
    assert "USING iceberg AS SELECT" in plan.steps[0].sql
    assert "_convert_tmp" in plan.steps[0].sql
    assert "RENAME TO" in plan.steps[1].sql
    assert "_pre_convert_" in plan.steps[1].sql
    assert "RENAME TO" in plan.steps[2].sql


def test_ctas_iceberg_inplace_without_keep_backup_drops_source():
    """`keep_backup=False` produces the same 3 steps but with a DROP
    instead of a RENAME-to-backup. Non-recoverable; only the operator
    can pick this knowingly."""
    plan = ctas_iceberg_inplace_plan("`cat`.`schema`.`tbl`", keep_backup=False)
    assert len(plan.steps) == 3
    drops = [s for s in plan.steps if s.sql.startswith("DROP TABLE")]
    assert len(drops) == 1


def test_ctas_parquet_inplace_emits_using_parquet():
    """Parquet target uses the same temp+rename dance as Iceberg, only
    the USING clause differs. Same plan shape; different SQL fragment
    in the create step."""
    plan = ctas_parquet_inplace_plan("`cat`.`schema`.`tbl`", keep_backup=True)
    create_sqls = [s.sql for s in plan.steps if "USING" in s.sql]
    assert len(create_sqls) == 1
    assert "USING parquet" in create_sqls[0]


def test_ctas_avro_inplace_emits_using_avro():
    """Avro target — row-oriented sink for streaming consumers. Same
    temp+rename plan shape as Parquet; only ``USING <fmt>`` differs."""
    plan = ctas_avro_inplace_plan("`cat`.`schema`.`tbl`", keep_backup=True)
    assert len(plan.steps) == 3
    assert "USING avro AS SELECT" in plan.steps[0].sql
    assert "_convert_tmp" in plan.steps[0].sql


def test_ctas_orc_inplace_emits_using_orc():
    """ORC target — Hive-era columnar interop. Same temp+rename plan
    shape as Parquet."""
    plan = ctas_orc_inplace_plan("`cat`.`schema`.`tbl`", keep_backup=True)
    assert len(plan.steps) == 3
    assert "USING orc AS SELECT" in plan.steps[0].sql
    assert "_convert_tmp" in plan.steps[0].sql


def test_ctas_avro_inplace_without_keep_backup_drops_source():
    """Same recoverability semantic as the other CTAS factories — when
    ``keep_backup=False``, the rename-aside step is replaced with a
    DROP TABLE on the source."""
    plan = ctas_avro_inplace_plan("`cat`.`schema`.`tbl`", keep_backup=False)
    drops = [s for s in plan.steps if s.sql.startswith("DROP TABLE")]
    assert len(drops) == 1


def test_ctas_orc_inplace_with_where_clause_renders_filter():
    """Partition-pruning via ``where`` works for ORC just like the
    other CTAS factories — the filter is appended to the SELECT in
    the create step."""
    plan = ctas_orc_inplace_plan(
        "`cat`.`schema`.`tbl`",
        where="region = 'eu'",
        keep_backup=True,
    )
    assert "WHERE region = 'eu'" in plan.steps[0].sql


def test_ctas_json_inplace_emits_using_json():
    """JSON target — export-shaped sink for HTTP webhooks / NoSQL
    pipelines. Same temp+rename plan shape as Parquet/AVRO/ORC."""
    plan = ctas_json_inplace_plan("`cat`.`schema`.`tbl`", keep_backup=True)
    assert len(plan.steps) == 3
    assert "USING json AS SELECT" in plan.steps[0].sql
    assert "_convert_tmp" in plan.steps[0].sql


def test_enable_uniform_hudi_plan_emits_three_steps_in_order():
    """Hudi UniForm follows the same shape as Iceberg UniForm — disable
    DV → REORG PURGE → SET props. The third step's TBLPROPERTIES MUST
    set both ``delta.enableHudiCompatV1`` and
    ``delta.universalFormat.enabledFormats = 'hudi'`` — Databricks
    rejects the alter if either one is missing or set to the wrong
    value."""
    plan = enable_uniform_hudi_plan("`cat`.`schema`.`tbl`")
    assert len(plan.steps) == 3
    assert plan.steps[0].label == "disable deletion vectors"
    assert "delta.enableDeletionVectors" in plan.steps[0].sql
    assert plan.steps[1].label == "purge deletion vector files"
    assert "REORG TABLE" in plan.steps[1].sql
    # The label includes "Beta" so the dry-run preview surfaces the
    # caveat.
    assert "Beta" in plan.steps[2].label
    assert "delta.enableHudiCompatV1" in plan.steps[2].sql
    assert "'hudi'" in plan.steps[2].sql


def test_export_to_volume_plan_emits_single_insert_overwrite_directory():
    """Export-shaped targets (PARQUET / AVRO / ORC / JSON) write raw
    files to a Volume — UC managed tables can't be these formats.
    Single-step plan: one ``INSERT OVERWRITE DIRECTORY '<volume>'
    USING <fmt> SELECT * FROM <source>``. Source preserved."""
    plan = export_to_volume_plan(
        "`cat`.`schema`.`tbl`",
        fmt="parquet",
        volume_path="/Volumes/cat/schema/exports/tbl_parquet/",
    )
    assert len(plan.steps) == 1
    sql = plan.steps[0].sql
    assert sql.startswith("INSERT OVERWRITE DIRECTORY ")
    assert "'/Volumes/cat/schema/exports/tbl_parquet/'" in sql
    assert "USING parquet" in sql
    assert "SELECT * FROM `cat`.`schema`.`tbl`" in sql


def test_export_to_volume_plan_supports_every_export_format():
    """The dispatch routes PARQUET / AVRO / ORC / JSON through this
    factory — assert each format renders cleanly so a future change
    that breaks one fmt doesn't slip past unit tests."""
    for fmt in ("parquet", "avro", "orc", "json"):
        plan = export_to_volume_plan(
            "`cat`.`schema`.`tbl`",
            fmt=fmt,
            volume_path=f"/Volumes/cat/schema/exports/tbl_{fmt}/",
        )
        assert f"USING {fmt}" in plan.steps[0].sql


def test_export_to_volume_plan_with_where_clause_filters_select():
    """``where`` lets operators partition-prune before writing — common
    when only the latest partition needs to land in the export sink."""
    plan = export_to_volume_plan(
        "`cat`.`schema`.`tbl`",
        fmt="json",
        volume_path="/Volumes/cat/schema/exports/tbl/",
        where="dt >= '2026-01-01'",
    )
    assert "WHERE dt >= '2026-01-01'" in plan.steps[0].sql


def test_enable_uniform_plan_still_emits_iceberg_props():
    """Regression guard for the refactor that introduced the generic
    `_enable_uniform_plan` helper — the Iceberg variant must still
    emit `delta.enableIcebergCompatV2` and `enabledFormats = 'iceberg'`
    (not the Hudi values)."""
    plan = enable_uniform_plan("`cat`.`schema`.`tbl`")
    set_step_sql = plan.steps[2].sql
    assert "delta.enableIcebergCompatV2" in set_step_sql
    assert "'iceberg'" in set_step_sql
    assert "Hudi" not in set_step_sql
    assert "hudi" not in set_step_sql


def test_ctas_with_where_clause_renders_filter():
    """The `where` argument applies to the SELECT, not the rename. A
    common operator use case is partition-pruning before format
    conversion to avoid copying historical partitions."""
    plan = ctas_iceberg_inplace_plan(
        "`cat`.`schema`.`tbl`",
        where="region = 'us'",
        keep_backup=True,
    )
    create_sql = plan.steps[0].sql
    assert "WHERE region = 'us'" in create_sql


def test_ctas_iceberg_inplace_supports_tbl_properties_via_alter():
    """ctas_iceberg_plan (the non-in-place variant used by the clone
    path) accepts tbl_properties — the in-place variant doesn't take
    the kwarg by design (post-rename TBLPROPERTIES on a table that
    might be mid-rename is fragile). Asserting the sig here so a
    future refactor that adds the kwarg has to reckon with the design
    decision."""
    import inspect

    sig = inspect.signature(ctas_iceberg_inplace_plan)
    assert "tbl_properties" not in sig.parameters
    # Whereas the non-in-place variant DOES accept it. Documenting via test.
    from src.format_strategies import ctas_iceberg_plan

    sig2 = inspect.signature(ctas_iceberg_plan)
    assert "tbl_properties" in sig2.parameters


@patch("src.format_strategies.execute_sql")
def test_plan_execute_runs_every_step_in_order(mock_sql):
    """Plan.execute fires execute_sql once per step in declaration
    order. Asserting the order so anyone refactoring `execute` to a
    parallel/async variant has to update this test deliberately."""
    plan = Plan()
    plan.add("step a", "SELECT 1")
    plan.add("step b", "SELECT 2")
    plan.add("step c", "SELECT 3")
    plan.execute(MagicMock(), "wh-1")
    assert mock_sql.call_count == 3
    sqls = [c[0][2] for c in mock_sql.call_args_list]
    assert sqls == ["SELECT 1", "SELECT 2", "SELECT 3"]


@patch("src.format_strategies.execute_sql")
def test_plan_execute_wraps_failure_with_step_label(mock_sql):
    """When a step raises, the wrapper re-raises with the step's label
    prefixed so the operator sees `step 'disable deletion vectors'
    failed: ...` rather than a bare SQL error. Preserves the original
    exception via `from e` for tracebacks."""
    mock_sql.side_effect = [None, RuntimeError("permission denied")]
    plan = Plan()
    plan.add("first step", "SELECT 1")
    plan.add("disable deletion vectors", "ALTER TABLE …")
    with pytest.raises(RuntimeError) as exc:
        plan.execute(MagicMock(), "wh-1")
    msg = str(exc.value)
    assert "disable deletion vectors" in msg
    assert "permission denied" in msg


@patch("src.format_strategies.execute_sql")
def test_plan_execute_dry_run_passes_flag_through(mock_sql):
    """dry_run=True propagates to execute_sql so the `[DRY RUN]` log
    discipline applies even for plans built outside the convert
    orchestrator (e.g. a future CLI surface that wants to render the
    plan without executing)."""
    plan = Plan()
    plan.add("only step", "SELECT 1")
    plan.execute(MagicMock(), "wh-1", dry_run=True)
    assert mock_sql.call_args.kwargs.get("dry_run") is True


def test_plan_step_dataclass_holds_label_and_sql():
    """Defensive: PlanStep is intentionally a flat dataclass so the
    audit layer can serialize it later if needed. Renaming fields
    would break that future surface."""
    step = PlanStep(label="x", sql="SELECT 1")
    assert step.label == "x"
    assert step.sql == "SELECT 1"
