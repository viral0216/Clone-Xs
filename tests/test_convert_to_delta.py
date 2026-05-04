"""Tests for src/convert_to_delta.py — backlog item #13.

Coverage of the safety gate (confirm_destructive), the supported-format
filter, the dry-run path, and per-table outcome shape. Endpoint-level
tests live in test_convert_to_delta_api.py.
"""

from unittest.mock import MagicMock, patch

import pytest

from src.convert_to_delta import (
    SUPPORTED_PAIRS,
    SUPPORTED_SOURCE_FORMATS,
    ConvertToDeltaError,
    convert_table_format,
    convert_table_to_delta,
    convert_tables_to_delta,
)


@patch("src.format_strategies.execute_sql")
def test_convert_table_emits_correct_sql_for_iceberg(mock_sql):
    """Iceberg source → ``CONVERT TO DELTA `cat`.`schema`.`tbl```. The
    backticks are added by ``_qualify`` so reserved-word table names
    don't break the SQL."""
    mock_sql.return_value = []
    result = convert_table_to_delta(
        MagicMock(),
        "wh-1",
        "edp_dev.bronze.events_iceberg",
        "ICEBERG",
    )
    assert result.status == "converted"
    sql = mock_sql.call_args[0][2]
    assert sql == "CONVERT TO DELTA `edp_dev`.`bronze`.`events_iceberg`"


@patch("src.format_strategies.execute_sql")
def test_convert_table_skips_already_delta_with_no_sql(mock_sql):
    """Delta sources are obvious no-ops. Important: we must not run any
    SQL at all — running CONVERT TO DELTA on a Delta table errors out
    in some DBR versions and would mask a more useful message."""
    result = convert_table_to_delta(
        MagicMock(),
        "wh-1",
        "edp_dev.bronze.already_delta",
        "DELTA",
    )
    assert result.status == "skipped"
    # Message is upper-cased now ("already DELTA") since the per-table
    # function normalises the format string before rendering. Same
    # semantic — already-target tables are no-ops.
    assert "already" in (result.error or "")
    assert "DELTA" in (result.error or "")
    assert mock_sql.call_count == 0


@patch("src.format_strategies.execute_sql")
def test_convert_table_skips_unsupported_format_with_no_sql(mock_sql):
    """``CONVERT TO DELTA`` doesn't accept arbitrary formats — only
    Parquet and Iceberg. Anything else (CSV, JSON, materialised views)
    is skipped with a clear error reason and no warehouse round-trip."""
    result = convert_table_to_delta(
        MagicMock(),
        "wh-1",
        "edp_dev.bronze.weird_csv",
        "CSV",
    )
    assert result.status == "skipped"
    # Pair-aware semantics now: the skip reason references the
    # (source, target) pair instead of just the source format. CSV→DELTA
    # isn't in SUPPORTED_PAIRS, so it surfaces as "pair CSV→DELTA not
    # yet supported".
    assert "CSV" in (result.error or "")
    assert "not yet supported" in (result.error or "")
    assert mock_sql.call_count == 0


@patch("src.format_strategies.execute_sql")
def test_convert_table_dry_run_does_not_execute(mock_sql):
    """Dry-run logs the SQL but never hits the warehouse. Wizard preview
    relies on this to show users exactly what would run."""
    result = convert_table_to_delta(
        MagicMock(),
        "wh-1",
        "edp_dev.bronze.events",
        "ICEBERG",
        dry_run=True,
    )
    assert result.status == "skipped"
    assert "dry-run" in (result.error or "")
    assert mock_sql.call_count == 0


@patch("src.format_strategies.execute_sql")
def test_convert_table_failure_is_captured_not_raised(mock_sql):
    """Per-table failures must be captured in the result (status='failed',
    error=str) rather than raised — the orchestrator continues with
    subsequent tables. Auto-stopping on first failure would leave the
    user with a worse-than-before state and no clear recovery path."""
    mock_sql.side_effect = Exception("USE CATALOG required")
    result = convert_table_to_delta(
        MagicMock(),
        "wh-1",
        "edp_dev.bronze.events",
        "ICEBERG",
    )
    assert result.status == "failed"
    assert "USE CATALOG required" in (result.error or "")


def test_convert_tables_refuses_without_confirm_destructive():
    """The safety gate. Without explicit confirmation, the orchestrator
    raises ConvertToDeltaError before any SQL runs. Defence in depth —
    the API model also validates this, but we don't trust callers to
    have done so."""
    with pytest.raises(ConvertToDeltaError) as exc:
        convert_tables_to_delta(
            MagicMock(),
            "wh-1",
            [("edp_dev.bronze.events", "ICEBERG")],
            confirm_destructive=False,
        )
    assert "destructive" in str(exc.value)
    assert "confirm_destructive" in str(exc.value)


def test_convert_tables_dry_run_bypasses_confirm_gate():
    """Dry-run is safe by definition — it never executes SQL — so the
    confirm-destructive gate doesn't apply. Wizard previews must work
    without users having to type the destructive-action confirmation
    just to see what would happen."""
    # No exception expected even though confirm_destructive=False.
    summary = convert_tables_to_delta(
        MagicMock(),
        "wh-1",
        [("edp_dev.bronze.events", "ICEBERG")],
        confirm_destructive=False,
        dry_run=True,
    )
    assert summary.total == 1
    assert summary.skipped == 1  # dry-run rows always count as skipped


@patch("src.format_strategies.execute_sql")
def test_convert_tables_aggregates_mixed_outcomes(mock_sql):
    """A batch with success + failure + skip produces a summary that
    breaks down each bucket. Important so callers can tell whether to
    re-try the batch (some failed) or move on (all succeeded)."""
    # First call (events_iceberg) succeeds, second (locked_table) fails.
    # already_delta is skipped before any SQL runs.
    mock_sql.side_effect = [[], Exception("permission denied")]
    summary = convert_tables_to_delta(
        MagicMock(),
        "wh-1",
        [
            ("edp_dev.bronze.events_iceberg", "ICEBERG"),
            ("edp_dev.bronze.already_delta", "DELTA"),
            ("edp_dev.bronze.locked_table", "PARQUET"),
        ],
        confirm_destructive=True,
    )
    assert summary.total == 3
    assert summary.converted == 1
    assert summary.skipped == 1
    assert summary.failed == 1
    assert mock_sql.call_count == 2  # delta one didn't hit warehouse


def test_supported_source_formats_set_is_complete():
    """Sanity-check the supported-format whitelist. If Databricks adds a
    new convertible format, this test reminds us to update the constant."""
    assert SUPPORTED_SOURCE_FORMATS == {"PARQUET", "ICEBERG"}


# Audit callback (#13 follow-up). The orchestrator fires audit_callback
# once per target with (result, started_at, completed_at). The API layer
# constructs a callback that writes to the convert_operations Delta
# table; the unit tests below use a list-collecting closure so we can
# assert the call shape independently of the audit-table SQL.


@patch("src.format_strategies.execute_sql")
def test_convert_table_fires_audit_callback_on_success(mock_sql):
    """A successful CONVERT TO DELTA fires the callback exactly once with
    status='converted' and timestamps that bracket the call."""
    mock_sql.return_value = []
    captured: list[tuple] = []

    def cb(result, started_at, completed_at):
        captured.append((result, started_at, completed_at))

    convert_table_to_delta(
        MagicMock(),
        "wh-1",
        "edp_dev.bronze.events",
        "ICEBERG",
        audit_callback=cb,
    )
    assert len(captured) == 1
    result, started_at, completed_at = captured[0]
    assert result.status == "converted"
    assert result.fqn == "edp_dev.bronze.events"
    assert completed_at >= started_at  # not strict because mocks run < 1ms


@patch("src.format_strategies.execute_sql")
def test_convert_table_fires_audit_callback_on_failure(mock_sql):
    """Failed conversions also produce an audit row — auditing only
    successes would let silent failures hide forever."""
    mock_sql.side_effect = Exception("permission denied")
    captured: list[tuple] = []

    def cb(result, started_at, completed_at):
        captured.append((result, started_at, completed_at))

    convert_table_to_delta(
        MagicMock(),
        "wh-1",
        "edp_dev.bronze.events",
        "ICEBERG",
        audit_callback=cb,
    )
    assert len(captured) == 1
    assert captured[0][0].status == "failed"
    assert "permission denied" in (captured[0][0].error or "")


@patch("src.format_strategies.execute_sql")
def test_convert_table_fires_audit_callback_on_skip(mock_sql):
    """Already-Delta and unsupported-format skips both produce audit
    rows so a per-table report shows the full set of input tables, not
    just the ones we attempted to mutate."""
    captured: list[tuple] = []

    def cb(result, _start, _end):
        captured.append(result)

    convert_table_to_delta(
        MagicMock(),
        "wh-1",
        "edp_dev.bronze.x",
        "DELTA",
        audit_callback=cb,
    )
    convert_table_to_delta(
        MagicMock(),
        "wh-1",
        "edp_dev.bronze.y",
        "CSV",
        audit_callback=cb,
    )
    assert [r.status for r in captured] == ["skipped", "skipped"]
    assert mock_sql.call_count == 0


@patch("src.format_strategies.execute_sql")
def test_convert_table_swallows_audit_callback_exceptions(mock_sql):
    """If the audit callback raises, the conversion result is still
    returned — audit failures must never fail the conversion. (Defence
    in depth: the API layer's callback already swallows internally, but
    a buggy custom callback shouldn't break the operation.)"""
    mock_sql.return_value = []

    def bad_cb(*_args, **_kwargs):
        raise RuntimeError("audit table dropped under us")

    result = convert_table_to_delta(
        MagicMock(),
        "wh-1",
        "edp_dev.bronze.events",
        "ICEBERG",
        audit_callback=bad_cb,
    )
    assert result.status == "converted"


# query_convert_history — SQL emission, filter wiring, defensive
# fallback to [] when the audit table doesn't exist. Endpoint-level
# behaviour lives in test_router_convert_to_delta.


@patch("src.audit_trail.execute_sql")
def test_query_convert_history_emits_correct_select(mock_sql):
    """Default call (no filters) emits an unfiltered SELECT ordered by
    recorded_at DESC. Limit clamps to the request value."""
    from src.audit_trail import query_convert_history

    mock_sql.return_value = []
    query_convert_history(MagicMock(), "wh-1", {}, limit=10)
    sql = mock_sql.call_args[0][2]
    # D1 added `destination_format` to the SELECT list — assert the
    # critical columns are present rather than the exact prefix string,
    # so future column additions don't break this test.
    assert "operation_id" in sql
    assert "fqn" in sql
    assert "source_format" in sql
    assert "destination_format" in sql
    assert "status" in sql
    assert "ORDER BY recorded_at DESC" in sql
    assert "LIMIT 10" in sql
    assert "WHERE" not in sql  # no filters supplied


@patch("src.audit_trail.execute_sql")
def test_query_convert_history_applies_filters(mock_sql):
    """Each filter renders a corresponding WHERE clause. Single-quote
    escaping is exercised to defend against SQL injection from user
    input on `fqn_like`."""
    from src.audit_trail import query_convert_history

    mock_sql.return_value = []
    query_convert_history(
        MagicMock(),
        "wh-1",
        {},
        status="failed",
        fqn_like="edp.bronze.%'",  # smuggled quote — must be escaped
        dry_run=False,
        operation_id="op-abc",
    )
    sql = mock_sql.call_args[0][2]
    assert "status = 'failed'" in sql
    assert "edp.bronze.%''" in sql  # escaped doubled quote
    assert "dry_run = false" in sql
    assert "operation_id = 'op-abc'" in sql


@patch("src.audit_trail.execute_sql")
def test_query_convert_history_caps_limit(mock_sql):
    """A caller asking for limit=99999 gets clamped to 1000. Protects
    the warehouse against accidentally pulling the whole history table
    in one round-trip."""
    from src.audit_trail import query_convert_history

    mock_sql.return_value = []
    query_convert_history(MagicMock(), "wh-1", {}, limit=99999)
    sql = mock_sql.call_args[0][2]
    assert "LIMIT 1000" in sql


@patch("src.audit_trail.execute_sql")
def test_query_convert_history_returns_empty_on_query_failure(mock_sql):
    """Audit table missing, permission denied, or any other SQL
    failure → empty list, not exception. The history endpoint relies
    on this for the no-rows-yet UX."""
    from src.audit_trail import query_convert_history

    mock_sql.side_effect = Exception("Table or view not found")
    result = query_convert_history(MagicMock(), "wh-1", {}, limit=10)
    assert result == []


@patch("src.format_strategies.execute_sql")
def test_convert_tables_propagates_audit_callback_to_each_target(mock_sql):
    """The batch orchestrator passes the same callback to each per-target
    convert. Important so callers wire audit once and get one row per
    target, not zero or duplicates."""
    mock_sql.return_value = []
    captured_fqns: list[str] = []

    def cb(result, _start, _end):
        captured_fqns.append(result.fqn)

    convert_tables_to_delta(
        MagicMock(),
        "wh-1",
        [
            ("edp_dev.bronze.a", "ICEBERG"),
            ("edp_dev.bronze.b", "PARQUET"),
        ],
        confirm_destructive=True,
        audit_callback=cb,
    )
    assert captured_fqns == ["edp_dev.bronze.a", "edp_dev.bronze.b"]


# --- D2 per-pair tests -------------------------------------------------
#
# One test per new (source, target) cell unlocked in D2. Each asserts:
#   1. The right strategy label surfaces in the result (so the audit row
#      records which physical path ran).
#   2. The expected SQL fragments hit the warehouse — enough to catch a
#      regression where someone reorders or collapses Plan steps.
#
# `check_pair_compat` is patched to a clean pass on every test so the
# preflight DESCRIBE doesn't dominate the mock setup; a dedicated test
# below exercises the refusal path.


def test_supported_pairs_covers_d1_through_d2_6_cells():
    """D2.6 adds JSON sinks (3 cells) and Delta→Hudi UniForm (1 cell)
    on top of the prior 12. Sixteen total. Fails loudly if a future PR
    forgets to update the registry when adding a new pair (or,
    conversely, removes a cell without updating tests downstream).
    """
    assert SUPPORTED_PAIRS == frozenset(
        {
            # D1
            ("PARQUET", "DELTA"),
            ("ICEBERG", "DELTA"),
            # D2
            ("DELTA", "ICEBERG"),
            ("PARQUET", "ICEBERG"),
            ("DELTA", "PARQUET"),
            ("ICEBERG", "PARQUET"),
            # D2.5 — Avro + ORC sinks
            ("DELTA", "AVRO"),
            ("ICEBERG", "AVRO"),
            ("PARQUET", "AVRO"),
            ("DELTA", "ORC"),
            ("ICEBERG", "ORC"),
            ("PARQUET", "ORC"),
            # D2.6 — JSON sinks + Delta→Hudi UniForm (Beta)
            ("DELTA", "JSON"),
            ("ICEBERG", "JSON"),
            ("PARQUET", "JSON"),
            ("DELTA", "HUDI"),
        }
    )


@patch("src.convert_to_delta.check_pair_compat", return_value=[])
@patch("src.format_strategies.execute_sql")
def test_convert_d2_delta_to_iceberg_uniform_path(mock_sql, _mock_compat):
    """Default Delta→Iceberg picks UniForm (no data movement) — three
    ALTERs in order: disable DV → REORG PURGE → SET props. Strategy
    label is `uniform` so the audit row distinguishes this from the
    physical CTAS sibling."""
    mock_sql.return_value = []
    result = convert_table_format(
        MagicMock(),
        "wh-1",
        "edp_dev.bronze.events",
        "DELTA",
        target_format="ICEBERG",
    )
    assert result.status == "converted"
    assert result.strategy_used == "uniform"
    assert mock_sql.call_count == 3
    sqls = [c[0][2] for c in mock_sql.call_args_list]
    assert "delta.enableDeletionVectors" in sqls[0]
    assert "REORG TABLE" in sqls[1]
    assert "delta.universalFormat.enabledFormats" in sqls[2]


@patch("src.convert_to_delta.check_pair_compat", return_value=[])
@patch("src.format_strategies.execute_sql")
def test_convert_d2_delta_to_iceberg_physical_path(mock_sql, _mock_compat):
    """`iceberg_physical=True` swaps the strategy from UniForm to a
    temp+rename CTAS that produces a real Iceberg table. Different
    physical outcome — the audit row's `strategy_used` lets operators
    tell post-hoc which path ran."""
    mock_sql.return_value = []
    result = convert_table_format(
        MagicMock(),
        "wh-1",
        "edp_dev.bronze.events",
        "DELTA",
        target_format="ICEBERG",
        iceberg_physical=True,
    )
    assert result.status == "converted"
    assert result.strategy_used == "ctas_iceberg"
    sqls = [c[0][2] for c in mock_sql.call_args_list]
    assert any("USING iceberg" in s for s in sqls)
    assert any("RENAME TO" in s for s in sqls)


@patch("src.convert_to_delta.check_pair_compat", return_value=[])
@patch("src.format_strategies.execute_sql")
def test_convert_d2_parquet_to_iceberg_uses_ctas(mock_sql, _mock_compat):
    """Parquet→Iceberg has no UniForm option (UniForm requires Delta
    base), so this pair always picks the CTAS path."""
    mock_sql.return_value = []
    result = convert_table_format(
        MagicMock(),
        "wh-1",
        "edp_dev.bronze.events",
        "PARQUET",
        target_format="ICEBERG",
    )
    assert result.status == "converted"
    assert result.strategy_used == "ctas_iceberg"
    sqls = [c[0][2] for c in mock_sql.call_args_list]
    assert any("USING iceberg" in s for s in sqls)


@patch("src.convert_to_delta.check_pair_compat", return_value=[])
@patch("src.format_strategies.execute_sql")
def test_convert_d27_delta_to_parquet_exports_to_volume(mock_sql, _mock_compat):
    """Delta→Parquet writes raw files to a Volume — UC managed tables
    must be Delta, so the previous CTAS-into-the-same-FQN approach is
    rejected by Databricks. The orchestrator now routes export-shaped
    targets (PARQUET / AVRO / ORC / JSON) through ``INSERT OVERWRITE
    DIRECTORY``. Source table at the FQN is preserved.
    """
    mock_sql.return_value = []
    volume = "/Volumes/edp_dev/bronze/clone_xs_exports/events_parquet/"
    result = convert_table_format(
        MagicMock(),
        "wh-1",
        "edp_dev.bronze.events",
        "DELTA",
        target_format="PARQUET",
        destination_path=volume,
    )
    assert result.status == "converted"
    assert result.strategy_used == "export_parquet"
    sqls = [c[0][2] for c in mock_sql.call_args_list]
    assert any("INSERT OVERWRITE DIRECTORY" in s for s in sqls)
    assert any(volume in s for s in sqls)
    assert any("USING parquet" in s for s in sqls)


@patch("src.convert_to_delta.check_pair_compat", return_value=[])
@patch("src.format_strategies.execute_sql")
def test_convert_d27_iceberg_to_parquet_exports_to_volume(mock_sql, _mock_compat):
    """Iceberg→Parquet shares the same export-to-Volume shape as the
    Delta source — only the source the SELECT reads from differs.
    """
    mock_sql.return_value = []
    volume = "/Volumes/edp_dev/bronze/clone_xs_exports/events_parquet/"
    result = convert_table_format(
        MagicMock(),
        "wh-1",
        "edp_dev.bronze.events",
        "ICEBERG",
        target_format="PARQUET",
        destination_path=volume,
    )
    assert result.status == "converted"
    assert result.strategy_used == "export_parquet"
    sqls = [c[0][2] for c in mock_sql.call_args_list]
    assert any("INSERT OVERWRITE DIRECTORY" in s for s in sqls)


@patch("src.convert_to_delta.check_pair_compat", return_value=[])
@patch("src.format_strategies.execute_sql")
def test_convert_d27_delta_to_avro_exports_to_volume(mock_sql, _mock_compat):
    """Delta→Avro — strategy label `export_avro`, single-step
    INSERT OVERWRITE DIRECTORY plan (one statement per target). The
    distinct strategy label keeps the audit row honest about which
    physical sink the orchestrator picked."""
    mock_sql.return_value = []
    volume = "/Volumes/edp_dev/bronze/clone_xs_exports/events_avro/"
    result = convert_table_format(
        MagicMock(),
        "wh-1",
        "edp_dev.bronze.events",
        "DELTA",
        target_format="AVRO",
        destination_path=volume,
    )
    assert result.status == "converted"
    assert result.strategy_used == "export_avro"
    sqls = [c[0][2] for c in mock_sql.call_args_list]
    assert any("USING avro" in s for s in sqls)
    assert any(volume in s for s in sqls)


@patch("src.convert_to_delta.check_pair_compat", return_value=[])
@patch("src.format_strategies.execute_sql")
def test_convert_d27_iceberg_to_orc_exports_to_volume(mock_sql, _mock_compat):
    """Iceberg→ORC. Strategy label `export_orc`. Validates the export
    matrix covers every (DELTA / ICEBERG / PARQUET → AVRO / ORC / JSON
    / PARQUET) cell."""
    mock_sql.return_value = []
    volume = "/Volumes/edp_dev/bronze/clone_xs_exports/events_orc/"
    result = convert_table_format(
        MagicMock(),
        "wh-1",
        "edp_dev.bronze.events",
        "ICEBERG",
        target_format="ORC",
        destination_path=volume,
    )
    assert result.status == "converted"
    assert result.strategy_used == "export_orc"
    sqls = [c[0][2] for c in mock_sql.call_args_list]
    assert any("USING orc" in s for s in sqls)


@patch("src.convert_to_delta.check_pair_compat", return_value=[])
@patch("src.format_strategies.execute_sql")
def test_convert_d27_delta_to_json_exports_to_volume(mock_sql, _mock_compat):
    """Delta→JSON — export-shaped sink for HTTP webhooks / NoSQL
    pipelines. Same single-step INSERT OVERWRITE DIRECTORY plan as
    AVRO / ORC / PARQUET; strategy label `export_json`."""
    mock_sql.return_value = []
    volume = "/Volumes/edp_dev/bronze/clone_xs_exports/events_json/"
    result = convert_table_format(
        MagicMock(),
        "wh-1",
        "edp_dev.bronze.events",
        "DELTA",
        target_format="JSON",
        destination_path=volume,
    )
    assert result.status == "converted"
    assert result.strategy_used == "export_json"
    sqls = [c[0][2] for c in mock_sql.call_args_list]
    assert any("USING json" in s for s in sqls)


@patch("src.convert_to_delta.check_pair_compat", return_value=[])
@patch("src.format_strategies.execute_sql")
@patch("src.convert_to_delta.execute_sql")
def test_convert_d27_export_target_auto_creates_volume(
    mock_convert_sql, _mock_strategies_sql, _mock_compat
):
    """Real bug from prod: the convert path used to assume the
    Volume in `destination_path` already existed. If it didn't, the
    `INSERT OVERWRITE DIRECTORY` failed with `UC_VOLUME_NOT_FOUND`
    after the operator had already typed the path into the cart row.
    The orchestrator now auto-CREATEs the Volume before the dispatch
    so the convert "just works" — same posture as the smoke
    endpoint's up-front auto-create."""
    mock_convert_sql.return_value = []
    result = convert_table_format(
        MagicMock(),
        "wh-1",
        "edp_dev.bronze.events",
        "DELTA",
        target_format="JSON",
        destination_path="/Volumes/edp_dev/bronze/clone_xs_exports/events_json/",
    )
    assert result.status == "converted"
    sqls = [c[0][2] for c in mock_convert_sql.call_args_list]
    assert sqls[0] == "CREATE VOLUME IF NOT EXISTS edp_dev.bronze.clone_xs_exports"


@patch("src.convert_to_delta.check_pair_compat", return_value=[])
@patch("src.format_strategies.execute_sql")
@patch("src.convert_to_delta.execute_sql")
def test_convert_d27_export_target_volume_create_failure_returns_friendly_error(
    mock_convert_sql, _mock_strategies_sql, _mock_compat
):
    """When CREATE VOLUME fails (most commonly: schema has no managed
    location), the orchestrator must surface a clean "failed" result
    with the actionable next step (run `ALTER SCHEMA ... SET MANAGED
    LOCATION`) — not propagate the raw Databricks error and not
    crash."""
    mock_convert_sql.side_effect = RuntimeError(
        "REQUIRES_MANAGED_STORAGE schema has no managed location"
    )
    result = convert_table_format(
        MagicMock(),
        "wh-1",
        "edp_dev.bronze.events",
        "DELTA",
        target_format="JSON",
        destination_path="/Volumes/edp_dev/bronze/clone_xs_exports/events_json/",
    )
    assert result.status == "failed"
    err = result.error or ""
    assert "Could not auto-create Volume edp_dev.bronze.clone_xs_exports" in err
    assert "ALTER SCHEMA edp_dev.bronze SET MANAGED LOCATION" in err
    assert "REQUIRES_MANAGED_STORAGE" in err


@patch("src.convert_to_delta.check_pair_compat", return_value=[])
def test_convert_d27_export_target_without_destination_path_skips(_mock_compat):
    """Defence-in-depth: if a CLI caller bypasses the API validator
    and passes an export-shaped target with no ``destination_path``,
    the orchestrator returns a clean "skipped" result instead of
    crashing. The API surface catches this earlier with a 422; this
    test pins the fallback for direct callers."""
    result = convert_table_format(
        MagicMock(),
        "wh-1",
        "edp_dev.bronze.events",
        "DELTA",
        target_format="JSON",
        # no destination_path — orchestrator must skip, not crash
    )
    assert result.status == "skipped"
    assert "not yet supported" in (result.error or "")


@patch("src.convert_to_delta.check_pair_compat", return_value=[])
@patch("src.format_strategies.execute_sql")
def test_convert_d26_hudi_runtime_not_supported_translated(mock_sql, _mock_compat):
    """Real failure mode from prod: older DBR runtimes don't recognise
    `delta.enableHudiCompatV1` and Databricks surfaces it as a generic
    ``DELTA_UNKNOWN_CONFIGURATION`` — opaque unless you know Hudi
    UniForm needs a recent DBR. The orchestrator must catch this
    specific error on the `uniform_hudi` strategy path and rewrite
    the message to name the actual root cause + recovery step.
    """
    mock_sql.side_effect = RuntimeError(
        "[DELTA_UNKNOWN_CONFIGURATION] Unknown configuration was specified: "
        "delta.enableHudiCompatV1. To disable this check, set "
        "spark.databricks.delta.allowArbitraryProperties.enabled=true"
    )
    result = convert_table_format(
        MagicMock(),
        "wh-1",
        "edp_dev.bronze.events",
        "DELTA",
        target_format="HUDI",
    )
    assert result.status == "failed"
    assert result.strategy_used == "uniform_hudi"
    assert "Hudi UniForm is not supported on this SQL warehouse runtime" in (result.error or "")
    assert "upgrade the warehouse" in (result.error or "")
    # The original Databricks error must still be in the message so
    # operators can search docs / raise tickets with the exact code.
    assert "DELTA_UNKNOWN_CONFIGURATION" in (result.error or "")


@patch("src.convert_to_delta.check_pair_compat", return_value=[])
@patch("src.format_strategies.execute_sql")
def test_convert_d26_delta_to_hudi_uniform(mock_sql, _mock_compat):
    """D2.6 — Delta→Hudi UniForm (Beta). Sidecar metadata only — no
    data movement. Three-step ALTER chain (disable DV → REORG PURGE
    → SET props with `delta.enableHudiCompatV1` + `enabledFormats =
    'hudi'`). Strategy label `uniform_hudi` so the audit row
    distinguishes this from the Iceberg UniForm sibling."""
    mock_sql.return_value = []
    result = convert_table_format(
        MagicMock(),
        "wh-1",
        "edp_dev.bronze.events",
        "DELTA",
        target_format="HUDI",
    )
    assert result.status == "converted"
    assert result.strategy_used == "uniform_hudi"
    sqls = [c[0][2] for c in mock_sql.call_args_list]
    # Assert each of the three required UniForm steps fired.
    assert any("delta.enableDeletionVectors" in s for s in sqls)
    assert any("REORG TABLE" in s for s in sqls)
    assert any("delta.enableHudiCompatV1" in s for s in sqls)
    assert any("'hudi'" in s for s in sqls)


@patch("src.convert_to_delta.check_pair_compat", return_value=[])
@patch("src.format_strategies.execute_sql")
def test_convert_d2_keep_backup_off_drops_source(mock_sql, _mock_compat):
    """`keep_backup=False` swaps the rename-to-backup step for a DROP
    TABLE. Non-recoverable — surfaced in the UI behind a confirmation
    so the operator picks it knowingly. Only meaningful for the
    physical-Iceberg CTAS path now (Parquet/Avro/ORC/JSON went to the
    export-to-Volume path which preserves the source unconditionally).
    """
    mock_sql.return_value = []
    result = convert_table_format(
        MagicMock(),
        "wh-1",
        "edp_dev.bronze.events",
        "DELTA",
        target_format="ICEBERG",
        iceberg_physical=True,
        keep_backup=False,
    )
    assert result.status == "converted"
    sqls = [c[0][2] for c in mock_sql.call_args_list]
    assert any(s.startswith("DROP TABLE ") for s in sqls)
    assert not any("_pre_convert_" in s for s in sqls)


@patch("src.convert_to_delta.check_pair_compat", return_value=[])
@patch("src.convert_to_delta.execute_sql")
@patch("src.format_strategies.execute_sql")
def test_convert_d2_ctas_replays_grants_and_owner(
    mock_strategies_sql, mock_convert_sql, _mock_compat
):
    # CTAS strategies replace the table entirely, so the new table at
    # the original FQN starts with no GRANTs and creator-owned. The
    # orchestrator captures GRANTs + owner before the plan and replays
    # them after — pin the round-trip so a future refactor that drops
    # the capture/replay step trips this test.
    from unittest.mock import MagicMock

    # Plan execution goes through src.format_strategies.execute_sql,
    # which is mocked separately above.
    mock_strategies_sql.return_value = []
    # Capture phase: SHOW GRANTS returns two rows; the GRANT + ALTER
    # OWNER replays go through src.convert_to_delta.execute_sql.
    mock_convert_sql.return_value = [
        {"Principal": "alice@example.com", "ActionType": "SELECT"},
        {"Principal": "data_eng_group", "ActionType": "MODIFY"},
    ]

    fake_client = MagicMock()
    fake_client.tables.get.return_value = MagicMock(owner="owner@example.com")

    result = convert_table_format(
        fake_client,
        "wh-1",
        "edp_dev.bronze.events",
        "DELTA",
        # CTAS replay is now only relevant for the physical-Iceberg
        # path; the former Parquet CTAS arm is now an export-to-Volume
        # path that preserves the source's permissions automatically.
        target_format="ICEBERG",
        iceberg_physical=True,
    )
    assert result.status == "converted"

    # Replay produced one GRANT per captured row + one ALTER OWNER.
    replay_sqls = [c[0][2] for c in mock_convert_sql.call_args_list]
    assert any("GRANT SELECT ON TABLE" in s and "alice@example.com" in s for s in replay_sqls)
    assert any("GRANT MODIFY ON TABLE" in s and "data_eng_group" in s for s in replay_sqls)
    assert any("OWNER TO `owner@example.com`" in s for s in replay_sqls)


@patch("src.convert_to_delta.check_pair_compat", return_value=[])
@patch("src.convert_to_delta.execute_sql")
@patch("src.format_strategies.execute_sql")
def test_convert_d2_uniform_skips_permission_replay(
    mock_strategies_sql, mock_convert_sql, _mock_compat
):
    # Non-CTAS strategies (uniform, convert_to_delta) keep the same
    # physical table — capturing and replaying GRANTs would be wasted
    # work + spurious SHOW GRANTS log lines. Verify the capture/replay
    # path is bypassed when strategy != ctas_*.
    mock_strategies_sql.return_value = []
    result = convert_table_format(
        MagicMock(),
        "wh-1",
        "edp_dev.bronze.events",
        "DELTA",
        target_format="ICEBERG",
        # iceberg_physical=False → uniform strategy
    )
    assert result.status == "converted"
    assert result.strategy_used == "uniform"
    # The convert-side execute_sql (the SHOW GRANTS / GRANT path) must
    # not have been hit at all.
    assert mock_convert_sql.call_count == 0


@patch("src.convert_to_delta.check_pair_compat", return_value=[])
@patch("src.convert_to_delta.execute_sql")
@patch("src.format_strategies.execute_sql")
def test_convert_d2_ctas_succeeds_when_show_grants_fails(
    mock_strategies_sql, mock_convert_sql, _mock_compat
):
    # If SHOW GRANTS errors out (perms, transient warehouse), the
    # conversion still succeeds — the table mutation is the primary
    # outcome, permission replay is best-effort. Same posture as the
    # clone-path's `_copy_grants_via_sql` fallback.
    mock_strategies_sql.return_value = []
    mock_convert_sql.side_effect = Exception("permission denied on SHOW GRANTS")
    result = convert_table_format(
        MagicMock(),
        "wh-1",
        "edp_dev.bronze.events",
        "DELTA",
        # Use the physical-Iceberg CTAS path now that the former
        # Parquet CTAS arm is gone (export-to-Volume preserves the
        # source's permissions automatically).
        target_format="ICEBERG",
        iceberg_physical=True,
    )
    assert result.status == "converted"


@patch("src.convert_to_delta.check_pair_compat")
@patch("src.format_strategies.execute_sql")
def test_convert_d2_compat_preflight_refuses_generated_column(mock_sql, mock_compat):
    """When the compat preflight returns reasons (e.g. Delta GENERATED
    column targeting Iceberg), the orchestrator skips the table with a
    structured error and never hits the warehouse. Crucial: SQL must
    not run after a refusal — that's the whole point of the preflight."""
    mock_compat.return_value = [
        "column `year` is GENERATED ALWAYS — Iceberg has no equivalent",
    ]
    result = convert_table_format(
        MagicMock(),
        "wh-1",
        "edp_dev.bronze.events",
        "DELTA",
        target_format="ICEBERG",
    )
    assert result.status == "skipped"
    assert "compat preflight refused" in (result.error or "")
    assert "GENERATED" in (result.error or "")
    assert mock_sql.call_count == 0


@patch("src.convert_to_delta.check_pair_compat", return_value=[])
@patch("src.format_strategies.execute_sql")
def test_convert_d2_dry_run_skips_compat_preflight(mock_sql, mock_compat):
    """Dry-run skips the compat preflight so the operator can preview
    the plan even when the source has known incompatibilities. The
    point of dry-run is to see what *would* run; a refusal at preview
    time would defeat the purpose."""
    convert_table_format(
        MagicMock(),
        "wh-1",
        "edp_dev.bronze.events",
        "DELTA",
        target_format="ICEBERG",
        dry_run=True,
    )
    assert mock_compat.call_count == 0
    assert mock_sql.call_count == 0  # dry_run never calls execute_sql
