"""Tests for src/convert_to_delta.py — backlog item #13.

Coverage of the safety gate (confirm_destructive), the supported-format
filter, the dry-run path, and per-table outcome shape. Endpoint-level
tests live in test_convert_to_delta_api.py.
"""

from unittest.mock import MagicMock, patch

import pytest

from src.convert_to_delta import (
    SUPPORTED_SOURCE_FORMATS,
    ConvertToDeltaError,
    convert_table_to_delta,
    convert_tables_to_delta,
)


@patch("src.convert_to_delta.execute_sql")
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


@patch("src.convert_to_delta.execute_sql")
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
    assert "already Delta" in (result.error or "")
    assert mock_sql.call_count == 0


@patch("src.convert_to_delta.execute_sql")
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
    assert "unsupported source format CSV" in (result.error or "")
    assert mock_sql.call_count == 0


@patch("src.convert_to_delta.execute_sql")
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


@patch("src.convert_to_delta.execute_sql")
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


@patch("src.convert_to_delta.execute_sql")
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


@patch("src.convert_to_delta.execute_sql")
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


@patch("src.convert_to_delta.execute_sql")
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


@patch("src.convert_to_delta.execute_sql")
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


@patch("src.convert_to_delta.execute_sql")
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
    assert "SELECT operation_id, fqn, source_format, status" in sql
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


@patch("src.convert_to_delta.execute_sql")
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
