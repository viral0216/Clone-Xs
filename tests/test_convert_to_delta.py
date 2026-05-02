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
