"""Tests for selective re-clone — only re-clones tables that have drifted
between source and target, leaving in-sync tables untouched.

Drift detection compares source vs target Delta versions directly via
DESCRIBE HISTORY. Tables present on source but not target count as drifted
(`never_cloned`); tables on both with source.version > target.version count
as `version_drift`; non-Delta sources count as `unable_to_compare` and are
treated as drifted (conservative — cheaper than missing real drift).
"""

from unittest.mock import MagicMock, patch

from src.incremental_sync import find_drifted_tables, get_table_current_version
from src.selective_reclone import _drift_breakdown, selective_reclone_catalog


# ---------------------------------------------------------------------------
# Drift detection — find_drifted_tables
# ---------------------------------------------------------------------------


@patch("src.incremental_sync.list_tables_sdk")
@patch("src.incremental_sync.get_table_history")
def test_find_drifted_tables_marks_missing_target_tables_never_cloned(mock_history, mock_list):
    """Tables on source but not on target → reason=never_cloned. Selective
    re-clone is additive, so these get cloned in on the next run."""
    # First call lists source, second lists target.
    mock_list.side_effect = [
        [{"table_name": "events"}, {"table_name": "users"}],  # source
        [{"table_name": "events"}],  # target — missing 'users'
    ]
    # No history reads needed for never-cloned tables. The events table is on
    # both → caller will compare versions; mock both as version 0 (in sync).
    mock_history.return_value = [{"version": 0}]

    drifted = find_drifted_tables(MagicMock(), "wh", "src", "dst", "schema1")

    assert len(drifted) == 1
    assert drifted[0]["table_name"] == "users"
    assert drifted[0]["reason"] == "never_cloned"


@patch("src.incremental_sync.list_tables_sdk")
@patch("src.incremental_sync.get_table_history")
def test_find_drifted_tables_detects_version_drift(mock_history, mock_list):
    """Tables on both sides where source.version > target.version → reason=version_drift."""
    mock_list.side_effect = [
        [{"table_name": "facts"}],  # source
        [{"table_name": "facts"}],  # target
    ]
    # First history call (source.facts) → version 5; second (target.facts) → version 3.
    mock_history.side_effect = [
        [{"version": 5}],
        [{"version": 3}],
    ]

    drifted = find_drifted_tables(MagicMock(), "wh", "src", "dst", "schema1")

    assert len(drifted) == 1
    assert drifted[0]["reason"] == "version_drift"
    assert drifted[0]["source_version"] == 5
    assert drifted[0]["target_version"] == 3


@patch("src.incremental_sync.list_tables_sdk")
@patch("src.incremental_sync.get_table_history")
def test_find_drifted_tables_skips_in_sync_tables(mock_history, mock_list):
    """Source.version == target.version → not drifted. The whole point of
    selective re-clone is to skip these and avoid re-transferring static data."""
    mock_list.side_effect = [
        [{"table_name": "events"}, {"table_name": "users"}],
        [{"table_name": "events"}, {"table_name": "users"}],
    ]
    # 4 history reads: src.events, src.users, dst.events, dst.users — all v7
    mock_history.return_value = [{"version": 7}]

    drifted = find_drifted_tables(MagicMock(), "wh", "src", "dst", "schema1")

    assert drifted == []


@patch("src.incremental_sync.list_tables_sdk")
@patch("src.incremental_sync.get_table_history")
def test_find_drifted_tables_marks_unreadable_version_as_drifted(mock_history, mock_list):
    """When DESCRIBE HISTORY returns nothing (Parquet/Iceberg source, transient
    failure), we can't compare versions — mark as drifted to be safe. Cheaper
    to re-clone than to silently miss real drift."""
    mock_list.side_effect = [
        [{"table_name": "iceberg_logs"}],
        [{"table_name": "iceberg_logs"}],
    ]
    # Source DESCRIBE HISTORY returns empty (e.g. non-Delta source).
    mock_history.return_value = []

    drifted = find_drifted_tables(MagicMock(), "wh", "src", "dst", "schema1")

    assert len(drifted) == 1
    assert drifted[0]["reason"] == "unable_to_compare"


@patch("src.incremental_sync.list_tables_sdk")
@patch("src.incremental_sync.get_table_history")
def test_find_drifted_tables_ignores_orphans_on_target(mock_history, mock_list):
    """Tables on target but not on source are NOT included — selective is
    additive, never destructive. (Use a separate compare/cleanup if needed.)"""
    mock_list.side_effect = [
        [{"table_name": "events"}],  # source — only events
        [{"table_name": "events"}, {"table_name": "stale_table"}],  # target has extra
    ]
    mock_history.return_value = [{"version": 1}]

    drifted = find_drifted_tables(MagicMock(), "wh", "src", "dst", "schema1")

    # Only events is on both at the same version → no drift; stale_table on
    # target is ignored.
    assert drifted == []


def test_get_table_current_version_handles_empty_history():
    """No DESCRIBE HISTORY rows (table missing, non-Delta) → return None.
    Caller treats None as "can't compare → drifted"."""
    with patch("src.incremental_sync.get_table_history") as mock_h:
        mock_h.return_value = []
        assert get_table_current_version(MagicMock(), "wh", "c", "s", "t") is None


def test_get_table_current_version_handles_garbage_version():
    """DESCRIBE HISTORY returned a non-int version (shouldn't happen, but
    defensive). Return None rather than raising."""
    with patch("src.incremental_sync.get_table_history") as mock_h:
        mock_h.return_value = [{"version": "not-a-number"}]
        assert get_table_current_version(MagicMock(), "wh", "c", "s", "t") is None


# ---------------------------------------------------------------------------
# Drift breakdown helper
# ---------------------------------------------------------------------------


def test_drift_breakdown_groups_by_reason():
    """Helper renders `(2 never_cloned, 3 version_drift)` for the schema log
    line — counts by reason, alpha-sorted for stable test output."""
    drifted = [
        {"reason": "version_drift"},
        {"reason": "never_cloned"},
        {"reason": "version_drift"},
        {"reason": "never_cloned"},
        {"reason": "version_drift"},
    ]
    assert _drift_breakdown(drifted) == "(2 never_cloned, 3 version_drift)"


# ---------------------------------------------------------------------------
# Orchestrator — selective_reclone_catalog
# ---------------------------------------------------------------------------


def _config():
    """Minimal config for orchestrator tests."""
    return {
        "source_catalog": "src_cat",
        "destination_catalog": "dst_cat",
        "sql_warehouse_id": "wh-123",
        "exclude_schemas": ["information_schema", "default"],
        "include_schemas": ["bronze"],
        "clone_type": "DEEP",
        "dry_run": False,
        "max_workers": 1,
        "parallel_tables": 1,
    }


@patch("src.selective_reclone._clone_single_table")
@patch("src.selective_reclone.list_tables_sdk")
@patch("src.selective_reclone.find_drifted_tables")
@patch("src.clone_catalog.get_schemas")
@patch("src.clone_catalog.create_schema_if_not_exists")
@patch("src.clone_catalog.create_catalog_if_not_exists")
def test_selective_reclone_only_clones_drifted_tables(
    _mk_cat,
    _mk_schema,
    mk_get_schemas,
    mk_drift,
    mk_list_tables,
    mk_clone,
):
    """Five tables in schema; two are drifted. Only those two should be
    cloned. Runtime is proportional to drift count, not schema size — that's
    the whole point of selective."""
    mk_get_schemas.return_value = ["bronze"]
    mk_drift.return_value = [
        {
            "table_name": "facts",
            "reason": "version_drift",
            "source_version": 5,
            "target_version": 3,
        },
        {
            "table_name": "users",
            "reason": "never_cloned",
            "source_version": None,
            "target_version": None,
        },
    ]
    # list_tables_sdk inside _reclone_drifted_in_schema queries source for format mapping
    mk_list_tables.return_value = [
        {"table_name": "facts", "data_source_format": "DELTA"},
        {"table_name": "users", "data_source_format": "DELTA"},
        {"table_name": "in_sync_a", "data_source_format": "DELTA"},
        {"table_name": "in_sync_b", "data_source_format": "DELTA"},
        {"table_name": "in_sync_c", "data_source_format": "DELTA"},
    ]
    # _clone_single_table returns (table_name, success, metrics)
    mk_clone.side_effect = lambda *args, **_kw: (args[5], True, None)

    summary = selective_reclone_catalog(MagicMock(), _config())

    # Exactly the 2 drifted tables were cloned — not the 5 in the schema.
    assert mk_clone.call_count == 2
    cloned_names = {call[0][5] for call in mk_clone.call_args_list}
    assert cloned_names == {"facts", "users"}
    # Both succeeded; force_reclone was passed True (positional arg index 19
    # in the _clone_single_table arg tuple — see args_for in selective_reclone.py).
    for call in mk_clone.call_args_list:
        assert call[0][19] is True  # force_reclone
    assert summary["mode"] == "selective"
    assert summary["total_drifted_tables"] == 2
    assert summary["tables"]["success"] == 2
    assert summary["tables"]["failed"] == 0


@patch("src.selective_reclone._clone_single_table")
@patch("src.selective_reclone.list_tables_sdk")
@patch("src.selective_reclone.find_drifted_tables")
@patch("src.clone_catalog.get_schemas")
@patch("src.clone_catalog.create_schema_if_not_exists")
@patch("src.clone_catalog.create_catalog_if_not_exists")
def test_selective_reclone_no_drift_clones_nothing(
    _mk_cat,
    _mk_schema,
    mk_get_schemas,
    mk_drift,
    mk_list,
    mk_clone,
):
    """Edge case from the roadmap: source unchanged since last clone → 0
    tables cloned, no error. Summary shows 0 drifted; orchestrator must not
    invoke `_clone_single_table` even once."""
    mk_get_schemas.return_value = ["bronze"]
    mk_drift.return_value = []
    mk_list.return_value = []

    summary = selective_reclone_catalog(MagicMock(), _config())

    assert mk_clone.call_count == 0
    assert summary["total_drifted_tables"] == 0
    assert summary["tables"]["success"] == 0
    assert summary["tables"]["failed"] == 0


@patch("src.selective_reclone._clone_single_table")
@patch("src.selective_reclone.list_tables_sdk")
@patch("src.selective_reclone.find_drifted_tables")
@patch("src.clone_catalog.get_schemas")
@patch("src.clone_catalog.create_schema_if_not_exists")
@patch("src.clone_catalog.create_catalog_if_not_exists")
def test_selective_reclone_aggregates_metrics_and_format_counter(
    _mk_cat,
    _mk_schema,
    mk_get_schemas,
    mk_drift,
    mk_list,
    mk_clone,
):
    """Selective re-clone benefits from the same Tier 1/2 fixes that the
    full clone does — verify metrics + format counters propagate. A drifted
    Parquet table should bump `formats[PARQUET]`, not `formats[DELTA]`."""
    mk_get_schemas.return_value = ["bronze"]
    mk_drift.return_value = [
        {
            "table_name": "delta_t",
            "reason": "version_drift",
            "source_version": 5,
            "target_version": 3,
        },
        {
            "table_name": "parquet_t",
            "reason": "never_cloned",
            "source_version": None,
            "target_version": None,
        },
    ]
    mk_list.return_value = [
        {"table_name": "delta_t", "data_source_format": "DELTA"},
        {"table_name": "parquet_t", "data_source_format": "PARQUET"},
    ]
    # Distinct metrics dicts so we can verify the sum.
    mk_clone.side_effect = [
        (
            "delta_t",
            True,
            {
                "copied_files_size": 1000,
                "num_copied_files": 5,
                "source_table_size": 1200,
                "source_num_of_files": 6,
            },
        ),
        (
            "parquet_t",
            True,
            {
                "copied_files_size": 500,
                "num_copied_files": 2,
                "source_table_size": 500,
                "source_num_of_files": 2,
            },
        ),
    ]

    summary = selective_reclone_catalog(MagicMock(), _config())

    # `_build_summary` rolls metric totals up to the top of the summary dict
    # (not nested under "tables"). Format counter is also top-level.
    assert summary["bytes_copied"] == 1500
    assert summary["files_copied"] == 7
    assert summary["formats"] == {"DELTA": 1, "PARQUET": 1}
