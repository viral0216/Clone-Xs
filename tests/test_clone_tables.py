from unittest.mock import MagicMock, patch

from src.clone_tables import clone_table, clone_tables_in_schema


# `clone_table()` returns `tuple[bool, dict | None]` since the Tier 1 work
# that captured Databricks per-CLONE metrics. Tests unpack `(success, metrics)`
# — `metrics` is None on dry-run / schema-only / WHERE-filtered (CTAS) paths
# and any case where the response didn't carry the expected counter columns.


@patch("src.clone_tables.execute_sql")
def test_clone_table_deep(mock_sql):
    mock_sql.return_value = []
    success, metrics = clone_table(
        MagicMock(),
        "wh-123",
        "src",
        "dst",
        "schema1",
        "table1",
        "DEEP",
    )
    assert success is True
    assert metrics is None  # empty rows → no metrics
    sql_called = mock_sql.call_args[0][2]
    assert "DEEP CLONE" in sql_called


@patch("src.clone_tables.execute_sql")
def test_clone_table_shallow(mock_sql):
    mock_sql.return_value = []
    success, _ = clone_table(
        MagicMock(),
        "wh-123",
        "src",
        "dst",
        "schema1",
        "table1",
        "SHALLOW",
    )
    assert success is True
    sql_called = mock_sql.call_args[0][2]
    assert "SHALLOW CLONE" in sql_called


@patch("src.clone_tables.execute_sql")
def test_clone_table_failure(mock_sql):
    mock_sql.side_effect = Exception("permission denied")
    success, metrics = clone_table(
        MagicMock(),
        "wh-123",
        "src",
        "dst",
        "schema1",
        "table1",
        "DEEP",
    )
    assert success is False
    assert metrics is None


@patch("src.clone_tables.execute_sql")
def test_clone_table_dry_run(mock_sql):
    mock_sql.return_value = []
    success, metrics = clone_table(
        MagicMock(),
        "wh-123",
        "src",
        "dst",
        "schema1",
        "table1",
        "DEEP",
        dry_run=True,
    )
    assert success is True
    assert metrics is None  # dry-run never returns metrics
    sql_called = mock_sql.call_args[0][2]
    assert "DEEP CLONE" in sql_called


@patch("src.clone_tables.execute_sql")
def test_clone_table_with_timestamp(mock_sql):
    mock_sql.return_value = []
    success, _ = clone_table(
        MagicMock(),
        "wh-123",
        "src",
        "dst",
        "schema1",
        "table1",
        "DEEP",
        as_of_timestamp="2024-01-15T00:00:00",
    )
    assert success is True
    sql_called = mock_sql.call_args[0][2]
    assert "TIMESTAMP AS OF" in sql_called
    assert "2024-01-15" in sql_called


@patch("src.clone_tables.execute_sql")
def test_clone_table_with_version(mock_sql):
    mock_sql.return_value = []
    success, _ = clone_table(
        MagicMock(),
        "wh-123",
        "src",
        "dst",
        "schema1",
        "table1",
        "DEEP",
        as_of_version=5,
    )
    assert success is True
    sql_called = mock_sql.call_args[0][2]
    assert "VERSION AS OF 5" in sql_called


@patch("src.clone_tables.execute_sql")
def test_clone_table_captures_metrics_when_returned(mock_sql):
    """Databricks returns a single-row DataFrame from each CLONE statement
    with file/byte counts. clone_table extracts these into a metrics dict."""
    mock_sql.return_value = [
        {
            "source_table_size": 1024 * 1024 * 500,  # 500 MB
            "source_num_of_files": 42,
            "num_copied_files": 42,
            "copied_files_size": 1024 * 1024 * 500,
            "num_removed_files": 0,
            "removed_files_size": 0,
        }
    ]
    success, metrics = clone_table(
        MagicMock(),
        "wh-123",
        "src",
        "dst",
        "schema1",
        "table1",
        "DEEP",
    )
    assert success is True
    assert metrics is not None
    assert metrics["copied_files_size"] == 1024 * 1024 * 500
    assert metrics["num_copied_files"] == 42


@patch("src.clone_tables.execute_sql")
def test_clone_table_emits_tbl_properties_clause(mock_sql):
    """`tbl_properties` parameter renders as inline TBLPROPERTIES (...) on
    the CLONE statement — primary archival use case (e.g. delta.logRetentionDuration).
    Setting via post-clone ALTER TABLE is too late for retention windows."""
    mock_sql.return_value = []
    success, _ = clone_table(
        MagicMock(),
        "wh-123",
        "src",
        "dst",
        "schema1",
        "table1",
        "DEEP",
        tbl_properties={"delta.logRetentionDuration": "3650 days"},
    )
    assert success is True
    sql_called = mock_sql.call_args[0][2]
    assert "TBLPROPERTIES" in sql_called
    assert "delta.logRetentionDuration" in sql_called
    assert "3650 days" in sql_called


# Mixed-format tests below cover Feature 1 (Parquet / Iceberg source support).
# Same `CREATE TABLE … CLONE source` SQL works for Delta, Parquet, and Iceberg
# sources registered in UC; the orchestrator's job is to track the mix per
# schema so the run summary can show `{DELTA: N, PARQUET: M, ICEBERG: K}`.


@patch("src.clone_tables.list_tables_sdk")
@patch("src.clone_tables.execute_sql")
def test_clone_tables_in_schema_aggregates_per_format_counters(mock_sql, mock_list):
    """Mixed-format catalog (Delta + Parquet + Iceberg) → `formats` rollup
    counts each cloned table under its source format. Defaults missing
    `data_source_format` to DELTA (most common UC table is unannotated Delta)."""
    mock_list.return_value = [
        {"table_name": "events", "table_type": "MANAGED", "data_source_format": "DELTA"},
        {"table_name": "users", "table_type": "MANAGED", "data_source_format": "DELTA"},
        {"table_name": "orders_parquet", "table_type": "EXTERNAL", "data_source_format": "PARQUET"},
        {"table_name": "iceberg_logs", "table_type": "EXTERNAL", "data_source_format": "ICEBERG"},
        {"table_name": "no_fmt", "table_type": "MANAGED", "data_source_format": None},
    ]
    mock_sql.return_value = []  # successful clone, no metrics row

    result = clone_tables_in_schema(
        MagicMock(),
        "wh-123",
        "src_cat",
        "dst_cat",
        "schema1",
        clone_type="DEEP",
        exclude_tables=[],
        load_type="FULL",
    )

    assert result["success"] == 5
    assert result["failed"] == 0
    # DELTA: 2 explicit + 1 None-fallback = 3; PARQUET: 1; ICEBERG: 1
    assert result["formats"] == {"DELTA": 3, "PARQUET": 1, "ICEBERG": 1}


@patch("src.clone_tables.list_tables_sdk")
@patch("src.clone_tables.execute_sql")
def test_clone_tables_in_schema_format_counter_excludes_failures(mock_sql, mock_list):
    """Failed tables don't bump the format counter — `formats` reflects what
    was actually migrated, not what was attempted. UI shows true success mix."""
    mock_list.return_value = [
        {"table_name": "good_delta", "table_type": "MANAGED", "data_source_format": "DELTA"},
        {"table_name": "bad_iceberg", "table_type": "EXTERNAL", "data_source_format": "ICEBERG"},
    ]
    # First call (good_delta) succeeds, second (bad_iceberg) raises.
    mock_sql.side_effect = [[], Exception("partition evolution not supported")]

    result = clone_tables_in_schema(
        MagicMock(),
        "wh-123",
        "src_cat",
        "dst_cat",
        "schema1",
        clone_type="DEEP",
        exclude_tables=[],
        load_type="FULL",
    )

    assert result["success"] == 1
    assert result["failed"] == 1
    # Only the cloned-DELTA shows up; failed Iceberg does not pollute the mix.
    assert result["formats"] == {"DELTA": 1}


@patch("src.clone_tables.list_tables_sdk")
@patch("src.clone_tables.execute_sql")
def test_clone_tables_in_schema_format_counter_uppercases(mock_sql, mock_list):
    """Defensive: `data_source_format` from older REST responses can be lower-
    or mixed-case; format counter normalises so `parquet` / `Parquet` / `PARQUET`
    don't fragment the rollup into three buckets."""
    mock_list.return_value = [
        {"table_name": "t1", "table_type": "EXTERNAL", "data_source_format": "parquet"},
        {"table_name": "t2", "table_type": "EXTERNAL", "data_source_format": "Parquet"},
        {"table_name": "t3", "table_type": "EXTERNAL", "data_source_format": "PARQUET"},
    ]
    mock_sql.return_value = []

    result = clone_tables_in_schema(
        MagicMock(),
        "wh-123",
        "src_cat",
        "dst_cat",
        "schema1",
        clone_type="DEEP",
        exclude_tables=[],
        load_type="FULL",
    )

    assert result["formats"] == {"PARQUET": 3}


# UniForm (Phase A — backlog item #9): when target_format=ICEBERG, the clone
# stays Delta but post-clone ALTER TABLE enables Iceberg-readable metadata
# (delta.universalFormat.enabledFormats=iceberg + columnMapping=name +
# IcebergCompatV2). Only effective on Delta sources; non-Delta sources are
# skipped with a warning. Dry-run never executes the ALTER.


@patch("src.clone_tables.execute_sql")
def test_clone_table_uniform_emitted_for_delta_source(mock_sql):
    """target_format=ICEBERG + Delta source → post-clone ALTER TABLE enables
    UniForm so external Iceberg engines can read the Delta target."""
    mock_sql.return_value = []
    success, _ = clone_table(
        MagicMock(),
        "wh-123",
        "src",
        "dst",
        "schema1",
        "table1",
        "DEEP",
        target_format="ICEBERG",
        source_format="DELTA",
    )
    assert success is True
    # Two SQL calls: the CLONE itself, then the UniForm ALTER.
    assert mock_sql.call_count == 2
    alter_sql = mock_sql.call_args_list[1][0][2]
    assert "ALTER TABLE" in alter_sql
    assert "delta.universalFormat.enabledFormats" in alter_sql
    assert "iceberg" in alter_sql
    assert "delta.columnMapping.mode" in alter_sql
    assert "delta.enableIcebergCompatV2" in alter_sql


@patch("src.clone_tables.execute_sql")
def test_clone_table_uniform_skipped_for_non_delta_source(mock_sql):
    """target_format=ICEBERG with a non-Delta source falls back to a plain
    Delta clone — UniForm requires Delta as the underlying format. Caller
    sees a warning in the log; only the CLONE statement is executed."""
    mock_sql.return_value = []
    success, _ = clone_table(
        MagicMock(),
        "wh-123",
        "src",
        "dst",
        "schema1",
        "table1",
        "DEEP",
        target_format="ICEBERG",
        source_format="PARQUET",
    )
    assert success is True
    # Only the CLONE itself runs — no ALTER TABLE for UniForm.
    assert mock_sql.call_count == 1
    assert "ALTER TABLE" not in mock_sql.call_args[0][2]


@patch("src.clone_tables.execute_sql")
def test_clone_table_uniform_skipped_in_dry_run(mock_sql):
    """Dry-run never executes the post-clone ALTER — same dry-run discipline
    the rest of the clone path follows. UI labels the run [DRY RUN] without
    the user worrying that some side effects landed."""
    mock_sql.return_value = []
    success, _ = clone_table(
        MagicMock(),
        "wh-123",
        "src",
        "dst",
        "schema1",
        "table1",
        "DEEP",
        dry_run=True,
        target_format="ICEBERG",
        source_format="DELTA",
    )
    assert success is True
    # Only the CLONE call (which itself is dry-run); no ALTER attempted.
    assert mock_sql.call_count == 1
    assert "ALTER TABLE" not in mock_sql.call_args[0][2]


# Phase B (#9) — automatic CTAS fallback for known Iceberg CLONE failures.
# When the CLONE statement raises a "partition evolution" or "truncated
# decimal" error (the documented Iceberg limitations), clone_table() retries
# as CTAS so the table still lands. CTAS targets always lose Delta history.


@patch("src.clone_iceberg.execute_sql")
@patch("src.clone_tables.execute_sql")
def test_clone_table_falls_back_to_ctas_on_partition_evolution(mock_sql, mock_iceberg_sql):
    """Iceberg CLONE that fails with `partition evolution` is retried as
    CTAS. Both the CLONE and the CTAS should appear in the SQL trace, with
    CTAS landing as the recovery path."""
    # Preflight DESCRIBE returns no hidden partitioning (so preflight passes
    # through to the CLONE attempt).
    mock_iceberg_sql.return_value = []
    mock_sql.side_effect = [
        Exception("partition evolution not supported for source"),
        [],  # CTAS succeeds
    ]
    success, metrics = clone_table(
        MagicMock(),
        "wh-123",
        "src",
        "dst",
        "schema1",
        "iceberg_evolved",
        "DEEP",
        source_format="ICEBERG",
    )
    assert success is True
    assert metrics is None  # CTAS path doesn't return Databricks CLONE metrics
    # Two SQL calls in clone_tables: the failed CLONE, then the CTAS retry.
    assert mock_sql.call_count == 2
    ctas_sql = mock_sql.call_args_list[1][0][2]
    assert ctas_sql.startswith("CREATE TABLE IF NOT EXISTS")
    assert "AS SELECT * FROM" in ctas_sql


@patch("src.clone_iceberg.execute_sql")
@patch("src.clone_tables.execute_sql")
def test_clone_table_no_ctas_fallback_for_unrecoverable_error(mock_sql, mock_iceberg_sql):
    """Permission denied (or any non-Iceberg-specific error) fails outright
    rather than silently retrying as CTAS — CTAS would hit the same error
    and auto-retry would mask the real cause."""
    mock_iceberg_sql.return_value = []
    mock_sql.side_effect = Exception("Permission denied: USE CATALOG required")
    success, _ = clone_table(
        MagicMock(),
        "wh-123",
        "src",
        "dst",
        "schema1",
        "iceberg_locked",
        "DEEP",
        source_format="ICEBERG",
    )
    assert success is False
    # Only the original CLONE attempt — no CTAS retry.
    assert mock_sql.call_count == 1


@patch("src.clone_iceberg.execute_sql")
@patch("src.clone_tables.execute_sql")
def test_clone_table_no_ctas_fallback_for_non_iceberg_source(mock_sql, mock_iceberg_sql):
    """Even a `partition evolution` error on a Parquet source doesn't trigger
    CTAS — the feature is scoped to Iceberg sources where the failure mode
    is documented and CTAS is known to recover. Other formats fail loud."""
    mock_iceberg_sql.return_value = []
    mock_sql.side_effect = Exception("partition evolution not supported")
    success, _ = clone_table(
        MagicMock(),
        "wh-123",
        "src",
        "dst",
        "schema1",
        "parquet_tbl",
        "DEEP",
        source_format="PARQUET",
    )
    assert success is False
    assert mock_sql.call_count == 1


@patch("src.clone_tables.preflight_iceberg_source")
@patch("src.clone_tables.execute_sql")
def test_clone_table_refuses_iceberg_with_hidden_partitioning(mock_sql, mock_preflight):
    """Preflight raising IcebergPreflightError aborts the clone before any
    DDL runs. Asserts that mock_sql was never called — no CLONE attempt.
    Patches `src.clone_tables.preflight_iceberg_source` (the import site, not
    the definition site) — clone_tables imports the name at module load,
    so that's the binding the call resolves through."""
    from src.clone_iceberg import IcebergPreflightError

    mock_preflight.side_effect = IcebergPreflightError("Source uses bucket(16, user_id) — refused")
    success, _ = clone_table(
        MagicMock(),
        "wh-123",
        "src",
        "dst",
        "schema1",
        "iceberg_bucketed",
        "DEEP",
        source_format="ICEBERG",
    )
    assert success is False
    # CLONE never ran — preflight short-circuited.
    assert mock_sql.call_count == 0


@patch("src.clone_tables.list_tables_sdk")
@patch("src.clone_tables.execute_sql")
def test_clone_tables_in_schema_propagates_target_format(mock_sql, mock_list):
    """target_format=ICEBERG fans out per-table: Delta sources get UniForm
    enabled, non-Delta sources are cloned plainly. Asserts the ALTER TABLE
    call appears for the Delta row only."""
    mock_list.return_value = [
        {"table_name": "delta_tbl", "table_type": "MANAGED", "data_source_format": "DELTA"},
        {"table_name": "parquet_tbl", "table_type": "EXTERNAL", "data_source_format": "PARQUET"},
    ]
    mock_sql.return_value = []

    result = clone_tables_in_schema(
        MagicMock(),
        "wh-123",
        "src_cat",
        "dst_cat",
        "schema1",
        clone_type="DEEP",
        exclude_tables=[],
        load_type="FULL",
        target_format="ICEBERG",
    )

    assert result["success"] == 2
    # Walk all SQL emitted across both tables. We expect:
    #   delta_tbl   → CREATE TABLE … DEEP CLONE  +  ALTER TABLE … UniForm
    #   parquet_tbl → CREATE TABLE … DEEP CLONE  (no ALTER, fallback)
    sqls = [c[0][2] for c in mock_sql.call_args_list]
    uniform_alters = [s for s in sqls if "ALTER TABLE" in s and "delta.universalFormat" in s]
    assert len(uniform_alters) == 1
    assert "`dst_cat`.`schema1`.`delta_tbl`" in uniform_alters[0]
