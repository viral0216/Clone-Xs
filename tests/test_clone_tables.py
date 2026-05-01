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
