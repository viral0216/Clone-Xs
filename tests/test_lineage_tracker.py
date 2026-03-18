from unittest.mock import MagicMock, patch

from src.lineage_tracker import (
    ensure_lineage_table,
    record_lineage,
    record_batch_lineage,
    query_lineage,
)


# ---------- ensure_lineage_table ----------

@patch("src.lineage_tracker.execute_sql")
def test_ensure_lineage_table_happy(mock_sql):
    mock_sql.return_value = []
    fqn = ensure_lineage_table(MagicMock(), "wh-1", lineage_catalog="my_cat")
    assert fqn == "my_cat.lineage.clone_lineage"
    # CREATE CATALOG, CREATE SCHEMA, CREATE TABLE
    assert mock_sql.call_count == 3


@patch("src.lineage_tracker.execute_sql")
def test_ensure_lineage_table_default_catalog(mock_sql):
    mock_sql.return_value = []
    fqn = ensure_lineage_table(MagicMock(), "wh-1")
    assert fqn == "clone_audit.lineage.clone_lineage"


@patch("src.lineage_tracker.execute_sql")
def test_ensure_lineage_table_sql_error(mock_sql):
    mock_sql.side_effect = Exception("permission denied")
    try:
        ensure_lineage_table(MagicMock(), "wh-1")
        assert False, "Should have raised"
    except Exception as e:
        assert "permission denied" in str(e)


# ---------- record_lineage ----------

@patch("src.lineage_tracker.execute_sql")
def test_record_lineage_happy(mock_sql):
    mock_sql.return_value = []
    record_lineage(
        MagicMock(), "wh-1", "op-1",
        "src_cat", "schema1", "table1",
        "dst_cat", "schema1", "table1",
        row_count=100, size_bytes=5000,
    )
    mock_sql.assert_called_once()
    sql_arg = mock_sql.call_args[0][2]
    assert "INSERT INTO" in sql_arg
    assert "src_cat.schema1.table1" in sql_arg
    assert "dst_cat.schema1.table1" in sql_arg
    assert "100" in sql_arg
    assert "5000" in sql_arg


@patch("src.lineage_tracker.execute_sql")
def test_record_lineage_null_counts(mock_sql):
    mock_sql.return_value = []
    record_lineage(
        MagicMock(), "wh-1", "op-1",
        "src", "s", "t", "dst", "s", "t",
    )
    sql_arg = mock_sql.call_args[0][2]
    assert "NULL" in sql_arg


@patch("src.lineage_tracker.execute_sql")
def test_record_lineage_sql_failure_no_raise(mock_sql):
    """SQL failure should be logged, not raised."""
    mock_sql.side_effect = Exception("insert failed")
    # Should not raise
    record_lineage(
        MagicMock(), "wh-1", "op-1",
        "src", "s", "t", "dst", "s", "t",
    )


# ---------- record_batch_lineage ----------

@patch("src.lineage_tracker.record_lineage")
def test_record_batch_lineage_happy(mock_record):
    objects = [
        {"schema": "s1", "table": "t1", "object_type": "TABLE", "status": "success"},
        {"schema": "s1", "table": "t2", "status": "success"},
    ]
    count = record_batch_lineage(
        MagicMock(), "wh-1", "op-1", "src", "dst", "DEEP", objects,
    )
    assert count == 2
    assert mock_record.call_count == 2


# ---------- query_lineage ----------

@patch("src.lineage_tracker.execute_sql")
def test_query_lineage_happy(mock_sql):
    mock_sql.return_value = [
        {
            "lineage_id": "l-1",
            "operation_id": "op-1",
            "source_fqn": "src.s.t",
            "dest_fqn": "dst.s.t",
            "clone_type": "DEEP",
            "clone_status": "success",
            "cloned_at": "2025-01-01",
            "object_type": "TABLE",
            "cloned_by": "user1",
            "row_count": 100,
            "size_bytes": 5000,
        }
    ]
    rows = query_lineage(MagicMock(), "wh-1", table_fqn="src.s.t")
    assert len(rows) == 1
    assert rows[0]["source_fqn"] == "src.s.t"


@patch("src.lineage_tracker.execute_sql")
def test_query_lineage_with_operation_id(mock_sql):
    mock_sql.return_value = []
    query_lineage(MagicMock(), "wh-1", operation_id="op-1")
    sql_arg = mock_sql.call_args[0][2]
    assert "operation_id = 'op-1'" in sql_arg


@patch("src.lineage_tracker.execute_sql")
def test_query_lineage_no_filters(mock_sql):
    mock_sql.return_value = []
    query_lineage(MagicMock(), "wh-1")
    sql_arg = mock_sql.call_args[0][2]
    assert "WHERE" not in sql_arg


@patch("src.lineage_tracker.execute_sql")
def test_query_lineage_sql_failure(mock_sql):
    mock_sql.side_effect = Exception("table not found")
    try:
        query_lineage(MagicMock(), "wh-1")
        assert False, "Should have raised"
    except Exception:
        pass
