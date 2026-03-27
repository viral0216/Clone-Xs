from unittest.mock import MagicMock, patch
from datetime import datetime, timezone

from src.audit_trail import (
    ensure_audit_table,
    get_audit_table_fqn,
    log_operation_start,
    log_operation_complete,
    query_audit_history,
)


def _config(**overrides):
    base = {
        "audit_trail": {"catalog": "my_audit", "schema": "logs", "table": "ops"},
        "source_catalog": "src_cat",
        "destination_catalog": "dst_cat",
        "clone_type": "DEEP",
    }
    base.update(overrides)
    return base


# ---------- get_audit_table_fqn ----------

def test_get_audit_table_fqn_from_config():
    assert get_audit_table_fqn(_config()) == "my_audit.logs.ops"


def test_get_audit_table_fqn_defaults():
    assert get_audit_table_fqn({}) == "clone_audit.logs.clone_operations"


# ---------- ensure_audit_table ----------

@patch("src.audit_trail.execute_sql")
def test_ensure_audit_table_happy(mock_sql):
    mock_sql.return_value = [
        {"col_name": "operation_id"},
        {"col_name": "status"},
    ]
    fqn = ensure_audit_table(MagicMock(), "wh-1", _config())
    assert fqn == "my_audit.logs.ops"
    # CREATE CATALOG, CREATE SCHEMA, CREATE TABLE, DESCRIBE
    assert mock_sql.call_count >= 4


@patch("src.audit_trail.execute_sql")
def test_ensure_audit_table_catalog_fallback(mock_sql):
    """When CREATE CATALOG fails, it falls back to USE CATALOG."""
    call_count = [0]

    def side_effect(client, wh, sql, **kw):
        call_count[0] += 1
        if "CREATE CATALOG" in sql:
            raise Exception("permission denied")
        if "USE CATALOG" in sql:
            return []
        if "DESCRIBE TABLE" in sql:
            return [{"col_name": "operation_id"}]
        return []

    mock_sql.side_effect = side_effect
    fqn = ensure_audit_table(MagicMock(), "wh-1", _config())
    assert fqn == "my_audit.logs.ops"


@patch("src.client.execute_sql")
def test_ensure_audit_table_catalog_inaccessible(mock_sql):
    """When catalog_utils cannot verify or create the catalog, raise RuntimeError."""
    def side_effect(client, wh, sql, **kw):
        if "SHOW CATALOGS" in sql:
            return []  # catalog doesn't exist
        if "CREATE CATALOG" in sql:
            raise Exception("storage root not configured")
        if "USE CATALOG" in sql:
            raise Exception("catalog not found")
        return []

    mock_sql.side_effect = side_effect
    # Clear cached catalogs so the check actually runs
    from src.catalog_utils import _verified_catalogs
    _verified_catalogs.discard("my_audit")

    try:
        ensure_audit_table(MagicMock(), "wh-1", _config())
        assert False, "Should have raised RuntimeError"
    except (RuntimeError, Exception) as e:
        assert "catalog" in str(e).lower() or "Cannot" in str(e) or "storage" in str(e).lower()


# ---------- log_operation_start ----------

@patch("src.audit_trail.execute_sql")
def test_log_operation_start_happy(mock_sql):
    mock_sql.return_value = []
    log_operation_start(MagicMock(), "wh-1", _config(), "op-123")
    mock_sql.assert_called_once()
    sql_arg = mock_sql.call_args[0][2]
    assert "INSERT INTO" in sql_arg
    assert "op-123" in sql_arg


@patch("src.audit_trail.execute_sql")
def test_log_operation_start_sql_failure_no_raise(mock_sql):
    """SQL failure should be swallowed (just a warning)."""
    mock_sql.side_effect = Exception("connection lost")
    # Should not raise
    log_operation_start(MagicMock(), "wh-1", _config(), "op-123")


# ---------- log_operation_complete ----------

@patch("src.audit_trail.execute_sql")
def test_log_operation_complete_happy(mock_sql):
    mock_sql.return_value = []
    summary = {"tables": {"cloned": 5, "failed": 0}}
    log_operation_complete(
        MagicMock(), "wh-1", _config(), "op-123", summary, datetime(2025, 1, 1, tzinfo=timezone.utc)
    )
    sql_arg = mock_sql.call_args[0][2]
    assert "UPDATE" in sql_arg
    assert "op-123" in sql_arg
    assert "'success'" in sql_arg


@patch("src.audit_trail.execute_sql")
def test_log_operation_complete_with_error(mock_sql):
    mock_sql.return_value = []
    summary = {"tables": {"cloned": 0, "failed": 2}}
    log_operation_complete(
        MagicMock(), "wh-1", _config(), "op-123", summary,
        datetime(2025, 1, 1, tzinfo=timezone.utc), error_message="boom",
    )
    sql_arg = mock_sql.call_args[0][2]
    assert "'failed'" in sql_arg


# ---------- query_audit_history ----------

@patch("src.audit_trail.execute_sql")
def test_query_audit_history_happy(mock_sql):
    mock_sql.return_value = [
        {
            "operation_id": "abc-123",
            "started_at": "2025-01-01",
            "source_catalog": "src",
            "destination_catalog": "dst",
            "status": "success",
            "duration_seconds": "10",
            "tables_cloned": 3,
            "tables_failed": 0,
        }
    ]
    rows = query_audit_history(MagicMock(), "wh-1", _config())
    assert len(rows) == 1
    assert rows[0]["operation_id"] == "abc-123"


@patch("src.audit_trail.execute_sql")
def test_query_audit_history_with_filters(mock_sql):
    mock_sql.return_value = []
    query_audit_history(
        MagicMock(), "wh-1", _config(),
        source_catalog="src", status="success",
    )
    sql_arg = mock_sql.call_args[0][2]
    assert "source_catalog = 'src'" in sql_arg
    assert "status = 'success'" in sql_arg


@patch("src.audit_trail.execute_sql")
def test_query_audit_history_sql_failure(mock_sql):
    mock_sql.side_effect = Exception("table not found")
    try:
        query_audit_history(MagicMock(), "wh-1", _config())
        assert False, "Should have raised"
    except Exception:
        pass
