"""Tests for the Delta table-based state store module."""

from unittest.mock import MagicMock, patch

from src.state_store import StateStore


class TestStateStoreInit:
    def test_stores_client_and_warehouse(self):
        client = MagicMock()
        store = StateStore(client, "wh-123")
        assert store.client is client
        assert store.warehouse_id == "wh-123"

    def test_default_catalog_and_schema(self):
        store = StateStore(MagicMock(), "wh-123")
        assert store.state_catalog == "clone_audit"
        assert store.state_schema == "state"

    def test_custom_catalog_and_schema(self):
        store = StateStore(MagicMock(), "wh-123", state_catalog="my_audit", state_schema="my_state")
        assert store.state_catalog == "my_audit"
        assert store.state_schema == "my_state"
        assert store._clone_state_table == "my_audit.my_state.clone_state"
        assert store._operations_table == "my_audit.my_state.clone_operations"


class TestStateStoreInitTables:
    @patch("src.state_store.execute_sql")
    def test_creates_catalog_schema_tables(self, mock_sql):
        mock_sql.return_value = []
        store = StateStore(MagicMock(), "wh-123")
        store.init_tables()

        # Should call execute_sql multiple times for CREATE statements
        assert mock_sql.call_count >= 4  # catalog, schema, 2 tables
        all_sql = [c[0][2] for c in mock_sql.call_args_list]

        assert any("CREATE CATALOG" in sql for sql in all_sql)
        assert any("CREATE SCHEMA" in sql for sql in all_sql)
        assert any("clone_state" in sql and "CREATE TABLE" in sql for sql in all_sql)
        assert any("clone_operations" in sql and "CREATE TABLE" in sql for sql in all_sql)


class TestStateStoreRecordTableClone:
    @patch("src.state_store.execute_sql")
    def test_records_clone(self, mock_sql):
        mock_sql.return_value = []
        store = StateStore(MagicMock(), "wh-123")
        store.record_table_clone(
            "src.schema1.table1", "dst.schema1.table1",
            status="success", row_count=1000,
        )

        assert mock_sql.called
        sql = mock_sql.call_args[0][2]
        assert "MERGE INTO" in sql
        assert "src.schema1.table1" in sql


class TestStateStoreGetSummary:
    @patch("src.state_store.execute_sql")
    def test_returns_summary(self, mock_sql):
        mock_sql.return_value = [
            {"total": "10", "synced": "7", "stale": "2", "failed": "1"}
        ]

        store = StateStore(MagicMock(), "wh-123")
        result = store.get_summary("src_cat", "dst_cat")

        assert result["total_tracked"] == 10
        assert result["synced"] == 7
        assert result["stale"] == 2
        assert result["failed"] == 1
