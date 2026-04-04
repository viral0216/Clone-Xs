"""Unit tests for src/lineage.py — lineage recording and querying."""

from unittest.mock import patch, MagicMock
import pytest


MOCK_ENTRIES = [
    {
        "source": "prod_cat",
        "dest": "dev_cat",
        "schema": "sales",
        "object_name": "orders",
        "object_type": "TABLE",
        "clone_type": "DEEP",
    },
    {
        "source": "prod_cat",
        "dest": "dev_cat",
        "schema": "sales",
        "object_name": "customers",
        "object_type": "TABLE",
        "clone_type": "SHALLOW",
    },
    {
        "source": "prod_cat",
        "dest": "dev_cat",
        "schema": "hr",
        "object_name": "employees",
        "object_type": "TABLE",
        "clone_type": "DEEP",
    },
]


@patch("src.table_registry.get_batch_insert_size", return_value=100)
@patch("src.client.utc_now", return_value="2026-01-01T00:00:00")
@patch("src.lineage.execute_sql")
class TestRecordLineageBatch:
    """Tests for record_lineage_batch."""

    def test_batch_insert_multiple_entries(
        self, mock_exec, mock_now, mock_batch
    ):
        from src.lineage import record_lineage_batch

        client = MagicMock()
        record_lineage_batch(
            client, "wh-1", "audit_cat", "audit_sch", MOCK_ENTRIES, dry_run=False
        )

        # First call = CREATE TABLE, second call = INSERT with 3 value rows
        assert mock_exec.call_count == 2
        create_sql = mock_exec.call_args_list[0].args[2]
        assert "CREATE TABLE IF NOT EXISTS" in create_sql

        insert_sql = mock_exec.call_args_list[1].args[2]
        assert "INSERT INTO" in insert_sql
        assert "prod_cat.sales.orders" in insert_sql
        assert "prod_cat.sales.customers" in insert_sql
        assert "prod_cat.hr.employees" in insert_sql

    def test_batch_insert_empty_entries_no_insert(
        self, mock_exec, mock_now, mock_batch
    ):
        from src.lineage import record_lineage_batch

        client = MagicMock()
        record_lineage_batch(
            client, "wh-1", "audit_cat", "audit_sch", [], dry_run=False
        )

        # Only CREATE TABLE, no INSERT
        assert mock_exec.call_count == 1
        assert "CREATE TABLE" in mock_exec.call_args_list[0].args[2]

    def test_batch_insert_passes_dry_run(
        self, mock_exec, mock_now, mock_batch
    ):
        from src.lineage import record_lineage_batch

        client = MagicMock()
        record_lineage_batch(
            client, "wh-1", "cat", "sch", MOCK_ENTRIES[:1], dry_run=True
        )

        for c in mock_exec.call_args_list:
            assert c.kwargs.get("dry_run") is True

    def test_batch_insert_respects_batch_size(
        self, mock_exec, mock_now, mock_batch
    ):
        """When batch_size < entries, multiple INSERTs are issued."""
        mock_batch.return_value = 2  # override to batch of 2

        from src.lineage import record_lineage_batch

        client = MagicMock()
        record_lineage_batch(
            client, "wh-1", "cat", "sch", MOCK_ENTRIES, dry_run=False
        )

        # 1 CREATE + 2 INSERT batches (2 + 1)
        assert mock_exec.call_count == 3


@patch("src.lineage.execute_sql")
class TestGetLineageForObject:
    """Tests for get_lineage_for_object."""

    def test_get_lineage_returns_rows(self, mock_exec):
        from src.lineage import get_lineage_for_object

        mock_exec.return_value = [
            {
                "source_object": "prod.sales.orders",
                "destination_object": "dev.sales.orders",
                "object_type": "TABLE",
                "clone_type": "DEEP",
                "clone_timestamp": "2026-01-01T00:00:00",
            }
        ]

        client = MagicMock()
        result = get_lineage_for_object(
            client, "wh-1", "audit_cat", "audit_sch", "prod.sales.orders"
        )

        assert len(result) == 1
        assert result[0]["source_object"] == "prod.sales.orders"

        sql = mock_exec.call_args.args[2]
        assert "prod.sales.orders" in sql
        assert "ORDER BY clone_timestamp DESC" in sql

    def test_get_lineage_returns_empty_on_exception(self, mock_exec):
        from src.lineage import get_lineage_for_object

        mock_exec.side_effect = RuntimeError("table not found")

        client = MagicMock()
        result = get_lineage_for_object(
            client, "wh-1", "cat", "sch", "nonexistent.path"
        )

        assert result == []


@patch("src.client.utc_now", return_value="2026-01-01T00:00:00")
@patch("src.lineage.execute_sql")
class TestRecordLineageToUC:
    """Tests for record_lineage_to_uc."""

    def test_record_lineage_to_uc_creates_and_inserts(self, mock_exec, mock_now):
        from src.lineage import record_lineage_to_uc

        client = MagicMock()
        record_lineage_to_uc(
            client, "wh-1", "audit", "lin", "src_cat", "dst_cat",
            "sales", "orders", "TABLE", "DEEP", dry_run=False,
        )

        assert mock_exec.call_count == 2
        assert "CREATE TABLE" in mock_exec.call_args_list[0].args[2]
        insert_sql = mock_exec.call_args_list[1].args[2]
        assert "INSERT INTO" in insert_sql
        assert "src_cat.sales.orders" in insert_sql
