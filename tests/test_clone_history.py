"""Tests for clone history."""

from unittest.mock import MagicMock, patch

from src.clone_history import CloneHistory


class TestCloneHistory:
    def _make_history(self, mock_sql_return=None):
        client = MagicMock()
        config = {"audit": {"catalog": "clone_audit", "schema": "logs", "table": "clone_audit_log"}}
        h = CloneHistory(client, "wh-123", config)
        return h

    @patch("src.clone_history.execute_sql")
    def test_list_operations(self, mock_sql):
        mock_sql.return_value = [
            {"operation_id": "op1", "source_catalog": "src", "status": "SUCCESS"}
        ]
        h = self._make_history()
        ops = h.list_operations(limit=10)
        assert len(ops) == 1
        assert ops[0]["operation_id"] == "op1"

    def test_list_no_audit_config(self):
        h = CloneHistory(MagicMock(), "wh-123", {})
        ops = h.list_operations()
        assert ops == []

    @patch("src.clone_history.execute_sql")
    def test_format_log(self, mock_sql):
        h = self._make_history()
        ops = [
            {
                "operation_id": "abc123",
                "user_name": "user@test.com",
                "started_at": "2024-01-01T00:00:00",
                "source_catalog": "src",
                "destination_catalog": "dst",
                "clone_type": "DEEP",
                "status": "SUCCESS",
                "tables_cloned": 10,
                "tables_failed": 0,
                "duration_seconds": 120,
            }
        ]
        output = h.format_log(ops)
        assert "abc123" in output
        assert "user@test.com" in output
        assert "src -> dst" in output

    @patch("src.clone_history.execute_sql")
    def test_diff_operations(self, mock_sql):
        mock_sql.side_effect = [
            [{"operation_id": "op1", "tables_cloned": 10, "tables_failed": 0, "started_at": "2024-01-01"}],
            [{"operation_id": "op2", "tables_cloned": 12, "tables_failed": 1, "started_at": "2024-01-02"}],
        ]
        h = self._make_history()
        diff = h.diff_operations("op1", "op2")
        assert len(diff["changes"]) > 0

    def test_format_log_empty(self):
        h = self._make_history()
        assert "No clone operations" in h.format_log([])
