"""Tests for clone history."""

from unittest.mock import MagicMock, patch

from src.clone_history import CloneHistory


class TestCloneHistory:
    def _make_history(self):
        client = MagicMock()
        config = {"audit": {"catalog": "clone_audit", "schema": "logs", "table": "clone_audit_log"}}
        return CloneHistory(client, "wh-123", config)

    # --- list_operations ---

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
    def test_list_with_source_filter(self, mock_sql):
        mock_sql.return_value = []
        h = self._make_history()
        h.list_operations(source_catalog="prod")
        sql_arg = mock_sql.call_args[0][2]
        assert "source_catalog = 'prod'" in sql_arg

    @patch("src.clone_history.execute_sql")
    def test_list_with_status_filter(self, mock_sql):
        mock_sql.return_value = []
        h = self._make_history()
        h.list_operations(status="FAILED")
        sql_arg = mock_sql.call_args[0][2]
        assert "status = 'FAILED'" in sql_arg

    @patch("src.clone_history.execute_sql", side_effect=Exception("DB error"))
    def test_list_operations_returns_empty_on_error(self, mock_sql):
        h = self._make_history()
        ops = h.list_operations()
        assert ops == []

    # --- show_operation ---

    @patch("src.clone_history.execute_sql")
    def test_show_operation_found(self, mock_sql):
        mock_sql.return_value = [{"operation_id": "op1", "status": "SUCCESS"}]
        h = self._make_history()
        result = h.show_operation("op1")
        assert result["operation_id"] == "op1"

    @patch("src.clone_history.execute_sql")
    def test_show_operation_not_found(self, mock_sql):
        mock_sql.return_value = []
        h = self._make_history()
        result = h.show_operation("nonexistent")
        assert result is None

    def test_show_operation_no_audit_config(self):
        h = CloneHistory(MagicMock(), "wh-123", {})
        result = h.show_operation("op1")
        assert result is None

    @patch("src.clone_history.execute_sql", side_effect=Exception("timeout"))
    def test_show_operation_returns_none_on_error(self, mock_sql):
        h = self._make_history()
        result = h.show_operation("op1")
        assert result is None

    # --- diff_operations ---

    @patch("src.clone_history.execute_sql")
    def test_diff_operations(self, mock_sql):
        mock_sql.side_effect = [
            [{"operation_id": "op1", "tables_cloned": 10, "tables_failed": 0, "started_at": "2024-01-01"}],
            [{"operation_id": "op2", "tables_cloned": 12, "tables_failed": 1, "started_at": "2024-01-02"}],
        ]
        h = self._make_history()
        diff = h.diff_operations("op1", "op2")
        assert len(diff["changes"]) > 0
        field_names = [c["field"] for c in diff["changes"]]
        assert "Tables cloned" in field_names

    @patch("src.clone_history.execute_sql")
    def test_diff_missing_operation(self, mock_sql):
        mock_sql.side_effect = [
            [],  # op1 not found
            [{"operation_id": "op2", "tables_cloned": 5, "started_at": "2024-01-02"}],
        ]
        h = self._make_history()
        diff = h.diff_operations("op1", "op2")
        assert "error" in diff

    @patch("src.clone_history.execute_sql")
    def test_diff_identical_operations(self, mock_sql):
        row = {"operation_id": "op1", "tables_cloned": 10, "tables_failed": 0, "started_at": "2024-01-01"}
        mock_sql.side_effect = [[row], [row]]
        h = self._make_history()
        diff = h.diff_operations("op1", "op1")
        assert diff["changes"] == []

    # --- format_log ---

    def test_format_log(self):
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
        assert "2m0s" in output

    def test_format_log_empty(self):
        h = self._make_history()
        assert "No clone operations" in h.format_log([])

    def test_format_log_failed_status(self):
        h = self._make_history()
        ops = [
            {
                "operation_id": "fail1",
                "user_name": "admin",
                "started_at": "2024-06-01",
                "source_catalog": "a",
                "destination_catalog": "b",
                "clone_type": "SHALLOW",
                "status": "FAILED",
                "tables_cloned": 0,
                "tables_failed": 5,
                "duration_seconds": 30,
            }
        ]
        output = h.format_log(ops)
        assert "- Clone a -> b" in output

    # --- format_diff ---

    def test_format_diff_with_changes(self):
        h = self._make_history()
        diff = {
            "operation_1": {"id": "op1", "started_at": "2024-01-01"},
            "operation_2": {"id": "op2", "started_at": "2024-01-02"},
            "changes": [{"field": "Tables cloned", "old_value": 10, "new_value": 12}],
        }
        output = h.format_diff(diff)
        assert "op1" in output
        assert "op2" in output
        assert "Tables cloned" in output

    def test_format_diff_error(self):
        h = self._make_history()
        diff = {"error": "One or both operations not found"}
        output = h.format_diff(diff)
        assert "Error" in output

    def test_format_diff_no_changes(self):
        h = self._make_history()
        diff = {
            "operation_1": {"id": "op1", "started_at": "2024-01-01"},
            "operation_2": {"id": "op2", "started_at": "2024-01-02"},
            "changes": [],
        }
        output = h.format_diff(diff)
        assert "No differences" in output
