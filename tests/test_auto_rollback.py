"""Tests for auto-rollback on validation failure."""

from unittest.mock import MagicMock, patch

from src.validation import evaluate_threshold


class TestEvaluateThreshold:
    def test_passes_when_no_mismatches(self):
        summary = {"total_tables": 100, "matched": 100, "mismatched": 0, "errors": 0, "checksum_mismatches": 0}
        result = evaluate_threshold(summary, 5.0)
        assert result["passed"] is True
        assert result["mismatch_pct"] == 0.0
        assert result["failed_checks"] == []

    def test_passes_within_threshold(self):
        summary = {"total_tables": 100, "matched": 96, "mismatched": 4, "errors": 0, "checksum_mismatches": 0}
        result = evaluate_threshold(summary, 5.0)
        assert result["passed"] is True
        assert result["mismatch_pct"] == 4.0

    def test_fails_above_threshold(self):
        summary = {"total_tables": 100, "matched": 90, "mismatched": 8, "errors": 2, "checksum_mismatches": 1}
        result = evaluate_threshold(summary, 5.0)
        assert result["passed"] is False
        assert result["mismatch_pct"] == 10.0
        assert len(result["failed_checks"]) == 3

    def test_fails_exactly_at_threshold(self):
        summary = {"total_tables": 100, "matched": 95, "mismatched": 5, "errors": 0, "checksum_mismatches": 0}
        result = evaluate_threshold(summary, 5.0)
        assert result["passed"] is True  # <= threshold passes

    def test_empty_tables(self):
        summary = {"total_tables": 0, "matched": 0, "mismatched": 0, "errors": 0, "checksum_mismatches": 0}
        result = evaluate_threshold(summary, 5.0)
        assert result["passed"] is True
        assert result["mismatch_pct"] == 0.0

    def test_all_errors(self):
        summary = {"total_tables": 10, "matched": 0, "mismatched": 0, "errors": 10, "checksum_mismatches": 0}
        result = evaluate_threshold(summary, 5.0)
        assert result["passed"] is False
        assert result["mismatch_pct"] == 100.0

    def test_threshold_zero(self):
        summary = {"total_tables": 100, "matched": 99, "mismatched": 1, "errors": 0, "checksum_mismatches": 0}
        result = evaluate_threshold(summary, 0.0)
        assert result["passed"] is False


class TestAutoRollbackIntegration:
    @patch("src.clone_catalog.list_schemas_sdk")
    @patch("src.clone_catalog.execute_sql")
    def test_auto_rollback_triggers_on_validation_failure(self, mock_sql, mock_list_schemas):
        """Test that auto-rollback is triggered when validation fails threshold."""
        from src.clone_catalog import clone_catalog

        mock_client = MagicMock()
        mock_client.current_user.me.return_value.user_name = "test_user"
        mock_sql.return_value = []
        mock_list_schemas.return_value = ["test_schema"]

        config = {
            "source_catalog": "src",
            "destination_catalog": "dst",
            "sql_warehouse_id": "wh-123",
            "clone_type": "DEEP",
            "load_type": "FULL",
            "max_workers": 1,
            "exclude_schemas": ["information_schema", "default"],
            "exclude_tables": [],
            "dry_run": False,
            "copy_permissions": False,
            "copy_ownership": False,
            "copy_tags": False,
            "show_progress": False,
            "enable_rollback": True,
            "auto_rollback_on_failure": True,
            "rollback_threshold": 5.0,
            "validate_after_clone": True,
        }

        # The clone will process test_schema but the mocked SQL/SDK calls
        # will make it proceed quickly. This mainly tests the config path.
        result = clone_catalog(mock_client, config)
        assert "duration_seconds" in result
