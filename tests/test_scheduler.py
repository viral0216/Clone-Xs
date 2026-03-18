"""Tests for scheduled cloning."""

import pytest
from unittest.mock import MagicMock, patch, call

from src.scheduler import parse_interval, parse_cron, check_drift, run_scheduled_clone


class TestParseInterval:
    def test_seconds(self):
        assert parse_interval("30s") == 30

    def test_minutes(self):
        assert parse_interval("5m") == 300

    def test_hours(self):
        assert parse_interval("1h") == 3600
        assert parse_interval("6h") == 21600

    def test_days(self):
        assert parse_interval("1d") == 86400

    def test_with_spaces(self):
        assert parse_interval(" 5m ") == 300

    def test_uppercase_normalized(self):
        assert parse_interval("5M") == 300

    def test_invalid(self):
        with pytest.raises(ValueError):
            parse_interval("abc")

    def test_no_unit(self):
        with pytest.raises(ValueError):
            parse_interval("5")

    def test_empty_string(self):
        with pytest.raises(ValueError):
            parse_interval("")

    def test_zero_value(self):
        assert parse_interval("0s") == 0


class TestParseCron:
    def test_returns_positive_seconds(self):
        # Any valid cron expression should return > 0 seconds
        result = parse_cron("0 * * * *")
        assert result > 0

    def test_invalid_field_count(self):
        with pytest.raises(ValueError, match="5 fields"):
            parse_cron("* *")

    def test_six_fields_invalid(self):
        with pytest.raises(ValueError, match="5 fields"):
            parse_cron("* * * * * *")

    def test_step_expression(self):
        result = parse_cron("*/15 * * * *")
        assert isinstance(result, int)
        assert result > 0

    def test_specific_hour(self):
        result = parse_cron("0 3 * * *")
        assert isinstance(result, int)
        assert result > 0

    def test_max_return_is_reasonable(self):
        # Should not exceed 48 hours (fallback is 86400)
        result = parse_cron("0 0 * * *")
        assert result <= 86400


class TestCheckDrift:
    @patch("src.diff.compare_catalogs")
    def test_no_drift(self, mock_compare):
        mock_compare.return_value = {
            "schemas": {"only_in_source": [], "only_in_dest": []},
            "tables": {"only_in_source": [], "only_in_dest": []},
            "views": {"only_in_source": [], "only_in_dest": []},
            "functions": {"only_in_source": [], "only_in_dest": []},
            "volumes": {"only_in_source": [], "only_in_dest": []},
        }
        result = check_drift(MagicMock(), "wh", "src", "dst", [])
        assert result is False

    @patch("src.diff.compare_catalogs")
    def test_with_drift(self, mock_compare):
        mock_compare.return_value = {
            "schemas": {"only_in_source": ["new_schema"], "only_in_dest": []},
            "tables": {"only_in_source": [], "only_in_dest": []},
            "views": {"only_in_source": [], "only_in_dest": []},
            "functions": {"only_in_source": [], "only_in_dest": []},
            "volumes": {"only_in_source": [], "only_in_dest": []},
        }
        result = check_drift(MagicMock(), "wh", "src", "dst", [])
        assert result is True

    @patch("src.diff.compare_catalogs", side_effect=Exception("error"))
    def test_drift_check_error_assumes_drift(self, mock_compare):
        result = check_drift(MagicMock(), "wh", "src", "dst", [])
        assert result is True


class TestRunScheduledClone:
    @patch("src.clone_catalog.clone_catalog")
    @patch("src.scheduler.check_drift", return_value=True)
    def test_runs_clone_when_drift_detected(self, mock_drift, mock_clone):
        mock_clone.return_value = {"tables": 5}

        config = {
            "source_catalog": "src", "destination_catalog": "dst",
            "exclude_schemas": [], "sql_warehouse_id": "wh",
        }
        result = run_scheduled_clone(MagicMock(), config)

        assert result["status"] == "completed"
        mock_clone.assert_called_once()

    @patch("src.scheduler.check_drift", return_value=False)
    def test_skips_clone_when_no_drift(self, mock_drift):
        config = {
            "source_catalog": "src", "destination_catalog": "dst",
            "exclude_schemas": [], "sql_warehouse_id": "wh",
            "drift_check_before_clone": True,
        }
        result = run_scheduled_clone(MagicMock(), config)

        assert result["status"] == "skipped"
        assert result["reason"] == "no_drift"

    @patch("src.clone_catalog.clone_catalog", side_effect=Exception("clone failed"))
    @patch("src.scheduler.check_drift", return_value=True)
    def test_handles_clone_error(self, mock_drift, mock_clone):
        config = {
            "source_catalog": "src", "destination_catalog": "dst",
            "exclude_schemas": [], "sql_warehouse_id": "wh",
        }
        result = run_scheduled_clone(MagicMock(), config)

        assert result["status"] == "failed"
        assert "clone failed" in result["error"]

    @patch("src.scheduler.check_drift", return_value=False)
    def test_on_complete_callback_called_on_skip(self, mock_drift):
        callback = MagicMock()
        config = {
            "source_catalog": "src", "destination_catalog": "dst",
            "exclude_schemas": [], "sql_warehouse_id": "wh",
        }
        run_scheduled_clone(MagicMock(), config, on_complete=callback)
        callback.assert_called_once()
        call_arg = callback.call_args[0][0]
        assert call_arg["status"] == "skipped"

    @patch("src.clone_catalog.clone_catalog")
    @patch("src.scheduler.check_drift", return_value=True)
    def test_on_complete_callback_called_on_success(self, mock_drift, mock_clone):
        mock_clone.return_value = {"tables": 1}
        callback = MagicMock()
        config = {
            "source_catalog": "src", "destination_catalog": "dst",
            "exclude_schemas": [], "sql_warehouse_id": "wh",
        }
        run_scheduled_clone(MagicMock(), config, on_complete=callback)
        callback.assert_called_once()

    @patch("src.clone_catalog.clone_catalog")
    @patch("src.scheduler.check_drift", return_value=True)
    def test_drift_check_disabled(self, mock_drift, mock_clone):
        mock_clone.return_value = {"tables": 1}
        config = {
            "source_catalog": "src", "destination_catalog": "dst",
            "exclude_schemas": [], "sql_warehouse_id": "wh",
            "drift_check_before_clone": False,
        }
        result = run_scheduled_clone(MagicMock(), config)
        assert result["status"] == "completed"
        mock_drift.assert_not_called()
