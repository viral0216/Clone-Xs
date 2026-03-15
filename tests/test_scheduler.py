"""Tests for scheduled cloning."""

from unittest.mock import MagicMock, patch

from src.scheduler import parse_interval, check_drift


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

    def test_invalid(self):
        import pytest
        with pytest.raises(ValueError):
            parse_interval("abc")

    def test_no_unit(self):
        import pytest
        with pytest.raises(ValueError):
            parse_interval("5")


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
