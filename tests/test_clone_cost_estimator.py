"""Tests for the clone cost estimator module."""

from unittest.mock import MagicMock, patch

from src.clone_cost_estimator import _format_bytes, _format_duration, estimate_clone_cost


class TestFormatBytes:
    def test_zero_bytes(self):
        assert _format_bytes(0) == "0.0 B"

    def test_bytes(self):
        assert _format_bytes(512) == "512.0 B"

    def test_kilobytes(self):
        assert _format_bytes(1024) == "1.0 KB"

    def test_megabytes(self):
        result = _format_bytes(1024 * 1024)
        assert "MB" in result
        assert result == "1.0 MB"

    def test_gigabytes(self):
        result = _format_bytes(1024 ** 3)
        assert result == "1.0 GB"

    def test_terabytes(self):
        result = _format_bytes(1024 ** 4)
        assert result == "1.0 TB"


class TestFormatDuration:
    def test_seconds(self):
        result = _format_duration(0.5)
        assert "seconds" in result

    def test_minutes(self):
        result = _format_duration(45)
        assert "45 minutes" == result

    def test_hours_and_minutes(self):
        result = _format_duration(90)
        assert result == "1h 30m"

    def test_one_minute(self):
        result = _format_duration(1)
        assert result == "1 minutes"


class TestEstimateCloneCost:
    @patch("src.clone_cost_estimator.execute_sql")
    def test_deep_clone_estimate(self, mock_sql):
        mock_sql.return_value = [
            {
                "table_schema": "sales",
                "table_name": "orders",
                "table_type": "MANAGED",
                "size_bytes": str(10 * 1024 ** 3),  # 10 GB
            },
            {
                "table_schema": "sales",
                "table_name": "customers",
                "table_type": "MANAGED",
                "size_bytes": str(5 * 1024 ** 3),  # 5 GB
            },
        ]

        result = estimate_clone_cost(
            MagicMock(), "wh-123", "my_catalog",
            clone_type="DEEP",
        )

        assert result["total_tables"] == 2
        assert result["total_size_gb"] == 15.0
        assert result["clone_type"] == "DEEP"
        assert result["cost_estimate"]["monthly_storage_cost_usd"] > 0

    @patch("src.clone_cost_estimator.execute_sql")
    def test_shallow_clone_zero_storage(self, mock_sql):
        mock_sql.return_value = [
            {
                "table_schema": "sales",
                "table_name": "orders",
                "table_type": "MANAGED",
                "size_bytes": str(10 * 1024 ** 3),
            },
        ]

        result = estimate_clone_cost(
            MagicMock(), "wh-123", "my_catalog",
            clone_type="SHALLOW",
        )

        assert result["cost_estimate"]["monthly_storage_cost_usd"] == 0.0

    @patch("src.clone_cost_estimator.execute_sql")
    def test_views_not_counted_in_size(self, mock_sql):
        mock_sql.return_value = [
            {
                "table_schema": "sales",
                "table_name": "orders_view",
                "table_type": "VIEW",
                "size_bytes": "0",
            },
        ]

        result = estimate_clone_cost(MagicMock(), "wh-123", "my_catalog")
        assert result["total_views"] == 1
        assert result["total_tables"] == 0
