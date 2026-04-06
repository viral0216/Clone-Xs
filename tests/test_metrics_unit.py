"""Unit tests for src/metrics.py — metrics collection and Delta persistence."""

from unittest.mock import patch, MagicMock
import pytest


@patch("src.metrics.execute_sql")
class TestSaveMetricsDelta:
    """Tests for save_metrics_delta."""

    @patch("src.catalog_utils.ensure_catalog_and_schema")
    def test_creates_table_and_inserts(self, mock_ensure, mock_exec):
        from src.metrics import save_metrics_delta

        client = MagicMock()
        metrics = {
            "source_catalog": "prod",
            "destination_catalog": "dev",
            "clone_type": "DEEP",
            "started_at": "2026-01-01T00:00:00",
            "completed_at": "2026-01-01T00:05:00",
            "duration_seconds": 300,
            "metrics": {
                "total_tables": 10,
                "successful": 9,
                "failed": 1,
                "failure_rate": 10.0,
                "throughput_tables_per_min": 2.0,
                "avg_table_clone_seconds": 30.0,
                "total_row_count": 5000,
                "total_size_bytes": 1048576,
            },
        }
        save_metrics_delta(client, "wh-1", metrics, "cat.sch.clone_metrics")

        assert mock_exec.call_count == 2
        create_sql = mock_exec.call_args_list[0].args[2]
        assert "CREATE TABLE IF NOT EXISTS cat.sch.clone_metrics" in create_sql

        insert_sql = mock_exec.call_args_list[1].args[2]
        assert "INSERT INTO cat.sch.clone_metrics" in insert_sql
        assert "prod" in insert_sql
        assert "dev" in insert_sql

    @patch("src.catalog_utils.ensure_catalog_and_schema")
    def test_status_is_success_when_no_failures(self, mock_ensure, mock_exec):
        from src.metrics import save_metrics_delta

        client = MagicMock()
        metrics = {
            "metrics": {"failed": 0, "total_tables": 5, "successful": 5,
                        "failure_rate": 0, "throughput_tables_per_min": 1,
                        "avg_table_clone_seconds": 1, "total_row_count": 0,
                        "total_size_bytes": 0},
            "duration_seconds": 10,
        }
        save_metrics_delta(client, "wh-1", metrics, "cat.sch.tbl")

        insert_sql = mock_exec.call_args_list[1].args[2]
        assert "'success'" in insert_sql

    @patch("src.catalog_utils.ensure_catalog_and_schema")
    def test_status_completed_with_errors(self, mock_ensure, mock_exec):
        from src.metrics import save_metrics_delta

        client = MagicMock()
        metrics = {
            "metrics": {"failed": 2, "total_tables": 10, "successful": 8,
                        "failure_rate": 20, "throughput_tables_per_min": 1,
                        "avg_table_clone_seconds": 1, "total_row_count": 0,
                        "total_size_bytes": 0},
            "duration_seconds": 10,
        }
        save_metrics_delta(client, "wh-1", metrics, "cat.sch.tbl")

        insert_sql = mock_exec.call_args_list[1].args[2]
        assert "'completed_with_errors'" in insert_sql

    @patch("src.catalog_utils.ensure_catalog_and_schema")
    def test_status_failed_on_error(self, mock_ensure, mock_exec):
        from src.metrics import save_metrics_delta

        client = MagicMock()
        metrics = {
            "error": "Something went wrong",
            "metrics": {"failed": 0, "total_tables": 0, "successful": 0,
                        "failure_rate": 0, "throughput_tables_per_min": 0,
                        "avg_table_clone_seconds": 0, "total_row_count": 0,
                        "total_size_bytes": 0},
            "duration_seconds": 0,
        }
        save_metrics_delta(client, "wh-1", metrics, "cat.sch.tbl")

        insert_sql = mock_exec.call_args_list[1].args[2]
        assert "'failed'" in insert_sql


class TestMetricsCollector:
    """Tests for MetricsCollector class."""

    def test_add_table_metric_and_get_summary(self):
        from src.metrics import MetricsCollector

        mc = MetricsCollector()
        mc.start_operation("prod", "dev", "DEEP")

        mc.record_table_clone("sales", "orders", 2.5, True, row_count=100, size_bytes=2048)
        mc.record_table_clone("sales", "returns", 1.0, True, row_count=50, size_bytes=1024)
        mc.record_table_clone("hr", "employees", 0.5, False)

        mc.end_operation({"tables_cloned": 2, "tables_failed": 1})

        summary = mc.get_summary()
        m = summary["metrics"]

        assert m["total_tables"] == 3
        assert m["successful"] == 2
        assert m["failed"] == 1
        assert m["failure_rate"] == pytest.approx(33.33, abs=0.1)
        assert m["total_row_count"] == 150
        assert m["total_size_bytes"] == 3072
        assert m["avg_table_clone_seconds"] == pytest.approx(1.33, abs=0.1)

    def test_empty_collector_summary(self):
        from src.metrics import MetricsCollector

        mc = MetricsCollector()
        mc.start_operation("a", "b", "SHALLOW")
        mc.end_operation({})

        summary = mc.get_summary()
        m = summary["metrics"]

        assert m["total_tables"] == 0
        assert m["successful"] == 0
        assert m["failed"] == 0
        assert m["failure_rate"] == 0
        assert m["avg_table_clone_seconds"] == 0

    def test_collector_records_operation_metadata(self):
        from src.metrics import MetricsCollector

        mc = MetricsCollector()
        mc.start_operation("src_cat", "dst_cat", "DEEP")
        mc.end_operation({})

        summary = mc.get_summary()
        assert summary["source_catalog"] == "src_cat"
        assert summary["destination_catalog"] == "dst_cat"
        assert summary["clone_type"] == "DEEP"
        assert "started_at" in summary
        assert "completed_at" in summary
        assert "duration_seconds" in summary
