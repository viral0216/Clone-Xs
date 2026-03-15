"""Tests for metrics collection."""

import json
import os
import tempfile

from src.metrics import MetricsCollector, save_metrics_json, save_metrics_prometheus, format_metrics_report


class TestMetricsCollector:
    def test_start_and_end(self):
        c = MetricsCollector()
        c.start_operation("src", "dst", "DEEP")
        c.record_table_clone("s1", "t1", 2.5, True, 1000, 5000)
        c.record_table_clone("s1", "t2", 1.5, False)
        c.end_operation({"schemas_processed": 1})
        summary = c.get_summary()
        assert summary["metrics"]["total_tables"] == 2
        assert summary["metrics"]["successful"] == 1
        assert summary["metrics"]["failed"] == 1
        assert summary["metrics"]["failure_rate"] == 50.0
        assert summary["source_catalog"] == "src"

    def test_empty_collector(self):
        c = MetricsCollector()
        c.start_operation("src", "dst", "DEEP")
        c.end_operation({})
        summary = c.get_summary()
        assert summary["metrics"]["total_tables"] == 0
        assert summary["metrics"]["failure_rate"] == 0

    def test_throughput(self):
        c = MetricsCollector()
        c.start_operation("src", "dst", "DEEP")
        c.record_table_clone("s1", "t1", 1.0, True)
        c.end_operation({})
        summary = c.get_summary()
        assert summary["metrics"]["throughput_tables_per_min"] > 0


class TestSaveMetrics:
    def test_save_json(self):
        metrics = {"source_catalog": "src", "metrics": {"total_tables": 5}}
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            path = f.name
        save_metrics_json(metrics, path)
        with open(path) as f:
            loaded = json.load(f)
        assert loaded["source_catalog"] == "src"
        os.remove(path)

    def test_save_prometheus(self):
        metrics = {
            "source_catalog": "src", "destination_catalog": "dst",
            "duration_seconds": 120,
            "metrics": {
                "successful": 10, "failed": 2, "failure_rate": 16.67,
                "throughput_tables_per_min": 6.0, "avg_table_clone_seconds": 5.0,
            },
        }
        with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as f:
            path = f.name
        save_metrics_prometheus(metrics, path)
        with open(path) as f:
            content = f.read()
        assert "clone_duration_seconds" in content
        assert "clone_failure_rate" in content
        os.remove(path)


class TestFormatMetrics:
    def test_empty_history(self):
        output = format_metrics_report([])
        assert "No metrics" in output

    def test_with_data(self):
        data = [{"operation_id": "abc", "source_catalog": "src", "destination_catalog": "dst",
                 "duration_seconds": 120, "total_tables": 10, "failed": 0}]
        output = format_metrics_report(data)
        assert "abc" in output
