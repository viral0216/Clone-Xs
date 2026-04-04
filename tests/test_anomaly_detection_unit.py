"""Unit tests for src/anomaly_detection.py — metric recording and anomaly detection."""

from unittest.mock import patch, MagicMock
import pytest


SCHEMA_PATCH = patch(
    "src.anomaly_detection._get_schema", return_value="clone_audit.data_quality"
)


@SCHEMA_PATCH
@patch("src.anomaly_detection._query_sql")
@patch("src.anomaly_detection._run_sql")
class TestRecordMetric:
    """Tests for record_metric."""

    def test_first_measurement_no_history(self, mock_run, mock_query, mock_schema):
        """With no prior history, z_score should be 0 and severity normal."""
        from src.anomaly_detection import record_metric

        mock_query.return_value = []  # no history

        result = record_metric(
            "cat.sch.tbl", "col1", "null_pct", 5.0,
            client=MagicMock(), warehouse_id="wh-1", config={},
        )

        assert result["z_score"] == 0.0
        assert result["severity"] == "normal"
        assert result["is_anomaly"] is False
        assert result["value"] == 5.0

        # ensure_tables + INSERT
        assert mock_run.call_count >= 1

    def test_enough_history_computes_z_score(self, mock_run, mock_query, mock_schema):
        """With >=3 historical values, mean/stddev/z_score are computed."""
        from src.anomaly_detection import record_metric

        # 5 prior values: mean=10, stddev=0 → any deviation gives large z
        mock_query.return_value = [
            {"value": 10.0},
            {"value": 10.0},
            {"value": 10.0},
            {"value": 10.0},
            {"value": 10.0},
        ]

        result = record_metric(
            "cat.sch.tbl", "col1", "avg_length", 10.0,
            client=MagicMock(), warehouse_id="wh-1", config={},
        )

        # All values identical → stddev=0 → z_score=0
        assert result["z_score"] == 0.0
        assert result["severity"] == "normal"

    def test_anomaly_detected_far_from_mean(self, mock_run, mock_query, mock_schema):
        """A value far from mean should be flagged as anomaly."""
        from src.anomaly_detection import record_metric

        # mean=10, stddev=1 → value=20 gives z=10 (critical)
        mock_query.return_value = [
            {"value": 9.0},
            {"value": 10.0},
            {"value": 11.0},
        ]

        result = record_metric(
            "cat.sch.tbl", "col1", "row_count", 100.0,
            client=MagicMock(), warehouse_id="wh-1", config={},
        )

        assert result["is_anomaly"] is True
        assert result["severity"] == "critical"
        assert result["z_score"] > 3.0

    def test_warning_severity_threshold(self, mock_run, mock_query, mock_schema):
        """A value moderately far from mean yields warning severity."""
        from src.anomaly_detection import record_metric
        import math

        # mean of [8, 10, 12] = 10, stddev ~= 1.633
        # value = 10 + 2.5 * 1.633 = ~14.08 → z ~2.5 → warning (>2, <3)
        mock_query.return_value = [
            {"value": 8.0},
            {"value": 10.0},
            {"value": 12.0},
        ]
        mean = 10.0
        stddev = math.sqrt(sum((v - mean) ** 2 for v in [8, 10, 12]) / 3)
        test_value = mean + 2.5 * stddev

        result = record_metric(
            "cat.sch.tbl", "col1", "metric", test_value,
            client=MagicMock(), warehouse_id="wh-1", config={},
        )

        assert result["severity"] == "warning"
        assert result["is_anomaly"] is True
        assert 2.0 < result["z_score"] < 3.0


@SCHEMA_PATCH
@patch("src.table_registry.get_batch_insert_size", return_value=100)
@patch("src.anomaly_detection._query_sql")
@patch("src.anomaly_detection._run_sql")
class TestRecordMetricsBatch:
    """Tests for record_metrics_batch."""

    def test_batch_insert_multiple_metrics(
        self, mock_run, mock_query, mock_batch, mock_schema
    ):
        from src.anomaly_detection import record_metrics_batch

        mock_query.return_value = []  # no history for any metric

        metrics = [
            {"table_fqn": "c.s.t", "column_name": "a", "metric_name": "null_pct", "value": 1.0},
            {"table_fqn": "c.s.t", "column_name": "b", "metric_name": "null_pct", "value": 2.0},
        ]
        results = record_metrics_batch(
            metrics, client=MagicMock(), warehouse_id="wh-1", config={},
        )

        assert len(results) == 2
        # ensure_tables run_sql + batch INSERT run_sql
        insert_calls = [
            c for c in mock_run.call_args_list
            if "INSERT INTO" in str(c)
        ]
        assert len(insert_calls) >= 1

    def test_batch_empty_returns_empty(
        self, mock_run, mock_query, mock_batch, mock_schema
    ):
        from src.anomaly_detection import record_metrics_batch

        results = record_metrics_batch(
            [], client=MagicMock(), warehouse_id="wh-1", config={},
        )
        assert results == []


@SCHEMA_PATCH
@patch("src.anomaly_detection._run_sql")
class TestEnsureTables:
    """Tests for ensure_tables."""

    @patch("src.catalog_utils.safe_ensure_schema_from_fqn")
    def test_ensure_tables_creates_metric_baselines(
        self, mock_ensure_schema, mock_run, mock_schema
    ):
        from src.anomaly_detection import ensure_tables

        client = MagicMock()
        ensure_tables(client, "wh-1", {})

        mock_ensure_schema.assert_called_once()
        assert mock_run.call_count == 1
        sql = mock_run.call_args.args[0]
        assert "CREATE TABLE IF NOT EXISTS" in sql
        assert "metric_baselines" in sql
