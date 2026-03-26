"""Tests for the analysis router — diff, compare, validate, stats, search, profile,
estimate, storage-metrics, optimize, vacuum, check-predictive-optimization, export,
snapshot, schema-drift, column-usage, table-usage."""

import pytest
from unittest.mock import patch, MagicMock

pytest.importorskip("fastapi")

_PAIR = {"source_catalog": "src_cat", "destination_catalog": "dst_cat"}
_SINGLE = {"source_catalog": "src_cat"}


def test_catalog_diff(client):
    with patch("src.diff.compare_catalogs", return_value={"missing": [], "extra": []}):
        resp = client.post("/api/diff", json=_PAIR)
        assert resp.status_code == 200


def test_deep_compare(client):
    with patch("src.compare.compare_catalogs_deep", return_value={"diffs": []}):
        resp = client.post("/api/compare", json=_PAIR)
        assert resp.status_code == 200


def test_validate_clone(client):
    with patch("src.validation.validate_catalog", return_value={"mismatches": []}):
        resp = client.post("/api/validate", json=_PAIR)
        assert resp.status_code == 200


def test_schema_drift(client):
    with patch("src.schema_drift.detect_schema_drift", return_value={"drifts": []}):
        resp = client.post("/api/schema-drift", json=_PAIR)
        assert resp.status_code == 200


def test_catalog_stats(client):
    with patch("src.stats.catalog_stats", return_value={"total_tables": 0}):
        resp = client.post("/api/stats", json=_SINGLE)
        assert resp.status_code == 200


def test_search_catalog(client):
    with patch("src.search.search_tables", return_value={"results": []}):
        resp = client.post("/api/search", json={**_SINGLE, "pattern": "test"})
        assert resp.status_code == 200


def test_profile_catalog(client):
    with patch("src.profiling.profile_catalog", return_value={"profiles": []}):
        resp = client.post("/api/profile", json=_SINGLE)
        assert resp.status_code == 200


def test_cost_estimate(client):
    with patch("src.cost_estimation.estimate_clone_cost", return_value={"total_cost": 0}):
        resp = client.post("/api/estimate", json=_SINGLE)
        assert resp.status_code == 200


def test_storage_metrics(client):
    with patch("src.storage_metrics.catalog_storage_metrics", return_value={"tables": []}):
        resp = client.post("/api/storage-metrics", json=_SINGLE)
        assert resp.status_code == 200


def test_optimize_tables(client):
    with patch("src.table_maintenance.run_optimize", return_value={"optimized": []}), \
         patch("src.table_maintenance._enumerate_tables", return_value=[]):
        resp = client.post("/api/optimize", json=_SINGLE)
        assert resp.status_code == 200


def test_vacuum_tables(client):
    with patch("src.table_maintenance.run_vacuum", return_value={"vacuumed": []}), \
         patch("src.table_maintenance._enumerate_tables", return_value=[]):
        resp = client.post("/api/vacuum", json=_SINGLE)
        assert resp.status_code == 200


def test_check_predictive_optimization(client):
    with patch("src.table_maintenance.check_predictive_optimization", return_value={"enabled": False}):
        resp = client.post("/api/check-predictive-optimization", json=_SINGLE)
        assert resp.status_code == 200


def test_export_metadata(client):
    with patch("src.export.export_catalog_metadata", return_value="/tmp/export.csv"):
        resp = client.post("/api/export", json=_SINGLE)
        assert resp.status_code == 200
        assert "output_path" in resp.json()


def test_create_snapshot(client):
    with patch("src.snapshot.create_snapshot", return_value="/tmp/snapshot.json"):
        resp = client.post("/api/snapshot", json=_SINGLE)
        assert resp.status_code == 200
        assert "output_path" in resp.json()


def test_column_usage(client):
    with patch("src.column_usage.get_column_usage_summary", return_value={"top_columns": []}):
        resp = client.post("/api/column-usage", json={"catalog": "my_cat"})
        assert resp.status_code == 200


def test_table_usage(client):
    with patch("src.usage_analysis.query_table_access_patterns", return_value=[]):
        resp = client.post("/api/table-usage", json={"catalog": "my_cat"})
        assert resp.status_code == 200
