"""Tests for the system-insights router — 11 endpoints."""

import pytest
from unittest.mock import patch

pytest.importorskip("fastapi")


def test_billing_usage(client):
    with patch("src.system_insights.query_billing_usage", return_value={"costs": []}):
        resp = client.post("/api/system-insights/billing", json={})
        assert resp.status_code == 200


def test_optimization_recs(client):
    with patch("src.system_insights.query_predictive_optimization", return_value={"recommendations": []}):
        resp = client.post("/api/system-insights/optimization", json={})
        assert resp.status_code == 200


def test_job_run_timeline(client):
    with patch("src.system_insights.query_job_run_timeline", return_value={"runs": []}):
        resp = client.post("/api/system-insights/jobs", json={})
        assert resp.status_code == 200


def test_system_summary(client):
    with patch("src.system_insights.get_system_insights_summary", return_value={"summary": {}}):
        resp = client.post("/api/system-insights/summary", json={})
        assert resp.status_code == 200


def test_warehouse_health(client):
    with patch("src.system_insights.query_warehouse_health", return_value={"warehouses": []}):
        resp = client.post("/api/system-insights/warehouses", json={})
        assert resp.status_code == 200


def test_cluster_health(client):
    with patch("src.system_insights.query_cluster_health", return_value={"clusters": []}):
        resp = client.post("/api/system-insights/clusters", json={})
        assert resp.status_code == 200


def test_pipeline_health(client):
    with patch("src.system_insights.query_dlt_pipeline_health", return_value={"pipelines": []}):
        resp = client.post("/api/system-insights/pipelines", json={})
        assert resp.status_code == 200


def test_query_performance(client):
    with patch("src.system_insights.query_query_performance", return_value={"queries": []}):
        resp = client.post("/api/system-insights/query-performance", json={})
        assert resp.status_code == 200


def test_metastore_summary(client):
    with patch("src.system_insights.query_metastore_summary", return_value={"metastore": {}}):
        resp = client.post("/api/system-insights/metastore", json={})
        assert resp.status_code == 200


def test_sql_alerts(client):
    with patch("src.system_insights.query_sql_alerts", return_value={"alerts": []}):
        resp = client.post("/api/system-insights/alerts", json={})
        assert resp.status_code == 200


def test_table_usage(client):
    with patch("src.system_insights.query_table_usage_summary", return_value={"tables": []}):
        resp = client.post("/api/system-insights/table-usage", json={"catalog": "my_catalog"})
        assert resp.status_code == 200
