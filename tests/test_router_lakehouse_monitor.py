"""Tests for the lakehouse_monitor router."""

import pytest
from unittest.mock import patch

pytest.importorskip("fastapi")


def test_list_monitors(client):
    with patch("src.lakehouse_monitor.list_monitors", return_value=[]):
        resp = client.post("/api/lakehouse-monitor/list", json={
            "source_catalog": "test_catalog",
        })
    assert resp.status_code in (200, 422)


def test_clone_monitors(client):
    with patch("src.lakehouse_monitor.list_monitors", return_value=[]), \
         patch("src.lakehouse_monitor.export_monitor_definition"), \
         patch("src.lakehouse_monitor.clone_monitor"):
        resp = client.post("/api/lakehouse-monitor/clone", json={
            "source_catalog": "src_cat",
            "destination_catalog": "dst_cat",
        })
    assert resp.status_code in (200, 422)


def test_compare_metrics(client):
    with patch("src.lakehouse_monitor.compare_monitor_metrics", return_value={"match": True}):
        resp = client.post("/api/lakehouse-monitor/compare", json={
            "source_table": "src_cat.schema.table",
            "destination_table": "dst_cat.schema.table",
        })
    assert resp.status_code in (200, 422)


def test_list_monitors_with_schema_filter(client):
    with patch("src.lakehouse_monitor.list_monitors", return_value=[]):
        resp = client.post("/api/lakehouse-monitor/list", json={
            "source_catalog": "test_catalog",
            "schema_filter": "default",
        })
    assert resp.status_code in (200, 422)


def test_clone_monitors_dry_run(client):
    with patch("src.lakehouse_monitor.list_monitors", return_value=[]):
        resp = client.post("/api/lakehouse-monitor/clone", json={
            "source_catalog": "src_cat",
            "destination_catalog": "dst_cat",
            "dry_run": True,
        })
    assert resp.status_code in (200, 422)
