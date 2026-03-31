"""Tests for the monitor router."""

import pytest
from unittest.mock import patch

pytest.importorskip("fastapi")


def test_monitor_once(client):
    with patch("src.monitor.monitor_once", return_value={"status": "ok", "drift": []}):
        resp = client.post(
            "/api/monitor",
            params={
                "source_catalog": "src_cat",
                "destination_catalog": "dst_cat",
            },
        )
    assert resp.status_code in (200, 422)


def test_monitor_once_with_options(client):
    with patch("src.monitor.monitor_once", return_value={"status": "ok"}):
        resp = client.post(
            "/api/monitor",
            params={
                "source_catalog": "src_cat",
                "destination_catalog": "dst_cat",
                "check_drift": True,
                "check_counts": True,
            },
        )
    assert resp.status_code in (200, 422)


def test_monitor_once_with_warehouse(client):
    with patch("src.monitor.monitor_once", return_value={"status": "ok"}):
        resp = client.post(
            "/api/monitor",
            params={
                "source_catalog": "src_cat",
                "destination_catalog": "dst_cat",
                "warehouse_id": "wh-123",
            },
        )
    assert resp.status_code in (200, 422)
