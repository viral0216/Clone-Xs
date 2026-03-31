"""Tests for the incremental router."""

import pytest
from unittest.mock import patch

pytest.importorskip("fastapi")


def test_check_changes(client):
    with patch("src.incremental_sync.get_tables_needing_sync", return_value=[]):
        resp = client.post("/api/incremental/check", json={
            "source_catalog": "src_cat",
            "destination_catalog": "dst_cat",
            "schema_name": "default",
        })
    assert resp.status_code in (200, 422)


def test_start_incremental_sync(client):
    resp = client.post("/api/incremental/sync", json={
        "source_catalog": "src_cat",
        "destination_catalog": "dst_cat",
        "schema_name": "default",
    })
    assert resp.status_code in (200, 422)


def test_check_cdf_status(client):
    with patch("src.incremental_sync.check_cdf_enabled", return_value=False):
        resp = client.post("/api/incremental/cdf-check", json={
            "source_catalog": "src_cat",
            "schema_name": "default",
            "table_name": "test_table",
        })
    assert resp.status_code in (200, 422)


def test_check_changes_with_warehouse_id(client):
    with patch("src.incremental_sync.get_tables_needing_sync", return_value=[]):
        resp = client.post("/api/incremental/check", json={
            "source_catalog": "src_cat",
            "destination_catalog": "dst_cat",
            "schema_name": "default",
            "warehouse_id": "wh-456",
        })
    assert resp.status_code in (200, 422)


def test_start_incremental_sync_dry_run(client):
    resp = client.post("/api/incremental/sync", json={
        "source_catalog": "src_cat",
        "destination_catalog": "dst_cat",
        "schema_name": "default",
        "dry_run": True,
    })
    assert resp.status_code in (200, 422)
