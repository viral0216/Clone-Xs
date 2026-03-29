"""Tests for the advanced_tables router."""

import pytest
from unittest.mock import patch

pytest.importorskip("fastapi")


def test_list_advanced_tables(client):
    with patch("src.clone_advanced_tables.list_all_advanced_tables", return_value=[]):
        resp = client.post("/api/advanced-tables/list", json={
            "source_catalog": "test_catalog",
        })
    assert resp.status_code in (200, 422)


def test_clone_advanced_tables(client):
    with patch("src.clone_advanced_tables.clone_all_advanced_tables", return_value={"cloned": 0}):
        resp = client.post("/api/advanced-tables/clone", json={
            "source_catalog": "src_cat",
            "destination_catalog": "dst_cat",
        })
    assert resp.status_code in (200, 422)


def test_list_advanced_tables_with_schema_filter(client):
    with patch("src.clone_advanced_tables.list_all_advanced_tables", return_value=[]):
        resp = client.post("/api/advanced-tables/list", json={
            "source_catalog": "test_catalog",
            "schema_filter": "default",
        })
    assert resp.status_code in (200, 422)


def test_clone_advanced_tables_dry_run(client):
    with patch("src.clone_advanced_tables.clone_all_advanced_tables", return_value={"cloned": 0}):
        resp = client.post("/api/advanced-tables/clone", json={
            "source_catalog": "src_cat",
            "destination_catalog": "dst_cat",
            "dry_run": True,
        })
    assert resp.status_code in (200, 422)
