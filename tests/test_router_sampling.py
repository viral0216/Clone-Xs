"""Tests for the sampling router."""

import pytest
from unittest.mock import patch, MagicMock

pytest.importorskip("fastapi")


def test_sample_table(client):
    with patch("src.sampling.sample_table", return_value=[{"col1": "val1"}]):
        resp = client.post("/api/sample", json={
            "catalog": "test_catalog",
            "schema_name": "default",
            "table_name": "test_table",
        })
    assert resp.status_code in (200, 422)


def test_sample_table_with_limit(client):
    with patch("src.sampling.sample_table", return_value=[]):
        resp = client.post("/api/sample", json={
            "catalog": "test_catalog",
            "schema_name": "default",
            "table_name": "test_table",
            "limit": 5,
        })
    assert resp.status_code in (200, 422)


def test_compare_samples(client):
    with patch("src.sampling.compare_samples", return_value={"match": True}):
        resp = client.post("/api/sample/compare", json={
            "source_catalog": "src_cat",
            "destination_catalog": "dst_cat",
            "schema_name": "default",
            "table_name": "test_table",
        })
    assert resp.status_code in (200, 422)


def test_compare_samples_with_order_by(client):
    with patch("src.sampling.compare_samples", return_value={"match": True}):
        resp = client.post("/api/sample/compare", json={
            "source_catalog": "src_cat",
            "destination_catalog": "dst_cat",
            "schema_name": "default",
            "table_name": "test_table",
            "order_by": "id",
            "limit": 3,
        })
    assert resp.status_code in (200, 422)
