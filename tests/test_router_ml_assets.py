"""Tests for the ml_assets router."""

import pytest
from unittest.mock import patch, MagicMock

pytest.importorskip("fastapi")


def test_list_ml_assets(client):
    with patch("src.clone_models.list_registered_models", return_value=[]), \
         patch("src.clone_feature_tables.list_feature_tables", return_value=[]), \
         patch("src.clone_vector_search.list_vector_indexes", return_value=[]), \
         patch("src.clone_serving_endpoints.list_serving_endpoints", return_value=[]):
        resp = client.post("/api/ml-assets/list", json={
            "source_catalog": "test_catalog",
        })
    assert resp.status_code in (200, 422)


def test_clone_ml_assets(client):
    with patch("src.clone_models.clone_all_models", return_value={"cloned": 0}):
        resp = client.post("/api/ml-assets/clone", json={
            "source_catalog": "src_cat",
            "destination_catalog": "dst_cat",
        })
    assert resp.status_code in (200, 422)


def test_list_models(client):
    with patch("src.clone_models.list_registered_models", return_value=[]):
        resp = client.post("/api/ml-assets/models/list", json={
            "source_catalog": "test_catalog",
        })
    assert resp.status_code in (200, 422)


def test_list_vector_indexes(client):
    with patch("src.clone_vector_search.list_vector_indexes", return_value=[]):
        resp = client.post("/api/ml-assets/vector-indexes/list", json={
            "source_catalog": "test_catalog",
        })
    assert resp.status_code in (200, 422)


def test_get_serving_endpoints(client):
    with patch("src.clone_serving_endpoints.list_serving_endpoints", return_value=[]):
        resp = client.get("/api/ml-assets/serving-endpoints")
    assert resp.status_code == 200


def test_export_serving_endpoint(client):
    with patch("src.clone_serving_endpoints.export_endpoint_config", return_value={"name": "ep1"}):
        resp = client.post("/api/ml-assets/serving-endpoints/export?name=ep1")
    assert resp.status_code in (200, 422)


def test_import_serving_endpoint(client):
    with patch("src.clone_serving_endpoints.import_endpoint_config", return_value={"status": "ok"}):
        resp = client.post("/api/ml-assets/serving-endpoints/import", json={
            "config": {"name": "ep1"},
        })
    assert resp.status_code in (200, 422)
