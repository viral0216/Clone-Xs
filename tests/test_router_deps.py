"""Tests for the deps (dependency analysis) router."""

import pytest
from unittest.mock import patch, MagicMock

pytest.importorskip("fastapi")


def test_list_functions(client):
    with patch("src.client.execute_sql", return_value=[]):
        resp = client.get("/api/functions/test_catalog")
    assert resp.status_code == 200


def test_list_functions_invalid_catalog(client):
    resp = client.get("/api/functions/invalid-catalog!")
    assert resp.status_code == 400


def test_list_views(client):
    with patch("src.client.execute_sql", return_value=[]):
        resp = client.get("/api/views/test_catalog")
    assert resp.status_code == 200


def test_list_views_invalid_catalog(client):
    resp = client.get("/api/views/invalid-catalog!")
    assert resp.status_code == 400


def test_view_dependencies(client):
    with patch("src.dependencies.get_view_dependencies", return_value=[]):
        resp = client.post("/api/dependencies/views", json={
            "catalog": "test_catalog",
            "schema_name": "default",
        })
    assert resp.status_code in (200, 422)


def test_function_dependencies(client):
    with patch("src.dependencies.get_function_dependencies", return_value=[]):
        resp = client.post("/api/dependencies/functions", json={
            "catalog": "test_catalog",
            "schema_name": "default",
        })
    assert resp.status_code in (200, 422)


def test_creation_order(client):
    with patch("src.dependencies.get_ordered_views", return_value=[]):
        resp = client.post("/api/dependencies/order", json={
            "catalog": "test_catalog",
            "schema_name": "default",
        })
    assert resp.status_code in (200, 422)


def test_view_dependencies_with_warehouse(client):
    with patch("src.dependencies.get_view_dependencies", return_value=[]):
        resp = client.post("/api/dependencies/views", json={
            "catalog": "test_catalog",
            "schema_name": "default",
            "warehouse_id": "wh-789",
        })
    assert resp.status_code in (200, 422)
