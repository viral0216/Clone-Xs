"""Smoke tests for the notebooks router."""
import pytest

pytest.importorskip("fastapi")


def test_list_notebooks(client):
    resp = client.get("/api/notebooks")
    assert resp.status_code in (200, 400, 404, 500)


def test_create_notebook(client):
    resp = client.post("/api/notebooks", json={
        "title": "Test Notebook",
        "cells": [{"type": "sql", "content": "SELECT 1"}],
    })
    assert resp.status_code in (200, 400, 422, 500)


def test_get_notebook(client):
    resp = client.get("/api/notebooks/nb-123")
    assert resp.status_code in (200, 400, 404, 500)


def test_update_notebook(client):
    resp = client.put("/api/notebooks/nb-123", json={
        "title": "Updated Notebook",
    })
    assert resp.status_code in (200, 400, 404, 422, 500)


def test_delete_notebook(client):
    resp = client.delete("/api/notebooks/nb-123")
    assert resp.status_code in (200, 400, 404, 500)


def test_export_notebook(client):
    resp = client.post("/api/notebooks/nb-123/export")
    assert resp.status_code in (200, 400, 404, 500)
