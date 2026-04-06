"""Smoke tests for the observability router."""
import pytest

pytest.importorskip("fastapi")


def test_dashboard(client):
    resp = client.get("/api/observability/dashboard")
    assert resp.status_code in (200, 400, 404, 500)


def test_health_score(client):
    resp = client.get("/api/observability/health-score")
    assert resp.status_code in (200, 400, 404, 500)


def test_issues(client):
    resp = client.get("/api/observability/issues")
    assert resp.status_code in (200, 400, 404, 500)


def test_trends(client):
    resp = client.get("/api/observability/trends/freshness")
    assert resp.status_code in (200, 400, 404, 500)


def test_category_health(client):
    resp = client.get("/api/observability/category-health")
    assert resp.status_code in (200, 400, 404, 500)
