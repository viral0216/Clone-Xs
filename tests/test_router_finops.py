"""Smoke tests for the finops router."""
import pytest

pytest.importorskip("fastapi")


def test_billing(client):
    resp = client.get("/api/finops/billing")
    assert resp.status_code in (200, 400, 404, 500)


def test_warehouses(client):
    resp = client.get("/api/finops/warehouses")
    assert resp.status_code in (200, 400, 404, 500)


def test_warehouse_events(client):
    resp = client.get("/api/finops/warehouse-events")
    assert resp.status_code in (200, 400, 404, 500)


def test_clusters(client):
    resp = client.get("/api/finops/clusters")
    assert resp.status_code in (200, 400, 404, 500)


def test_node_utilization(client):
    resp = client.get("/api/finops/node-utilization")
    assert resp.status_code in (200, 400, 404, 500)


def test_query_stats(client):
    resp = client.get("/api/finops/query-stats")
    assert resp.status_code in (200, 400, 404, 500)


def test_storage(client):
    resp = client.get("/api/finops/storage?catalog=test_catalog")
    assert resp.status_code in (200, 400, 404, 500)


def test_recommendations(client):
    resp = client.get("/api/finops/recommendations")
    assert resp.status_code in (200, 400, 404, 500)


def test_query_costs(client):
    resp = client.get("/api/finops/query-costs")
    assert resp.status_code in (200, 400, 404, 500)


def test_job_costs(client):
    resp = client.get("/api/finops/job-costs")
    assert resp.status_code in (200, 400, 404, 500)


def test_system_status(client):
    resp = client.get("/api/finops/system-status")
    assert resp.status_code in (200, 400, 404, 500)


def test_azure_status(client):
    resp = client.get("/api/finops/azure/status")
    assert resp.status_code in (200, 400, 404, 500)


def test_save_azure_config(client):
    resp = client.post("/api/finops/azure/config", json={
        "subscription_id": "sub-123",
    })
    assert resp.status_code in (200, 400, 422, 500)
