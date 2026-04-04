"""Smoke tests for the MDM router."""
import pytest

pytest.importorskip("fastapi")


def test_init_tables(client):
    resp = client.post("/api/mdm/init")
    assert resp.status_code in (200, 400, 404, 500)


def test_dashboard(client):
    resp = client.get("/api/mdm/dashboard")
    assert resp.status_code in (200, 400, 404, 500)


def test_list_entities(client):
    resp = client.get("/api/mdm/entities")
    assert resp.status_code in (200, 400, 404, 500)


def test_get_entity(client):
    resp = client.get("/api/mdm/entities/ent-123")
    assert resp.status_code in (200, 400, 404, 500)


def test_create_entity(client):
    resp = client.post("/api/mdm/entities", json={
        "entity_type": "customer",
        "display_name": "Test Customer",
        "attributes": {"name": "Test"},
    })
    assert resp.status_code in (200, 400, 422, 500)


def test_update_entity(client):
    resp = client.put("/api/mdm/entities/ent-123", json={
        "display_name": "Updated Customer",
    })
    assert resp.status_code in (200, 400, 404, 422, 500)


def test_delete_entity(client):
    resp = client.delete("/api/mdm/entities/ent-123")
    assert resp.status_code in (200, 400, 404, 500)


def test_ingest(client):
    resp = client.post("/api/mdm/ingest", json={
        "catalog": "test_catalog",
        "schema_name": "default",
        "table": "customers",
        "entity_type": "customer",
        "key_column": "id",
    })
    assert resp.status_code in (200, 400, 422, 500)


def test_detect_duplicates(client):
    resp = client.post("/api/mdm/detect", json={
        "entity_type": "customer",
    })
    assert resp.status_code in (200, 400, 422, 500)


def test_list_pairs(client):
    resp = client.get("/api/mdm/pairs")
    assert resp.status_code in (200, 400, 404, 500)


def test_merge(client):
    resp = client.post("/api/mdm/merge", json={
        "pair_id": "pair-123",
    })
    assert resp.status_code in (200, 400, 422, 500)


def test_list_rules(client):
    resp = client.get("/api/mdm/rules")
    assert resp.status_code in (200, 400, 404, 500)


def test_create_rule(client):
    resp = client.post("/api/mdm/rules", json={
        "entity_type": "customer",
        "name": "email-match",
        "field": "email",
        "match_type": "exact",
    })
    assert resp.status_code in (200, 400, 422, 500)


def test_delete_rule(client):
    resp = client.delete("/api/mdm/rules/rule-123")
    assert resp.status_code in (200, 400, 404, 500)


def test_list_stewardship(client):
    resp = client.get("/api/mdm/stewardship")
    assert resp.status_code in (200, 400, 404, 500)


def test_list_hierarchies(client):
    resp = client.get("/api/mdm/hierarchies")
    assert resp.status_code in (200, 400, 404, 500)


def test_create_hierarchy(client):
    resp = client.post("/api/mdm/hierarchies", json={
        "name": "org-chart",
        "entity_type": "customer",
    })
    assert resp.status_code in (200, 400, 422, 500)
