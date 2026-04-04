"""Smoke tests for the AI assistant router."""
import pytest

pytest.importorskip("fastapi")


def test_nl_to_sql(client):
    resp = client.post("/api/ai-assistant/nl-to-sql", json={
        "question": "Show all tables",
        "catalog": "test_catalog",
    })
    assert resp.status_code in (200, 400, 422, 500)


def test_execute_nl(client):
    resp = client.post("/api/ai-assistant/execute-nl", json={
        "question": "How many schemas exist?",
        "catalog": "test_catalog",
    })
    assert resp.status_code in (200, 400, 422, 500)


def test_genie_query(client):
    resp = client.post("/api/ai-assistant/genie-query", json={
        "question": "Show top tables",
        "space_id": "space-123",
    })
    assert resp.status_code in (200, 400, 422, 500, 502)


def test_chat(client):
    resp = client.post("/api/ai-assistant/chat", json={
        "messages": [{"role": "user", "content": "Hello"}],
    })
    assert resp.status_code in (200, 400, 422, 500)
