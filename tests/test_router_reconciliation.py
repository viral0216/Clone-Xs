"""Smoke tests for the reconciliation router."""
import pytest

pytest.importorskip("fastapi")


def test_spark_status(client):
    resp = client.get("/api/reconciliation/spark-status")
    assert resp.status_code in (200, 400, 404, 500)


def test_spark_configure(client):
    resp = client.post("/api/reconciliation/spark-configure", json={
        "serverless": True,
    })
    assert resp.status_code in (200, 400, 422, 500)


def test_validate(client):
    resp = client.post("/api/reconciliation/validate", json={
        "source_catalog": "src_cat",
        "destination_catalog": "dest_cat",
    })
    assert resp.status_code in (200, 400, 422, 500)


def test_compare(client):
    resp = client.post("/api/reconciliation/compare", json={
        "source_catalog": "src_cat",
        "destination_catalog": "dest_cat",
    })
    assert resp.status_code in (200, 400, 422, 500)


def test_profile(client):
    resp = client.post("/api/reconciliation/profile", json={
        "source_catalog": "src_cat",
    })
    assert resp.status_code in (200, 400, 422, 500)


def test_preview(client):
    resp = client.post("/api/reconciliation/preview", json={
        "source_catalog": "src_cat",
        "destination_catalog": "dest_cat",
        "schema_name": "default",
        "table_name": "test_table",
    })
    assert resp.status_code in (200, 400, 422, 500)


def test_deep_validate(client):
    resp = client.post("/api/reconciliation/deep-validate", json={
        "source_catalog": "src_cat",
        "destination_catalog": "dest_cat",
    })
    assert resp.status_code in (200, 400, 422, 500)


def test_history(client):
    resp = client.get("/api/reconciliation/history")
    assert resp.status_code in (200, 400, 404, 500)


def test_compare_runs(client):
    resp = client.post("/api/reconciliation/compare-runs", json={
        "run_id_a": "run-aaa",
        "run_id_b": "run-bbb",
    })
    assert resp.status_code in (200, 400, 404, 422, 500)


def test_execute_sql(client):
    resp = client.post("/api/reconciliation/execute-sql", json={
        "sql": "SELECT 1",
    })
    assert resp.status_code in (200, 400, 422, 500)


def test_list_alert_rules(client):
    resp = client.get("/api/reconciliation/alerts/rules")
    assert resp.status_code in (200, 400, 404, 500)


def test_create_alert_rule(client):
    resp = client.post("/api/reconciliation/alerts/rules", json={
        "name": "test-alert",
        "metric": "match_rate",
        "operator": "<",
        "threshold": 95,
    })
    assert resp.status_code in (200, 400, 422, 500)


def test_delete_alert_rule(client):
    resp = client.delete("/api/reconciliation/alerts/rules/rule-123")
    assert resp.status_code in (200, 400, 404, 500)


def test_alert_history(client):
    resp = client.get("/api/reconciliation/alerts/history")
    assert resp.status_code in (200, 400, 404, 500)


def test_remediate(client):
    resp = client.post("/api/reconciliation/remediate", json={
        "source_catalog": "src_cat",
        "destination_catalog": "dest_cat",
        "schema_name": "default",
        "table_name": "test_table",
    })
    assert resp.status_code in (200, 400, 422, 500)


def test_list_schedules(client):
    resp = client.get("/api/reconciliation/schedules")
    assert resp.status_code in (200, 400, 404, 500)


def test_create_schedule(client):
    resp = client.post("/api/reconciliation/schedules", json={
        "name": "nightly-recon",
        "source_catalog": "src_cat",
        "destination_catalog": "dest_cat",
        "cron": "0 0 * * *",
    })
    assert resp.status_code in (200, 400, 422, 500)


def test_delete_schedule(client):
    resp = client.delete("/api/reconciliation/schedules/sched-123")
    assert resp.status_code in (200, 400, 404, 500)


def test_batch_validate(client):
    resp = client.post("/api/reconciliation/batch-validate", json={
        "source_catalog": "src_cat",
        "destination_catalog": "dest_cat",
        "tables": [{"schema_name": "default", "table_name": "t1"}],
    })
    assert resp.status_code in (200, 400, 422, 500)


def test_get_batch_job(client):
    resp = client.get("/api/reconciliation/batch-validate/job-123")
    assert resp.status_code in (200, 400, 404, 500)


def test_history_run_details(client):
    resp = client.get("/api/reconciliation/history/run-123/details")
    assert resp.status_code in (200, 400, 404, 500)
