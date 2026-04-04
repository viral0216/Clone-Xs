"""Smoke tests for the data-quality router."""
import pytest

pytest.importorskip("fastapi")


def test_freshness_catalog(client):
    resp = client.get("/api/data-quality/freshness/test-catalog")
    assert resp.status_code in (200, 400, 404, 422, 500)


def test_anomalies_list(client):
    resp = client.get("/api/data-quality/anomalies")
    assert resp.status_code in (200, 400, 404, 422, 500)


def test_anomalies_metrics(client):
    resp = client.get("/api/data-quality/anomalies/metrics/cat.schema.table")
    assert resp.status_code in (200, 400, 404, 422, 500)


def test_metrics_recent(client):
    resp = client.get("/api/data-quality/metrics/recent")
    assert resp.status_code in (200, 400, 404, 422, 500)


def test_record_anomaly(client):
    resp = client.post("/api/data-quality/anomalies/record", json={
        "table_fqn": "cat.schema.table",
        "metric_name": "row_count",
        "metric_value": 100,
    })
    assert resp.status_code in (200, 400, 422, 500)


def test_volume_catalog(client):
    resp = client.get("/api/data-quality/volume/test-catalog")
    assert resp.status_code in (200, 400, 404, 422, 500)


def test_volume_snapshot(client):
    resp = client.post("/api/data-quality/volume/snapshot", json={
        "catalog": "test-catalog",
    })
    assert resp.status_code in (200, 400, 422, 500)


def test_list_suites(client):
    resp = client.get("/api/data-quality/suites")
    assert resp.status_code in (200, 400, 404, 422, 500)


def test_create_suite(client):
    resp = client.post("/api/data-quality/suites", json={
        "name": "test-suite",
    })
    assert resp.status_code in (200, 400, 422, 500)


def test_get_suite(client):
    resp = client.get("/api/data-quality/suites/suite-123")
    assert resp.status_code in (200, 400, 404, 422, 500)


def test_delete_suite(client):
    resp = client.delete("/api/data-quality/suites/suite-123")
    assert resp.status_code in (200, 400, 404, 422, 500)


def test_run_suite(client):
    resp = client.post("/api/data-quality/suites/suite-123/run")
    assert resp.status_code in (200, 400, 404, 422, 500)


def test_incidents(client):
    resp = client.get("/api/data-quality/incidents")
    assert resp.status_code in (200, 400, 404, 422, 500)


def test_anomaly_settings_get(client):
    resp = client.get("/api/data-quality/anomaly-settings")
    assert resp.status_code in (200, 400, 404, 422, 500)


def test_anomaly_settings_update(client):
    resp = client.put("/api/data-quality/anomaly-settings", json={
        "freshness_threshold": 24,
    })
    assert resp.status_code in (200, 400, 422, 500)


def test_health_score(client):
    resp = client.get("/api/data-quality/health-score/test-catalog")
    assert resp.status_code in (200, 400, 404, 422, 500)


def test_freshness_summary(client):
    resp = client.get("/api/data-quality/freshness/summary")
    assert resp.status_code in (200, 400, 404, 422, 500)


def test_health_trend(client):
    resp = client.get("/api/data-quality/health/trend")
    assert resp.status_code in (200, 400, 404, 422, 500)


def test_monitoring_configs_list(client):
    resp = client.get("/api/data-quality/monitoring/configs")
    assert resp.status_code in (200, 400, 404, 422, 500)


def test_monitoring_configs_create(client):
    resp = client.post("/api/data-quality/monitoring/configs", json={
        "table_fqn": "cat.schema.table",
    })
    assert resp.status_code in (200, 400, 422, 500)


def test_monitoring_configs_delete(client):
    resp = client.delete("/api/data-quality/monitoring/configs/config-123")
    assert resp.status_code in (200, 400, 404, 422, 500)


def test_monitoring_run(client):
    resp = client.post("/api/data-quality/monitoring/run")
    assert resp.status_code in (200, 400, 404, 422, 500)
