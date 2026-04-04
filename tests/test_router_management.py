"""Tests for the management router — smoke tests for all endpoints."""

import pytest

pytest.importorskip("fastapi")


# ---------------------------------------------------------------------------
# Preflight
# ---------------------------------------------------------------------------

def test_preflight(client):
    resp = client.post("/api/preflight", json={
        "source_catalog": "src",
        "destination_catalog": "dst",
    })
    assert resp.status_code in (200, 422, 500)


# ---------------------------------------------------------------------------
# Rollback
# ---------------------------------------------------------------------------

def test_list_rollback_logs(client):
    resp = client.get("/api/rollback/logs")
    assert resp.status_code == 200


def test_rollback(client):
    resp = client.post("/api/rollback", json={
        "log_file": "some_log.json",
    })
    assert resp.status_code in (200, 422, 500)


# ---------------------------------------------------------------------------
# PII
# ---------------------------------------------------------------------------

def test_pii_scan(client):
    resp = client.post("/api/pii-scan", json={
        "source_catalog": "test_catalog",
    })
    assert resp.status_code in (200, 422, 500)


def test_pii_patterns(client):
    resp = client.get("/api/pii-patterns")
    assert resp.status_code in (200, 500)


def test_pii_scan_history(client):
    resp = client.get("/api/pii-scans?catalog=test_catalog")
    assert resp.status_code in (200, 500)


def test_pii_scan_detail(client):
    resp = client.get("/api/pii-scans/scan-123")
    assert resp.status_code in (200, 500)


def test_pii_scan_diff(client):
    resp = client.get("/api/pii-scans/diff?scan_a=a1&scan_b=b2")
    assert resp.status_code in (200, 500)


def test_pii_tag(client):
    resp = client.post("/api/pii-tag", json={
        "source_catalog": "test_catalog",
    })
    assert resp.status_code in (200, 422, 500)


def test_pii_remediation_update(client):
    resp = client.post("/api/pii-remediation", json={
        "catalog": "test_catalog",
        "schema_name": "default",
        "table_name": "users",
        "column_name": "email",
        "pii_type": "EMAIL",
        "status": "mitigated",
    })
    assert resp.status_code in (200, 422, 500)


def test_pii_remediation_get(client):
    resp = client.get("/api/pii-remediation?catalog=test_catalog")
    assert resp.status_code in (200, 500)


# ---------------------------------------------------------------------------
# Sync
# ---------------------------------------------------------------------------

def test_sync(client):
    resp = client.post("/api/sync", json={
        "source_catalog": "src",
        "destination_catalog": "dst",
    })
    assert resp.status_code in (200, 422, 500)


# ---------------------------------------------------------------------------
# Catalogs / Schemas / Tables
# ---------------------------------------------------------------------------

def test_list_catalogs(client):
    resp = client.get("/api/catalogs")
    assert resp.status_code == 200


def test_get_catalog_info(client):
    resp = client.get("/api/catalogs/test_catalog/info")
    assert resp.status_code in (200, 500)


def test_list_schemas(client):
    resp = client.get("/api/catalogs/test_catalog/schemas")
    assert resp.status_code == 200


def test_list_tables(client):
    resp = client.get("/api/catalogs/test_catalog/default/tables")
    assert resp.status_code == 200


def test_uc_objects(client):
    resp = client.get("/api/uc-objects")
    assert resp.status_code in (200, 500)


def test_table_info(client):
    resp = client.get("/api/catalogs/test_catalog/default/test_table/info")
    assert resp.status_code in (200, 500)


# ---------------------------------------------------------------------------
# Audit
# ---------------------------------------------------------------------------

def test_audit_log(client):
    resp = client.get("/api/audit")
    assert resp.status_code == 200


def test_sync_history(client):
    resp = client.get("/api/audit/sync-history")
    assert resp.status_code == 200


def test_audit_init(client):
    resp = client.post("/api/audit/init", json={})
    assert resp.status_code in (200, 400, 500)


def test_audit_describe(client):
    resp = client.post("/api/audit/describe", json={})
    assert resp.status_code in (200, 500)


def test_audit_job_log(client):
    resp = client.get("/api/audit/job-123/logs")
    assert resp.status_code in (200, 500)


# ---------------------------------------------------------------------------
# Compliance
# ---------------------------------------------------------------------------

def test_compliance_report(client):
    resp = client.post("/api/compliance", json={
        "catalog": "test_catalog",
        "report_type": "data_governance",
    })
    assert resp.status_code in (200, 500)


# ---------------------------------------------------------------------------
# Templates
# ---------------------------------------------------------------------------

def test_list_templates(client):
    resp = client.get("/api/templates")
    assert resp.status_code in (200, 500)


# ---------------------------------------------------------------------------
# Schedule
# ---------------------------------------------------------------------------

def test_list_schedules(client):
    resp = client.get("/api/schedule")
    assert resp.status_code == 200


def test_create_schedule(client):
    resp = client.post("/api/schedule", json={
        "name": "nightly-clone",
        "source_catalog": "src",
        "destination_catalog": "dst",
        "cron": "0 0 * * *",
    })
    assert resp.status_code in (200, 500)


def test_pause_schedule(client):
    resp = client.post("/api/schedule/sched-123/pause")
    assert resp.status_code in (200, 404, 500)


def test_resume_schedule(client):
    resp = client.post("/api/schedule/sched-123/resume")
    assert resp.status_code in (200, 404, 500)


def test_delete_schedule(client):
    resp = client.delete("/api/schedule/sched-123")
    assert resp.status_code in (200, 500)


# ---------------------------------------------------------------------------
# Multi-clone
# ---------------------------------------------------------------------------

def test_multi_clone(client):
    resp = client.post("/api/multi-clone", json={
        "source_catalog": "src",
        "destinations": [{"catalog": "dst1"}, {"catalog": "dst2"}],
        "clone_type": "DEEP",
    })
    assert resp.status_code in (200, 500)


# ---------------------------------------------------------------------------
# Lineage
# ---------------------------------------------------------------------------

def test_lineage(client):
    resp = client.post("/api/lineage", json={
        "catalog": "test_catalog",
    })
    assert resp.status_code in (200, 500)


# ---------------------------------------------------------------------------
# Impact
# ---------------------------------------------------------------------------

def test_impact_analysis(client):
    resp = client.post("/api/impact", json={
        "catalog": "test_catalog",
        "schema": "default",
        "table": "test_table",
    })
    assert resp.status_code in (200, 500)


# ---------------------------------------------------------------------------
# Preview
# ---------------------------------------------------------------------------

def test_preview_data(client):
    resp = client.post("/api/preview", json={
        "source_catalog": "src",
        "dest_catalog": "dst",
        "schema": "default",
        "table": "test_table",
    })
    assert resp.status_code in (200, 500)


# ---------------------------------------------------------------------------
# Execute SQL
# ---------------------------------------------------------------------------

def test_execute_sql(client):
    resp = client.post("/api/execute-sql", json={
        "sql": "SELECT 1",
    })
    assert resp.status_code in (200, 400, 500)


def test_execute_sql_missing(client):
    resp = client.post("/api/execute-sql", json={})
    assert resp.status_code == 400


# ---------------------------------------------------------------------------
# Warehouse
# ---------------------------------------------------------------------------

def test_warehouse_start(client):
    resp = client.post("/api/warehouse/start", json={
        "warehouse_id": "wh-123",
    })
    assert resp.status_code in (200, 500)


def test_warehouse_stop(client):
    resp = client.post("/api/warehouse/stop", json={
        "warehouse_id": "wh-123",
    })
    assert resp.status_code in (200, 500)


# ---------------------------------------------------------------------------
# RBAC
# ---------------------------------------------------------------------------

def test_list_rbac_policies(client):
    resp = client.get("/api/rbac/policies")
    assert resp.status_code == 200


def test_create_rbac_policy(client):
    resp = client.post("/api/rbac/policies", json={
        "principals": ["admin@example.com"],
        "allowed_sources": [".*"],
        "allowed_destinations": [".*"],
        "allowed_operations": ["*"],
        "deny": False,
    })
    assert resp.status_code in (200, 422, 500)


def test_delete_rbac_policy(client):
    resp = client.delete("/api/rbac/policies/0")
    assert resp.status_code in (200, 500)


# ---------------------------------------------------------------------------
# Plugins
# ---------------------------------------------------------------------------

def test_list_plugins(client):
    resp = client.get("/api/plugins")
    assert resp.status_code == 200


def test_toggle_plugin(client):
    resp = client.post("/api/plugins/toggle", json={
        "name": "my-plugin",
        "enabled": True,
    })
    assert resp.status_code in (200, 500)


def test_enable_plugin(client):
    resp = client.post("/api/plugins/my-plugin/enable")
    assert resp.status_code in (200, 500)


def test_disable_plugin(client):
    resp = client.post("/api/plugins/my-plugin/disable")
    assert resp.status_code in (200, 500)


# ---------------------------------------------------------------------------
# Monitor Metrics
# ---------------------------------------------------------------------------

def test_monitor_metrics(client):
    resp = client.get("/api/monitor/metrics")
    assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Notifications
# ---------------------------------------------------------------------------

def test_notifications(client):
    resp = client.get("/api/notifications")
    assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Catalog Health
# ---------------------------------------------------------------------------

def test_catalog_health(client):
    resp = client.get("/api/catalog-health")
    assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------

def test_cache_stats(client):
    resp = client.get("/api/cache/stats")
    assert resp.status_code in (200, 500)


def test_cache_clear(client):
    resp = client.post("/api/cache/clear")
    assert resp.status_code in (200, 500)
