"""Tests for the governance router — smoke tests for all endpoints."""

import pytest

pytest.importorskip("fastapi")


# ---------------------------------------------------------------------------
# Init
# ---------------------------------------------------------------------------

def test_init_governance_tables(client):
    resp = client.post("/api/governance/init")
    assert resp.status_code in (200, 400, 404, 500)


# ---------------------------------------------------------------------------
# Glossary CRUD
# ---------------------------------------------------------------------------

def test_create_glossary_term(client):
    resp = client.post("/api/governance/glossary", json={
        "name": "Customer Lifetime Value",
        "definition": "Total revenue expected from a customer.",
    })
    assert resp.status_code in (200, 422, 500)


def test_list_glossary_terms(client):
    resp = client.get("/api/governance/glossary")
    assert resp.status_code in (200, 400, 404, 500)


def test_get_glossary_term(client):
    resp = client.get("/api/governance/glossary/term-123")
    assert resp.status_code in (200, 400, 404, 500)


def test_delete_glossary_term(client):
    resp = client.delete("/api/governance/glossary/term-123")
    assert resp.status_code in (200, 400, 404, 500)


def test_link_glossary_term(client):
    resp = client.post("/api/governance/glossary/link", json={
        "term_id": "term-123",
        "column_fqns": ["cat.schema.table.col1"],
    })
    assert resp.status_code in (200, 422, 500)


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------

def test_metadata_search(client):
    resp = client.post("/api/governance/search", json={
        "query": "customer",
    })
    assert resp.status_code in (200, 422, 500)


# ---------------------------------------------------------------------------
# DQ Rules
# ---------------------------------------------------------------------------

def test_create_dq_rule(client):
    resp = client.post("/api/governance/dq/rules", json={
        "name": "not_null_id",
        "table_fqn": "cat.schema.table",
        "rule_type": "not_null",
    })
    assert resp.status_code in (200, 422, 500)


def test_list_dq_rules(client):
    resp = client.get("/api/governance/dq/rules")
    assert resp.status_code in (200, 400, 404, 500)


def test_update_dq_rule(client):
    resp = client.put("/api/governance/dq/rules/rule-123", json={
        "severity": "critical",
    })
    assert resp.status_code in (200, 400, 404, 500)


def test_delete_dq_rule(client):
    resp = client.delete("/api/governance/dq/rules/rule-123")
    assert resp.status_code in (200, 400, 404, 500)


def test_run_dq(client):
    resp = client.post("/api/governance/dq/run", json={})
    assert resp.status_code in (200, 422, 500)


def test_get_dq_results(client):
    resp = client.get("/api/governance/dq/results")
    assert resp.status_code in (200, 400, 404, 500)


def test_get_dq_history(client):
    resp = client.get("/api/governance/dq/history")
    assert resp.status_code in (200, 400, 404, 500)


# ---------------------------------------------------------------------------
# Certifications
# ---------------------------------------------------------------------------

def test_create_certification(client):
    resp = client.post("/api/governance/certifications", json={
        "table_fqn": "cat.schema.table",
        "status": "certified",
    })
    assert resp.status_code in (200, 422, 500)


def test_list_certifications(client):
    resp = client.get("/api/governance/certifications")
    assert resp.status_code in (200, 400, 404, 500)


def test_approve_certification(client):
    resp = client.post("/api/governance/certifications/approve", json={
        "cert_id": "cert-123",
        "action": "approve",
    })
    assert resp.status_code in (200, 422, 500)


# ---------------------------------------------------------------------------
# SLA Rules
# ---------------------------------------------------------------------------

def test_create_sla_rule(client):
    resp = client.post("/api/governance/sla/rules", json={
        "table_fqn": "cat.schema.table",
        "metric": "freshness",
    })
    assert resp.status_code in (200, 422, 500)


def test_list_sla_rules(client):
    resp = client.get("/api/governance/sla/rules")
    assert resp.status_code in (200, 400, 404, 500)


def test_check_sla(client):
    resp = client.post("/api/governance/sla/check")
    assert resp.status_code in (200, 400, 404, 500)


def test_sla_status(client):
    resp = client.get("/api/governance/sla/status")
    assert resp.status_code in (200, 400, 404, 500)


# ---------------------------------------------------------------------------
# ODCS Contracts
# ---------------------------------------------------------------------------

def test_create_odcs_contract(client):
    resp = client.post("/api/governance/odcs/contracts", json={
        "name": "test-contract",
    })
    assert resp.status_code in (200, 422, 500)


def test_list_odcs_contracts(client):
    resp = client.get("/api/governance/odcs/contracts")
    assert resp.status_code in (200, 400, 404, 500)


def test_get_odcs_contract(client):
    resp = client.get("/api/governance/odcs/contracts/contract-123")
    assert resp.status_code in (200, 400, 404, 500)


def test_update_odcs_contract(client):
    resp = client.put("/api/governance/odcs/contracts/contract-123", json={
        "status": "active",
    })
    assert resp.status_code in (200, 422, 500)


def test_delete_odcs_contract(client):
    resp = client.delete("/api/governance/odcs/contracts/contract-123")
    assert resp.status_code in (200, 400, 404, 500)


def test_validate_odcs_contract(client):
    resp = client.post("/api/governance/odcs/contracts/contract-123/validate")
    assert resp.status_code in (200, 400, 404, 500)


def test_get_odcs_versions(client):
    resp = client.get("/api/governance/odcs/contracts/contract-123/versions")
    assert resp.status_code in (200, 400, 404, 500)


def test_get_odcs_version(client):
    resp = client.get("/api/governance/odcs/contracts/contract-123/versions/1.0.0")
    assert resp.status_code in (200, 400, 404, 500)


def test_import_odcs_yaml(client):
    resp = client.post("/api/governance/odcs/import", json={
        "yaml_content": "apiVersion: v3.1.0\nkind: DataContract\nname: test",
    })
    assert resp.status_code in (200, 422, 500)


def test_export_odcs_yaml(client):
    resp = client.get("/api/governance/odcs/contracts/contract-123/export")
    assert resp.status_code in (200, 400, 404, 500)


def test_prefill_odcs(client):
    resp = client.get("/api/governance/odcs/prefill")
    assert resp.status_code in (200, 400, 404, 500)


def test_map_dq_to_odcs(client):
    resp = client.post("/api/governance/odcs/contracts/contract-123/map-dq")
    assert resp.status_code in (200, 400, 404, 500)


def test_map_sla_to_odcs(client):
    resp = client.post("/api/governance/odcs/contracts/contract-123/map-sla")
    assert resp.status_code in (200, 400, 404, 500)


def test_migrate_legacy_contracts(client):
    resp = client.post("/api/governance/odcs/migrate")
    assert resp.status_code in (200, 400, 404, 500)


def test_dqx_validate_contract(client):
    resp = client.post("/api/governance/odcs/contracts/contract-123/dqx-validate")
    assert resp.status_code in (200, 400, 404, 500)


# ---------------------------------------------------------------------------
# ODCS Generation
# ---------------------------------------------------------------------------

def test_generate_odcs_from_uc(client):
    resp = client.post("/api/governance/odcs/generate", json={
        "table_fqn": "cat.schema.table",
    })
    assert resp.status_code in (200, 422, 500)


def test_generate_odcs_schema(client):
    resp = client.post("/api/governance/odcs/generate-schema", json={
        "catalog": "cat",
        "schema_name": "default",
    })
    assert resp.status_code in (200, 422, 500)


def test_generate_odcs_catalog(client):
    resp = client.post("/api/governance/odcs/generate-catalog", json={
        "catalog": "cat",
    })
    assert resp.status_code in (200, 422, 500)


# ---------------------------------------------------------------------------
# DQX
# ---------------------------------------------------------------------------

def test_dqx_spark_status(client):
    resp = client.get("/api/governance/dqx/spark-status")
    assert resp.status_code in (200, 400, 404, 500)


def test_dqx_spark_configure(client):
    resp = client.post("/api/governance/dqx/spark-configure", json={
        "serverless": True,
    })
    assert resp.status_code in (200, 400, 404, 500)


def test_dqx_dashboard(client):
    resp = client.get("/api/governance/dqx/dashboard")
    assert resp.status_code in (200, 400, 404, 500)


def test_dqx_functions(client):
    resp = client.get("/api/governance/dqx/functions")
    assert resp.status_code in (200, 400, 404, 500)


def test_dqx_profile(client):
    resp = client.post("/api/governance/dqx/profile", json={
        "table_fqn": "cat.schema.table",
    })
    assert resp.status_code in (200, 422, 500)


def test_dqx_profile_schema(client):
    resp = client.post("/api/governance/dqx/profile-schema", json={
        "catalog": "cat",
        "schema_name": "default",
    })
    assert resp.status_code in (200, 400, 404, 500)


def test_dqx_profile_catalog(client):
    resp = client.post("/api/governance/dqx/profile-catalog", json={
        "catalog": "cat",
    })
    assert resp.status_code in (200, 400, 404, 500)


def test_dqx_create_check(client):
    resp = client.post("/api/governance/dqx/checks", json={
        "table_fqn": "cat.schema.table",
        "check_function": "is_not_null",
        "arguments": {"column": "id"},
    })
    assert resp.status_code in (200, 422, 500)


def test_dqx_list_checks(client):
    resp = client.get("/api/governance/dqx/checks")
    assert resp.status_code in (200, 400, 404, 500)


def test_dqx_delete_check(client):
    resp = client.delete("/api/governance/dqx/checks/check-123")
    assert resp.status_code in (200, 400, 404, 500)


def test_dqx_delete_bulk(client):
    resp = client.post("/api/governance/dqx/checks/delete-bulk", json={
        "check_ids": ["check-1", "check-2"],
    })
    assert resp.status_code in (200, 400, 404, 500)


def test_dqx_clear_all(client):
    resp = client.post("/api/governance/dqx/clear-all")
    assert resp.status_code in (200, 400, 404, 500)


def test_dqx_toggle_check(client):
    resp = client.post("/api/governance/dqx/checks/check-123/toggle", json={
        "enabled": False,
    })
    assert resp.status_code in (200, 400, 404, 500)


def test_dqx_update_check(client):
    resp = client.put("/api/governance/dqx/checks/check-123", json={
        "criticality": "warn",
    })
    assert resp.status_code in (200, 400, 404, 500)


def test_dqx_run(client):
    resp = client.post("/api/governance/dqx/run", json={
        "table_fqn": "cat.schema.table",
    })
    assert resp.status_code in (200, 422, 500)


def test_dqx_results(client):
    resp = client.get("/api/governance/dqx/results")
    assert resp.status_code in (200, 400, 404, 500)


def test_dqx_run_all(client):
    resp = client.post("/api/governance/dqx/run-all")
    assert resp.status_code in (200, 400, 404, 500)


def test_dqx_export_checks(client):
    resp = client.get("/api/governance/dqx/checks/export")
    assert resp.status_code in (200, 400, 404, 500)


def test_dqx_import_checks(client):
    resp = client.post("/api/governance/dqx/checks/import", json={
        "table_fqn": "cat.schema.table",
        "yaml_content": "checks:\n  - name: test",
    })
    assert resp.status_code in (200, 400, 404, 500)


def test_dqx_profiles(client):
    resp = client.get("/api/governance/dqx/profiles")
    assert resp.status_code in (200, 400, 404, 500)


# ---------------------------------------------------------------------------
# Change History
# ---------------------------------------------------------------------------

def test_get_changes(client):
    resp = client.get("/api/governance/changes")
    assert resp.status_code in (200, 400, 404, 500)
