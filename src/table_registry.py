"""Central registry of all Delta tables used by Clone-Xs.

Single source of truth for table names, schemas, and their ensure functions.
Used by Settings UI init, health checks, and anywhere tables need discovery.
"""


def get_audit_catalog(config: dict) -> str:
    """Get the audit catalog from config."""
    audit = config.get("audit_trail", {})
    return audit.get("catalog", "")


# Each entry: (section_key, section_title, schema_name, table_name, ensure_module, ensure_func)
# ensure_module/ensure_func can be None if no dedicated ensure function exists

TABLE_SECTIONS = [
    {
        "key": "logs",
        "title": "Audit & Logs",
        "subtitle": "Clone run logs, audit trail, and rollback history",
        "schema": "logs",
        "schema_from_config": True,  # uses config.audit_trail.schema
        "tables": ["run_logs", "clone_operations", "rollback_logs"],
    },
    {
        "key": "metrics",
        "title": "Metrics",
        "subtitle": "Clone operation metrics and performance data",
        "schema": "metrics",
        "tables": ["clone_metrics"],
    },
    {
        "key": "pii",
        "title": "PII Detection",
        "subtitle": "PII scan results, detections, and remediation tracking",
        "schema": "pii",
        "tables": ["pii_scans", "pii_detections", "pii_remediation"],
    },
    {
        "key": "governance",
        "title": "Governance",
        "subtitle": "Business glossary, certifications, and change history",
        "schema": "governance",
        "tables": ["business_glossary", "glossary_links", "certifications", "change_history"],
    },
    {
        "key": "dq_rules",
        "title": "DQ Rules & Results",
        "subtitle": "Data quality rules engine and execution results",
        "schema": "governance",
        "tables": ["dq_rules", "dq_results"],
    },
    {
        "key": "sla",
        "title": "SLA & Contracts",
        "subtitle": "SLA rules, checks, and legacy data contracts",
        "schema": "governance",
        "tables": ["sla_rules", "sla_checks", "data_contracts"],
    },
    {
        "key": "odcs",
        "title": "ODCS Contracts",
        "subtitle": "Open Data Contract Standard v3.1.0 contracts and validation",
        "schema": "governance",
        "tables": ["odcs_contracts", "odcs_contract_versions", "odcs_validation_results"],
    },
    {
        "key": "dqx",
        "title": "DQX Engine",
        "subtitle": "DQX profiles, checks, definitions, and run results",
        "schema": "governance",
        "tables": ["dqx_profiles", "dqx_checks", "dqx_run_results", "dqx_check_definitions"],
    },
    {
        "key": "reconciliation",
        "title": "Reconciliation",
        "subtitle": "Run history, per-table details, alert rules, quality rules",
        "schema": "reconciliation",
        "tables": [
            "reconciliation_runs", "reconciliation_details",
            "alert_rules", "alert_history",
            "quality_rules", "quality_violations",
        ],
    },
    {
        "key": "data_quality",
        "title": "Data Quality Monitoring",
        "subtitle": "Anomaly detection baselines and freshness tracking",
        "schema": "data_quality",
        "tables": ["metric_baselines", "freshness_history"],
    },
    {
        "key": "lineage",
        "title": "Lineage",
        "subtitle": "Clone lineage edges tracking source-to-destination relationships",
        "schema": "lineage",
        "tables": ["clone_lineage"],
    },
    {
        "key": "rtbf",
        "title": "RTBF / Right to Be Forgotten",
        "subtitle": "GDPR Article 17 erasure requests, actions, and deletion certificates",
        "schema": "rtbf",
        "tables": ["rtbf_requests", "rtbf_actions", "rtbf_certificates"],
    },
    {
        "key": "dsar",
        "title": "DSAR / Right of Access",
        "subtitle": "GDPR Article 15 access requests, actions, and exports",
        "schema": "dsar",
        "tables": ["dsar_requests", "dsar_actions", "dsar_exports"],
    },
    {
        "key": "pipelines",
        "title": "Clone Pipelines",
        "subtitle": "Pipeline definitions, execution runs, and step results",
        "schema": "pipelines",
        "tables": ["pipelines", "pipeline_runs", "pipeline_step_results"],
    },
]


def get_all_table_fqns(config: dict) -> list[dict]:
    """Get all table FQNs grouped by section.

    Returns list of dicts: [{key, title, subtitle, schema_fqn, tables: [{name, fqn}]}]
    """
    catalog = get_audit_catalog(config)
    audit_schema = config.get("audit_trail", {}).get("schema", "logs")

    result = []
    for section in TABLE_SECTIONS:
        schema = audit_schema if section.get("schema_from_config") else section["schema"]
        schema_fqn = f"{catalog}.{schema}"
        tables = [{"name": t, "fqn": f"{schema_fqn}.{t}"} for t in section["tables"]]
        result.append({
            "key": section["key"],
            "title": section["title"],
            "subtitle": section["subtitle"],
            "schema": schema,
            "schema_fqn": schema_fqn,
            "tables": tables,
        })
    return result


def get_flat_table_list(config: dict) -> list[str]:
    """Get a flat list of all table FQNs."""
    fqns = []
    for section in get_all_table_fqns(config):
        for t in section["tables"]:
            fqns.append(t["fqn"])
    return fqns
