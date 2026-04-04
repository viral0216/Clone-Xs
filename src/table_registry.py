"""Central registry of all Delta tables used by Clone-Xs.

Single source of truth for table names, schemas, and their ensure functions.
Used by Settings UI init, health checks, and anywhere tables need discovery.

All modules should use ``get_catalog``, ``get_schema_fqn``, and
``get_table_fqn`` instead of hardcoding catalog/schema names.
"""

_FALLBACK_CATALOG = "clone_audit"

_DEFAULT_SCHEMAS: dict[str, str] = {
    "logs": "logs",
    "metrics": "metrics",
    "governance": "governance",
    "reconciliation": "reconciliation",
    "data_quality": "data_quality",
    "lineage": "lineage",
    "pii": "pii",
    "rtbf": "rtbf",
    "dsar": "dsar",
    "mdm": "mdm",
    "pipelines": "pipelines",
    "data_contracts": "data_contracts",
    "state": "state",
}


def get_catalog(config: dict) -> str:
    """Return the catalog used for all internal tables.

    Resolution order:
      1. ``config["tables"]["catalog"]``
      2. ``config["audit_trail"]["catalog"]``
      3. ``"clone_audit"`` (hard fallback)
    """
    tables = config.get("tables")
    if isinstance(tables, dict) and tables.get("catalog"):
        return tables["catalog"]
    audit = config.get("audit_trail")
    if isinstance(audit, dict) and audit.get("catalog"):
        return audit["catalog"]
    return _FALLBACK_CATALOG


def get_schema_fqn(config: dict, section_key: str) -> str:
    """Return ``catalog.schema`` for the given section key.

    *section_key* must match a key in ``tables.schemas`` (e.g. ``"logs"``,
    ``"governance"``, ``"pii"``).
    """
    catalog = get_catalog(config)
    tables = config.get("tables")
    if isinstance(tables, dict):
        schemas = tables.get("schemas")
        if isinstance(schemas, dict) and section_key in schemas:
            return f"{catalog}.{schemas[section_key]}"
    return f"{catalog}.{_DEFAULT_SCHEMAS.get(section_key, section_key)}"


def get_table_fqn(config: dict, section_key: str, table_name: str) -> str:
    """Return ``catalog.schema.table`` for a specific table."""
    return f"{get_schema_fqn(config, section_key)}.{table_name}"


# ── Backward-compatible alias ───────────────────────────────────────


def get_batch_insert_size(config: dict) -> int:
    """Return the batch size for INSERT statements (default 50)."""
    return int(config.get("batch_insert_size", 50))


def get_audit_catalog(config: dict) -> str:
    """Get the audit catalog from config."""
    return get_catalog(config)


# Each entry: (section_key, section_title, schema_name, table_name, ensure_module, ensure_func)
# ensure_module/ensure_func can be None if no dedicated ensure function exists

TABLE_SECTIONS = [
    {
        "key": "logs",
        "title": "Audit & Logs",
        "subtitle": "Clone run logs, audit trail, and rollback history",
        "schema": "logs",
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
        "schema_key": "governance",
        "title": "DQ Rules & Results",
        "subtitle": "Data quality rules engine and execution results",
        "schema": "governance",
        "tables": ["dq_rules", "dq_results"],
    },
    {
        "key": "sla",
        "schema_key": "governance",
        "title": "SLA & Contracts",
        "subtitle": "SLA rules, checks, and legacy data contracts",
        "schema": "governance",
        "tables": ["sla_rules", "sla_checks", "data_contracts"],
    },
    {
        "key": "odcs",
        "schema_key": "governance",
        "title": "ODCS Contracts",
        "subtitle": "Open Data Contract Standard v3.1.0 contracts and validation",
        "schema": "governance",
        "tables": ["odcs_contracts", "odcs_contract_versions", "odcs_validation_results"],
    },
    {
        "key": "dqx",
        "schema_key": "governance",
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
        "key": "mdm",
        "title": "Master Data Management",
        "subtitle": "Golden records, matching, stewardship, and hierarchies",
        "schema": "mdm",
        "tables": ["mdm_entities", "mdm_source_records", "mdm_match_pairs", "mdm_matching_rules", "mdm_stewardship_queue", "mdm_hierarchies"],
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
    result = []
    for section in TABLE_SECTIONS:
        section_key = section.get("schema_key", section["key"])
        schema_fqn = get_schema_fqn(config, section_key)
        schema_name = schema_fqn.split(".", 1)[1] if "." in schema_fqn else schema_fqn
        tables = [{"name": t, "fqn": f"{schema_fqn}.{t}"} for t in section["tables"]]
        result.append({
            "key": section["key"],
            "title": section["title"],
            "subtitle": section["subtitle"],
            "schema": schema_name,
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
