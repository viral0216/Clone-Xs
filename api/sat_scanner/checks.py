"""SAT Scanner check definitions, constants, and configuration data.

Check metadata (title, category, severity, description, recommendation, etc.)
is loaded from YAML files in the checks/ directory.  Infrastructure constants
(region maps, item extractors, shared state) remain here in Python.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

# ─────────────────────────────────────────────────────────────────────────────
# Load check metadata from YAML files
# ─────────────────────────────────────────────────────────────────────────────

_CHECKS_DIR = Path(__file__).parent / "checks"

SAT_CHECKS: dict[str, dict] = {}
_EFFORT_MAP: dict[str, str] = {}
CHECK_API_ENDPOINTS: dict[str, str] = {}
CHECK_BENEFITS: dict[str, str] = {}
_WS_CONF_EVIDENCE: dict[str, str] = {}
PORTAL_LINKS: dict[str, str] = {}
CHECK_REMEDIATION_OVERRIDES: dict[str, dict] = {}

for _yaml_file in sorted(_CHECKS_DIR.glob("*.yaml")):
    with open(_yaml_file) as _f:
        _checks = yaml.safe_load(_f) or {}
    for _check_id, _data in _checks.items():
        SAT_CHECKS[_check_id] = {
            "title": _data["title"],
            "category": _data["category"],
            "severity": _data["severity"],
            "description": _data["description"],
            "recommendation": _data["recommendation"],
            "reference_url": _data.get("reference_url", ""),
        }
        if _data.get("effort"):
            _EFFORT_MAP[_check_id] = _data["effort"]
        if _data.get("api_endpoint"):
            CHECK_API_ENDPOINTS[_check_id] = _data["api_endpoint"]
        if _data.get("benefits"):
            CHECK_BENEFITS[_check_id] = _data["benefits"]
        if _data.get("ws_conf_keys"):
            _WS_CONF_EVIDENCE[_check_id] = _data["ws_conf_keys"]
        if _data.get("portal_link"):
            PORTAL_LINKS[_check_id] = _data["portal_link"]
        # Optional remediation plan overrides
        _rem_ov: dict = {}
        for _rk in ("prerequisites", "impact_assessment", "rollback_guidance",
                     "remediation_steps", "estimated_duration_hours", "stakeholders"):
            if _data.get(_rk):
                _rem_ov[_rk] = _data[_rk]
        if _rem_ov:
            CHECK_REMEDIATION_OVERRIDES[_check_id] = _rem_ov


def _get_effort(check_id: str) -> str:
    """Return the remediation effort estimate for a check."""
    return _EFFORT_MAP.get(check_id, "Moderate (1–4 hrs)")


# ─────────────────────────────────────────────────────────────────────────────
# Portal link category defaults (prefix-match fallbacks)
# ─────────────────────────────────────────────────────────────────────────────

PORTAL_LINKS.update({
    "SAT-IAM":     "settings/workspace/identity-and-access",
    "SAT-IA":      "settings/workspace/identity-and-access",
    "SAT-NET":     "settings/workspace",
    "SAT-NS":      "settings/workspace",
    "SAT-DATA":    "explore/data",
    "SAT-COMPUTE": "compute",
    "SAT-SQL":     "sql/warehouses",
    "SAT-SEC":     "secrets/scopes",
    "SAT-LOG":     "settings/workspace/advanced",
    "SAT-GOV":     "settings/workspace/advanced",
    "SAT-INFO":    "settings/workspace/advanced",
    "SAT-PERF":    "compute",
    "SAT-COST":    "compute",
    "SAT-REL":     "jobs",
    "SAT-OPS":     "jobs",
    "SAT-DA":      "explore/data",
    "SAT-ML-FEATURE-ACL": "explore/data",
    "SAT-AI":      "ml/endpoints",
    "SAT-ML":      "ml",
    "SAT-FEAT":    "explore/data",
    "SAT-SCAN":    "workspace",
    "SAT-GEO":     "https://accounts.azuredatabricks.net/workspaces?account_id={account_id}",
    "SAT-OPT":     "compute",
    "SAT-DQ":      "explore/data",
    "SAT-SPARK":   "compute",
    "SAT-DEV":     "workspace",
    "SAT-ACL":     "workspace",
    "SAT-AUDIT":   "settings/workspace/audit-logs",
    "SAT-EXFIL":   "settings/workspace",
    "SAT-SRVL":    "compute",
    "SAT-HOOK":    "settings/workspace/notifications",
    "SAT-COMP":    "settings/workspace/security",
})


# ─────────────────────────────────────────────────────────────────────────────
# Category definitions — single source of truth for Excel, HTML, and combined
# ─────────────────────────────────────────────────────────────────────────────

CATEGORY_DEFINITIONS: list[tuple[str, str]] = [
    ("Identity & Access", "Checks for SCIM provisioning, admin user count, service principal usage, group-based access, and entitlement management."),
    ("Network Security", "Evaluates IP access lists, VNet peering, private endpoints, public access restrictions, and firewall rules."),
    ("Data Protection", "Assesses encryption at rest and in transit, Delta Sharing security, table ACLs, and Unity Catalog data governance."),
    ("Compute Security", "Reviews cluster policies, runtime versions, init script security, credential passthrough, and Azure Spot VM configs."),
    ("SQL Warehouses", "Checks SQL warehouse channel settings, sizing, tagging, and access control configuration."),
    ("Secrets & Credentials", "Evaluates secret scope usage, backend types (Azure Key Vault vs Databricks), and whether plaintext secrets exist."),
    ("Audit & Logging", "Verifies diagnostic logging, audit log delivery, verbose audit logging, and log retention policies."),
    ("Governance", "Assesses Unity Catalog adoption, metastore assignment, catalog/schema hygiene, data lineage, ACLs, and tagging."),
    ("AI / ML Governance", "Reviews inference payload logging, AI Gateway guardrails, PII filtering, rate limits, serving auth, model drift monitoring, UC model registry, experiment ACLs, and GPU compute limits."),
    ("Informational", "Low-severity checks for best practices, security headers, and operational hygiene."),
    ("Secret Scanning", "Scans notebook source code, cluster configs, job definitions, init scripts, DLT pipelines, and SQL warehouses for hardcoded secrets using TruffleHog."),
    ("Operations", "Reviews job health, auto-termination settings, DLT pipeline quality, notification destinations, and operational monitoring."),
    ("Performance", "Evaluates Photon adoption, serverless SQL warehouses, runtime version currency (LTS/EOL), AQE config, and Delta optimization jobs."),
    ("Cost Optimization", "Checks autoscaling adoption, spot/preemptible instances, auto-termination, cost tagging, and instance pool usage."),
    ("Reliability", "Reviews job retry and timeout configuration, job run success rates, and cluster log delivery coverage."),
    ("Data Architecture", "Assesses medallion pattern adoption, UC Volumes, DLT pipeline health, and external locations governance."),
    ("Ops Excellence", "Checks Git repos adoption, job ownership (SP vs user), idle cluster detection, SQL alert coverage, and job notifications."),
    ("Governance Data Quality", "Evaluates Lakehouse Monitors, metadata completeness, hive metastore migration readiness, UC tag adoption, and PII classification."),
    ("Feature Adoption", "Checks adoption of Vector Search, Feature Store, Genie, Lakeview, Apps, Repos, Clean Rooms, Marketplace, Model Serving, and Agent Framework."),
    ("Account Governance", "Evaluates account-level user inventory, empty groups, SP inventory, Account Console IP ACLs, metastore coverage, budgets, and workspace tiers."),
    ("Advanced Governance", "Checks advanced governance controls including data lineage, system tables, and workspace-level compliance settings."),
    ("Advanced Performance", "Evaluates advanced performance optimizations including query execution patterns and resource utilization."),
    ("Data Residency", "Verifies workspace and storage data residency requirements, geo-fencing, and cross-region data transfer controls."),
    ("Table Optimization", "Checks predictive optimization (catalog-level), optimized writes, auto compaction, Delta cache, maintenance scheduling, SQL warehouse Photon, Liquid Clustering, auto-stop, serverless jobs, and Delta format."),
    ("Data Quality", "Evaluates Lakehouse Monitor coverage, table/column descriptions, table ownership, DLT pipeline freshness and errors, compute right-sizing, and SQL alert coverage."),
    ("Spark Best Practices", "Checks Spark cluster configuration, job settings, cluster policies, UC governance, naming conventions, table statistics, and runtime enforcement for adherence to performance, cost, and reliability best practices."),
    ("Dev Practices", "Checks connected Git repos for CI/CD configs, linting setup, and DABs adoption. Scans notebook source for anti-patterns and template compliance. Verifies data quality dashboard existence."),
    ("Workspace Object ACLs", "Checks that notebooks, folders, dashboards, queries, and ML experiments have explicit permission grants rather than open defaults."),
    ("Audit Delivery", "Verifies audit log delivery configuration, freshness, alerting on high-risk events, and retention policy compliance."),
    ("Network Exfiltration", "Fine-grained checks for data exfiltration prevention: egress firewall rules, DBFS mount trust boundaries, external location governance, and init script package sources."),
    ("Serverless Governance", "Checks governance, cost control, and network security for serverless compute including budgets, NCC, usage restrictions, cost tracking, and warehouse sizing."),
    ("Webhook Security", "Validates webhook and notification destination security: HTTPS enforcement, authentication headers, stale endpoint detection, and model registry webhook scoping."),
    ("Compliance", "Evaluates regulatory compliance posture for HIPAA, SOC2, GDPR right-to-delete, data retention policies, and end-to-end audit trail completeness."),
    ("Delta Best Practices", "Checks Delta table configuration best practices including vacuum retention, Change Data Feed, deletion vectors, column mapping, UniForm, and clone strategy."),
    ("DLT Best Practices", "Checks DLT pipeline configuration for multi-layer architecture, quarantine patterns, Unity Catalog integration, freshness scheduling, and serverless adoption."),
]


# ─────────────────────────────────────────────────────────────────────────────
# Azure Region → Databricks Geography mapping
# Ref: https://learn.microsoft.com/en-us/azure/databricks/resources/designated-services
# ─────────────────────────────────────────────────────────────────────────────

AZURE_REGION_TO_GEO: dict[str, str] = {
    # United States
    "eastus": "US", "eastus2": "US", "centralus": "US", "northcentralus": "US",
    "southcentralus": "US", "westcentralus": "US", "westus": "US", "westus2": "US", "westus3": "US",
    # EU Data Boundary
    "northeurope": "EU", "westeurope": "EU", "francecentral": "EU",
    "germanywestcentral": "EU", "switzerlandnorth": "EU", "switzerlandwest": "EU",
    "norwayeast": "EU", "swedencentral": "EU",
    # United Kingdom (separate Geo from EU per Designated Services table)
    "uksouth": "United Kingdom", "ukwest": "United Kingdom",
    # Canada
    "canadacentral": "Canada", "canadaeast": "Canada",
    # Brazil
    "brazilsouth": "Brazil",
    # Asia Pacific
    "eastasia": "Asia Pacific", "southeastasia": "Asia Pacific",
    # Japan
    "japaneast": "Japan", "japanwest": "Japan",
    # South Korea
    "koreacentral": "South Korea",
    # Australia
    "australiaeast": "Australia", "australiasoutheast": "Australia",
    "australiacentral": "Australia", "australiacentral2": "Australia",
    # India
    "centralindia": "India", "southindia": "India", "westindia": "India",
    # UAE
    "uaenorth": "UAE",
    # Qatar
    "qatarcentral": "Qatar",
    # South Africa
    "southafricanorth": "South Africa",
    # Mexico
    "mexicocentral": "Mexico",
}

# Geos where cross-Geo processing is DISABLED by default
CROSS_GEO_DISABLED_BY_DEFAULT = {"US", "EU"}

# Side-channel: workspace URL → Azure region (populated by azure_login_flow / azure_tenant_flow)
_WORKSPACE_REGIONS: dict[str, str] = {}
# Side-channel: workspace URL → Databricks account_id UUID (populated during login/scan)
_WORKSPACE_ACCOUNT_IDS: dict[str, str] = {}
# Side-channel: workspace URL → Azure ARM info (populated during Azure login flows)
# Each value is {"resource_id": "/subscriptions/.../workspaces/...", "tenant": "xxx.onmicrosoft.com"}
_WORKSPACE_ARM_INFO: dict[str, dict[str, str]] = {}
# Side-channel: Azure management token for ARM API calls (populated during Azure login flows)
_AZURE_MGMT_TOKEN: str = ""

def _resolve_geo(azure_region: str) -> str:
    """Map Azure region name to Databricks Geography. Returns 'Unknown' if unmapped."""
    return AZURE_REGION_TO_GEO.get(azure_region.lower().replace(" ", ""), "Unknown")


# ─────────────────────────────────────────────────────────────────────────────
# Misc constants
# ─────────────────────────────────────────────────────────────────────────────

EXCEL_CELL_LIMIT = 32_767


# ─────────────────────────────────────────────────────────────────────────────
# Item extractors: API endpoint path → (json_list_key, item_name_key)
# name_key supports dotted notation e.g. "settings.name"
# ─────────────────────────────────────────────────────────────────────────────

_ITEM_EXTRACTORS: dict[str, tuple[str, str]] = {
    "/api/2.0/clusters/list": ("clusters", "cluster_name"),
    "/api/2.1/jobs/list": ("jobs", "settings.name"),
    "/api/2.0/sql/warehouses": ("warehouses", "name"),
    "/api/2.0/secrets/scopes/list": ("scopes", "name"),
    "/api/2.0/token/list": ("token_infos", "comment"),
    "/api/2.0/ip-access-lists": ("ip_access_lists", "label"),
    "/api/2.0/policies/clusters/list": ("policies", "name"),
    "/api/2.1/unity-catalog/metastores": ("metastores", "name"),
    "/api/2.1/unity-catalog/catalogs": ("catalogs", "name"),
    "/api/2.1/unity-catalog/schemas": ("schemas", "name"),
    "/api/2.1/unity-catalog/shares": ("shares", "name"),
    "/api/2.1/unity-catalog/recipients": ("recipients", "name"),
    "/api/2.1/unity-catalog/external-locations": ("external_locations", "name"),
    "/api/2.1/unity-catalog/storage-credentials": ("storage_credentials", "name"),
    "/api/2.0/global-init-scripts": ("scripts", "name"),
    "/api/2.0/serving-endpoints": ("endpoints", "name"),
    "/api/2.0/mlflow/registered-models/search": ("registered_models", "name"),
    "/api/2.0/instance-pools/list": ("instance_pools", "instance_pool_name"),
    "/api/2.0/pipelines": ("statuses", "name"),
    "/api/2.0/notification-destinations": ("results", "name"),
    "/api/2.0/preview/scim/v2/Groups": ("Resources", "displayName"),
    "/api/2.0/preview/scim/v2/Users": ("Resources", "userName"),
    "/api/2.0/preview/scim/v2/ServicePrincipals": ("Resources", "displayName"),
    "/api/2.0/lakeview/dashboards": ("dashboards", "name"),
    "/api/2.0/dbfs/list": ("files", "path"),
    "/api/2.0/libraries/all-cluster-statuses": ("statuses", "cluster_id"),
    "/api/2.1/unity-catalog/tables": ("tables", "name"),
    "/api/2.0/sql/history/queries": ("res", "query_id"),
    "/api/2.0/sql/alerts": ("results", "name"),
    "/api/2.0/vector-search/endpoints": ("endpoints", "name"),
    "/api/2.0/feature-store/feature-tables": ("feature_tables", "name"),
    "/api/2.0/genie/spaces": ("spaces", "title"),
    "/api/2.0/apps": ("apps", "name"),
    "/api/2.0/repos": ("repos", "path"),
    "/api/2.0/clean-rooms": ("clean_rooms", "name"),
    "/api/2.1/marketplace-consumer/listings": ("listings", "summary.name"),
    "/api/2.0/online-tables": ("online_tables", "name"),
    "/api/2.1/jobs/runs/list": ("runs", "run_name"),
    # Phase 2/3 new endpoints
    "/api/2.1/unity-catalog/volumes": ("volumes", "name"),
    "/api/2.0/workspace/list": ("objects", "path"),
    # ML/AI governance endpoints
    "/api/2.1/unity-catalog/registered-models": ("registered_models", "full_name"),
    "/api/2.0/mlflow/experiments/search": ("experiments", "name"),
    # Token management, connections, cluster events
    "/api/2.0/token-management/tokens": ("token_infos", "created_by_username"),
    "/api/2.1/unity-catalog/connections": ("connections", "name"),
    "/api/2.0/clusters/events": ("events", "cluster_id"),
}
