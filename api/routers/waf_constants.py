"""WAF pillar definitions and category mapping for Clone-Xs assessment portal.

Maps the 34 fine-grained scanner categories to the 7 Databricks
Well-Architected Framework pillars. The scanner package is unchanged;
this is a pure translation layer in Clone-Xs.

Reference: https://docs.databricks.com/aws/en/lakehouse-architecture/well-architected
"""

from __future__ import annotations

WAF_PILLARS: list[tuple[str, str, str]] = [
    # (pillar_name, icon_key, description)
    (
        "Security",
        "shield",
        "Identity & access management, network controls, data protection, secrets, "
        "audit logging, compliance, and exfiltration prevention.",
    ),
    (
        "Data Governance",
        "database",
        "Unity Catalog adoption, grants & ACLs, metadata quality, data lineage, "
        "account-level governance, data residency, and informational posture.",
    ),
    (
        "Operational Excellence",
        "settings",
        "Job reliability, monitoring, notifications, dev practices, CI/CD, "
        "Git integration, and Databricks feature adoption.",
    ),
    (
        "Performance Efficiency",
        "zap",
        "Spark tuning, Delta/table optimization, SQL warehouse configuration, "
        "serverless compute, and advanced query patterns.",
    ),
    (
        "Cost Optimization",
        "trending-down",
        "Idle resource detection, cluster auto-termination, cost anomaly monitoring, "
        "storage tiering, and budget guardrails.",
    ),
    (
        "Reliability",
        "activity",
        "Job retry & timeout policies, DLT pipeline health, data quality monitoring, "
        "table freshness, and pipeline error tracking.",
    ),
    (
        "AI & ML",
        "cpu",
        "ML model governance, inference payload logging, AI Gateway guardrails, "
        "PII filtering, rate limits, drift monitoring, and UC model registry.",
    ),
]

WAF_PILLAR_NAMES: list[str] = [p[0] for p in WAF_PILLARS]

# Maps each of the 34 scanner category strings → WAF pillar name.
CATEGORY_TO_PILLAR: dict[str, str] = {
    # Security
    "Identity & Access":    "Security",
    "Network Security":     "Security",
    "Data Protection":      "Security",
    "Compute Security":     "Security",
    "Secrets & Credentials":"Security",
    "Secret Scanning":      "Security",
    "Audit & Logging":      "Security",
    "Audit Delivery":       "Security",
    "Compliance":           "Security",
    "Webhook Security":     "Security",
    "Network Exfiltration": "Security",
    "Workspace Object ACLs":"Security",
    # Data Governance
    "Governance":           "Data Governance",
    "Advanced Governance":  "Data Governance",
    "Account Governance":   "Data Governance",
    "Governance Data Quality":"Data Governance",
    "Data Residency":       "Data Governance",
    "Data Architecture":    "Data Governance",
    "Informational":        "Data Governance",
    # Operational Excellence
    "Operations":           "Operational Excellence",
    "Ops Excellence":       "Operational Excellence",
    "Dev Practices":        "Operational Excellence",
    "Feature Adoption":     "Operational Excellence",
    # Performance Efficiency
    "Spark Best Practices": "Performance Efficiency",
    "Table Optimization":   "Performance Efficiency",
    "Performance":          "Performance Efficiency",
    "Advanced Performance": "Performance Efficiency",
    "SQL Warehouses":       "Performance Efficiency",
    "DLT Best Practices":   "Performance Efficiency",
    "Delta Best Practices": "Performance Efficiency",
    "Serverless Governance":"Performance Efficiency",
    # Cost Optimization
    "Cost Optimization":    "Cost Optimization",
    # Reliability
    "Reliability":          "Reliability",
    "Data Quality":         "Reliability",
    # AI & ML
    "AI / ML Governance":   "AI & ML",
}


def category_to_pillar(category: str) -> str:
    """Return the WAF pillar for a scanner category, defaulting to 'Data Governance'."""
    return CATEGORY_TO_PILLAR.get(category, "Data Governance")
