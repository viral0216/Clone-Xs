"""Remediation plan auto-generation and timeline builder for SAT Scanner.

Generates structured remediation plans from existing finding metadata
(severity, effort, category, recommendation). Supports optional YAML
overrides for check-specific data.
"""

from __future__ import annotations

import math
import re
from typing import Any

# ---------------------------------------------------------------------------
# Static maps for auto-generation
# ---------------------------------------------------------------------------

_EFFORT_HOURS: dict[str, float] = {
    "Quick Fix (5\u201315 min)": 0.25,
    "Moderate (1\u20134 hrs)": 2.5,
    "Significant (1\u20133 days)": 16,
    "Project (1+ weeks)": 40,
}

_EFFORT_DOWNTIME: dict[str, str] = {
    "Quick Fix (5\u201315 min)": "none",
    "Moderate (1\u20134 hrs)": "none",
    "Significant (1\u20133 days)": "brief",
    "Project (1+ weeks)": "scheduled",
}

_CATEGORY_BLAST_RADIUS: dict[str, str] = {
    "Network Security": "account",
    "Identity & Access": "account",
    "Account Governance": "account",
    "Data Residency": "account",
    "Network Exfiltration": "account",
    "Compute Security": "workspace",
    "SQL Warehouses": "workspace",
    "Data Protection": "workspace",
    "Governance": "workspace",
    "Secrets & Credentials": "workspace",
    "Audit & Logging": "workspace",
    "AI / ML Governance": "workspace",
    "Compliance": "account",
}

_CATEGORY_SERVICES: dict[str, str] = {
    "Network Security": "Network connectivity, API access, VPN/peering",
    "Identity & Access": "User authentication, SSO, SCIM provisioning",
    "Account Governance": "Account-level settings, workspace provisioning",
    "Compute Security": "Clusters, jobs, init scripts, runtime",
    "SQL Warehouses": "SQL endpoints, BI queries, dashboards",
    "Data Protection": "Tables, Delta Sharing, encryption, UC catalogs",
    "Secrets & Credentials": "Secret scopes, Key Vault integration",
    "Audit & Logging": "Diagnostic logs, audit trail, log retention",
    "Governance": "Unity Catalog, metastore, data lineage",
    "AI / ML Governance": "Model serving, experiments, MLflow, AI Gateway",
    "Operations": "Jobs, DLT pipelines, notifications",
    "Performance": "Photon, AQE, serverless, runtime versions",
    "Cost Optimization": "Autoscaling, spot instances, auto-termination",
    "Reliability": "Job retries, timeouts, cluster log delivery",
    "Compliance": "HIPAA, SOC2, GDPR, data retention",
    "Network Exfiltration": "Egress rules, DBFS mounts, external locations",
    "Data Residency": "Geo-fencing, cross-region transfers",
}

_CATEGORY_STAKEHOLDERS: dict[str, list[str]] = {
    "Network Security": ["Network Security Team", "Platform Engineering"],
    "Identity & Access": ["Identity Team", "Security Team"],
    "Account Governance": ["Platform Engineering", "Security Team"],
    "Compute Security": ["Platform Engineering", "Data Engineering"],
    "SQL Warehouses": ["Data Engineering", "BI Team"],
    "Data Protection": ["Data Governance Team", "Security Team"],
    "Secrets & Credentials": ["Security Team", "Platform Engineering"],
    "Audit & Logging": ["Security Team", "Compliance Team"],
    "Governance": ["Data Governance Team", "Platform Engineering"],
    "AI / ML Governance": ["ML Engineering", "Security Team"],
    "Operations": ["Platform Engineering", "Data Engineering"],
    "Performance": ["Data Engineering", "Platform Engineering"],
    "Cost Optimization": ["FinOps Team", "Platform Engineering"],
    "Reliability": ["Platform Engineering", "SRE Team"],
    "Compliance": ["Compliance Team", "Security Team", "Legal"],
    "Network Exfiltration": ["Network Security Team", "Security Team"],
    "Data Residency": ["Compliance Team", "Platform Engineering"],
}

_CATEGORY_POST_VALIDATION: dict[str, list[str]] = {
    "Network Security": ["Test network connectivity from approved sources", "Verify API access still works"],
    "Identity & Access": ["Verify SSO/SCIM authentication flow", "Confirm user access is unaffected"],
    "Compute Security": ["Launch a test cluster to verify policy compliance", "Run a sample job"],
    "SQL Warehouses": ["Run a test query on SQL warehouse", "Verify BI dashboard connectivity"],
    "Data Protection": ["Verify data access permissions", "Test Delta Sharing if applicable"],
    "Secrets & Credentials": ["Verify secret retrieval works", "Test Key Vault integration"],
    "Audit & Logging": ["Confirm logs are flowing to destination", "Verify audit events appear"],
    "Governance": ["Verify UC catalog/schema access", "Test data lineage tracking"],
}

_SEVERITY_CHANGE_TYPE: dict[str, str] = {
    "critical": "Emergency",
    "high": "Emergency",
    "medium": "Standard",
    "low": "Normal",
}

_SEVERITY_COMM_PLAN: dict[str, str] = {
    "critical": "Notify affected teams immediately. Escalate to security leadership.",
    "high": "Notify affected teams 24 hours before change.",
    "medium": "Standard change notification to affected teams.",
    "low": "Include in regular change summary.",
}


# ---------------------------------------------------------------------------
# Rollback heuristic
# ---------------------------------------------------------------------------

_INVERSION_PATTERNS: list[tuple[str, str]] = [
    (r"\bEnable\b", "Disable"),
    (r"\bDisable\b", "Enable"),
    (r"\bAdd\b", "Remove"),
    (r"\bRemove\b", "Re-add"),
    (r"\bCreate\b", "Delete"),
    (r"\bSet\b", "Revert"),
    (r"\bEnforce\b", "Remove enforcement of"),
    (r"\bRestrict\b", "Remove restriction on"),
    (r"\bBlock\b", "Unblock"),
    (r"\bDeny\b", "Allow"),
    (r"\bUpgrade\b", "Downgrade"),
    (r"\bRotate\b", "Restore previous"),
]


def _generate_rollback(recommendation: str) -> str:
    """Heuristically invert the recommendation to produce rollback guidance."""
    if not recommendation:
        return "Revert configuration to pre-change state. Restore from backup if available."
    # Take the first sentence
    first = recommendation.split(". ")[0].strip()
    for pattern, replacement in _INVERSION_PATTERNS:
        if re.search(pattern, first, re.IGNORECASE):
            rollback = re.sub(pattern, replacement, first, count=1, flags=re.IGNORECASE)
            return f"{rollback}. Verify services are restored."
    return "Revert configuration to pre-change state. Restore from backup if available."


def _split_steps(recommendation: str) -> list[str]:
    """Split recommendation text into actionable steps."""
    if not recommendation:
        return ["Apply the recommended configuration change"]
    # Split on numbered patterns like "1. " or "1) " or bullet points
    numbered = re.split(r"(?:^|\n)\s*\d+[\.\)]\s*", recommendation)
    numbered = [s.strip() for s in numbered if s.strip()]
    if len(numbered) > 1:
        return numbered
    # Split on ". " but keep meaningful sentences
    sentences = [s.strip() for s in recommendation.split(". ") if s.strip() and len(s.strip()) > 10]
    if sentences:
        return sentences
    return [recommendation.strip()]


# ---------------------------------------------------------------------------
# Main generator
# ---------------------------------------------------------------------------

def generate_remediation_plan(
    check_id: str,
    title: str,
    category: str,
    severity: str,
    effort: str,
    recommendation: str,
    status: str,
    *,
    overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Generate a structured remediation plan for a finding.

    Uses deterministic rules based on severity, effort, and category.
    YAML overrides (if provided) replace specific auto-generated fields.
    """
    ov = overrides or {}
    effort = effort or "Moderate (1\u20134 hrs)"
    severity = severity.lower()
    est_hours = ov.get("estimated_duration_hours") or _EFFORT_HOURS.get(effort, 2.5)

    # Prerequisites
    prerequisites = ov.get("prerequisites") or _auto_prerequisites(severity, effort, category)

    # Impact assessment
    impact = ov.get("impact_assessment") or {
        "downtime": _EFFORT_DOWNTIME.get(effort, "none"),
        "blast_radius": _CATEGORY_BLAST_RADIUS.get(category, "workspace"),
        "affected_services": _CATEGORY_SERVICES.get(category, "Platform services"),
        "risk_level": severity,
    }

    # Rollback
    rollback_guidance = ov.get("rollback_guidance") or _generate_rollback(recommendation)

    # Checklist
    if ov.get("remediation_steps"):
        checklist = {
            "pre_checks": ov["remediation_steps"].get("pre_checks", []),
            "steps": ov["remediation_steps"].get("steps", []),
            "post_validation": ov["remediation_steps"].get("post_validation", []),
            "rollback": ov["remediation_steps"].get("rollback", []),
        }
    else:
        checklist = _auto_checklist(severity, effort, category, recommendation, rollback_guidance)

    # Stakeholders
    stakeholders = ov.get("stakeholders") or _CATEGORY_STAKEHOLDERS.get(category, ["Platform Team"])

    # Change management
    change_mgmt = _auto_change_management(severity, effort, category, impact.get("downtime", "none"))

    return {
        "prerequisites": prerequisites,
        "impact_assessment": impact,
        "rollback_guidance": rollback_guidance,
        "checklist": checklist,
        "estimated_duration_hours": est_hours,
        "stakeholders": stakeholders,
        "change_management": change_mgmt,
    }


def _auto_prerequisites(severity: str, effort: str, category: str) -> list[str]:
    prereqs = ["Verify you have Admin permissions for the target scope"]
    if effort in ("Significant (1\u20133 days)", "Project (1+ weeks)"):
        prereqs.append("Back up current configuration before making changes")
        prereqs.append("Schedule a maintenance window if required")
    if severity in ("critical", "high"):
        prereqs.append("Review impact on production workloads")
    cat_prereqs = {
        "Network Security": "Collect approved IP ranges/CIDR blocks from network team",
        "Identity & Access": "Verify IdP/SCIM configuration is available",
        "Secrets & Credentials": "Ensure Azure Key Vault access is configured",
        "Data Protection": "Verify Unity Catalog is enabled on the workspace",
        "Compliance": "Review applicable regulatory requirements",
    }
    if category in cat_prereqs:
        prereqs.append(cat_prereqs[category])
    return prereqs


def _auto_checklist(
    severity: str, effort: str, category: str,
    recommendation: str, rollback_guidance: str,
) -> dict[str, list[str]]:
    # Pre-checks
    pre_checks = ["Verify required permissions are in place"]
    if effort in ("Significant (1\u20133 days)", "Project (1+ weeks)"):
        pre_checks.append("Notify stakeholders of upcoming change")
        pre_checks.append("Confirm maintenance window is scheduled")
    if severity in ("critical", "high"):
        pre_checks.append("Assess impact on production workloads")

    # Steps
    steps = _split_steps(recommendation)

    # Post-validation
    post_validation = ["Verify the configuration change is applied"]
    cat_pv = _CATEGORY_POST_VALIDATION.get(category, [])
    post_validation.extend(cat_pv)
    post_validation.append("Monitor for errors or unexpected behaviour for 24 hours")

    # Rollback
    rollback = [rollback_guidance] if rollback_guidance else ["Revert to previous configuration"]

    return {
        "pre_checks": pre_checks,
        "steps": steps,
        "post_validation": post_validation,
        "rollback": rollback,
    }


def _auto_change_management(
    severity: str, effort: str, category: str, downtime: str,
) -> dict[str, Any]:
    change_type = _SEVERITY_CHANGE_TYPE.get(severity, "Standard")
    approval_required = severity in ("critical", "high", "medium")

    if downtime in ("scheduled", "extended"):
        window = "Scheduled maintenance window"
    elif downtime == "brief":
        window = "Low-traffic period recommended"
    else:
        window = "Business hours"

    testing = "Test in non-production environment first."
    cat_testing = {
        "Network Security": " Validate network connectivity after change.",
        "Identity & Access": " Verify authentication flows after change.",
        "Compute Security": " Launch test cluster to verify policy.",
        "SQL Warehouses": " Run test queries to verify warehouse.",
        "Data Protection": " Verify data access permissions.",
    }
    testing += cat_testing.get(category, " Verify expected behaviour after change.")

    communication = _SEVERITY_COMM_PLAN.get(severity, "Standard change notification.")

    return {
        "change_type": change_type,
        "approval_required": approval_required,
        "suggested_change_window": window,
        "testing_plan": testing,
        "communication_plan": communication,
    }


# ---------------------------------------------------------------------------
# Timeline builder
# ---------------------------------------------------------------------------

_PHASE_LABELS: dict[str, str] = {
    "P1": "Phase 1: Immediate (Week 1)",
    "P2": "Phase 2: Short-term (Weeks 2\u20133)",
    "P3": "Phase 3: Medium-term (Weeks 4\u20138)",
    "P4": "Phase 4: Long-term (Backlog)",
}

# Available working days per phase (8 hrs/day)
_PHASE_AVAILABLE_DAYS: dict[str, int] = {
    "P1": 5,    # 1 week
    "P2": 10,   # 2 weeks
    "P3": 25,   # 5 weeks
    "P4": 40,   # backlog / flexible
}

_HOURS_PER_DAY = 8


def _hours_to_days(hours: float) -> float:
    """Convert effort hours to working days (8h/day), rounded to 1 decimal."""
    return round(hours / _HOURS_PER_DAY, 1)


def _estimate_resources(working_days: float, phase: str) -> int:
    """Estimate number of parallel resources needed for a phase.

    Based on available working days in each phase window.
    Minimum 1 resource.
    """
    avail = _PHASE_AVAILABLE_DAYS.get(phase, 40)
    if working_days <= 0:
        return 0
    return max(1, math.ceil(working_days / avail))


def build_remediation_timeline(prio_items: list[dict]) -> dict[str, Any]:
    """Build a remediation timeline/roadmap grouped by priority then category.

    *prio_items* is the output of ``_build_prioritised_recommendations()``
    enriched with ``remediation_plan``.
    """
    phases: list[dict[str, Any]] = []

    for px in ("P1", "P2", "P3", "P4"):
        bucket = [
            i for i in prio_items
            if i.get("priority_label", "").startswith(px)
        ]
        if not bucket:
            phases.append({
                "phase": _PHASE_LABELS[px],
                "priority": px,
                "categories": {},
                "total_hours": 0,
                "total_working_days": 0,
                "estimated_resources": 0,
            })
            continue

        # Group by category
        cats: dict[str, dict[str, Any]] = {}
        for item in bucket:
            cat = item.get("category", "Other")
            if cat not in cats:
                cats[cat] = {"findings": [], "subtotal_hours": 0}
            plan = item.get("remediation_plan", {})
            hrs = plan.get("estimated_duration_hours", 2.5) if plan else 2.5
            cats[cat]["findings"].append({
                "check_id": item.get("check_id", ""),
                "title": item.get("title", ""),
                "severity": item.get("severity", ""),
                "effort": item.get("effort", ""),
                "effort_hours": hrs,
                "working_days": _hours_to_days(hrs),
            })
            cats[cat]["subtotal_hours"] = round(cats[cat]["subtotal_hours"] + hrs, 2)

        # Add working days per category
        for cat_data in cats.values():
            cat_data["subtotal_working_days"] = _hours_to_days(cat_data["subtotal_hours"])

        # Sort categories by subtotal descending
        sorted_cats = dict(
            sorted(cats.items(), key=lambda x: -x[1]["subtotal_hours"])
        )

        total_hrs = round(sum(c["subtotal_hours"] for c in sorted_cats.values()), 2)
        total_days = _hours_to_days(total_hrs)
        phases.append({
            "phase": _PHASE_LABELS[px],
            "priority": px,
            "categories": sorted_cats,
            "total_hours": total_hrs,
            "total_working_days": total_days,
            "estimated_resources": _estimate_resources(total_days, px),
        })

    total_findings = len(prio_items)
    total_hours = round(sum(p["total_hours"] for p in phases), 2)
    total_working_days = _hours_to_days(total_hours)
    total_resources = sum(p["estimated_resources"] for p in phases)
    unique_cats = len({i.get("category", "") for i in prio_items})

    return {
        "summary": {
            "total_findings": total_findings,
            "total_effort_hours": total_hours,
            "total_working_days": total_working_days,
            "estimated_resources": total_resources,
            "categories": unique_cats,
        },
        "phases": phases,
    }
