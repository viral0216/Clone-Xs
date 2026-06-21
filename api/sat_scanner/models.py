"""SAT Scanner data models."""

from __future__ import annotations

from typing import Any


class SATFinding:
    """A single SAT check result."""
    __slots__ = (
        "check_id", "category", "title", "description", "severity",
        "status", "current_state", "recommendation", "details", "reference_url",
        "is_api_error", "evidence", "portal_link", "benefits", "effort",
        "remediation_plan",
    )

    def __init__(
        self,
        check_id: str,
        category: str,
        title: str,
        description: str,
        severity: str,
        status: str,
        current_state: str,
        recommendation: str,
        details: dict | None = None,
        reference_url: str = "",
        is_api_error: bool = False,
        evidence: dict | None = None,
        portal_link: str = "",
        benefits: str = "",
        effort: str = "",
        remediation_plan: dict | None = None,
    ):
        self.check_id = check_id
        self.category = category
        self.title = title
        self.description = description
        self.severity = severity
        self.status = status
        self.current_state = current_state
        self.recommendation = recommendation
        self.details = details or {}
        self.reference_url = reference_url
        self.is_api_error = is_api_error
        self.evidence = evidence
        self.portal_link = portal_link
        self.benefits = benefits
        self.effort = effort
        self.remediation_plan = remediation_plan

    def to_dict(self) -> dict:
        return {s: getattr(self, s) for s in self.__slots__}


class SATScanResult:
    """Full scan result."""
    def __init__(
        self,
        workspace_url: str,
        scanned_at: str,
        overall_score: int,
        total_checks: int,
        passed: int,
        failed: int,
        warnings: int,
        not_applicable: int,
        findings: list[SATFinding],
        category_scores: dict[str, int],
        databricks_version: str | None = None,
        workspace_name: str = "",
        api_errors: int = 0,
    ):
        self.workspace_url = workspace_url
        self.scanned_at = scanned_at
        self.overall_score = overall_score
        self.total_checks = total_checks
        self.passed = passed
        self.failed = failed
        self.warnings = warnings
        self.not_applicable = not_applicable
        self.findings = findings
        self.category_scores = category_scores
        self.databricks_version = databricks_version
        self.workspace_name = workspace_name
        self.api_errors = api_errors
        self.endpoint_summary: dict = {}
        # Optional Unity Catalog + Azure inventory (set when --with-inventory)
        self.inventory_obj = None

    def to_dict(self) -> dict:
        from .checks import SAT_CHECKS, _get_effort, CATEGORY_DEFINITIONS

        d = {
            "workspace_url": self.workspace_url,
            "workspace_name": self.workspace_name,
            "scanned_at": self.scanned_at,
            "overall_score": self.overall_score,
            "total_checks": self.total_checks,
            "passed": self.passed,
            "failed": self.failed,
            "warnings": self.warnings,
            "not_applicable": self.not_applicable,
            "api_errors": self.api_errors,
            "scoring_method": {
                "severity_weights": {"critical": 10, "high": 7, "medium": 4, "low": 2},
                "fail_penalty": "full weight",
                "warn_penalty": "half weight (0.5x)",
                "pass_penalty": "none",
                "excluded": "NOT_APPLICABLE and API Error findings",
                "formula": "Score = (1 - penalty_sum / total_weights) x 100",
            },
            "status_definitions": {
                "PASS": "Compliant — the security control is correctly configured. No action needed.",
                "FAIL": "Action Required — confirmed security gap that must be fixed.",
                "WARN": "Review Needed — borderline result that needs manual investigation. Genuine warnings only (API errors are separated).",
                "NOT_APPLICABLE": "Skipped — feature not in use on this workspace (e.g. no Delta Sharing recipients, no MLflow models, Unity Catalog not enabled). Excluded from score.",
                "API ERROR": "Could Not Evaluate — the check could not run due to an API failure (HTTP 400/401/403/404, timeout, or connection error). Excluded from score.",
            },
            "grade_definitions": {
                "Good (80-100)": "Strong security posture. Address remaining findings as maintenance items.",
                "Needs Improvement (60-79)": "Gaps exist that weaken security posture. Prioritize High and Critical findings.",
                "Critical (0-59)": "Significant security risks are present. Immediate remediation is required.",
            },
            "findings": [f.to_dict() for f in self.findings],
            "category_scores": self.category_scores,
            "databricks_version": self.databricks_version,
        }
        # Score breakdown
        _sw = {"critical": 10, "high": 7, "medium": 4, "low": 2}
        _scorable = [f for f in self.findings if not f.is_api_error]
        _applicable = [f for f in _scorable if f.status != "NOT_APPLICABLE"]
        _fails = [f for f in _applicable if f.status == "FAIL"]
        _warns = [f for f in _applicable if f.status == "WARN"]
        _passes = [f for f in _applicable if f.status == "PASS"]
        _tot = sum(_sw.get(SAT_CHECKS.get(f.check_id, {}).get("severity", "low"), 2) for f in _applicable)
        _fp = sum(_sw.get(SAT_CHECKS.get(f.check_id, {}).get("severity", "low"), 2) for f in _fails)
        _wp = sum(_sw.get(SAT_CHECKS.get(f.check_id, {}).get("severity", "low"), 2) * 0.5 for f in _warns)
        d["score_breakdown"] = {
            "scored_checks": len(_applicable),
            "total_weight_pool": _tot,
            "fail_penalty": round(_fp, 1),
            "fail_count": len(_fails),
            "warn_penalty": round(_wp, 1),
            "warn_count": len(_warns),
            "pass_count": len(_passes),
            "total_penalty": round(_fp + _wp, 1),
            "grade": "Good" if self.overall_score >= 80 else ("Needs Improvement" if self.overall_score >= 60 else "Critical"),
        }
        if self.endpoint_summary:
            d["endpoint_summary"] = self.endpoint_summary
        # Category definitions
        d["category_definitions"] = [{"category": cat, "definition": defn} for cat, defn in CATEGORY_DEFINITIONS]
        # All checks reference
        d["checks_reference"] = [
            {"check_id": cid, "title": ck["title"], "category": ck["category"],
             "severity": ck["severity"], "effort": _get_effort(cid),
             "description": ck.get("description", ""),
             "recommendation": ck.get("recommendation", ""), "reference_url": ck.get("reference_url", "")}
            for cid, ck in SAT_CHECKS.items()
        ]
        # Effort methodology
        d["effort_methodology"] = {
            "effort_levels": {
                "Quick Fix (5–15 min)": "Single configuration toggle or setting change. No cross-team coordination or testing required.",
                "Moderate (1–4 hrs)": "Multi-step configuration, policy creation, or changes that require testing/validation. May involve IaC code updates or build/deploy pipelines.",
                "Significant (1–3 days)": "Architecture changes requiring cross-team coordination, downtime planning, IaC refactoring, and phased rollout.",
                "Project (1+ weeks)": "Major infrastructure migration or org-wide policy rollout. Requires project planning, stakeholder approval, IaC redesign, and multi-phase implementation.",
            },
            "estimation_factors": [
                "Configuration Steps — number of settings, APIs, or UI steps required",
                "Access Requirements — admin console, API/IaC, or Azure Portal changes",
                "Testing & Validation — functional, security, or user acceptance testing",
                "Cross-team Coordination — network, security, identity, or platform teams",
                "Blast Radius — single workspace, multiple workspaces, or entire organization",
            ],
            "prerequisites_not_included": {
                "Terraform / IaC Updates": "+30 min – 2 hrs per change",
                "CI/CD Pipeline Runs": "+15 min – 1 hr per deployment",
                "Change Management / CAB Approval": "+1 – 5 business days",
                "Non-prod Testing": "+1 – 4 hrs per environment",
                "Provider / Module Upgrades": "+1 – 4 hrs",
                "Security Review": "+1 – 3 business days",
                "Documentation Updates": "+30 min – 2 hrs",
            },
            "note": "Effort estimates reflect core remediation work and are approximate. Add prerequisite time as applicable to your organization.",
        }
        # Effort summary for actionable findings
        _actionable = [f for f in self.findings if f.status in ("FAIL", "WARN") and not f.is_api_error]
        if _actionable:
            _eff_summary: dict[str, int] = {}
            for f in _actionable:
                e = f.effort or "Moderate (1–4 hrs)"
                _eff_summary[e] = _eff_summary.get(e, 0) + 1
            d["effort_summary"] = {
                "actionable_findings": len(_actionable),
                "by_effort_level": _eff_summary,
            }
        # Prioritised recommendations
        from .scoring import _build_prioritised_recommendations
        d["prioritised_recommendations"] = _build_prioritised_recommendations(self.findings)
        d["cost_disclaimer"] = "Cost figures (cost_low, cost_high) are illustrative examples only. Actual costs vary with usage, region, and pricing tier."
        # Remediation timeline
        from .remediation import build_remediation_timeline
        d["remediation_timeline"] = build_remediation_timeline(d["prioritised_recommendations"])
        # Optional Unity Catalog + Azure inventory enrichment
        if self.inventory_obj is not None:
            try:
                d["unity_catalog_inventory"] = self.inventory_obj.to_dict()
            except Exception:
                pass
        return d
