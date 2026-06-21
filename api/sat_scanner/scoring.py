"""SAT Scanner — score computation and summary display."""

from __future__ import annotations

from .models import SATFinding, SATScanResult
from .checks import SAT_CHECKS, _ITEM_EXTRACTORS
from .helpers import _pl


SEV_WEIGHT = {"critical": 10, "high": 7, "medium": 4, "low": 2}


def _compute_sat_score(findings: list[SATFinding]) -> tuple[int, dict[str, int]]:
    WARN_DISCOUNT = 0.5  # WARNs penalise at half the weight of FAILs
    # Exclude API errors from scoring — they are infrastructure issues, not security findings
    scorable = [f for f in findings if not f.is_api_error]
    by_cat: dict[str, list[SATFinding]] = {}
    for f in scorable:
        by_cat.setdefault(f.category, []).append(f)

    def _penalty(findings_list: list[SATFinding]) -> tuple[float, float]:
        """Return (total_pts, penalty_pts) for a list of applicable findings."""
        total = sum(SEV_WEIGHT.get(SAT_CHECKS.get(f.check_id, {}).get("severity", "low"), 2) for f in findings_list)
        penalty = 0.0
        for f in findings_list:
            w = SEV_WEIGHT.get(SAT_CHECKS.get(f.check_id, {}).get("severity", "low"), 2)
            if f.status == "FAIL":
                penalty += w
            elif f.status == "WARN":
                penalty += w * WARN_DISCOUNT
        return total, penalty

    cat_scores: dict[str, int] = {}
    for cat, cat_findings in by_cat.items():
        applicable = [f for f in cat_findings if f.status != "NOT_APPLICABLE"]
        if not applicable:
            cat_scores[cat] = 100
            continue
        total_pts, fail_pts = _penalty(applicable)
        cat_scores[cat] = round((1 - (fail_pts / total_pts)) * 100) if total_pts else 100

    applicable = [f for f in scorable if f.status != "NOT_APPLICABLE"]
    if not applicable:
        return 100, cat_scores
    total_pts, fail_pts = _penalty(applicable)
    overall = round((1 - (fail_pts / total_pts)) * 100) if total_pts else 100
    return overall, cat_scores


def _build_endpoint_summary(findings: list[SATFinding]) -> dict:
    """Build a summary of all API endpoints queried and their results.

    Returns a dict with keys:
        total, with_items, config, empty, error,
        endpoints: list of {endpoint, status, items_count, error_code}
    """
    ep_with_items: dict[str, int] = {}
    ep_config: set[str] = set()
    ep_empty: set[str] = set()
    ep_error: dict[str, int] = {}  # endpoint -> HTTP status code
    for f in findings:
        ep = f.details.get("api_endpoint", "")
        if not ep:
            continue
        items = f.details.get("items", [])
        api_resp = f.details.get("api_response")
        err_code = f.details.get("api_error_code")
        if items:
            ep_with_items[ep] = max(ep_with_items.get(ep, 0), len(items))
        elif err_code:
            ep_error.setdefault(ep, err_code)
        else:
            # Determine if this is a list endpoint (returns items) or a config endpoint
            # by checking the primary path against _ITEM_EXTRACTORS
            primary = ep.split(" + ")[0].split("?")[0]
            if primary in _ITEM_EXTRACTORS:
                ep_empty.add(ep)  # list endpoint with 0 items
            elif isinstance(api_resp, dict) and api_resp:
                ep_config.add(ep)
            else:
                ep_empty.add(ep)
    # An endpoint with items in one check might show as config/error in another — items wins
    ep_error = {k: v for k, v in ep_error.items() if k not in ep_with_items}
    ep_config -= set(ep_with_items.keys()) | set(ep_error.keys())
    ep_empty -= set(ep_with_items.keys()) | ep_config | set(ep_error.keys())
    endpoints: list[dict] = []
    for ep, count in sorted(ep_with_items.items(), key=lambda x: -x[1]):
        endpoints.append({"endpoint": ep, "status": "items", "items_count": count, "error_code": 0})
    for ep in sorted(ep_config):
        endpoints.append({"endpoint": ep, "status": "config", "items_count": 0, "error_code": 0})
    for ep in sorted(ep_empty):
        endpoints.append({"endpoint": ep, "status": "empty", "items_count": 0, "error_code": 0})
    for ep, code in sorted(ep_error.items()):
        endpoints.append({"endpoint": ep, "status": "error", "items_count": 0, "error_code": code})
    return {
        "total": len(ep_with_items) + len(ep_config) + len(ep_empty) + len(ep_error),
        "with_items": len(ep_with_items),
        "config": len(ep_config),
        "empty": len(ep_empty),
        "error": len(ep_error),
        "endpoints": endpoints,
    }


EFFORT_MULTIPLIER = {
    "Quick Fix (5–15 min)": 4,
    "Moderate (1–4 hrs)": 3,
    "Significant (1–3 days)": 2,
    "Project (1+ weeks)": 1,
}

# ── Estimated monthly cloud operational cost of each misconfiguration ──
# IMPORTANT: These are *illustrative examples only*, not guarantees.
# Values are (low, high) in USD/month based on typical Azure Databricks
# pricing for a mid-size workspace (~20 clusters, ~5 SQL warehouses,
# ~50 scheduled jobs).  Actual cost varies significantly with usage,
# region, pricing tier, and workload patterns.
# "None" = security/governance only, no direct spend.
# These are *per-workspace* estimates.
OPERATIONAL_COST_MAP: dict[str, tuple[int, int, str]] = {
    # ── Cost Optimization ──
    "SAT-COST-1":  (500,  5000,  "Over-provisioned fixed-size clusters waste compute when idle or under partial load"),
    "SAT-COST-2":  (1000, 8000,  "On-demand VMs cost 60-90% more than Azure Spot instances for fault-tolerant workloads"),
    "SAT-COST-3":  (500,  3000,  "Clusters without auto-termination run 24/7 even when unused"),
    "SAT-COST-4":  (0,    0,     "Missing cost tags prevent accurate chargeback — indirect cost visibility impact"),
    "SAT-COST-5":  (200,  1500,  "Without instance pools, every cluster start pays full VM boot time (~5 min × DBU rate)"),
    # ── Compute ──
    "SAT-COMPUTE-4": (500,  5000,  "Fixed-size interactive clusters waste compute during idle/low-use periods"),
    "SAT-COMPUTE-PHOTON": (300, 2000, "Non-Photon clusters run queries 2-8× slower, consuming more DBU-hours"),
    # ── Performance ──
    "SAT-PERF-1":  (300,  2000,  "Photon-eligible clusters without Photon use 2-8× more DBU-hours for the same queries"),
    "SAT-PERF-2":  (500,  4000,  "Classic SQL warehouses pay for always-on infra; serverless auto-scales to zero"),
    "SAT-PERF-3":  (100,  500,   "End-of-support runtimes miss performance optimizations in newer Spark/Photon releases"),
    "SAT-PERF-4":  (200,  1500,  "Without AQE, queries use static execution plans leading to skew, spills, and excess shuffles"),
    "SAT-PERF-5":  (200,  2000,  "Without scheduled OPTIMIZE/VACUUM, tables accumulate small files → slower reads → more DBUs"),
    # ── SQL Warehouses ──
    "SAT-SQL-1":   (500,  3000,  "Warehouses without auto-stop run continuously even when idle"),
    # ── Table Optimization ──
    "SAT-OPT-PRED-CATALOG":  (200,  2000,  "Without predictive optimization, tables miss automatic OPTIMIZE/VACUUM/ANALYZE"),
    "SAT-OPT-OPTIMIZE-WRITE":(100,  1000,  "Without optimized writes, ingestion creates many small files → expensive reads"),
    "SAT-OPT-AUTO-COMPACT":  (100,  1000,  "Without auto compaction, small files accumulate → slower queries → more DBUs"),
    "SAT-OPT-DELTA-CACHE":   (100,  800,   "Without Delta cache, repeated reads hit remote storage (slower, higher I/O cost)"),
    "SAT-OPT-MAINT-SCHEDULE":(200,  2000,  "Ad-hoc-only maintenance misses runs, causing small-file bloat and higher scan costs"),
    "SAT-OPT-WH-PHOTON":     (300,  2000,  "SQL warehouses without Photon process queries slower, consuming more DBU-hours"),
    "SAT-OPT-WH-AUTO-STOP":  (500,  3000,  "Warehouses without timely auto-stop idle at full cost between queries"),
    "SAT-OPT-SERVERLESS-JOBS":(300,  3000,  "All-purpose clusters for jobs pay for idle time between tasks; serverless/job clusters don't"),
    "SAT-OPT-LIQUID-CLUSTER": (100,  1000,  "Without Liquid Clustering, queries scan more data due to suboptimal file layout"),
    # ── Ops Excellence ──
    "SAT-OPS-3":   (500,  3000,  "Idle interactive clusters consume DBUs while no users are connected"),
    # ── Data Quality ──
    "SAT-DQ-COMPUTE-RIGHT-SIZE": (200, 2000, "Autoscale max >> min means clusters over-provision workers during scale-up"),
    "SAT-DQ-DLT-FRESHNESS":     (100, 500,  "Stale DLT pipelines may indicate wasted scheduled compute with no useful output"),
    "SAT-DQ-PIPELINE-ERRORS":   (100, 500,  "Failing pipelines waste compute on retries without producing results"),
    # ── Reliability ──
    "SAT-REL-1":   (100,  500,   "Jobs without retry/timeout settings waste compute on hung or repeatedly-failing runs"),
    "SAT-REL-2":   (200,  1000,  "Low success-rate jobs burn compute on failures; each failed run is wasted DBU spend"),
    # ── Cost Optimization (Extended) ──
    "SAT-COST-ZOMBIE-VOLUMES":    (100,  800,   "Unused UC Volumes accumulate storage costs without providing value"),
    "SAT-COST-ABANDONED-TABLES":  (200,  2000,  "Abandoned tables consume storage and maintenance compute indefinitely"),
    "SAT-COST-OVERPROVISIONED":   (500,  5000,  "Over-provisioned clusters waste 40-70% of their compute budget on idle capacity"),
    "SAT-COST-STORAGE-TIERING":   (200,  3000,  "Large tables on premium storage without tiering pay hot-tier rates for cold data"),
    # ── Serverless Governance ──
    "SAT-SRVL-BUDGET":   (500,  5000,  "Serverless compute without budget alerts can spike unexpectedly with no warning"),
    "SAT-SRVL-WH-SIZING":(200,  2000,  "Over-sized serverless warehouses waste DBUs on idle capacity beyond actual demand"),
    # ── Operations (Extended) ──
    "SAT-OPS-DR-PLAN":   (0,    0,     "DR plan is governance — but an outage without DR can cost $10K-$100K/hour in downtime"),
}

PRIORITY_LABELS = {
    range(28, 100): "P1 — Fix Immediately",
    range(16, 28):  "P2 — Fix This Sprint",
    range(7, 16):   "P3 — Plan for Next Sprint",
    range(0, 7):    "P4 — Backlog",
}


def _priority_score(finding: SATFinding) -> int:
    """Compute priority score for a finding: severity_weight × effort_multiplier.

    Higher score = fix first (high severity + quick fix bubbles to top).
    """
    sev = SAT_CHECKS.get(finding.check_id, {}).get("severity", finding.severity)
    weight = SEV_WEIGHT.get(sev, 2)
    effort = finding.effort or "Moderate (1–4 hrs)"
    multiplier = EFFORT_MULTIPLIER.get(effort, 3)
    return weight * multiplier


def _priority_label(score: int) -> str:
    """Return the priority label (P1–P4) for a given priority score."""
    for rng, label in PRIORITY_LABELS.items():
        if score in rng:
            return label
    return "P4 — Backlog"


def _estimated_cost(check_id: str) -> tuple[int, int, str]:
    """Return (low, high, reason) monthly operational cost estimate for a check.

    Returns (0, 0, "") for checks with no direct cloud cost impact.
    """
    return OPERATIONAL_COST_MAP.get(check_id, (0, 0, ""))


def _build_prioritised_recommendations(findings: list[SATFinding]) -> list[dict]:
    """Build a prioritised list of actionable findings (FAIL + WARN), sorted by priority score.

    Returns list of dicts with keys: check_id, category, severity, status, effort,
    title, recommendation, benefits, portal_link, reference_url, priority_score,
    priority_label, cost_low, cost_high, cost_reason.
    """
    actionable = [f for f in findings if f.status in ("FAIL", "WARN") and not f.is_api_error]
    items = []
    for f in actionable:
        score = _priority_score(f)
        sev = SAT_CHECKS.get(f.check_id, {}).get("severity", f.severity)
        cost_low, cost_high, cost_reason = _estimated_cost(f.check_id)
        items.append({
            "check_id": f.check_id,
            "category": f.category,
            "severity": sev,
            "status": f.status,
            "effort": f.effort or "Moderate (1–4 hrs)",
            "title": f.title,
            "current_state": f.current_state,
            "recommendation": f.recommendation,
            "benefits": f.benefits or "",
            "portal_link": f.portal_link or "",
            "reference_url": f.reference_url,
            "priority_score": score,
            "priority_label": _priority_label(score),
            "cost_low": cost_low,
            "cost_high": cost_high,
            "cost_reason": cost_reason,
        })
    # Enrich with remediation plans
    from .remediation import generate_remediation_plan
    from .checks import CHECK_REMEDIATION_OVERRIDES
    for item in items:
        item["remediation_plan"] = generate_remediation_plan(
            check_id=item["check_id"],
            title=item["title"],
            category=item["category"],
            severity=item["severity"],
            effort=item["effort"],
            recommendation=item["recommendation"],
            status=item["status"],
            overrides=CHECK_REMEDIATION_OVERRIDES.get(item["check_id"]),
        )
    items.sort(key=lambda x: (-x["priority_score"], x["check_id"]))
    return items


def _print_summary(result: SATScanResult):
    """Print a colourful console summary."""
    print(f"\n{'='*70}")
    print(f"  SCAN COMPLETE — Overall Score: {result.overall_score}/100")
    print(f"{'='*70}")
    print(f"  Total Checks:   {result.total_checks}")
    print(f"  ✅ {_pl(result.passed, 'Passed', 'Passed')}")
    print(f"  ❌ {_pl(result.failed, 'Failed', 'Failed')}")
    print(f"  ⚠️  {_pl(result.warnings, 'Warning')}")
    print(f"  ➖ {_pl(result.not_applicable, 'N/A', 'N/A')}")
    if result.api_errors:
        print(f"  🔌 {_pl(result.api_errors, 'API Error')}")
    print()

    grade = "Good (80-100)" if result.overall_score >= 80 else ("Needs Improvement (60-79)" if result.overall_score >= 60 else "Critical (0-59)")
    print(f"  Grade: {grade}")
    print()

    # Category scores
    print(f"  {'Category':<35} {'Score':>6}  {'Grade':<20}")
    print(f"  {'─'*35} {'─'*6}  {'─'*20}")
    for cat, score in sorted(result.category_scores.items(), key=lambda x: x[1]):
        g = "Good" if score >= 80 else ("Needs Improvement" if score >= 60 else "Critical")
        indicator = "✅" if score >= 80 else ("⚠️ " if score >= 60 else "❌")
        print(f"  {indicator} {cat:<33} {score:>5}/100  {g}")
    print()

    # Top failed checks
    failed = [f for f in result.findings if f.status == "FAIL"]
    if failed:
        print(f"  ❌ Top {_pl(len(failed), 'Failed Check')}:")
        print(f"  {'─'*66}")
        for f in failed[:15]:
            sev_icon = {"critical": "🔴", "high": "🟠", "medium": "🟡", "low": "🔵"}.get(f.severity, "⚪")
            print(f"    {sev_icon} [{f.check_id}] {f.title}")
            print(f"       {f.current_state[:100]}")
            # Show secret scan findings table in console
            if f.details and "findings" in f.details:
                secret_findings = f.details["findings"]
                print(f"       {'─'*60}")
                print(f"       {'#':<4} {'Detector':<16} {'Source File':<40} {'Line':<6}")
                print(f"       {'─'*60}")
                for si, sf in enumerate(secret_findings[:10], 1):
                    det = str(sf.get("detector_name", "unknown"))[:15]
                    src = sf.get("source_file", "unknown").replace("__", "/")
                    # Truncate long paths
                    if len(src) > 38:
                        src = "..." + src[-35:]
                    line_no = str(sf.get("line_number", "—"))
                    print(f"       {si:<4} {det:<16} {src:<40} {line_no:<6}")
                if len(secret_findings) > 10:
                    print(f"       ... and {len(secret_findings) - 10} more")
                print(f"       {'─'*60}")
        if len(failed) > 15:
            print(f"    ... and {len(failed) - 15} more")
        print()

    # API Errors — checks that could not be evaluated due to API failures
    api_errs = [f for f in result.findings if f.is_api_error]
    if api_errs:
        print(f"  🔌 API Errors ({len(api_errs)} checks could not be evaluated):")
        print(f"  {'─'*66}")
        # Group by HTTP status for cleaner output
        by_http: dict[str, list[SATFinding]] = {}
        for f in api_errs:
            http_code = f.details.get("http_status", "N/A") if f.details else "N/A"
            label = f"HTTP {http_code}" if http_code and http_code != "N/A" else "Exception"
            by_http.setdefault(label, []).append(f)
        for code_label, errs in sorted(by_http.items()):
            # Show justification for this HTTP error group (same justification for all in group)
            justification = errs[0].details.get("justification", "") if errs[0].details else ""
            print(f"    {code_label} ({len(errs)} checks):")
            if justification:
                print(f"      Reason: {justification}")
            for f in errs[:10]:
                print(f"      [{f.check_id}] {f.title}")
                print(f"        {f.current_state[:100]}")
            if len(errs) > 10:
                print(f"      ... and {len(errs) - 10} more")
        print()
        print(f"  ℹ️  API errors are excluded from the security score.")
        print(f"     Fix permission/connectivity issues and re-scan for complete results.")
        print()
