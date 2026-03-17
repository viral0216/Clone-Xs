"""Compliance API — structured compliance reports for the Web UI."""

import logging
from datetime import datetime

from src.client import execute_sql

logger = logging.getLogger(__name__)


def generate_compliance_report_api(
    client,
    warehouse_id: str,
    config: dict,
    catalog: str,
    report_type: str = "data_governance",
    from_date: str | None = None,
    to_date: str | None = None,
) -> dict:
    """Generate a compliance report for the UI.

    Returns structured JSON with score, status, summary, and sections.
    Each section has optional table_data/table_columns for DataTable rendering.
    """
    report_runners = {
        "data_governance": _report_data_governance,
        "pii_audit": _report_pii_audit,
        "permission_audit": _report_permission_audit,
        "tag_coverage": _report_tag_coverage,
        "ownership_audit": _report_ownership_audit,
    }

    if report_type == "full_report":
        sections = []
        for runner in report_runners.values():
            sections.extend(runner(client, warehouse_id, config, catalog, from_date, to_date))
    elif report_type in report_runners:
        sections = report_runners[report_type](client, warehouse_id, config, catalog, from_date, to_date)
    else:
        sections = report_runners["data_governance"](client, warehouse_id, config, catalog, from_date, to_date)

    score, status = _compute_overall_score(sections)
    passed = sum(1 for s in sections if s.get("status") == "COMPLIANT")
    warnings = sum(1 for s in sections if s.get("status") == "WARNING")
    failures = sum(1 for s in sections if s.get("status") == "NON_COMPLIANT")

    return {
        "status": status,
        "score": score,
        "summary": {
            "catalog": catalog,
            "report_type": report_type,
            "generated_at": datetime.utcnow().isoformat(),
            "total_checks": len(sections),
            "passed": passed,
            "warnings": warnings,
            "failures": failures,
        },
        "sections": sections,
    }


def _classify_owner(owner: str) -> str:
    """Classify an owner as service_principal, group, user, or missing."""
    if not owner or owner == "(none)":
        return "missing"
    o = owner.strip().lower()
    # Service principals typically have UUID-style or app-id patterns
    if any(hint in o for hint in ("@serviceprincipal", "spn-", "sp-", "application/")):
        return "service_principal"
    # Groups often don't contain @ or have -group, -team suffixes
    if any(hint in o for hint in ("-group", "-team", "-admins", "-owners", "-readers", "-writers",
                                   "-contributors", "group:", "team:")):
        return "group"
    # If it looks like an email, it's a user
    if "@" in o:
        return "user"
    # No @, not matching SP/group patterns — likely a group or SP display name
    return "group"


def _owner_recommendation(owner_type: str) -> str:
    if owner_type == "missing":
        return "Assign a service principal or group"
    if owner_type == "user":
        return "Transfer to service principal or group"
    return ""


def _compute_overall_score(sections: list[dict]) -> tuple[int, str]:
    scores = [s["score"] for s in sections if s.get("score") is not None]
    if not scores:
        return 0, "WARNING"
    avg = sum(scores) // len(scores)
    if avg >= 80:
        return avg, "COMPLIANT"
    if avg >= 50:
        return avg, "WARNING"
    return avg, "NON_COMPLIANT"


def _section(title, description, score, items=None, table_data=None, table_columns=None):
    if score >= 80:
        status = "COMPLIANT"
    elif score >= 50:
        status = "WARNING"
    else:
        status = "NON_COMPLIANT"
    s = {"title": title, "description": description, "score": score, "status": status}
    if items:
        s["items"] = items
    if table_data is not None:
        s["table_data"] = table_data
    if table_columns is not None:
        s["table_columns"] = table_columns
    return s


# ---------------------------------------------------------------------------
# Data Governance Report
# ---------------------------------------------------------------------------

def _report_data_governance(client, warehouse_id, config, catalog, from_date, to_date):
    sections = []

    # 1. Audit Trail Coverage
    audit_cfg = config.get("audit") or config.get("audit_trail")
    if audit_cfg and isinstance(audit_cfg, dict):
        cat = audit_cfg.get("catalog", "clone_audit")
        schema = audit_cfg.get("schema", "logs")
        table = audit_cfg.get("table", "clone_operations")
        fqn = f"`{cat}`.`{schema}`.`{table}`"
        try:
            conditions = []
            if from_date:
                conditions.append(f"started_at >= '{from_date}'")
            if to_date:
                conditions.append(f"started_at <= '{to_date}'")
            where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
            rows = execute_sql(client, warehouse_id, f"SELECT * FROM {fqn} {where} ORDER BY started_at DESC LIMIT 100")
            total = len(rows)
            successful = sum(1 for r in rows if str(r.get("status", "")).upper() in ("SUCCESS", "COMPLETED"))
            failed = total - successful
            score = 100 if total > 0 else 50
            items = [
                {"name": f"{total} clone operations recorded", "status": "COMPLIANT" if total > 0 else "WARNING"},
                {"name": f"{successful} successful, {failed} failed", "status": "COMPLIANT" if failed == 0 else "WARNING"},
            ]
            sections.append(_section("Audit Trail Coverage", f"Audit trail at {fqn} — {total} operations found.", score, items=items))
        except Exception as e:
            sections.append(_section("Audit Trail Coverage", f"Could not query audit trail: {e}", 30,
                                     items=[{"name": str(e), "status": "NON_COMPLIANT"}]))
    else:
        sections.append(_section("Audit Trail Coverage", "Audit trail not configured in settings.", 0,
                                 items=[{"name": "Configure audit trail in Settings to enable tracking", "status": "NON_COMPLIANT"}]))

    # 2. Validation Settings
    validate = config.get("validate_after_clone", False)
    checksum = config.get("validate_checksum", False)
    val_score = (50 if validate else 0) + (50 if checksum else 0)
    sections.append(_section(
        "Validation Settings",
        "Post-clone validation ensures data integrity.",
        val_score,
        items=[
            {"name": f"Post-clone validation: {'Enabled' if validate else 'Disabled'}", "status": "COMPLIANT" if validate else "WARNING"},
            {"name": f"Checksum validation: {'Enabled' if checksum else 'Disabled'}", "status": "COMPLIANT" if checksum else "WARNING"},
        ],
    ))

    # 3. Clone Configuration Hygiene
    hygiene_checks = [
        ("copy_permissions", "Copy Permissions"),
        ("copy_ownership", "Copy Ownership"),
        ("copy_tags", "Copy Tags"),
        ("enable_rollback", "Rollback Enabled"),
    ]
    enabled = sum(1 for k, _ in hygiene_checks if config.get(k, False))
    hyg_score = (enabled * 100) // len(hygiene_checks)
    items = [{"name": f"{label}: {'Enabled' if config.get(key, False) else 'Disabled'}",
              "status": "COMPLIANT" if config.get(key, False) else "WARNING"}
             for key, label in hygiene_checks]
    sections.append(_section("Clone Configuration Hygiene", "Best-practice clone settings.", hyg_score, items=items))

    return sections


# ---------------------------------------------------------------------------
# PII Audit Report
# ---------------------------------------------------------------------------

def _report_pii_audit(client, warehouse_id, config, catalog, from_date, to_date):
    from src.pii_detection import detect_pii_by_column_names, HIGH_RISK_TYPES

    sections = []

    try:
        findings = detect_pii_by_column_names(client, warehouse_id, catalog,
                                               config.get("exclude_schemas", []))
    except Exception as e:
        return [_section("PII Column Detection", f"Failed to scan: {e}", 0,
                         items=[{"name": str(e), "status": "NON_COMPLIANT"}])]

    pii_count = len(findings)
    high_risk = [f for f in findings if f.get("pii_type") in HIGH_RISK_TYPES]

    # 1. PII Column Detection
    det_score = max(0, 100 - (pii_count * 5))  # -5 per PII column found
    table_data = [{
        "schema": f["schema"], "table": f["table"], "column": f["column"],
        "data_type": f.get("data_type", ""), "pii_type": f["pii_type"],
        "confidence": f.get("confidence", ""), "masking": f.get("suggested_masking", ""),
    } for f in findings[:100]]
    table_columns = [
        {"key": "schema", "label": "Schema"},
        {"key": "table", "label": "Table"},
        {"key": "column", "label": "Column"},
        {"key": "pii_type", "label": "PII Type"},
        {"key": "confidence", "label": "Confidence"},
        {"key": "masking", "label": "Suggested Masking"},
    ]
    sections.append(_section(
        "PII Column Detection",
        f"Found {pii_count} potential PII columns in catalog '{catalog}'.",
        det_score,
        items=[{"name": f"{pii_count} PII columns detected", "status": "WARNING" if pii_count > 0 else "COMPLIANT"}],
        table_data=table_data if table_data else None,
        table_columns=table_columns if table_data else None,
    ))

    # 2. Masking Coverage
    masking_rules = config.get("masking_rules") or []
    masked_cols = {r.get("column", "") for r in masking_rules} if masking_rules else set()
    unmasked = [f for f in findings if f.get("column") not in masked_cols]
    mask_score = 100 if pii_count == 0 else max(0, 100 - int((len(unmasked) / max(pii_count, 1)) * 100))
    sections.append(_section(
        "Masking Coverage",
        f"{len(masking_rules)} masking rules configured, {len(unmasked)} PII columns without rules.",
        mask_score,
        items=[
            {"name": f"{len(masking_rules)} masking rules defined", "status": "COMPLIANT" if masking_rules else "WARNING"},
            {"name": f"{len(unmasked)} PII columns unmasked", "status": "NON_COMPLIANT" if unmasked else "COMPLIANT"},
        ],
    ))

    # 3. High-Risk PII Exposure
    hr_unmasked = [f for f in high_risk if f.get("column") not in masked_cols]
    hr_score = 0 if hr_unmasked else 100
    sections.append(_section(
        "High-Risk PII Exposure",
        f"{len(high_risk)} high-risk PII columns found ({', '.join(set(f['pii_type'] for f in high_risk)) or 'none'}).",
        hr_score,
        items=[
            {"name": f"{len(high_risk)} high-risk columns (SSN, credit cards, credentials, etc.)",
             "status": "WARNING" if high_risk else "COMPLIANT"},
            {"name": f"{len(hr_unmasked)} high-risk columns without masking",
             "status": "NON_COMPLIANT" if hr_unmasked else "COMPLIANT"},
        ],
    ))

    return sections


# ---------------------------------------------------------------------------
# Permission Audit Report
# ---------------------------------------------------------------------------

def _report_permission_audit(client, warehouse_id, config, catalog, from_date, to_date):
    sections = []

    # 1. Catalog Grants
    try:
        grants = execute_sql(client, warehouse_id, f"SHOW GRANTS ON CATALOG `{catalog}`")
        broad_grants = [g for g in grants if "ALL" in str(g.get("ActionType", g.get("privilege", ""))).upper()]
        grant_score = max(0, 100 - (len(broad_grants) * 20))

        table_data = [{
            "principal": g.get("Principal", g.get("principal", "")),
            "privilege": g.get("ActionType", g.get("privilege", "")),
            "object_type": g.get("ObjectType", g.get("object_type", "")),
            "object_key": g.get("ObjectKey", g.get("object_key", "")),
        } for g in grants]
        table_columns = [
            {"key": "principal", "label": "Principal"},
            {"key": "privilege", "label": "Privilege"},
            {"key": "object_type", "label": "Object Type"},
            {"key": "object_key", "label": "Object"},
        ]
        items = [
            {"name": f"{len(grants)} grants on catalog '{catalog}'", "status": "COMPLIANT"},
            {"name": f"{len(broad_grants)} overly broad grants (ALL PRIVILEGES)",
             "status": "NON_COMPLIANT" if broad_grants else "COMPLIANT"},
        ]
        sections.append(_section("Catalog Grants", f"{len(grants)} grants found.", grant_score,
                                 items=items, table_data=table_data, table_columns=table_columns))
    except Exception as e:
        sections.append(_section("Catalog Grants", f"Could not query grants: {e}", 30,
                                 items=[{"name": str(e), "status": "NON_COMPLIANT"}]))

    # 2. Table Ownership (with owner type classification)
    try:
        rows = execute_sql(client, warehouse_id, f"""
            SELECT table_schema, table_name, table_owner
            FROM {catalog}.information_schema.tables
            WHERE table_schema NOT IN ('information_schema', 'default')
        """)
        total = len(rows)
        no_owner = [r for r in rows if not r.get("table_owner")]
        user_owned = [r for r in rows if _classify_owner(r.get("table_owner", "")) == "user"]
        sp_or_group = [r for r in rows if _classify_owner(r.get("table_owner", "")) in ("service_principal", "group")]

        # Score: penalize missing owners heavily, user-owned partially
        missing_pct = len(no_owner) / max(total, 1)
        user_pct = len(user_owned) / max(total, 1)
        own_score = max(0, int(100 - (missing_pct * 100) - (user_pct * 25))) if total > 0 else 50

        items = [
            {"name": f"{total} tables found", "status": "COMPLIANT"},
            {"name": f"{len(no_owner)} tables without owner",
             "status": "NON_COMPLIANT" if no_owner else "COMPLIANT"},
            {"name": f"{len(user_owned)} tables owned by individual users (recommend service principal or group)",
             "status": "WARNING" if user_owned else "COMPLIANT"},
            {"name": f"{len(sp_or_group)} tables owned by service principal or group",
             "status": "COMPLIANT"},
        ]

        # Show tables that need attention (missing + user-owned)
        needs_attention = no_owner + user_owned
        if needs_attention:
            table_data = [{
                "schema": r["table_schema"],
                "table": r["table_name"],
                "owner": r.get("table_owner") or "(none)",
                "owner_type": _classify_owner(r.get("table_owner", "")),
                "recommendation": _owner_recommendation(_classify_owner(r.get("table_owner", ""))),
            } for r in needs_attention[:50]]
            table_columns = [
                {"key": "schema", "label": "Schema"}, {"key": "table", "label": "Table"},
                {"key": "owner", "label": "Current Owner"}, {"key": "owner_type", "label": "Type"},
                {"key": "recommendation", "label": "Recommendation"},
            ]
            sections.append(_section(
                "Table Ownership",
                f"{len(needs_attention)} of {total} tables need ownership review — service principal or group ownership is recommended.",
                own_score, items=items, table_data=table_data, table_columns=table_columns,
            ))
        else:
            sections.append(_section("Table Ownership", f"All {total} tables are properly owned by service principals or groups.",
                                     own_score, items=items))
    except Exception as e:
        sections.append(_section("Table Ownership", f"Could not query tables: {e}", 30,
                                 items=[{"name": str(e), "status": "NON_COMPLIANT"}]))

    return sections


# ---------------------------------------------------------------------------
# Tag Coverage Report
# ---------------------------------------------------------------------------

def _report_tag_coverage(client, warehouse_id, config, catalog, from_date, to_date):
    sections = []

    # 1. Table Tag Coverage
    try:
        all_tables = execute_sql(client, warehouse_id, f"""
            SELECT table_schema, table_name
            FROM {catalog}.information_schema.tables
            WHERE table_schema NOT IN ('information_schema', 'default')
        """)
        total_tables = len(all_tables)

        tagged_tables = execute_sql(client, warehouse_id, f"""
            SELECT DISTINCT schema_name, table_name
            FROM {catalog}.information_schema.table_tags
        """)
        tagged_count = len(tagged_tables)
        pct = int((tagged_count / max(total_tables, 1)) * 100) if total_tables > 0 else 0
        score = pct

        untagged = []
        tagged_set = {(r["schema_name"], r["table_name"]) for r in tagged_tables}
        for t in all_tables:
            if (t["table_schema"], t["table_name"]) not in tagged_set:
                untagged.append({"schema": t["table_schema"], "table": t["table_name"]})

        items = [
            {"name": f"{tagged_count} of {total_tables} tables have tags ({pct}%)",
             "status": "COMPLIANT" if pct >= 80 else "WARNING" if pct >= 50 else "NON_COMPLIANT"},
        ]
        table_data = untagged[:50] if untagged else None
        table_columns = [{"key": "schema", "label": "Schema"}, {"key": "table", "label": "Table"}] if untagged else None
        sections.append(_section("Table Tag Coverage", f"{pct}% of tables have at least one tag.",
                                 score, items=items, table_data=table_data, table_columns=table_columns))
    except Exception as e:
        sections.append(_section("Table Tag Coverage", f"Could not query tags: {e}", 30,
                                 items=[{"name": str(e), "status": "WARNING"}]))

    # 2. Column Tag Coverage
    try:
        col_tags = execute_sql(client, warehouse_id, f"""
            SELECT DISTINCT schema_name, table_name, column_name
            FROM {catalog}.information_schema.column_tags
        """)
        tagged_col_count = len(col_tags)
        col_score = min(100, tagged_col_count * 5)  # rough heuristic
        sections.append(_section(
            "Column Tag Coverage",
            f"{tagged_col_count} columns have tags across the catalog.",
            col_score,
            items=[{"name": f"{tagged_col_count} tagged columns found",
                    "status": "COMPLIANT" if tagged_col_count > 0 else "WARNING"}],
        ))
    except Exception as e:
        sections.append(_section("Column Tag Coverage", f"Could not query column tags: {e}", 30,
                                 items=[{"name": str(e), "status": "WARNING"}]))

    return sections


# ---------------------------------------------------------------------------
# Ownership Audit Report
# ---------------------------------------------------------------------------

def _report_ownership_audit(client, warehouse_id, config, catalog, from_date, to_date):
    sections = []

    # 1. Table Ownership (with owner type classification)
    try:
        rows = execute_sql(client, warehouse_id, f"""
            SELECT table_schema, table_name, table_owner
            FROM {catalog}.information_schema.tables
            WHERE table_schema NOT IN ('information_schema', 'default')
        """)
        total = len(rows)
        no_owner = [r for r in rows if not r.get("table_owner")]
        user_owned = [r for r in rows if _classify_owner(r.get("table_owner", "")) == "user"]
        sp_owned = [r for r in rows if _classify_owner(r.get("table_owner", "")) == "service_principal"]
        group_owned = [r for r in rows if _classify_owner(r.get("table_owner", "")) == "group"]
        with_owner = total - len(no_owner)
        pct = int((with_owner / max(total, 1)) * 100) if total > 0 else 0

        # Score: full marks only if owned by SP or group, penalty for user-owned
        missing_pct = len(no_owner) / max(total, 1)
        user_pct = len(user_owned) / max(total, 1)
        own_score = max(0, int(100 - (missing_pct * 100) - (user_pct * 25)))

        table_data = [{
            "schema": r["table_schema"],
            "table": r["table_name"],
            "owner": r.get("table_owner") or "(none)",
            "owner_type": _classify_owner(r.get("table_owner", "")),
            "recommendation": _owner_recommendation(_classify_owner(r.get("table_owner", ""))),
        } for r in rows[:100]]
        table_columns = [
            {"key": "schema", "label": "Schema"}, {"key": "table", "label": "Table"},
            {"key": "owner", "label": "Owner"}, {"key": "owner_type", "label": "Type"},
            {"key": "recommendation", "label": "Recommendation"},
        ]
        sections.append(_section(
            "Table Ownership",
            f"{with_owner} of {total} tables have owners ({pct}%). Service principal or group ownership is recommended.",
            own_score,
            items=[
                {"name": f"{with_owner}/{total} tables have assigned owners", "status": "COMPLIANT" if pct >= 80 else "WARNING"},
                {"name": f"{len(no_owner)} tables missing owner", "status": "NON_COMPLIANT" if no_owner else "COMPLIANT"},
                {"name": f"{len(user_owned)} tables owned by individual users",
                 "status": "WARNING" if user_owned else "COMPLIANT"},
                {"name": f"{len(sp_owned)} tables owned by service principals",
                 "status": "COMPLIANT"},
                {"name": f"{len(group_owned)} tables owned by groups",
                 "status": "COMPLIANT"},
            ],
            table_data=table_data, table_columns=table_columns,
        ))
    except Exception as e:
        sections.append(_section("Table Ownership", f"Could not query: {e}", 0,
                                 items=[{"name": str(e), "status": "NON_COMPLIANT"}]))

    # 2. Schema Ownership (with owner type classification)
    try:
        schemas = execute_sql(client, warehouse_id, f"""
            SELECT schema_name, schema_owner
            FROM {catalog}.information_schema.schemata
            WHERE schema_name NOT IN ('information_schema', 'default')
        """)
        total_s = len(schemas)
        no_owner_s = [s for s in schemas if not s.get("schema_owner")]
        user_owned_s = [s for s in schemas if _classify_owner(s.get("schema_owner", "")) == "user"]
        sp_or_group_s = [s for s in schemas if _classify_owner(s.get("schema_owner", "")) in ("service_principal", "group")]
        missing_pct_s = len(no_owner_s) / max(total_s, 1)
        user_pct_s = len(user_owned_s) / max(total_s, 1)
        pct_s = max(0, int(100 - (missing_pct_s * 100) - (user_pct_s * 25)))

        table_data_s = [{
            "schema": s["schema_name"],
            "owner": s.get("schema_owner") or "(none)",
            "owner_type": _classify_owner(s.get("schema_owner", "")),
            "recommendation": _owner_recommendation(_classify_owner(s.get("schema_owner", ""))),
        } for s in schemas]
        table_columns_s = [
            {"key": "schema", "label": "Schema"}, {"key": "owner", "label": "Owner"},
            {"key": "owner_type", "label": "Type"}, {"key": "recommendation", "label": "Recommendation"},
        ]
        sections.append(_section(
            "Schema Ownership",
            f"{total_s - len(no_owner_s)} of {total_s} schemas have owners. Service principal or group ownership is recommended.",
            pct_s,
            items=[
                {"name": f"{total_s} schemas found", "status": "COMPLIANT"},
                {"name": f"{len(no_owner_s)} schemas missing owner",
                 "status": "NON_COMPLIANT" if no_owner_s else "COMPLIANT"},
                {"name": f"{len(user_owned_s)} schemas owned by individual users",
                 "status": "WARNING" if user_owned_s else "COMPLIANT"},
                {"name": f"{len(sp_or_group_s)} schemas owned by service principal or group",
                 "status": "COMPLIANT"},
            ],
            table_data=table_data_s, table_columns=table_columns_s,
        ))
    except Exception as e:
        sections.append(_section("Schema Ownership", f"Could not query: {e}", 0,
                                 items=[{"name": str(e), "status": "NON_COMPLIANT"}]))

    return sections
