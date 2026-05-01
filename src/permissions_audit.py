"""Permissions audit — surface risky GRANTs across a catalog.

The Catalog Explorer's PII tab finds *which* columns hold sensitive
data; this module finds *who can read them*. Together they give the
complete picture: "the customer SSN column is in `prod.crm.users` and
account users can SELECT it."

Approach: one bulk query against `<catalog>.information_schema.
table_privileges` returns every (grantee × table × privilege) tuple
in the catalog. Each tuple is classified into a risk level
(CRITICAL / HIGH / MEDIUM / LOW) using a small set of heuristics:

- **Public groups** (`account users`, `users`) with SELECT or write
  privileges are inherently risky.
- **Write privileges** (`MODIFY`, `ALL PRIVILEGES`) escalate the risk
  for any non-owner principal.
- **Tables with PII detections** push their findings up one risk
  level — a public-group SELECT on a regular table is HIGH; on a
  PII-bearing table it's CRITICAL.

PII intersection is opt-in: pass `pii_columns` (the `columns` list
from `scan_catalog_for_pii`) to flag tables whose schema.name appears
there. When omitted, all classifications use base rules and findings
list `pii_columns: []`.

Failure isolation: per-catalog (no fan-out here — see
`permissions_audit_multi`). If `information_schema.table_privileges`
is inaccessible the function returns `findings: []` plus a single
`error` field rather than raising — keeps the UI's audit tab
gracefully degraded.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any

from databricks.sdk import WorkspaceClient

from src.client import execute_sql

logger = logging.getLogger(__name__)


# Principals that grant access to "everyone" (or close to it). When
# any of these holds a risky privilege on a table, escalate.
PUBLIC_PRINCIPALS = frozenset({
    "account users",  # all UC-enabled users in the workspace's account
    "users",          # legacy / hive-style "all users" group
})

# Privileges that allow data access (read or write). Owners and
# `MANAGE` privilege are intentionally separate — owners are the legit
# control plane, and MANAGE is a privilege-grant rather than a data op.
READ_PRIVILEGES = frozenset({"SELECT"})
WRITE_PRIVILEGES = frozenset({"MODIFY", "ALL PRIVILEGES", "ALL_PRIVILEGES"})
DESTRUCTIVE_PRIVILEGES = frozenset({"ALL PRIVILEGES", "ALL_PRIVILEGES"})

_RISK_ORDER = {"INFO": 0, "LOW": 1, "MEDIUM": 2, "HIGH": 3, "CRITICAL": 4}


def _is_public(principal: str) -> bool:
    """A principal is "public" when granting it makes data widely visible.
    Case-insensitive — UC normalises but `SHOW GRANTS` output retains
    casing in some surfaces, so we lower() defensively."""
    return (principal or "").strip().lower() in PUBLIC_PRINCIPALS


def _classify_finding(
    *,
    principal: str,
    privileges: set[str],
    has_pii: bool,
    grantor: str | None,
) -> tuple[str, str]:
    """Pick (risk_level, suggested_action) for one (principal × table)
    grant cluster. Pure function — easy to unit-test by varying inputs.
    """
    privs_upper = {p.upper() for p in privileges}
    is_public = _is_public(principal)
    has_destructive = bool(privs_upper & DESTRUCTIVE_PRIVILEGES)
    has_write = bool(privs_upper & WRITE_PRIVILEGES)
    has_read = bool(privs_upper & READ_PRIVILEGES)

    # CRITICAL: public group has any access (read or write) to a
    # PII-bearing table. The marquee finding for the PII × access
    # overlay — directly maps to a compliance ask.
    if is_public and (has_read or has_write) and has_pii:
        return "CRITICAL", "Revoke public-group access to PII table"

    # HIGH: public group with destructive privilege on any table; OR
    # non-public principal with destructive privilege on PII table.
    if is_public and has_destructive:
        return "HIGH", "Revoke ALL PRIVILEGES from public group"
    if has_destructive and has_pii:
        return "HIGH", f"Audit '{principal}' destructive access on PII table"

    # MEDIUM: public group with SELECT (no PII), or non-public with
    # write-but-not-destructive on PII tables.
    if is_public and has_read:
        return "MEDIUM", "Review public-group SELECT — likely too broad"
    if is_public and has_write:
        return "MEDIUM", "Revoke public-group write privilege"
    if has_write and has_pii:
        return "MEDIUM", f"Audit '{principal}' write access on PII table"
    if has_destructive:
        return "MEDIUM", f"Audit '{principal}' ALL PRIVILEGES grant"

    # LOW: non-public read or write that doesn't touch PII —
    # informational, surfaced so the auditor can spot-check.
    if has_write:
        return "LOW", "Non-PII write access — verify principal role"
    if has_read:
        return "LOW", "Routine SELECT access"

    # Anything else (USAGE, MANAGE, etc.) is informational only.
    return "INFO", "No data access privilege"


def _bulk_privileges_query(catalog: str, exclude_schemas: list[str]) -> str:
    """Single SQL: every (table, grantee, privilege) row in the catalog.

    `information_schema.table_privileges` is the canonical UC view for
    this — much faster than calling `client.grants.get_effective()`
    per-table (which is the per-table fallback in `src.permissions`).
    """
    excl = ",".join(f"'{s}'" for s in (exclude_schemas or ["information_schema", "default"]))
    return f"""
        SELECT grantor, grantee, table_schema, table_name,
               privilege_type, is_grantable
        FROM {catalog}.information_schema.table_privileges
        WHERE table_schema NOT IN ({excl})
        ORDER BY table_schema, table_name, grantee, privilege_type
    """.strip()


def _build_pii_table_set(pii_columns: list[dict] | None) -> set[tuple[str, str]]:
    """Convert a `scan_catalog_for_pii` result into a `{(schema, table)}`
    set for fast `has_pii` lookup. Accepts None or [] for no overlay."""
    if not pii_columns:
        return set()
    out: set[tuple[str, str]] = set()
    for d in pii_columns:
        schema = d.get("schema")
        table = d.get("table")
        if schema and table:
            out.add((schema, table))
    return out


def audit_catalog_permissions(
    client: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
    *,
    pii_columns: list[dict] | None = None,
    exclude_schemas: list[str] | None = None,
) -> dict[str, Any]:
    """Audit grants across a catalog. Optionally cross-reference PII findings.

    Args:
        catalog: Catalog name to audit.
        pii_columns: Optional — pass the `columns` list from a prior
            `scan_catalog_for_pii` run to escalate findings on PII
            tables. Without this, all PII-related rules are skipped.
        exclude_schemas: Defaults to `["information_schema", "default"]`.

    Returns:
        {
            "catalog": str,
            "total_grants_scanned": int,
            "findings": [{schema, table, principal, privileges,
                          risk_level, suggested_action, has_pii,
                          pii_columns: [...]}, ...],
            "summary": {
                "by_risk_level": {"CRITICAL": N, "HIGH": N, ...},
                "by_principal_type": {"public_group": N, "user": N,
                                      "service_principal": N, "group": N},
                "tables_audited": int,
                "pii_overlay_applied": bool,
            },
            "error": str | None,  # set if the bulk query failed
        }
    """
    if exclude_schemas is None:
        exclude_schemas = ["information_schema", "default"]

    pii_tables = _build_pii_table_set(pii_columns)
    pii_overlay = bool(pii_tables)
    # For finding-level visibility, also map (schema, table) to the
    # actual PII column names so the UI can show "this table has
    # `ssn`, `email` flagged" without a second lookup.
    pii_cols_by_table: dict[tuple[str, str], list[str]] = defaultdict(list)
    for d in pii_columns or []:
        schema = d.get("schema")
        table = d.get("table")
        col = d.get("column")
        if schema and table and col:
            pii_cols_by_table[(schema, table)].append(col)

    try:
        rows = execute_sql(
            client, warehouse_id,
            _bulk_privileges_query(catalog, exclude_schemas),
        ) or []
    except Exception as e:
        logger.warning(f"Permissions audit query failed for {catalog!r}: {e}")
        return {
            "catalog": catalog,
            "total_grants_scanned": 0,
            "findings": [],
            "summary": {
                "by_risk_level": {},
                "by_principal_type": {},
                "tables_audited": 0,
                "pii_overlay_applied": pii_overlay,
            },
            "error": str(e),
        }

    # Group rows by (schema, table, grantee) → set of privileges. The
    # classifier expects the full privilege set per cluster so it can
    # short-circuit when ALL_PRIVILEGES is held.
    grouped: dict[tuple[str, str, str], dict[str, Any]] = {}
    tables_seen: set[tuple[str, str]] = set()
    total_rows = 0

    for r in rows:
        schema = r.get("table_schema")
        table = r.get("table_name")
        grantee = r.get("grantee")
        priv = r.get("privilege_type")
        if not (schema and table and grantee and priv):
            continue
        total_rows += 1
        tables_seen.add((schema, table))
        key = (schema, table, grantee)
        if key not in grouped:
            grouped[key] = {
                "schema": schema, "table": table, "principal": grantee,
                "privileges": set(), "grantors": set(),
                "is_grantable": False,
            }
        grouped[key]["privileges"].add(priv)
        if r.get("grantor"):
            grouped[key]["grantors"].add(r["grantor"])
        if str(r.get("is_grantable", "")).upper() in ("TRUE", "YES", "1"):
            grouped[key]["is_grantable"] = True

    # Classify each cluster.
    findings: list[dict[str, Any]] = []
    by_risk: dict[str, int] = defaultdict(int)
    by_principal_type: dict[str, int] = defaultdict(int)

    for (schema, table, principal), cluster in grouped.items():
        privs = cluster["privileges"]
        has_pii = (schema, table) in pii_tables
        risk_level, action = _classify_finding(
            principal=principal,
            privileges=privs,
            has_pii=has_pii,
            grantor=next(iter(cluster["grantors"])) if cluster["grantors"] else None,
        )
        # Drop INFO findings from the response — not actionable, just
        # noise on the UI. Aggregate counts still reflect them.
        by_risk[risk_level] += 1
        ptype = _principal_type(principal)
        by_principal_type[ptype] += 1
        if risk_level == "INFO":
            continue
        findings.append({
            "schema": schema,
            "table": table,
            "principal": principal,
            "principal_type": ptype,
            "privileges": sorted(privs),
            "is_grantable": cluster["is_grantable"],
            "risk_level": risk_level,
            "suggested_action": action,
            "has_pii": has_pii,
            "pii_columns": list(pii_cols_by_table.get((schema, table), [])),
        })

    # Sort findings: highest risk first, then PII tables, then alphabetical.
    findings.sort(key=lambda f: (
        -_RISK_ORDER.get(f["risk_level"], 0),
        not f["has_pii"],
        f["schema"], f["table"], f["principal"],
    ))

    return {
        "catalog": catalog,
        "total_grants_scanned": total_rows,
        "findings": findings,
        "summary": {
            "by_risk_level": dict(by_risk),
            "by_principal_type": dict(by_principal_type),
            "tables_audited": len(tables_seen),
            "pii_overlay_applied": pii_overlay,
        },
        "error": None,
    }


def _principal_type(principal: str) -> str:
    """Best-effort classification of a principal name → type bucket.

    UC doesn't expose principal type directly in `table_privileges`,
    but we can infer from naming conventions:
    - email-shaped → `user`
    - UUID-shaped → `service_principal`
    - in PUBLIC_PRINCIPALS → `public_group`
    - otherwise → `group`

    Used for the summary's `by_principal_type` rollup so the UI can
    show "12 grants to public groups, 47 to users, 3 to SPs".
    """
    p = (principal or "").strip().lower()
    if p in PUBLIC_PRINCIPALS:
        return "public_group"
    if "@" in p:
        return "user"
    # Service principals in Databricks are GUIDs (8-4-4-4-12 hex).
    if len(p) == 36 and p.count("-") == 4:
        return "service_principal"
    return "group"
