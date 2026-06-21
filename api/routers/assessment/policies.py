"""Governance policy engine: built-in rules, custom policy CRUD, and evaluation."""

from __future__ import annotations

import json

from fastapi import APIRouter, HTTPException, Query

from ._storage import _STORE, _CUSTOM_POLICIES_PATH, _latest_result

router = APIRouter()

# ---------------------------------------------------------------------------
# Built-in policies
# ---------------------------------------------------------------------------

_BUILT_IN_POLICIES: list[dict] = [
    {"id": "table_needs_owner",       "name": "Tables must have an owner",           "severity": "high"},
    {"id": "table_needs_description", "name": "Tables must have a description",       "severity": "medium"},
    {"id": "schema_needs_owner",      "name": "Schemas must have an owner",           "severity": "medium"},
    {"id": "no_catalog_all_privs",    "name": "No ALL PRIVILEGES grants on catalogs", "severity": "critical"},
    {"id": "pii_must_be_masked",      "name": "PII-named columns must have masking",  "severity": "high"},
]

_PII_PATTERNS = [
    "email", "ssn", "social_security", "phone", "mobile", "dob", "birth",
    "passport", "salary", "credit_card", "card_number", "cvv", "tax_id",
    "address", "zip", "postal", "ip_address",
]

_CUSTOM_RULE_TYPES = [
    "tables_need_owner",
    "tables_need_description",
    "schemas_need_owner",
    "no_all_privs_on_catalog",
    "pii_columns_must_be_masked",
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _load_custom_policies() -> list[dict]:
    if _CUSTOM_POLICIES_PATH.exists():
        try:
            return json.loads(_CUSTOM_POLICIES_PATH.read_text())
        except Exception:
            pass
    return []


def _evaluate_custom_policy(policy: dict, inv: dict) -> list[dict]:
    """Run a single custom policy rule against an inventory snapshot."""
    rt     = policy.get("rule_type", "")
    scope  = (policy.get("catalog_scope")  or "").strip()
    pat    = (policy.get("column_pattern") or "").lower()

    violations: list[dict] = []

    for cat in inv.get("catalogs", []):
        if scope and cat["name"] != scope:
            continue

        if rt == "no_all_privs_on_catalog":
            for g in cat.get("grants", []):
                if "ALL PRIVILEGES" in g.get("privileges", []) or "MANAGE" in g.get("privileges", []):
                    violations.append({"object": cat["name"], "principal": g.get("principal", "")})
            continue

        for sch in cat.get("schemas", []):
            fn_sch = sch.get("full_name") or f"{cat['name']}.{sch['name']}"

            if rt == "schemas_need_owner" and not (sch.get("owner") or "").strip():
                violations.append({"object": fn_sch})

            for tbl in sch.get("tables", []):
                fn = tbl.get("full_name") or f"{cat['name']}.{sch['name']}.{tbl['name']}"
                if rt == "tables_need_owner" and not (tbl.get("owner") or "").strip():
                    violations.append({"object": fn})
                elif rt == "tables_need_description" and not (tbl.get("comment") or "").strip():
                    violations.append({"object": fn})
                elif rt == "pii_columns_must_be_masked":
                    for col in tbl.get("columns", []):
                        col_lower = (col.get("name") or "").lower()
                        if (
                            (not pat or pat in col_lower)
                            and any(p in col_lower for p in _PII_PATTERNS)
                            and not col.get("mask")
                        ):
                            violations.append({"object": fn, "column": col.get("name", "")})

    return violations


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("/policies/evaluate")
async def evaluate_policies(scan_id: str | None = Query(None)):
    """Evaluate built-in governance policies against the UC inventory."""
    sid = scan_id or ((_latest_result() or {}).get("scan_id"))
    if not sid:
        raise HTTPException(status_code=404, detail="No scan found")
    p = _STORE / sid / "inventory.json"
    if not p.exists():
        raise HTTPException(status_code=404, detail="Inventory not available — run an inventory scan first")
    inv = json.loads(p.read_text())

    violations: dict[str, list] = {pol["id"]: [] for pol in _BUILT_IN_POLICIES}

    for cat in inv.get("catalogs", []):
        for g in cat.get("grants", []):
            privs = g.get("privileges", [])
            if "ALL PRIVILEGES" in privs or "MANAGE" in privs:
                violations["no_catalog_all_privs"].append({
                    "object":     cat["name"],
                    "principal":  g.get("principal", ""),
                    "privileges": privs,
                })
        for sch in cat.get("schemas", []):
            fn_sch = sch.get("full_name") or f"{cat['name']}.{sch['name']}"
            if not (sch.get("owner") or "").strip():
                violations["schema_needs_owner"].append({"object": fn_sch})
            for tbl in sch.get("tables", []):
                fn = tbl.get("full_name") or f"{cat['name']}.{sch['name']}.{tbl['name']}"
                if not (tbl.get("owner") or "").strip():
                    violations["table_needs_owner"].append({"object": fn})
                if not (tbl.get("comment") or "").strip():
                    violations["table_needs_description"].append({"object": fn})
                for col in tbl.get("columns", []):
                    col_lower = (col.get("name") or "").lower()
                    if any(pat in col_lower for pat in _PII_PATTERNS) and not col.get("mask"):
                        violations["pii_must_be_masked"].append({
                            "object": fn,
                            "column": col.get("name", ""),
                        })

    custom_policies = _load_custom_policies()
    custom_results  = [
        {
            **pol,
            "violations": _evaluate_custom_policy(pol, inv),
            "type": "custom",
        }
        for pol in custom_policies
    ]
    for cr in custom_results:
        cr["count"]  = len(cr["violations"])
        cr["status"] = "pass" if not cr["violations"] else "fail"

    all_policies = [
        {
            **pol,
            "violations": violations[pol["id"]],
            "count":      len(violations[pol["id"]]),
            "status":     "pass" if not violations[pol["id"]] else "fail",
            "type":       "built_in",
        }
        for pol in _BUILT_IN_POLICIES
    ] + custom_results

    total_violations = sum(p["count"] for p in all_policies)

    return {
        "scan_id":  sid,
        "policies": all_policies,
        "summary":  {
            "total":            len(all_policies),
            "passing":          sum(1 for p in all_policies if p["status"] == "pass"),
            "failing":          sum(1 for p in all_policies if p["status"] == "fail"),
            "total_violations": total_violations,
        },
    }


@router.get("/policies")
async def list_policies():
    """Return built-in and custom policy definitions."""
    return {"built_in": _BUILT_IN_POLICIES, "custom": _load_custom_policies()}


@router.post("/policies")
async def create_policy(body: dict):
    """Create a custom governance policy."""
    import time
    policies = _load_custom_policies()
    new_pol  = {
        "id":             f"custom_{int(time.time() * 1000)}",
        "name":           (body.get("name") or "Custom Policy").strip(),
        "severity":       body.get("severity",       "medium"),
        "rule_type":      body.get("rule_type",      "tables_need_owner"),
        "catalog_scope":  body.get("catalog_scope",  ""),
        "column_pattern": body.get("column_pattern", ""),
        "type":           "custom",
    }
    policies.append(new_pol)
    _CUSTOM_POLICIES_PATH.write_text(json.dumps(policies, indent=2))
    return new_pol


@router.delete("/policies/{policy_id}")
async def delete_policy(policy_id: str):
    """Delete a custom policy by ID."""
    policies = [p for p in _load_custom_policies() if p.get("id") != policy_id]
    _CUSTOM_POLICIES_PATH.write_text(json.dumps(policies, indent=2))
    return {"deleted": policy_id}
