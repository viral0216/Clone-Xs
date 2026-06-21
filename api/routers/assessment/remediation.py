"""Remediation tracking and AI-powered remediation plan endpoints."""

from __future__ import annotations

import json

from fastapi import APIRouter, Header, HTTPException, Query
from pydantic import BaseModel

from ._storage import _STORE, _latest_result

router = APIRouter()


class RemediateRequest(BaseModel):
    finding_id: str
    action: str
    catalog: str | None = None
    schema: str | None = None
    table: str | None = None


@router.post("/remediate")
async def quick_remediate(
    body: RemediateRequest,
    x_databricks_host: str | None = Header(None),
    x_databricks_token: str | None = Header(None),
):
    """Execute a quick remediation action for a finding.

    Actions:
      - set_owner: ALTER TABLE {fqn} SET OWNER TO current_user() via Databricks SQL
      - acknowledge: mark the finding status as ACKNOWLEDGED in scan storage
    """
    import httpx

    action = body.action

    if action == "acknowledge":
        # Mark the finding as acknowledged in the latest scan
        meta = _latest_result()
        scan_id = meta.get("scan_id") if meta else None
        if scan_id:
            path = _STORE / scan_id / "remediation.json"
            data: dict = {}
            if path.exists():
                try:
                    data = json.loads(path.read_text(encoding="utf-8"))
                except Exception:
                    data = {}
            data[body.finding_id] = {"status": "ACKNOWLEDGED", "note": "Acknowledged via quick action"}
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(data, indent=2), encoding="utf-8")
        return {"success": True, "message": f"Finding {body.finding_id} marked as acknowledged"}

    if action == "set_owner":
        host = (x_databricks_host or "").rstrip("/")
        token = x_databricks_token or ""
        if not host or not token:
            raise HTTPException(status_code=401, detail="Databricks credentials required")

        # Build fully qualified name
        parts = [p for p in [body.catalog, body.schema, body.table] if p]
        if len(parts) < 3:
            raise HTTPException(status_code=400, detail="catalog, schema, and table are required for set_owner action")
        fqn = ".".join(f"`{p}`" for p in parts)

        sql = f"ALTER TABLE {fqn} SET OWNER TO current_user()"
        try:
            async with httpx.AsyncClient(timeout=30) as c:
                # Start statement execution
                r = await c.post(
                    f"{host}/api/2.0/sql/statements",
                    headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                    json={"statement": sql, "wait_timeout": "20s"},
                )
            if r.status_code not in (200, 201):
                raise HTTPException(status_code=r.status_code, detail=f"Databricks SQL error: {r.text[:300]}")
            result = r.json()
            state = result.get("status", {}).get("state", "")
            if state in ("SUCCEEDED", "RUNNING", "PENDING"):
                return {"success": True, "message": f"Owner updated for {fqn}"}
            err_msg = result.get("status", {}).get("error", {}).get("message", "Unknown error")
            raise HTTPException(status_code=400, detail=f"SQL failed: {err_msg}")
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"Databricks unreachable: {exc}")

    raise HTTPException(status_code=400, detail=f"Unknown action: {action}")


@router.get("/remediation/{scan_id}")
async def get_remediation(scan_id: str):
    """Return all remediation statuses for a scan as {check_id: {status, note}}."""
    path = _STORE / scan_id / "remediation.json"
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


@router.put("/remediation/{scan_id}/{check_id}")
async def update_remediation(scan_id: str, check_id: str, body: dict):
    """Persist remediation status for a single finding."""
    scan_dir = _STORE / scan_id
    if not scan_dir.exists():
        raise HTTPException(status_code=404, detail=f"Scan {scan_id} not found")
    path = scan_dir / "remediation.json"
    data: dict = {}
    if path.exists():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            data = {}
    data[check_id] = {
        "status": body.get("status", "open"),
        "note":   body.get("note",   ""),
    }
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    return data[check_id]


@router.post("/ai/remediation-plan")
async def ai_remediation_plan(
    body: dict,
    x_databricks_host: str | None = Header(None),
    x_databricks_token: str | None = Header(None),
    model: str = Query("databricks-meta-llama-3-1-70b-instruct"),
):
    """Generate a step-by-step remediation plan for a security finding via Databricks Foundation Model."""
    import httpx
    host  = (x_databricks_host  or "").rstrip("/")
    token = (x_databricks_token or "")
    if not host or not token:
        raise HTTPException(status_code=401, detail="Databricks credentials required")

    finding = body.get("finding", {})
    system_prompt = (
        "You are a Databricks security expert specialising in Unity Catalog governance and workspace security. "
        "Generate a concise, actionable step-by-step remediation plan for the given security finding. "
        "Use markdown. Include specific Databricks SQL or Python snippets where helpful. "
        "Keep the plan focused — no unnecessary padding."
    )
    user_prompt = f"""## Security Finding

**Check ID:** {finding.get('check_id', '')}
**Title:** {finding.get('title', '')}
**Category:** {finding.get('category', '')}
**Severity:** {finding.get('severity', '')}
**Description:** {finding.get('description', '')}
**Current Recommendation:** {finding.get('recommendation', '')}
**Effort:** {finding.get('effort', '')}

Please provide:
1. **Root Cause** (1-2 sentences on why this happens)
2. **Step-by-Step Fix** (numbered steps with Databricks SQL/Python snippets where applicable)
3. **Validation** (how to confirm the fix worked)
4. **Prevention** (how to avoid recurrence)
5. **Quick Win** (if there's a sub-5-minute partial fix, highlight it)"""

    try:
        async with httpx.AsyncClient(timeout=45) as c:
            r = await c.post(
                f"{host}/serving-endpoints/{model}/invocations",
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                json={
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user",   "content": user_prompt},
                    ],
                    "max_tokens":  1500,
                    "temperature": 0.1,
                },
            )
        if r.status_code != 200:
            raise HTTPException(status_code=r.status_code, detail=f"AI model error: {r.text[:500]}")
        content = r.json()["choices"][0]["message"]["content"]
        return {"plan": content, "model": model, "check_id": finding.get("check_id", "")}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"AI service unreachable: {exc}")
