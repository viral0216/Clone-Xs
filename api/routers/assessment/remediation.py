"""Remediation tracking and AI-powered remediation plan endpoints."""

from __future__ import annotations

import json

from fastapi import APIRouter, Header, HTTPException, Query

from ._storage import _STORE

router = APIRouter()


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
