"""Permission Audit Matrix endpoint — queries system.information_schema.grants."""

from __future__ import annotations

import asyncio

from fastapi import APIRouter, Header, HTTPException, Query

router = APIRouter()


# ---------------------------------------------------------------------------
# Shared helper: execute a SQL statement via Databricks SQL Statements API
# ---------------------------------------------------------------------------

async def _exec_sql(host: str, token: str, warehouse_id: str, statement: str) -> dict:
    """Execute SQL via Databricks /api/2.0/sql/statements and return the result."""
    import httpx

    base = host.rstrip("/")
    hdrs = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    payload = {
        "statement": statement,
        "warehouse_id": warehouse_id,
        "wait_timeout": "50s",
        "on_wait_timeout": "CANCEL",
        "disposition": "INLINE",
        "format": "JSON_ARRAY",
    }

    async with httpx.AsyncClient(timeout=60) as c:
        r = await c.post(f"{base}/api/2.0/sql/statements", headers=hdrs, json=payload)
        if r.status_code not in (200, 201):
            raise HTTPException(status_code=r.status_code, detail=r.text[:500])
        data = r.json()

    # Poll if still pending/running
    stmt_id = data.get("statement_id")
    for _ in range(30):
        state = data.get("status", {}).get("state", "")
        if state in ("SUCCEEDED", "FAILED", "CANCELED", "CLOSED"):
            break
        if not stmt_id:
            break
        await asyncio.sleep(2)
        async with httpx.AsyncClient(timeout=30) as c:
            r = await c.get(f"{base}/api/2.0/sql/statements/{stmt_id}", headers=hdrs)
            if r.status_code == 200:
                data = r.json()

    state = data.get("status", {}).get("state", "")
    if state == "FAILED":
        err_msg = data.get("status", {}).get("error", {}).get("message", "Unknown SQL error")
        raise HTTPException(status_code=422, detail=f"SQL execution failed: {err_msg}")

    result = data.get("result", {})
    schema = data.get("manifest", {}).get("schema", {}).get("columns", [])
    columns = [col.get("name", f"col{i}") for i, col in enumerate(schema)]
    rows = result.get("data_array", [])

    return {"columns": columns, "rows": rows, "total": len(rows)}


# ---------------------------------------------------------------------------
# Grants / Permission Audit endpoint
# ---------------------------------------------------------------------------

GRANTS_SQL_NO_CATALOG = """
SELECT
  grantor,
  grantee,
  privilege_type,
  object_type,
  object_name,
  inherited_from
FROM system.information_schema.grants
ORDER BY grantee, object_name
LIMIT 500
"""

GRANTS_SQL_WITH_CATALOG = """
SELECT
  grantor,
  grantee,
  privilege_type,
  object_type,
  object_name,
  inherited_from
FROM system.information_schema.grants
WHERE object_name LIKE '{catalog}%' OR object_name = '{catalog}'
ORDER BY grantee, object_name
LIMIT 500
"""


@router.get("/permissions/grants")
async def permission_grants(
    catalog: str | None = Query(None, description="Filter grants for objects in this catalog"),
    x_databricks_host: str | None = Header(None),
    x_databricks_token: str | None = Header(None),
    x_databricks_warehouse: str | None = Header(None),
):
    """Return permission grants from system.information_schema.grants.

    Optionally filter to objects belonging to a specific catalog.
    Highlights ALL PRIVILEGES grants and inherited grants for audit purposes.
    """
    host = (x_databricks_host or "").rstrip("/")
    token = x_databricks_token or ""
    warehouse_id = x_databricks_warehouse or ""

    if not host or not token:
        raise HTTPException(
            status_code=401,
            detail="Databricks credentials required (X-Databricks-Host, X-Databricks-Token headers)",
        )
    if not warehouse_id:
        raise HTTPException(
            status_code=400,
            detail="SQL Warehouse ID required (X-Databricks-Warehouse header)",
        )

    if catalog:
        sql = GRANTS_SQL_WITH_CATALOG.format(catalog=catalog.replace("'", "''"))
    else:
        sql = GRANTS_SQL_NO_CATALOG

    try:
        return await _exec_sql(host, token, warehouse_id, sql)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Databricks SQL API unreachable: {exc}")
