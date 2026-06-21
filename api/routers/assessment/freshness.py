"""Data Freshness / Staleness Tracker endpoint — queries system.information_schema.tables."""

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
# Freshness/Staleness endpoint
# ---------------------------------------------------------------------------

FRESHNESS_SQL_BASE = """
SELECT
  table_catalog,
  table_schema,
  table_name,
  table_type,
  last_altered,
  DATEDIFF(current_timestamp(), last_altered) AS days_since_update
FROM system.information_schema.tables
WHERE table_catalog != 'system'
{catalog_filter}
ORDER BY days_since_update DESC NULLS LAST
LIMIT 500
"""


@router.get("/freshness/tables")
async def freshness_tables(
    catalog: str | None = Query(None, description="Filter to a specific catalog"),
    stale_days: int = Query(30, ge=1, description="Days threshold to consider a table stale"),
    x_databricks_host: str | None = Header(None),
    x_databricks_token: str | None = Header(None),
    x_databricks_warehouse: str | None = Header(None),
):
    """Return table freshness/staleness data from system.information_schema.tables.

    Tables not updated within stale_days are considered stale.
    Tables with NULL last_altered are considered never-written (dead).
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

    catalog_filter = f"AND table_catalog = '{catalog}'" if catalog else ""
    sql = FRESHNESS_SQL_BASE.format(catalog_filter=catalog_filter)

    try:
        result = await _exec_sql(host, token, warehouse_id, sql)
        result["stale_days"] = stale_days
        return result
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Databricks SQL API unreachable: {exc}")
