"""PII & Sensitive Data Scanner endpoint — queries system.information_schema for PII-named/tagged columns."""

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
# PII Scan endpoint
# ---------------------------------------------------------------------------

PII_SQL = """
SELECT
  c.table_catalog,
  c.table_schema,
  c.table_name,
  c.column_name,
  c.data_type,
  t.tag_name,
  t.tag_value
FROM system.information_schema.columns c
LEFT JOIN system.information_schema.column_tags t
  ON c.table_catalog = t.catalog_name
 AND c.table_schema  = t.schema_name
 AND c.table_name    = t.table_name
 AND c.column_name   = t.column_name
WHERE
  LOWER(c.column_name) REGEXP '(email|ssn|phone|mobile|credit.card|passport|national.id|dob|birth|gender|salary|account.number|routing|iban|ip.address|mac.address|password|secret|token|api.key|pii|sensitive)'
  OR LOWER(t.tag_name) IN ('pii','sensitive','classified','phi','pci')
{catalog_filter}
ORDER BY c.table_catalog, c.table_schema, c.table_name
LIMIT {limit}
"""


@router.get("/pii/scan")
async def scan_pii_columns(
    catalog: str | None = Query(None, description="Filter to a specific catalog"),
    limit: int = Query(200, ge=1, le=2000, description="Max rows to return"),
    x_databricks_host: str | None = Header(None),
    x_databricks_token: str | None = Header(None),
    x_databricks_warehouse: str | None = Header(None),
):
    """Scan Unity Catalog for PII-named or PII-tagged columns.

    Uses system.information_schema.columns and column_tags to identify columns
    that match PII keyword patterns or carry PII/sensitive/PHI/PCI tags.
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

    catalog_filter = f"AND c.table_catalog = '{catalog}'" if catalog else ""
    sql = PII_SQL.format(catalog_filter=catalog_filter, limit=limit)

    try:
        return await _exec_sql(host, token, warehouse_id, sql)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Databricks SQL API unreachable: {exc}")
