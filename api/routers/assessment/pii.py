"""PII & Sensitive Data Scanner endpoint — queries system.information_schema for PII-named/tagged columns."""

from __future__ import annotations

from fastapi import APIRouter, Header, HTTPException, Query

from ._creds import exec_sql, resolve_sql_auth

router = APIRouter()


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
    x_clone_session: str | None = Header(None),
):
    """Scan Unity Catalog for PII-named or PII-tagged columns.

    Uses system.information_schema.columns and column_tags to identify columns
    that match PII keyword patterns or carry PII/sensitive/PHI/PCI tags.
    """
    base_host, authorization = resolve_sql_auth(
        x_databricks_host, x_databricks_token, x_clone_session
    )
    warehouse_id = x_databricks_warehouse or ""
    if not warehouse_id:
        raise HTTPException(
            status_code=400,
            detail="SQL Warehouse ID required (X-Databricks-Warehouse header)",
        )

    catalog_filter = f"AND c.table_catalog = '{catalog}'" if catalog else ""
    sql = PII_SQL.format(catalog_filter=catalog_filter, limit=limit)

    try:
        return await exec_sql(base_host, authorization, warehouse_id, sql)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Databricks SQL API unreachable: {exc}")
