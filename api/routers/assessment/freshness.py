"""Data Freshness / Staleness Tracker endpoint — queries system.information_schema.tables."""

from __future__ import annotations

from fastapi import APIRouter, Header, HTTPException, Query

from ._creds import exec_sql, resolve_sql_auth

router = APIRouter()


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
    x_clone_session: str | None = Header(None),
):
    """Return table freshness/staleness data from system.information_schema.tables.

    Tables not updated within stale_days are considered stale.
    Tables with NULL last_altered are considered never-written (dead).
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

    catalog_filter = f"AND table_catalog = '{catalog}'" if catalog else ""
    sql = FRESHNESS_SQL_BASE.format(catalog_filter=catalog_filter)

    try:
        result = await exec_sql(base_host, authorization, warehouse_id, sql)
        result["stale_days"] = stale_days
        return result
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Databricks SQL API unreachable: {exc}")
