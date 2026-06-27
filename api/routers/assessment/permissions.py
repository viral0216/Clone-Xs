"""Permission Audit Matrix endpoint — queries system.information_schema.grants."""

from __future__ import annotations

from fastapi import APIRouter, Header, HTTPException, Query

from ._creds import exec_sql, resolve_sql_auth

router = APIRouter()


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
    x_clone_session: str | None = Header(None),
):
    """Return permission grants from system.information_schema.grants.

    Optionally filter to objects belonging to a specific catalog.
    Highlights ALL PRIVILEGES grants and inherited grants for audit purposes.
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

    if catalog:
        sql = GRANTS_SQL_WITH_CATALOG.format(catalog=catalog.replace("'", "''"))
    else:
        sql = GRANTS_SQL_NO_CATALOG

    try:
        return await exec_sql(base_host, authorization, warehouse_id, sql)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Databricks SQL API unreachable: {exc}")
