"""Column lineage proxy endpoint — forwards requests to Databricks lineage-tracking API."""

from __future__ import annotations

from fastapi import APIRouter, Header, HTTPException, Query

router = APIRouter()


@router.get("/lineage/table")
async def table_lineage(
    table_name: str = Query(..., description="Full table name: catalog.schema.table"),
    x_databricks_host: str | None = Header(None),
    x_databricks_token: str | None = Header(None),
):
    """Proxy Databricks lineage-tracking/table-lineage for a given table."""
    import httpx
    host  = (x_databricks_host  or "").rstrip("/")
    token = (x_databricks_token or "")
    if not host or not token:
        raise HTTPException(
            status_code=401,
            detail="Databricks credentials required (X-Databricks-Host, X-Databricks-Token headers)",
        )
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get(
                f"{host}/api/2.0/lineage-tracking/table-lineage",
                headers={"Authorization": f"Bearer {token}"},
                params={"table_name": table_name, "include_entity_lineage": "true"},
            )
        if r.status_code == 404:
            return {"upstream_tables": [], "downstream_tables": [], "table_name": table_name}
        if r.status_code != 200:
            raise HTTPException(status_code=r.status_code, detail=r.text[:500])
        data = r.json()
        data["table_name"] = table_name
        return data
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Databricks lineage API unreachable: {exc}")
