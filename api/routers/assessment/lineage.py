"""Lineage proxy endpoints — forwards to Databricks lineage-tracking API."""

from __future__ import annotations

from fastapi import APIRouter, Header, HTTPException, Query

from ._creds import resolve_sql_auth

router = APIRouter()


# ── helpers ──────────────────────────────────────────────────────────────────


def _full_name(info: dict) -> str:
    """Reconstruct catalog.schema.table from tableInfo fields."""
    name = info.get("name") or info.get("table_name") or ""
    catalog = info.get("catalog_name") or info.get("catalogName") or ""
    schema = info.get("schema_name") or info.get("schemaName") or ""
    if name and "." not in name and catalog and schema:
        return f"{catalog}.{schema}.{name}"
    return name


def _extract_all(items: list) -> list:
    """Normalise every entity type returned by the Databricks lineage API.

    Databricks returns entities of type TABLE, VIEW, NOTEBOOK, JOB,
    DASHBOARD, PIPELINE, QUERY, and FILE.  We normalise each into a
    predictable dict so the frontend doesn't have to understand the raw
    API shape.
    """
    result = []
    for item in items or []:
        entity_type = (item.get("entityType") or "TABLE").upper()

        if entity_type in ("TABLE", "VIEW"):
            info = item.get("tableInfo") or {}
            name = _full_name(info)
            if not name:
                continue
            result.append(
                {
                    "entity_type": entity_type,
                    "table_name": name,
                    "table_type": info.get("table_type") or entity_type,
                    "owner": info.get("owner") or "",
                }
            )

        elif entity_type == "NOTEBOOK":
            info = item.get("notebookInfo") or {}
            nb_id = str(info.get("notebook_id") or "")
            path = info.get("path") or info.get("notebook_path") or f"Notebook {nb_id}"
            result.append(
                {
                    "entity_type": "NOTEBOOK",
                    "name": path,
                    "notebook_id": nb_id,
                    "workspace_id": str(info.get("workspace_id") or ""),
                }
            )

        elif entity_type == "JOB":
            info = item.get("jobInfo") or {}
            job_id = str(info.get("job_id") or "")
            run_id = str(info.get("job_run_id") or info.get("run_id") or "")
            result.append(
                {
                    "entity_type": "JOB",
                    "name": info.get("job_name") or f"Job {job_id}",
                    "job_id": job_id,
                    "run_id": run_id,
                    "workspace_id": str(info.get("workspace_id") or ""),
                }
            )

        elif entity_type == "DASHBOARD":
            info = item.get("dashboardInfo") or {}
            d_id = str(info.get("dashboard_id") or "")
            result.append(
                {
                    "entity_type": "DASHBOARD",
                    "name": info.get("dashboard_name") or f"Dashboard {d_id}",
                    "dashboard_id": d_id,
                    "workspace_id": str(info.get("workspace_id") or ""),
                }
            )

        elif entity_type == "PIPELINE":
            info = item.get("pipelineInfo") or {}
            p_id = str(info.get("pipeline_id") or "")
            result.append(
                {
                    "entity_type": "PIPELINE",
                    "name": info.get("pipeline_name") or f"Pipeline {p_id}",
                    "pipeline_id": p_id,
                }
            )

        elif entity_type == "QUERY":
            info = item.get("queryInfo") or {}
            q_id = str(info.get("query_id") or "")
            text = (info.get("query_text") or "")[:80]
            result.append(
                {
                    "entity_type": "QUERY",
                    "name": text or f"Query {q_id}",
                    "query_id": q_id,
                }
            )

        elif entity_type == "FILE":
            info = item.get("fileInfo") or {}
            path = info.get("path") or ""
            if path:
                result.append(
                    {
                        "entity_type": "FILE",
                        "name": path,
                    }
                )

    return result


def _auth(host: str | None, token: str | None, session_id: str | None = None):
    """Resolve (base_host, authorization_header) — supports PAT, session, and app auth."""
    return resolve_sql_auth(host, token, session_id)


# ── table lineage ─────────────────────────────────────────────────────────────


@router.get("/lineage/table")
async def table_lineage(
    table_name: str = Query(..., description="Full table name: catalog.schema.table"),
    start_time_ms: int | None = Query(None, description="Filter: start epoch ms"),
    end_time_ms: int | None = Query(None, description="Filter: end epoch ms"),
    x_databricks_host: str | None = Header(None),
    x_databricks_token: str | None = Header(None),
    x_clone_session: str | None = Header(None),
):
    """Proxy Databricks lineage-tracking/table-lineage."""
    import httpx

    host, authorization = _auth(x_databricks_host, x_databricks_token, x_clone_session)

    params: dict = {"table_name": table_name, "include_entity_lineage": "true"}
    if start_time_ms is not None:
        params["start_time_ms"] = start_time_ms
    if end_time_ms is not None:
        params["end_time_ms"] = end_time_ms

    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get(
                f"{host}/api/2.0/lineage-tracking/table-lineage",
                headers={"Authorization": authorization},
                params=params,
            )
        if r.status_code == 404:
            return {"table_name": table_name, "upstream_tables": [], "downstream_tables": []}
        if r.status_code != 200:
            raise HTTPException(status_code=r.status_code, detail=r.text[:500])
        data = r.json()
        return {
            "table_name": table_name,
            "upstream_tables": _extract_all(
                data.get("upstreams") or data.get("upstream_tables", [])
            ),
            "downstream_tables": _extract_all(
                data.get("downstreams") or data.get("downstream_tables", [])
            ),
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Databricks lineage API unreachable: {exc}")


# ── column lineage ────────────────────────────────────────────────────────────


@router.get("/lineage/column")
async def column_lineage(
    table_name: str = Query(..., description="Full table name: catalog.schema.table"),
    column_name: str = Query(..., description="Column name"),
    x_databricks_host: str | None = Header(None),
    x_databricks_token: str | None = Header(None),
    x_clone_session: str | None = Header(None),
):
    """Proxy Databricks lineage-tracking/column-lineage."""
    import httpx

    host, authorization = _auth(x_databricks_host, x_databricks_token, x_clone_session)

    def _norm_col(col: dict) -> dict:
        cat = col.get("catalog_name") or col.get("catalogName") or ""
        schema = col.get("schema_name") or col.get("schemaName") or ""
        tbl = col.get("table_name") or col.get("tableName") or ""
        name = col.get("name") or col.get("column_name") or ""
        # Reconstruct FQN if needed
        if tbl and "." not in tbl and cat and schema:
            tbl = f"{cat}.{schema}.{tbl}"
        return {
            "name": name,
            "table_name": tbl,
            "catalog_name": cat,
            "schema_name": schema,
            "table_type": col.get("table_type") or "TABLE",
        }

    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get(
                f"{host}/api/2.0/lineage-tracking/column-lineage",
                headers={"Authorization": authorization},
                params={"table_name": table_name, "column_name": column_name},
            )
        if r.status_code == 404:
            return {
                "table_name": table_name,
                "column_name": column_name,
                "upstream_cols": [],
                "downstream_cols": [],
            }
        if r.status_code != 200:
            raise HTTPException(status_code=r.status_code, detail=r.text[:500])
        data = r.json()
        return {
            "table_name": table_name,
            "column_name": column_name,
            "upstream_cols": [_norm_col(c) for c in (data.get("upstream_cols") or [])],
            "downstream_cols": [_norm_col(c) for c in (data.get("downstream_cols") or [])],
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=502, detail=f"Databricks column lineage API unreachable: {exc}"
        )


# ── system tables ─────────────────────────────────────────────────────────────


@router.get("/lineage/system-events")
async def lineage_system_events(
    table_name: str = Query(..., description="Full table name: catalog.schema.table"),
    limit: int = Query(50, description="Max rows to return"),
    x_databricks_host: str | None = Header(None),
    x_databricks_token: str | None = Header(None),
    x_databricks_warehouse: str | None = Header(None),
    x_clone_session: str | None = Header(None),
):
    """Query system.access.table_lineage for raw lineage events for a table."""
    import httpx

    host, authorization = _auth(x_databricks_host, x_databricks_token, x_clone_session)
    warehouse_id = x_databricks_warehouse or ""
    if not warehouse_id:
        raise HTTPException(
            status_code=400,
            detail="SQL Warehouse ID required (X-Databricks-Warehouse header)",
        )

    sql = (
        f"SELECT event_time, event_type, entity_type, entity_id, entity_run_id, "
        f"source_table_full_name, target_table_full_name, workspace_id "
        f"FROM system.access.table_lineage "
        f"WHERE source_table_full_name = '{table_name}' "
        f"   OR target_table_full_name = '{table_name}' "
        f"ORDER BY event_time DESC "
        f"LIMIT {min(limit, 200)}"
    )

    try:
        async with httpx.AsyncClient(timeout=30) as c:
            r = await c.post(
                f"{host}/api/2.0/sql/statements",
                headers={"Authorization": authorization},
                json={
                    "statement": sql,
                    "warehouse_id": warehouse_id,
                    "wait_timeout": "20s",
                    "on_wait_timeout": "CANCEL",
                },
            )
        if r.status_code != 200:
            raise HTTPException(status_code=r.status_code, detail=r.text[:500])
        data = r.json()
        cols = [
            c["name"] for c in (data.get("manifest", {}).get("schema", {}).get("columns") or [])
        ]
        rows = data.get("result", {}).get("data_array") or []
        return {"columns": cols, "rows": rows}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"System tables query failed: {exc}")
