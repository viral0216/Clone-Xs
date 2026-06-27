"""Shared credential resolution + SQL execution for assessment endpoints.

The assessment endpoints call the Databricks REST API directly (SQL Statements,
lineage-tracking, serving endpoints), so they need a host + ``Authorization``
header rather than a SDK ``WorkspaceClient``. They were originally written to
accept only raw PAT headers (``X-Databricks-Host`` / ``X-Databricks-Token``),
which breaks Azure AD / OAuth / Service Principal logins — those authenticate
via a server-side session and never store a PAT in the browser.

``resolve_sql_auth`` restores parity with the rest of the app by resolving
credentials from, in order:

  1. Direct PAT headers      (``X-Databricks-Host`` / ``X-Databricks-Token``)
  2. Server-side session     (``X-Clone-Session``) — Azure AD / OAuth / SP
  3. Databricks App runtime / env vars / CLI profile
"""

from __future__ import annotations

import asyncio

import httpx
from fastapi import HTTPException


def _auth_from_client(client) -> tuple[str, str]:
    """Extract ``(base_host, "Bearer <token>")`` from an authenticated client.

    Works for every auth type (PAT, OAuth, Azure AD, Service Principal).
    """
    from src.auth import auth_headers_from_client

    base = (client.config.host or "").rstrip("/")
    authorization = auth_headers_from_client(client).get("Authorization")
    if not base or not authorization:
        raise HTTPException(
            status_code=401,
            detail="Could not resolve Databricks credentials from the active session.",
        )
    return base, authorization


def resolve_sql_auth(
    host: str | None,
    token: str | None,
    session_id: str | None = None,
) -> tuple[str, str]:
    """Resolve ``(base_host, authorization_header)`` for direct Databricks REST calls.

    Order: PAT headers → server-side session → Databricks App / env.
    Raises HTTP 401 if no credentials can be resolved.
    """
    # 1. Direct PAT credentials
    if host and token:
        return host.rstrip("/"), f"Bearer {token}"

    # 2. Server-side session (Azure AD / OAuth / Service Principal)
    if session_id:
        from api.routers.auth import get_session_client

        client = get_session_client(session_id)
        if client:
            return _auth_from_client(client)

    # 3. Databricks App runtime / env vars / CLI profile
    from src.auth import is_databricks_app

    if is_databricks_app():
        from src.auth import get_client

        return _auth_from_client(get_client())

    raise HTTPException(
        status_code=401,
        detail=(
            "Databricks credentials required. Log in via Settings, or pass "
            "X-Databricks-Host and X-Databricks-Token headers."
        ),
    )


async def exec_sql(base_host: str, authorization: str, warehouse_id: str, statement: str) -> dict:
    """Execute SQL via the Databricks SQL Statements API → ``{columns, rows, total}``."""
    base = base_host.rstrip("/")
    hdrs = {"Authorization": authorization, "Content-Type": "application/json"}
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
