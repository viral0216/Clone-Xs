"""Workspace resource collection endpoints."""

from __future__ import annotations

import asyncio
import json
from typing import Any

from fastapi import APIRouter, Header, HTTPException, Query

from ._storage import _STORE, _latest_result

router = APIRouter()


async def _collect_notebooks_bfs(base: str, hdrs: dict) -> list:
    """2-level BFS workspace listing to collect notebooks and files.

    Root → level-1 dirs → level-2 dirs (capped at 50) so we don't flood
    the API. Returns up to 1000 items total.
    """
    import httpx

    async def list_path(path: str) -> list:
        try:
            async with httpx.AsyncClient(timeout=15) as c:
                r = await c.get(f"{base}/api/2.0/workspace/list", headers=hdrs, params={"path": path})
                return r.json().get("objects", []) if r.status_code == 200 else []
        except Exception:
            return []

    level0 = await list_path("/")
    l1_dirs = [o for o in level0 if o.get("object_type") == "DIRECTORY"]
    l1_items = list(await asyncio.gather(*[list_path(d["path"]) for d in l1_dirs]))

    found: list = []
    subdirs: list = []
    for items in l1_items:
        for obj in items:
            ot = obj.get("object_type")
            if ot in ("NOTEBOOK", "FILE"):
                found.append(obj)
            elif ot == "DIRECTORY":
                subdirs.append(obj)

    for items in await asyncio.gather(*[list_path(d["path"]) for d in subdirs[:50]]):
        for obj in items:
            if obj.get("object_type") in ("NOTEBOOK", "FILE"):
                found.append(obj)

    return found[:1000]


async def _collect_workspace_resources(host: str, token: str) -> dict:
    """Fetch raw workspace resource lists from Databricks REST APIs.

    Called during every scan using the live credentials. Results saved to
    workspace_resources.json alongside result.json / inventory.json.
    All failures are silently swallowed so they never abort the main scan.
    """
    import httpx

    base = host.rstrip("/")
    hdrs = {"Authorization": f"Bearer {token}"}
    resources: dict[str, Any] = {}

    async def get(path: str, params: dict | None = None) -> dict | None:
        try:
            async with httpx.AsyncClient(timeout=20) as c:
                r = await c.get(f"{base}{path}", headers=hdrs, params=params)
                if r.status_code == 200:
                    return r.json()
        except Exception:
            pass
        return None

    async def collect(key: str, path: str, list_key: str, params: dict | None = None):
        resp = await get(path, params)
        if not list_key:
            resources[key] = resp if isinstance(resp, list) else []
        else:
            resources[key] = (resp or {}).get(list_key, [])

    await asyncio.gather(
        collect("jobs",                     "/api/2.1/jobs/list",                         "jobs",                       {"expand_tasks": "false", "limit": 200}),
        collect("job_runs",                 "/api/2.1/jobs/runs/list",                    "runs",                       {"limit": 50}),
        collect("clusters",                 "/api/2.0/clusters/list",                     "clusters"),
        collect("cluster_policies",         "/api/2.0/policies/clusters/list",            "policies"),
        collect("instance_pools",           "/api/2.0/instance-pools/list",               "instance_pools"),
        collect("warehouses",               "/api/2.0/sql/warehouses",                    "warehouses"),
        collect("serving_endpoints",        "/api/2.0/serving-endpoints",                 "endpoints"),
        collect("tokens",                   "/api/2.0/token-management/tokens",           "token_infos"),
        collect("users",                    "/api/2.0/preview/scim/v2/Users",             "Resources",                  {"count": 200}),
        collect("groups",                   "/api/2.0/preview/scim/v2/Groups",            "Resources",                  {"count": 200}),
        collect("service_principals",       "/api/2.0/preview/scim/v2/ServicePrincipals", "Resources",                  {"count": 200}),
        collect("pipelines",                "/api/2.0/pipelines",                         "statuses"),
        collect("secret_scopes",            "/api/2.0/secrets/scopes/list",               "scopes"),
        collect("repos",                    "/api/2.0/repos",                             "repos"),
        collect("apps",                     "/api/2.0/apps",                              "apps"),
        collect("genie_spaces",             "/api/2.0/genie/spaces",                      "spaces"),
        collect("dashboards",               "/api/2.0/lakeview/dashboards",               "dashboards"),
        collect("vector_search",            "/api/2.0/vector-search/endpoints",           "endpoints"),
        collect("global_init_scripts",      "/api/2.0/global-init-scripts",               "scripts"),
        collect("experiments",              "/api/2.0/mlflow/experiments/search",         "experiments",                {"max_results": 200, "view_type": "ACTIVE_ONLY"}),
        collect("sql_queries",              "/api/2.0/sql/queries",                       "results",                    {"page_size": 100}),
        collect("sql_alerts",               "/api/2.0/sql/alerts",                        ""),
        collect("dbfs_mounts",              "/api/2.0/dbfs/list",                         "files",                      {"path": "/mnt"}),
        collect("marketplace_listings",     "/api/2.1/marketplace-consumer/listings",     "listings",                   {"page_size": 50}),
        collect("notification_destinations","/api/2.0/notification-destinations",         "notification_destinations",   {"page_size": 100}),
        collect("git_credentials",          "/api/2.0/git-credentials",                   "credentials"),
    )

    resources["notebooks"] = await _collect_notebooks_bfs(base, hdrs)
    return resources


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("/workspace-resources")
async def get_workspace_resources(
    scan_id: str | None = Query(None),
    resource_type: str | None = Query(None),
):
    """Return raw workspace resource lists (jobs, clusters, tokens, etc.).

    Populated during every scan. Pass ?resource_type=jobs to get just one list.
    Returns 404 if the scan predates this feature — re-run to populate.
    """
    sid = scan_id
    if not sid:
        meta = _latest_result()
        sid = meta["scan_id"] if meta else None
    if not sid:
        raise HTTPException(status_code=404, detail="No scan results found")

    path = _STORE / sid / "workspace_resources.json"
    if not path.exists():
        raise HTTPException(
            status_code=404,
            detail="No workspace resource data for this scan. Re-run the scan to populate.",
        )
    try:
        data = json.loads(path.read_text())
    except Exception:
        raise HTTPException(status_code=500, detail="Failed to read workspace resources")

    if resource_type:
        if resource_type not in data:
            raise HTTPException(status_code=404, detail=f"Resource type '{resource_type}' not found")
        return data[resource_type]
    return data


@router.post("/collect-resources")
async def collect_resources(
    x_databricks_host: str | None = Header(None),
    x_databricks_token: str | None = Header(None),
    scan_id: str | None = Query(None),
):
    """Collect raw workspace resources for an existing scan (no full re-scan needed).

    Supply Databricks credentials via X-Databricks-Host and X-Databricks-Token headers.
    Saves workspace_resources.json to the scan directory (default: latest scan).
    """
    host = x_databricks_host or ""
    token = x_databricks_token or ""
    if not host or not token:
        raise HTTPException(status_code=401, detail="Databricks host and token required")

    sid = scan_id
    if not sid:
        meta = _latest_result()
        sid = meta["scan_id"] if meta else None
    if not sid:
        raise HTTPException(status_code=404, detail="No scan found to attach resources to")

    try:
        ws_resources = await _collect_workspace_resources(host.rstrip("/"), token)
        path = _STORE / sid / "workspace_resources.json"
        path.write_text(json.dumps(ws_resources, default=str, indent=2))
        summary = {k: len(v) for k, v in ws_resources.items() if isinstance(v, list)}
        return {"scan_id": sid, "resources_collected": summary}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
