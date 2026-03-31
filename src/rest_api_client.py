"""Direct Databricks REST API client — fallback when SDK methods fail.

Uses the WorkspaceClient's authenticated config (host + token) to make
raw HTTP calls to the Databricks REST API. This provides resilience against
SDK version mismatches, enum changes, and known SDK bugs.

Usage:
    from src.rest_api_client import DatabricksRestClient

    rest = DatabricksRestClient(workspace_client)
    catalogs = rest.list_catalogs()
    schemas = rest.list_schemas("my_catalog")
"""

import logging
import time
from typing import Optional
from urllib.parse import quote

import requests

logger = logging.getLogger(__name__)


class DatabricksRestClient:
    """Direct REST API client using the SDK's authenticated credentials."""

    def __init__(self, workspace_client):
        """Initialize from an existing WorkspaceClient.

        Extracts host and token from the SDK client's config so we don't
        need to manage auth separately.
        """
        self._ws = workspace_client
        config = workspace_client.config
        self.host = (config.host or "").rstrip("/")
        # Build auth headers from the SDK config
        self._headers = {"Content-Type": "application/json"}
        try:
            # SDK >= 0.20: use the config's authenticate method to get headers
            auth_headers = {}
            config.authenticate(auth_headers)
            self._headers.update(auth_headers)
        except Exception:
            # Fallback: try to extract token directly
            token = getattr(config, "token", None)
            if token:
                self._headers["Authorization"] = f"Bearer {token}"

    # ── Generic HTTP ─────────────────────────────────────────────────

    def _request(
        self,
        method: str,
        path: str,
        body: Optional[dict] = None,
        params: Optional[dict] = None,
        timeout: int = 30,
    ) -> dict:
        """Make an authenticated request to the Databricks REST API."""
        url = f"{self.host}{path}"
        try:
            resp = requests.request(
                method,
                url,
                headers=self._headers,
                json=body,
                params=params,
                timeout=timeout,
            )
            resp.raise_for_status()
            if resp.content:
                return resp.json()
            return {}
        except requests.HTTPError as e:
            detail = ""
            try:
                detail = e.response.json().get("message", e.response.text[:200])
            except Exception:
                detail = str(e)
            logger.error(f"REST API {method} {path} failed: {detail}")
            raise RuntimeError(f"Databricks REST API error: {detail}") from e

    def _get(self, path: str, params: Optional[dict] = None) -> dict:
        return self._request("GET", path, params=params)

    def _post(self, path: str, body: Optional[dict] = None) -> dict:
        return self._request("POST", path, body=body)

    def _patch(self, path: str, body: Optional[dict] = None) -> dict:
        return self._request("PATCH", path, body=body)

    def _delete(self, path: str) -> dict:
        return self._request("DELETE", path)

    def _paginate(self, path: str, key: str, params: Optional[dict] = None) -> list[dict]:
        """Paginate through a Unity Catalog list endpoint."""
        all_items = []
        params = dict(params or {})
        while True:
            data = self._get(path, params=params)
            items = data.get(key, [])
            all_items.extend(items)
            token = data.get("next_page_token")
            if not token:
                break
            params["page_token"] = token
        return all_items

    # ── Unity Catalog: Catalogs ─────────────────────────────────────

    def list_catalogs(self) -> list[dict]:
        """GET /api/2.1/unity-catalog/catalogs"""
        return self._paginate("/api/2.1/unity-catalog/catalogs", "catalogs")

    def get_catalog(self, name: str) -> dict:
        """GET /api/2.1/unity-catalog/catalogs/{name}"""
        return self._get(f"/api/2.1/unity-catalog/catalogs/{quote(name, safe='')}")

    # ── Unity Catalog: Schemas ──────────────────────────────────────

    def list_schemas(self, catalog_name: str) -> list[dict]:
        """GET /api/2.1/unity-catalog/schemas?catalog_name=X"""
        return self._paginate(
            "/api/2.1/unity-catalog/schemas",
            "schemas",
            params={"catalog_name": catalog_name},
        )

    def get_schema(self, full_name: str) -> dict:
        """GET /api/2.1/unity-catalog/schemas/{full_name}"""
        return self._get(f"/api/2.1/unity-catalog/schemas/{quote(full_name, safe='')}")

    # ── Unity Catalog: Tables ───────────────────────────────────────

    def list_tables(self, catalog_name: str, schema_name: str) -> list[dict]:
        """GET /api/2.1/unity-catalog/tables?catalog_name=X&schema_name=Y"""
        return self._paginate(
            "/api/2.1/unity-catalog/tables",
            "tables",
            params={"catalog_name": catalog_name, "schema_name": schema_name},
        )

    def get_table(self, full_name: str) -> dict:
        """GET /api/2.1/unity-catalog/tables/{full_name}"""
        return self._get(f"/api/2.1/unity-catalog/tables/{quote(full_name, safe='')}")

    def delete_table(self, full_name: str) -> None:
        """DELETE /api/2.1/unity-catalog/tables/{full_name}"""
        self._delete(f"/api/2.1/unity-catalog/tables/{quote(full_name, safe='')}")

    # ── Unity Catalog: Volumes ──────────────────────────────────────

    def list_volumes(self, catalog_name: str, schema_name: str) -> list[dict]:
        """GET /api/2.1/unity-catalog/volumes"""
        return self._paginate(
            "/api/2.1/unity-catalog/volumes",
            "volumes",
            params={"catalog_name": catalog_name, "schema_name": schema_name},
        )

    # ── Unity Catalog: Functions ────────────────────────────────────

    def list_functions(self, catalog_name: str, schema_name: str) -> list[dict]:
        """GET /api/2.1/unity-catalog/functions"""
        return self._paginate(
            "/api/2.1/unity-catalog/functions",
            "functions",
            params={"catalog_name": catalog_name, "schema_name": schema_name},
        )

    # ── Permissions / Grants ────────────────────────────────────────

    def get_grants(self, securable_type: str, full_name: str) -> dict:
        """GET /api/2.1/unity-catalog/permissions/{type}/{name}"""
        st = securable_type.lower()
        return self._get(
            f"/api/2.1/unity-catalog/permissions/{quote(st, safe='')}/{quote(full_name, safe='')}"
        )

    def get_effective_grants(self, securable_type: str, full_name: str) -> dict:
        """GET /api/2.1/unity-catalog/effective-permissions/{type}/{name}"""
        st = securable_type.lower()
        return self._get(
            f"/api/2.1/unity-catalog/effective-permissions/{quote(st, safe='')}/{quote(full_name, safe='')}"
        )

    def update_grants(self, securable_type: str, full_name: str, changes: dict) -> dict:
        """PATCH /api/2.1/unity-catalog/permissions/{type}/{name}"""
        st = securable_type.lower()
        return self._patch(
            f"/api/2.1/unity-catalog/permissions/{quote(st, safe='')}/{quote(full_name, safe='')}",
            body=changes,
        )

    # ── SQL Warehouses ──────────────────────────────────────────────

    def list_warehouses(self) -> list[dict]:
        """GET /api/2.0/sql/warehouses"""
        data = self._get("/api/2.0/sql/warehouses")
        return data.get("warehouses", [])

    def get_warehouse(self, warehouse_id: str) -> dict:
        """GET /api/2.0/sql/warehouses/{id}"""
        return self._get(f"/api/2.0/sql/warehouses/{warehouse_id}")

    def start_warehouse(self, warehouse_id: str) -> None:
        """POST /api/2.0/sql/warehouses/{id}/start"""
        self._post(f"/api/2.0/sql/warehouses/{warehouse_id}/start")

    # ── SQL Statement Execution ─────────────────────────────────────

    def execute_sql(self, warehouse_id: str, statement: str, wait_timeout: str = "50s") -> dict:
        """POST /api/2.0/sql/statements — execute SQL and wait for result."""
        result = self._post("/api/2.0/sql/statements", body={
            "warehouse_id": warehouse_id,
            "statement": statement,
            "wait_timeout": wait_timeout,
        })
        # Poll if still running
        status = result.get("status", {})
        while status.get("state") in ("RUNNING", "PENDING"):
            time.sleep(2)
            stmt_id = result.get("statement_id")
            result = self._get(f"/api/2.0/sql/statements/{stmt_id}")
            status = result.get("status", {})

        if status.get("state") == "FAILED":
            error = status.get("error", {}).get("message", "Unknown SQL error")
            raise RuntimeError(f"SQL execution failed: {error}")

        return result

    # ── Current User ────────────────────────────────────────────────

    def get_current_user(self) -> dict:
        """GET /api/2.0/preview/scim/v2/Me"""
        return self._get("/api/2.0/preview/scim/v2/Me")

    # ── Jobs API ────────────────────────────────────────────────────

    def list_jobs(self) -> list[dict]:
        """GET /api/2.1/jobs/list"""
        data = self._get("/api/2.1/jobs/list")
        return data.get("jobs", [])

    def create_job(self, settings: dict) -> dict:
        """POST /api/2.1/jobs/create"""
        return self._post("/api/2.1/jobs/create", body=settings)

    def run_now(self, job_id: int) -> dict:
        """POST /api/2.1/jobs/run-now"""
        return self._post("/api/2.1/jobs/run-now", body={"job_id": job_id})


# ── Singleton access ─────────────────────────────────────────────────

_rest_clients: dict[str, DatabricksRestClient] = {}


def get_rest_client(workspace_client) -> DatabricksRestClient:
    """Get or create a DatabricksRestClient for the given WorkspaceClient."""
    host = (workspace_client.config.host or "").rstrip("/")
    if host not in _rest_clients:
        _rest_clients[host] = DatabricksRestClient(workspace_client)
    return _rest_clients[host]
