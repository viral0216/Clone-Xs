"""Authentication endpoints."""

import json
import logging
import os
import secrets
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from databricks.sdk import WorkspaceClient
from fastapi import APIRouter, Depends, Header, HTTPException

from api.dependencies import get_db_client
from api.models.auth import (
    AuthStatus,
    LoginRequest,
    OAuthLoginRequest,
    ServicePrincipalRequest,
    WarehouseInfo,
)
from src.auth import clear_cache, ensure_authenticated, get_client, is_databricks_app

logger = logging.getLogger(__name__)

router = APIRouter()

# ── Server-side session store ──────────────────────────────────────────
# Maps session_id → SessionEntry so Azure/OAuth/SP logins persist across
# requests without the frontend needing raw tokens.
#
# The live WorkspaceClient is held in memory (it cannot be serialised), but a
# small "recreate descriptor" is also written to disk so sessions survive a
# backend restart (e.g. uvicorn --reload during development). On a cache miss
# the client is lazily rebuilt from that descriptor — see _rehydrate_session.
#
# created_at uses wall-clock time (time.time) so the TTL is consistent across
# restarts. The on-disk file may contain secrets (PAT token / SP secret) for
# the auth methods that have no other credential source, so it is written with
# 0600 permissions. Host-only methods (azure-cli, oauth-u2m, databricks-app)
# persist NO secrets — they rebuild from the host + the machine's own creds.

SESSION_TTL_SECONDS = 8 * 60 * 60  # 8 hours
MAX_SESSIONS = 100


def _session_file() -> Path:
    """Path to the on-disk session store. Overridable via env for tests."""
    override = os.environ.get("CLONE_XS_SESSION_FILE")
    return Path(override) if override else Path.home() / ".clone-xs" / "sessions.json"


@dataclass
class SessionEntry:
    client: WorkspaceClient
    user: str
    host: str
    auth_method: str
    created_at: float = field(default_factory=time.time)


_sessions: dict[str, SessionEntry] = {}
_sessions_lock = threading.Lock()
_persist_lock = threading.Lock()


# ── Persistence helpers (file ops guarded by _persist_lock) ─────────────


def _persist_load_all() -> dict:
    """Read the whole on-disk session map. Returns {} if missing/corrupt."""
    path = _session_file()
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except (FileNotFoundError, json.JSONDecodeError):
        return {}
    except Exception:
        logger.warning("Failed to read session store %s", path, exc_info=True)
        return {}


def _persist_write_all(data: dict) -> None:
    """Atomically write the session map with 0600 permissions."""
    path = _session_file()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".json.tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f)
        os.chmod(tmp, 0o600)
        os.replace(tmp, path)
    except Exception:
        logger.warning("Failed to write session store %s", path, exc_info=True)


def _persist_save(
    session_id: str, user: str, host: str, auth_method: str, created_at: float, recreate: dict
) -> None:
    with _persist_lock:
        data = _persist_load_all()
        data[session_id] = {
            "user": user,
            "host": host,
            "auth_method": auth_method,
            "created_at": created_at,
            "recreate": recreate,
        }
        _persist_write_all(data)


def _persist_load(session_id: str) -> Optional[dict]:
    with _persist_lock:
        return _persist_load_all().get(session_id)


def _persist_delete(session_id: str) -> None:
    with _persist_lock:
        data = _persist_load_all()
        if session_id in data:
            del data[session_id]
            _persist_write_all(data)


def _purge_expired_persisted() -> None:
    """Drop expired records from disk on startup."""
    with _persist_lock:
        data = _persist_load_all()
        now = time.time()
        fresh = {
            sid: rec
            for sid, rec in data.items()
            if now - rec.get("created_at", 0) <= SESSION_TTL_SECONDS
        }
        if len(fresh) != len(data):
            _persist_write_all(fresh)


def _recreate_client(rec: dict) -> Optional[WorkspaceClient]:
    """Rebuild a WorkspaceClient from a persisted recreate descriptor."""
    from databricks.sdk.config import Config

    host = rec.get("host") or ""
    r = rec.get("recreate") or {}
    kind = r.get("kind")

    if kind == "pat":
        return get_client(host, r.get("token"))
    if kind == "service-principal":
        if r.get("auth_type") == "azure" and r.get("tenant_id"):
            return WorkspaceClient(
                host=host,
                azure_client_id=r.get("client_id"),
                azure_client_secret=r.get("client_secret"),
                azure_tenant_id=r.get("tenant_id"),
            )
        return WorkspaceClient(
            host=host, client_id=r.get("client_id"), client_secret=r.get("client_secret")
        )
    if kind == "azure-cli":
        return WorkspaceClient(config=Config(host=host, auth_type="azure-cli"))
    if kind == "oauth":
        return get_client(host)
    if kind == "app":
        return get_client()
    return None


# ── In-memory store + rehydration ───────────────────────────────────────


def _evict_expired() -> list[str]:
    """Remove expired sessions from memory. Must hold _sessions_lock. Returns evicted IDs."""
    now = time.time()
    expired = [
        sid for sid, entry in _sessions.items() if now - entry.created_at > SESSION_TTL_SECONDS
    ]
    for sid in expired:
        del _sessions[sid]
    return expired


def create_session(
    client: WorkspaceClient,
    user: str = "",
    host: str = "",
    auth_method: str = "",
    recreate: Optional[dict] = None,
) -> str:
    """Store an authenticated client and return a session ID.

    If ``recreate`` is provided, a descriptor is also persisted to disk so the
    session can be rebuilt after a backend restart.
    """
    session_id = secrets.token_hex(16)
    created_at = time.time()
    evicted: list[str] = []
    with _sessions_lock:
        evicted = _evict_expired()
        # Cap session count to prevent unbounded growth
        if len(_sessions) >= MAX_SESSIONS:
            oldest = min(_sessions, key=lambda s: _sessions[s].created_at)
            del _sessions[oldest]
            evicted.append(oldest)
        _sessions[session_id] = SessionEntry(
            client=client, user=user, host=host, auth_method=auth_method, created_at=created_at
        )
    # File I/O outside the lock
    for sid in evicted:
        _persist_delete(sid)
    if recreate is not None:
        _persist_save(session_id, user, host, auth_method, created_at, recreate)
    return session_id


def _rehydrate_session(session_id: str) -> Optional[SessionEntry]:
    """Rebuild an in-memory session from its persisted descriptor after a restart."""
    rec = _persist_load(session_id)
    if not rec:
        return None
    created_at = rec.get("created_at", 0)
    if time.time() - created_at > SESSION_TTL_SECONDS:
        _persist_delete(session_id)
        return None
    try:
        client = _recreate_client(rec)
    except Exception:
        logger.warning(
            "Could not rehydrate session %s (%s); removing",
            session_id[:8],
            rec.get("auth_method"),
            exc_info=True,
        )
        _persist_delete(session_id)
        return None
    if client is None:
        return None
    entry = SessionEntry(
        client=client,
        user=rec.get("user", ""),
        host=rec.get("host", ""),
        auth_method=rec.get("auth_method", ""),
        created_at=created_at,
    )
    with _sessions_lock:
        _sessions[session_id] = entry
    logger.info("Rehydrated session %s (%s) after restart", session_id[:8], entry.auth_method)
    return entry


def get_session(session_id: Optional[str]) -> Optional[SessionEntry]:
    """Look up a session by ID, rebuilding from disk if not in memory. None if expired."""
    if not session_id:
        return None
    expired = False
    with _sessions_lock:
        entry = _sessions.get(session_id)
        if entry:
            if time.time() - entry.created_at > SESSION_TTL_SECONDS:
                del _sessions[session_id]
                expired = True
            else:
                return entry
    if expired:
        _persist_delete(session_id)
        return None
    # Cache miss — try to rebuild from the persisted descriptor (survives restarts)
    return _rehydrate_session(session_id)


def get_session_client(session_id: Optional[str]) -> Optional[WorkspaceClient]:
    """Look up a cached client by session ID."""
    entry = get_session(session_id)
    return entry.client if entry else None


def delete_session(session_id: Optional[str]):
    """Remove a session from memory and disk."""
    if session_id:
        with _sessions_lock:
            _sessions.pop(session_id, None)
        _persist_delete(session_id)


# Drop stale records left on disk from previous runs
_purge_expired_persisted()


def _auto_start_warehouse(client: WorkspaceClient):
    """Start the first available SQL warehouse in the background.

    Finds the configured warehouse (from config) or the first stopped one
    and issues a start command. Runs in a daemon thread so login is not blocked.
    """

    def _start():
        try:
            from src.auth import list_warehouses

            warehouses = list_warehouses(client)
            if not warehouses:
                logger.debug("No warehouses found — skipping auto-start")
                return

            # Prefer the warehouse configured in settings (clone_config.yaml)
            try:
                from src.config import load_config

                cfg = load_config()
                configured_wid = cfg.get("sql_warehouse_id", "")
            except Exception:
                configured_wid = ""

            target = None
            if configured_wid:
                target = next((w for w in warehouses if w["id"] == configured_wid), None)

            if target:
                logger.info(
                    "Using configured default warehouse: %s (%s)",
                    target.get("name", ""),
                    target["id"],
                )

            # Fall back to the first stopped warehouse if no default is configured
            if not target:
                target = next((w for w in warehouses if w["state"] == "STOPPED"), None)

            if not target:
                logger.debug("No stopped warehouses to start")
                return

            wid = target["id"]
            state = target["state"]
            if state in ("RUNNING", "STARTING"):
                logger.debug("Warehouse %s already %s", wid, state)
                return

            logger.info("Auto-starting warehouse %s (%s)", target.get("name", wid), wid)
            client.warehouses.start(wid)
        except Exception as e:
            logger.debug("Auto-start warehouse failed (non-fatal): %s", e)

    thread = threading.Thread(target=_start, daemon=True, name="warehouse-autostart")
    thread.start()


@router.get("/auto-login")
async def auto_login():
    """Auto-login when running as a Databricks App (service principal injected)."""
    if not is_databricks_app():
        raise HTTPException(status_code=404, detail="Not running as Databricks App")
    try:
        info = ensure_authenticated()
        client = get_client()
        user = info.get("user", "")
        host = info.get("host", "")
        session_id = create_session(
            client,
            user=user,
            host=host,
            auth_method="databricks-app",
            recreate={"kind": "app"},
        )
        _auto_start_warehouse(client)
        return AuthStatus(
            authenticated=True,
            user=user,
            host=host,
            auth_method="databricks-app",
            session_id=session_id,
        )
    except Exception as e:
        raise HTTPException(status_code=401, detail=str(e))


@router.post("/login")
async def login(req: LoginRequest):
    """Authenticate to a Databricks workspace."""
    try:
        clear_cache()
        client = get_client(req.host, req.token)
        info = ensure_authenticated(req.host, req.token)
        user = info.get("user", "")
        host = info.get("host", "")
        method = info.get("auth_method", "pat")
        session_id = create_session(
            client,
            user=user,
            host=host,
            auth_method=method,
            recreate={"kind": "pat", "token": req.token},
        )
        _auto_start_warehouse(client)
        return AuthStatus(
            authenticated=True,
            user=user,
            host=host,
            auth_method=method,
            session_id=session_id,
        )
    except Exception as e:
        raise HTTPException(status_code=401, detail=str(e))


@router.get("/status")
async def auth_status(
    x_databricks_host: Optional[str] = Header(None),
    x_databricks_token: Optional[str] = Header(None),
    x_clone_session: Optional[str] = Header(None),
):
    """Check current authentication status.

    Does NOT depend on get_db_client so unauthenticated callers get a stable
    authenticated=false response instead of a 401 error.

    Fast path: if a valid session exists, return cached user info instantly
    (no network call to Databricks). This is what makes page loads fast
    after Azure/OAuth login.
    """
    # Fast path — return cached session info without hitting Databricks API
    session = get_session(x_clone_session)
    if session:
        return AuthStatus(
            authenticated=True,
            user=session.user,
            host=session.host,
            auth_method=session.auth_method,
        )

    # Slow path — try to resolve a client from headers/env without raising 401
    try:
        import os

        # Databricks App runtime
        if is_databricks_app():
            client = get_client()
        elif x_databricks_host and x_databricks_token:
            client = get_client(x_databricks_host, x_databricks_token)
        else:
            # No credentials at all — report unauthenticated
            return AuthStatus(authenticated=False)

        me = client.current_user.me()
        user = me.user_name or me.display_name or ""
        host = str(client.config.host or "")

        # Determine auth method from the client config
        auth_type = getattr(client.config, "auth_type", None) or ""
        profile = os.environ.get("DATABRICKS_CONFIG_PROFILE", "")
        client_id = os.environ.get("DATABRICKS_CLIENT_ID", "")
        azure_auth = os.environ.get("DATABRICKS_AUTH_TYPE", "")

        if azure_auth == "azure-cli":
            method = "azure-cli"
        elif client_id:
            method = "service-principal"
        elif auth_type == "pat":
            method = "pat"
        elif auth_type in ("oauth-m2m", "oauth-u2m"):
            method = auth_type
        elif profile:
            method = f"cli-profile:{profile}"
        elif auth_type:
            method = auth_type
        else:
            method = "cli-profile:DEFAULT"

        return AuthStatus(
            authenticated=True,
            user=user,
            host=host,
            auth_method=method,
        )
    except Exception:
        return AuthStatus(authenticated=False)


@router.post("/oauth-login")
async def oauth_login(req: OAuthLoginRequest):
    """Trigger browser-based OAuth login."""
    from src.auth import ensure_logged_in

    try:
        _username = ensure_logged_in(host=req.host, force=True)
        info = ensure_authenticated()
        client = get_client(req.host)
        user = info.get("user", "")
        host = info.get("host", "")
        session_id = create_session(
            client,
            user=user,
            host=host,
            auth_method="oauth-u2m",
            recreate={"kind": "oauth"},
        )
        _auto_start_warehouse(client)
        return AuthStatus(
            authenticated=True, user=user, host=host, auth_method="oauth-u2m", session_id=session_id
        )
    except Exception as e:
        raise HTTPException(status_code=401, detail=str(e))


@router.post("/service-principal")
async def service_principal_login(req: ServicePrincipalRequest):
    """Authenticate with service principal credentials."""
    from databricks.sdk import WorkspaceClient

    try:
        clear_cache()
        if req.auth_type == "azure" and req.tenant_id:
            client = WorkspaceClient(
                host=req.host,
                azure_client_id=req.client_id,
                azure_client_secret=req.client_secret,
                azure_tenant_id=req.tenant_id,
            )
        else:
            client = WorkspaceClient(
                host=req.host,
                client_id=req.client_id,
                client_secret=req.client_secret,
            )
        me = client.current_user.me()
        user = me.user_name or me.display_name or ""
        session_id = create_session(
            client,
            user=user,
            host=req.host,
            auth_method="service-principal",
            recreate={
                "kind": "service-principal",
                "client_id": req.client_id,
                "client_secret": req.client_secret,
                "tenant_id": req.tenant_id,
                "auth_type": req.auth_type,
            },
        )
        _auto_start_warehouse(client)
        return AuthStatus(
            authenticated=True,
            user=user,
            host=req.host,
            auth_method="service-principal",
            session_id=session_id,
        )
    except Exception as e:
        raise HTTPException(status_code=401, detail=str(e))


@router.post("/azure-login")
async def azure_login():
    """Trigger Azure CLI browser login (az login)."""
    import shutil
    import subprocess

    # Check if Azure CLI is installed before attempting login
    if not shutil.which("az"):
        raise HTTPException(
            status_code=400,
            detail="Azure CLI (az) is not installed. Install it from https://aka.ms/install-azure-cli then retry.",
        )

    try:
        subprocess.run(
            ["az", "login", "--only-show-errors"], check=True, capture_output=True, timeout=120
        )
        return {"status": "ok", "message": "Azure login successful"}
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=408, detail="Login timed out")
    except subprocess.CalledProcessError as e:
        detail = e.stderr.decode().strip() if e.stderr else "az login failed"
        raise HTTPException(status_code=401, detail=f"Azure login failed: {detail}")
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Azure login failed: {e}")


@router.get("/azure/tenants")
async def azure_tenants():
    """List Azure tenants."""
    from src.auth import list_tenants

    return list_tenants()


@router.get("/azure/subscriptions")
async def azure_subscriptions(tenant_id: str = ""):
    """List Azure subscriptions (optionally filtered by tenant)."""
    from src.auth import list_subscriptions

    return list_subscriptions(tenant_id)


@router.get("/azure/workspaces")
async def azure_workspaces(subscription_id: str = ""):
    """List Databricks workspaces in a subscription."""
    from src.auth import list_databricks_workspaces

    if not subscription_id:
        raise HTTPException(status_code=400, detail="subscription_id required")
    return list_databricks_workspaces(subscription_id)


@router.post("/azure/connect")
async def azure_connect_workspace(req: OAuthLoginRequest):
    """Connect to a Databricks workspace discovered via Azure."""
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.config import Config

    try:
        clear_cache()
        config = Config(host=req.host, auth_type="azure-cli")
        client = WorkspaceClient(config=config)
        me = client.current_user.me()
        user = me.user_name or me.display_name or ""
        session_id = create_session(
            client,
            user=user,
            host=req.host,
            auth_method="azure-cli",
            recreate={"kind": "azure-cli"},
        )
        _auto_start_warehouse(client)
        return AuthStatus(
            authenticated=True,
            user=user,
            host=req.host,
            auth_method="azure-cli",
            session_id=session_id,
        )
    except Exception as e:
        raise HTTPException(status_code=401, detail=str(e))


@router.get("/env-vars")
async def get_env_vars():
    """Check which Databricks environment variables are set."""
    import os

    vars_to_check = [
        "DATABRICKS_HOST",
        "DATABRICKS_TOKEN",
        "DATABRICKS_CLIENT_ID",
        "DATABRICKS_CLIENT_SECRET",
        "AZURE_CLIENT_ID",
        "AZURE_CLIENT_SECRET",
        "AZURE_TENANT_ID",
        "DATABRICKS_CONFIG_PROFILE",
    ]
    result = {}
    for var in vars_to_check:
        val = os.environ.get(var, "")
        if val:
            # Mask sensitive values
            if "TOKEN" in var or "SECRET" in var:
                result[var] = val[:4] + "..." + val[-4:] if len(val) > 8 else "****"
            else:
                result[var] = val
        else:
            result[var] = None
    return result


@router.get("/warehouses")
async def list_warehouses(client=Depends(get_db_client)) -> list[WarehouseInfo]:
    """List available SQL warehouses."""
    from src.auth import list_warehouses

    warehouses = list_warehouses(client)
    return [WarehouseInfo(**wh) for wh in warehouses]


@router.post("/test-warehouse")
async def test_warehouse(req: dict, client=Depends(get_db_client)):
    """Test a SQL warehouse by running SELECT 1."""
    warehouse_id = req.get("warehouse_id", "").strip()
    if not warehouse_id:
        raise HTTPException(status_code=400, detail="warehouse_id is required")
    from src.client import execute_sql

    try:
        result = execute_sql(client, warehouse_id, "SELECT 1 AS ok", max_retries=1)
        return {"status": "ok", "message": "Warehouse is reachable", "result": result}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Warehouse test failed: {e}")


@router.get("/volumes")
async def list_volumes(client=Depends(get_db_client)):
    """List available Unity Catalog volumes."""
    from src.serverless import list_volumes as _list_volumes

    try:
        return _list_volumes(client)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list volumes: {e}")


@router.post("/logout")
async def logout(x_clone_session: Optional[str] = Header(None)):
    """Clear authentication cache and session."""
    from src.auth import clear_session

    clear_cache()
    clear_session()
    delete_session(x_clone_session)
    return {"status": "ok", "message": "Logged out successfully"}


@router.get("/serving-endpoints")
async def list_serving_endpoints(client=Depends(get_db_client)):
    """List Databricks Model Serving endpoints for AI model selection."""
    try:
        endpoints = []
        for ep in client.serving_endpoints.list():
            name = ep.name or ""
            state = str(ep.state.ready) if ep.state else "UNKNOWN"
            # Extract provider and task from served entities
            provider = "custom"
            task = ""
            try:
                entities = ep.config.served_entities if ep.config else []
                if entities:
                    task = str(getattr(entities[0], "task", "") or "")
                    ext = getattr(entities[0], "external_model", None)
                    if ext and hasattr(ext, "provider"):
                        provider = ext.provider or "custom"
            except Exception:
                pass

            # Exclude embedding-only endpoints — they don't support chat invocations
            _EMBED_KEYWORDS = {"embed", "bge", "e5-large", "e5-small", "e5-base", "nomic", "rerank"}
            is_embedding = (
                any(kw in name.lower() for kw in _EMBED_KEYWORDS) or "embed" in task.lower()
            )
            if is_embedding:
                continue

            endpoints.append(
                {
                    "name": name,
                    "state": state,
                    "provider": provider,
                    "is_claude": "claude" in name.lower() or "anthropic" in provider.lower(),
                }
            )
        return {"success": True, "endpoints": endpoints}
    except Exception as e:
        return {"success": False, "endpoints": [], "error": str(e)}


@router.get("/genie-spaces")
async def list_genie_spaces(client=Depends(get_db_client)):
    """List Databricks Genie spaces for natural language SQL."""
    try:
        import requests as req

        from src.auth import auth_headers_from_client

        config = client.config
        host = (config.host or "").rstrip("/")
        headers = auth_headers_from_client(client)

        r = req.get(f"{host}/api/2.0/genie/spaces", headers=headers, timeout=15)
        if r.status_code == 200:
            spaces = []
            for sp in r.json().get("spaces", []):
                spaces.append(
                    {
                        "space_id": sp.get("space_id", sp.get("id", "")),
                        "title": sp.get("title", sp.get("name", "")),
                        "description": sp.get("description", ""),
                    }
                )
            return {"success": True, "spaces": spaces}
        return {"success": False, "spaces": [], "error": f"HTTP {r.status_code}"}
    except Exception as e:
        return {"success": False, "spaces": [], "error": str(e)}
