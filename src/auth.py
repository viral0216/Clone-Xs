"""Databricks authentication — PAT, service principal, OAuth browser, CLI profile, notebook native.

Follows the same pattern as azure-scanner/auth.py: multiple auth methods with
fallback chain, client caching, browser-based login, and pre-operation verification.

Auth method priority:
  1. Explicit host + token (passed as arguments)
  2. Service principal (DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET + DATABRICKS_HOST)
  3. Azure AD service principal (AZURE_CLIENT_ID + AZURE_CLIENT_SECRET + AZURE_TENANT_ID)
  4. Environment variables (DATABRICKS_HOST + DATABRICKS_TOKEN)
  5. Databricks CLI profile (~/.databrickscfg)
  6. Notebook native auth (auto-detected inside Databricks Runtime)

Browser-based login (like azure-scanner's `az login`):
  - ensure_logged_in(force=True) runs `databricks auth login` to open browser OAuth flow
  - Creates/refreshes an OAuth token stored in ~/.databrickscfg
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import time as _time

from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv

load_dotenv()


def is_databricks_app() -> bool:
    """Detect if running as a Databricks App (service principal auth is injected)."""
    return os.getenv("CLONE_XS_RUNTIME") == "databricks-app"

logger = logging.getLogger(__name__)

# ── Module-level client cache ─────────────────────────────────────────

_cached_client: WorkspaceClient | None = None
_cached_client_key: str = ""  # tracks which credentials produced the cached client
_client_verified: bool = False
_client_verify_time: float = 0
_VERIFY_TTL = 3600  # re-verify every hour
_SESSION_FILE = os.path.expanduser("~/.clxs-session.json")


def _cache_key(host: str = "", token: str = "", profile: str = "") -> str:
    """Build a cache key from auth parameters."""
    return f"{host}|{token[:8] if token else ''}|{profile}"


def _is_client_valid(key: str) -> bool:
    """Return True if cached client matches the requested credentials and is verified."""
    if _cached_client is None or _cached_client_key != key:
        return False
    if not _client_verified:
        return False
    if _time.time() - _client_verify_time > _VERIFY_TTL:
        return False
    return True


def _save_session(host: str, warehouse_id: str = "") -> None:
    """Save session info to disk so subsequent CLI invocations reuse it."""
    session = {"host": host, "timestamp": _time.time()}
    if warehouse_id:
        session["warehouse_id"] = warehouse_id
    try:
        with open(_SESSION_FILE, "w") as f:
            json.dump(session, f)
        logger.debug("Session saved to %s", _SESSION_FILE)
    except Exception as e:
        logger.debug("Failed to save session: %s", e)


def _load_session() -> dict:
    """Load saved session. Returns empty dict if expired or missing."""
    try:
        with open(_SESSION_FILE) as f:
            session = json.load(f)
        # Sessions expire after 8 hours
        if _time.time() - session.get("timestamp", 0) > 28800:
            logger.debug("Session expired")
            return {}
        return session
    except Exception:
        return {}


def clear_cache() -> None:
    """Clear the cached client (useful for testing or re-authentication)."""
    global _cached_client, _cached_client_key, _client_verified, _client_verify_time
    _cached_client = None
    _cached_client_key = ""
    _client_verified = False
    _client_verify_time = 0
    try:
        from src.client import clear_metadata_cache
        clear_metadata_cache()
    except ImportError:
        pass
    logger.debug("Auth and metadata cache cleared")


def clear_session() -> None:
    """Remove the saved session file."""
    try:
        if os.path.exists(_SESSION_FILE):
            os.remove(_SESSION_FILE)
            logger.debug("Session file removed: %s", _SESSION_FILE)
    except Exception as e:
        logger.debug("Failed to remove session file: %s", e)


# ── CLI helpers ────────────────────────────────────────────────────────

# Suppress az CLI browser popups, telemetry, and interactive prompts
_AZ_ENV = {
    **os.environ,
    "AZURE_CORE_COLLECT_TELEMETRY": "0",
    "AZURE_CORE_NO_COLOR": "1",
    "AZURE_CORE_NO_PROMPT": "1",
    "AZURE_CORE_ONLY_SHOW_ERRORS": "true",
    "AZURE_CORE_DISABLE_CHECK_UPDATE": "true",
    "AZURE_CORE_SURVEY_MESSAGE": "false",
    "AZURE_CORE_OUTPUT": "json",
    "BROWSER": "",
}


def _run_az(*args: str, timeout: int = 30) -> dict | list:
    """Run an az CLI command and return parsed JSON output."""
    cmd = ["az", *args, "--output", "json", "--only-show-errors"]
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout, env=_AZ_ENV,
        )
        if result.returncode != 0:
            raise RuntimeError(f"az {' '.join(args)} failed: {result.stderr.strip()}")
        return json.loads(result.stdout) if result.stdout.strip() else {}
    except FileNotFoundError:
        raise RuntimeError(
            "Azure CLI not found. Install: https://learn.microsoft.com/en-us/cli/azure/install-azure-cli"
        )


def _run_databricks_cli(*args: str, timeout: int = 30) -> dict | list | str:
    """Run a databricks CLI command and return parsed JSON output."""
    cmd = ["databricks", *args, "--output", "json"]
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout,
        )
        if result.returncode != 0:
            raise RuntimeError(f"databricks {' '.join(args)} failed: {result.stderr.strip()}")
        if result.stdout.strip():
            return json.loads(result.stdout)
        return {}
    except FileNotFoundError:
        raise RuntimeError(
            "Databricks CLI not found. Install with: pip install databricks-cli\n"
            "Or: curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh"
        )


# ── Public API ─────────────────────────────────────────────────────────

def ensure_logged_in(host: str | None = None, force: bool = False) -> str:
    """Ensure user is logged in via Databricks CLI. Returns username.

    Similar to azure-scanner's ensure_logged_in() which runs `az login`.
    This runs `databricks auth login` to open a browser-based OAuth flow.

    If *force* is True, always run `databricks auth login` even if a session exists.
    If *host* is provided, authenticates against that specific workspace.

    Args:
        host: Databricks workspace URL (required for first-time login).
        force: If True, force re-authentication via browser.

    Returns:
        Username string from the authenticated session.
    """
    workspace_host = host or os.getenv("DATABRICKS_HOST", "")

    if not force:
        # Check if we already have a valid session
        try:
            client = WorkspaceClient(host=workspace_host) if workspace_host else WorkspaceClient()
            me = client.current_user.me()
            username = me.user_name or me.display_name or "Databricks User"
            logger.debug("Already logged in as %s", username)
            return username
        except Exception:
            pass

    # Run databricks auth login (opens browser)
    if not workspace_host:
        raise RuntimeError(
            "Workspace host required for browser login. Provide --host or set DATABRICKS_HOST.\n"
            "Example: clxs auth --login --host https://adb-xxx.azuredatabricks.net"
        )

    print(f"Opening Databricks login in browser for {workspace_host}...")
    try:
        cmd = ["databricks", "auth", "login", "--host", workspace_host]
        result = subprocess.run(
            cmd, capture_output=False, timeout=120,
        )
        if result.returncode != 0:
            raise RuntimeError("databricks auth login failed. Check your workspace URL.")
    except FileNotFoundError:
        raise RuntimeError(
            "Databricks CLI not found. Install with: pip install databricks-cli\n"
            "Or: curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh"
        )

    # Verify the login worked
    clear_cache()
    try:
        client = WorkspaceClient(host=workspace_host)
        me = client.current_user.me()
        username = me.user_name or me.display_name or "Databricks User"
        logger.info("Logged in as %s on %s", username, workspace_host)
        return username
    except Exception as e:
        raise RuntimeError(f"Login completed but verification failed: {e}") from e


def list_profiles() -> list[dict]:
    """List configured Databricks CLI profiles from ~/.databrickscfg.

    Similar to azure-scanner's list_tenants() — shows available auth contexts.

    Returns:
        List of dicts with profile name, host, and auth type.
    """
    config_path = os.path.expanduser("~/.databrickscfg")
    if not os.path.exists(config_path):
        return []

    import configparser
    cfg = configparser.ConfigParser()
    cfg.read(config_path)

    profiles = []
    for section in cfg.sections():
        profile = {"name": section, "host": cfg.get(section, "host", fallback="")}

        # Detect auth type
        if cfg.get(section, "token", fallback=""):
            profile["auth_type"] = "pat"
        elif cfg.get(section, "client_id", fallback=""):
            profile["auth_type"] = "oauth-sp"
        elif cfg.get(section, "azure_client_id", fallback=""):
            profile["auth_type"] = "azure-ad-sp"
        else:
            profile["auth_type"] = "oauth-u2m"  # browser-based OAuth

        profiles.append(profile)

    return profiles


def list_tenants() -> list[dict]:
    """List Azure tenants the user has access to.

    Same pattern as azure-scanner's list_tenants().
    """
    tenants: list[dict] = []
    try:
        data = _run_az("account", "tenant", "list")
        for t in data:
            tenants.append({
                "tenant_id": t.get("tenantId", ""),
                "name": t.get("displayName", "") or t.get("tenantId", ""),
            })
    except Exception:
        pass

    # Enrich with active status from account list
    try:
        accounts = _run_az("account", "list", "--all")
        active_tenants = {a.get("tenantId") for a in accounts if a.get("state") == "Enabled"}
        if not tenants:
            seen: set[str] = set()
            for acc in accounts:
                tid = acc.get("tenantId", "")
                if tid and tid not in seen:
                    seen.add(tid)
                    tenants.append({
                        "tenant_id": tid,
                        "name": acc.get("tenantDisplayName", "") or tid,
                        "is_active": tid in active_tenants,
                    })
        else:
            for t in tenants:
                t["is_active"] = t["tenant_id"] in active_tenants
    except Exception:
        pass

    return tenants


def list_subscriptions(tenant_id: str = "") -> list[dict]:
    """List Azure subscriptions.

    Same pattern as azure-scanner's list_subscriptions().
    """
    accounts = _run_az("account", "list", "--all")
    subs: list[dict] = []
    seen: set[str] = set()
    for acc in accounts:
        sub_id = acc.get("id", "")
        if not sub_id or sub_id in seen:
            continue
        if tenant_id and acc.get("tenantId", "") != tenant_id:
            continue
        seen.add(sub_id)
        subs.append({
            "subscription_id": sub_id,
            "name": acc.get("name", sub_id),
            "tenant_id": acc.get("tenantId", ""),
            "state": acc.get("state", ""),
        })
    subs.sort(key=lambda s: (s["state"] != "Enabled", s["name"].lower()))
    return subs


def list_databricks_workspaces(subscription_id: str) -> list[dict]:
    """List Databricks workspaces in an Azure subscription via ARM API.

    Uses `az rest` to query the Azure Resource Manager API for
    Microsoft.Databricks/workspaces resources.
    """
    try:
        url = (
            f"https://management.azure.com/subscriptions/{subscription_id}"
            f"/providers/Microsoft.Databricks/workspaces"
            f"?api-version=2024-05-01"
        )
        data = _run_az("rest", "--method", "GET", "--url", url, timeout=30)
        workspaces = []
        for ws in data.get("value", []):
            props = ws.get("properties", {})
            workspace_url = props.get("workspaceUrl", "")
            if workspace_url and not workspace_url.startswith("https://"):
                workspace_url = f"https://{workspace_url}"
            workspaces.append({
                "name": ws.get("name", ""),
                "host": workspace_url,
                "location": ws.get("location", ""),
                "resource_group": ws.get("id", "").split("/resourceGroups/")[-1].split("/")[0]
                    if "/resourceGroups/" in ws.get("id", "") else "",
                "sku": ws.get("sku", {}).get("name", ""),
                "state": props.get("provisioningState", ""),
                "workspace_id": props.get("workspaceId", ""),
            })
        return workspaces
    except Exception as e:
        logger.debug("Failed to list workspaces: %s", e)
        return []


def interactive_login() -> dict:
    """Full interactive login flow: browser auth -> tenant -> subscription -> workspace.

    Same pattern as azure-scanner/sat-scanner:
      1. Opens browser for Azure AD login (az login)
      2. Select tenant (if multiple)
      3. Select subscription (if multiple)
      4. Discover and select Databricks workspace
      5. Auto-configure and verify

    Returns:
        dict with workspace host, user, tenant, subscription info.
    """
    print()
    print("  ============================================")
    print("  Databricks Interactive Login")
    print("  ============================================")

    # ── Step 1: Auth decision ──────────────────────────────────────
    print()
    print("  No credentials provided.")
    print("  Options:")
    print("    1. Azure login (opens browser)")
    print("    2. Use existing az CLI session")
    print("    3. Use existing Databricks CLI profile")
    print("    4. Exit")
    choice = input("\n  Choose [1/2/3/4]: ").strip()

    if choice == "4":
        raise SystemExit(0)

    if choice == "3":
        # Use existing Databricks profile
        profiles = list_profiles()
        if not profiles:
            print("  No profiles found in ~/.databrickscfg")
            raise SystemExit(1)
        print(f"\n  Available profiles ({len(profiles)}):")
        for i, p in enumerate(profiles, 1):
            print(f"    {i}. {p['name']:<20} {p['host']:<50} {p['auth_type']}")
        pick = input(f"\n  Select profile [1-{len(profiles)}] (default: 1): ").strip()
        idx = int(pick) - 1 if pick.isdigit() and 1 <= int(pick) <= len(profiles) else 0
        selected = profiles[idx]
        print(f"  Using profile: {selected['name']}")
        info = ensure_authenticated(profile=selected["name"])
        info["auth_flow"] = "cli-profile"
        return info

    if choice == "1":
        # Browser login via az CLI
        print("\n  Opening Azure login in browser...")
        try:
            result = subprocess.run(
                ["az", "login", "--output", "json", "--only-show-errors"],
                capture_output=True, text=True, timeout=120, env=_AZ_ENV,
            )
            if result.returncode != 0:
                print(f"  Error: az login failed: {result.stderr.strip()}")
                raise SystemExit(1)
            accounts = json.loads(result.stdout) if result.stdout.strip() else []
            if accounts:
                user = accounts[0].get("user", {}).get("name", "Azure User")
                print(f"  Logged in as: {user}")
        except FileNotFoundError:
            print("  Error: Azure CLI not found. Install: https://aka.ms/install-azure-cli")
            raise SystemExit(1)
    else:
        # Verify existing session
        try:
            info = _run_az("account", "show")
            user = info.get("user", {}).get("name", "Azure CLI User")
            print(f"  Using existing session: {user}")
        except Exception:
            print("  Error: No active Azure session. Choose option 1 to login.")
            raise SystemExit(1)

    # ── Step 2: Tenant selection ───────────────────────────────────
    tenant_id = ""
    tenants = list_tenants()
    if len(tenants) > 1:
        print(f"\n  Available tenants ({len(tenants)}):")
        for i, t in enumerate(tenants, 1):
            active_mark = " *" if t.get("is_active") else ""
            print(f"    {i}. {t['name']} ({t['tenant_id'][:8]}...){active_mark}")
        pick = input(f"\n  Select tenant [1-{len(tenants)}] (default: 1): ").strip()
        idx = int(pick) - 1 if pick.isdigit() and 1 <= int(pick) <= len(tenants) else 0
        tenant_id = tenants[idx]["tenant_id"]
        print(f"  Tenant: {tenants[idx]['name']}")
    elif len(tenants) == 1:
        tenant_id = tenants[0]["tenant_id"]
        print(f"  Tenant: {tenants[0]['name']}")

    # ── Step 3: Subscription selection ─────────────────────────────
    subs = list_subscriptions(tenant_id)
    enabled = [s for s in subs if s.get("state") == "Enabled"]

    if not enabled:
        print("  Error: No enabled Azure subscriptions found.")
        raise SystemExit(1)

    if len(enabled) == 1:
        sub_id = enabled[0]["subscription_id"]
        print(f"  Subscription: {enabled[0]['name']}")
    else:
        print(f"\n  Available subscriptions ({len(enabled)}):")
        for i, s in enumerate(enabled, 1):
            print(f"    {i}. {s['name']} ({s['subscription_id'][:8]}...)")
        pick = input(f"\n  Select subscription [1-{len(enabled)}] (default: 1): ").strip()
        idx = int(pick) - 1 if pick.isdigit() and 1 <= int(pick) <= len(enabled) else 0
        sub_id = enabled[idx]["subscription_id"]
        print(f"  Subscription: {enabled[idx]['name']}")

    # ── Step 4: Workspace discovery ────────────────────────────────
    print("\n  Discovering Databricks workspaces...")
    workspaces = list_databricks_workspaces(sub_id)

    if not workspaces:
        print("  No Databricks workspaces found in this subscription.")
        host = input("  Enter workspace URL manually: ").strip()
        if not host:
            raise SystemExit(1)
    elif len(workspaces) == 1:
        ws = workspaces[0]
        host = ws["host"]
        print(f"  Workspace: {ws['name']} ({ws['location']}) - {host}")
    else:
        print(f"\n  Databricks workspaces ({len(workspaces)}):")
        for i, ws in enumerate(workspaces, 1):
            state_mark = " *" if ws.get("state") == "Succeeded" else ""
            print(f"    {i}. {ws['name']:<30} {ws['location']:<15} {ws['sku']:<12}{state_mark}")
            print(f"       {ws['host']}")
        pick = input(f"\n  Select workspace [1-{len(workspaces)}] (default: 1): ").strip()
        idx = int(pick) - 1 if pick.isdigit() and 1 <= int(pick) <= len(workspaces) else 0
        ws = workspaces[idx]
        host = ws["host"]
        print(f"  Workspace: {ws['name']}")

    # ── Step 5: Authenticate to workspace ──────────────────────────
    print(f"\n  Connecting to {host}...")
    clear_cache()
    try:
        # Try using existing Azure CLI auth with WorkspaceClient
        client = WorkspaceClient(host=host)
        me = client.current_user.me()
        username = me.user_name or me.display_name or "unknown"

        result = {
            "authenticated": True,
            "user": username,
            "host": host,
            "auth_method": "azure-cli-interactive",
            "tenant_id": tenant_id,
            "subscription_id": sub_id,
            "auth_flow": "interactive",
        }

        print(f"  Authenticated as: {username}")
        print(f"  Workspace: {host}")
        print()
        print("  ============================================")
        print("  Ready to use!")
        print("  ============================================")
        print()

        # Cache the client
        global _cached_client, _cached_client_key, _client_verified, _client_verify_time
        _cached_client = client
        _cached_client_key = _cache_key(host)
        _client_verified = True
        _client_verify_time = _time.time()

        # Persist for subsequent CLI invocations
        os.environ["DATABRICKS_HOST"] = host
        _save_session(host)

        return result

    except Exception as e:
        print(f"  Error connecting to workspace: {e}")
        print("  Try: clxs auth --login --host " + host)
        raise SystemExit(1)


def switch_profile(profile: str) -> dict:
    """Switch to a different CLI profile and verify authentication.

    Similar to azure-scanner's switch_tenant().

    Args:
        profile: Databricks CLI profile name from ~/.databrickscfg.

    Returns:
        Auth info dict (same as ensure_authenticated).
    """
    clear_cache()
    return ensure_authenticated(profile=profile)


def get_client(
    host: str | None = None,
    token: str | None = None,
    profile: str | None = None,
) -> WorkspaceClient:
    """Get an authenticated WorkspaceClient with caching.

    Tries auth methods in priority order:
      1. Explicit host + token
      2. Service principal (Databricks OAuth or Azure AD)
      3. Environment variables (DATABRICKS_HOST + DATABRICKS_TOKEN)
      4. Databricks CLI profile
      5. Notebook native auth (WorkspaceClient() with no args)

    Args:
        host: Databricks workspace URL (e.g. "https://adb-xxx.azuredatabricks.net").
        token: Databricks personal access token.
        profile: Databricks CLI profile name from ~/.databrickscfg.

    Returns:
        Authenticated WorkspaceClient instance.
    """
    global _cached_client, _cached_client_key, _client_verified, _client_verify_time

    key = _cache_key(host or "", token or "", profile or "")

    if _is_client_valid(key):
        return _cached_client

    client = _create_client(host, token, profile)
    _cached_client = client
    _cached_client_key = key
    _client_verified = False
    _client_verify_time = 0

    return client


def _create_client(
    host: str | None = None,
    token: str | None = None,
    profile: str | None = None,
) -> WorkspaceClient:
    """Create a new WorkspaceClient using the best available auth method."""

    # Method 0: Databricks App (auto-injected service principal)
    if is_databricks_app():
        logger.debug("Auth: Databricks App runtime — using injected credentials")
        return WorkspaceClient()

    # Method 1: Explicit host + token
    if host and token:
        logger.debug("Auth: using explicit host + token")
        return WorkspaceClient(host=host, token=token)

    # Method 2: Databricks OAuth service principal
    client_id = os.getenv("DATABRICKS_CLIENT_ID", "")
    client_secret = os.getenv("DATABRICKS_CLIENT_SECRET", "")
    env_host = os.getenv("DATABRICKS_HOST", "")
    if client_id and client_secret and env_host:
        logger.debug("Auth: using Databricks OAuth service principal")
        return WorkspaceClient(
            host=env_host,
            client_id=client_id,
            client_secret=client_secret,
        )

    # Method 3: Azure AD service principal
    azure_client_id = os.getenv("AZURE_CLIENT_ID", "")
    azure_client_secret = os.getenv("AZURE_CLIENT_SECRET", "")
    azure_tenant_id = os.getenv("AZURE_TENANT_ID", "")
    if azure_client_id and azure_client_secret and azure_tenant_id and env_host:
        logger.debug("Auth: using Azure AD service principal")
        return WorkspaceClient(
            host=env_host,
            azure_client_id=azure_client_id,
            azure_client_secret=azure_client_secret,
            azure_tenant_id=azure_tenant_id,
        )

    # Method 4: Environment variables (PAT)
    env_token = os.getenv("DATABRICKS_TOKEN", "")
    if env_host and env_token:
        logger.debug("Auth: using DATABRICKS_HOST + DATABRICKS_TOKEN env vars")
        return WorkspaceClient(host=env_host, token=env_token)

    # Method 5: Databricks CLI profile
    if profile:
        logger.debug("Auth: using CLI profile '%s'", profile)
        return WorkspaceClient(profile=profile)

    # Method 5b: Saved session from interactive login
    session = _load_session()
    if session.get("host") and not env_host:
        session_host = session["host"]
        session_token = session.get("token")
        if session_token:
            logger.debug("Auth: using saved session for %s (with token)", session_host)
            return WorkspaceClient(host=session_host, token=session_token)
        # Host-only session — try azure-cli auth non-interactively
        import shutil
        if shutil.which("az"):
            logger.debug("Auth: using saved session for %s (azure-cli)", session_host)
            from databricks.sdk.config import Config
            return WorkspaceClient(config=Config(host=session_host, auth_type="azure-cli"))
        logger.debug("Auth: saved session for %s but no token or az CLI", session_host)

    # Check for default profile in ~/.databrickscfg
    config_path = os.path.expanduser("~/.databrickscfg")
    if os.path.exists(config_path) and not env_host:
        logger.debug("Auth: using default Databricks CLI profile")
        try:
            # Use pat auth type to prevent SDK from opening browser
            return WorkspaceClient(auth_type="pat")
        except Exception:
            # pat auth may fail if profile uses different auth — fall through
            pass

    # Final fallback — raise instead of calling WorkspaceClient() which may open browser
    raise RuntimeError(
        "No authentication configured. Log in via the Clone-Xs UI (Settings → Authentication) "
        "or set DATABRICKS_HOST + DATABRICKS_TOKEN environment variables."
    )


def ensure_authenticated(
    host: str | None = None,
    token: str | None = None,
    profile: str | None = None,
) -> dict:
    """Verify authentication by making a test API call.

    Returns a dict with auth status, user info, and workspace details.
    Raises RuntimeError if authentication fails.

    Example:
        >>> info = ensure_authenticated()
        >>> print(f"Logged in as {info['user']} on {info['host']}")
    """
    global _client_verified, _client_verify_time

    client = get_client(host, token, profile)

    try:
        me = client.current_user.me()
        host_url = client.config.host or "unknown"

        info = {
            "authenticated": True,
            "user": me.user_name or me.display_name or "unknown",
            "user_id": me.id or "",
            "host": host_url,
            "auth_method": _detect_auth_method(host, token, profile),
        }

        _client_verified = True
        _client_verify_time = _time.time()

        logger.info("Authenticated as %s on %s (%s)", info["user"], info["host"], info["auth_method"])
        return info

    except Exception as e:
        clear_cache()
        raise RuntimeError(
            f"Authentication failed: {e}\n\n"
            "Configure one of:\n"
            "  1. Set DATABRICKS_HOST + DATABRICKS_TOKEN env vars\n"
            "  2. Set DATABRICKS_HOST + DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET\n"
            "  3. Set DATABRICKS_HOST + AZURE_CLIENT_ID + AZURE_CLIENT_SECRET + AZURE_TENANT_ID\n"
            "  4. Run 'databricks configure' to set up a CLI profile\n"
            "  5. Run inside a Databricks notebook (auto-auth)\n"
        ) from e


def _detect_auth_method(
    host: str | None = None,
    token: str | None = None,
    profile: str | None = None,
) -> str:
    """Detect which auth method is being used (for display purposes)."""
    if is_databricks_app():
        return "databricks-app"
    if host and token:
        return "explicit-token"
    if os.getenv("DATABRICKS_CLIENT_ID") and os.getenv("DATABRICKS_CLIENT_SECRET"):
        return "databricks-oauth-sp"
    if os.getenv("AZURE_CLIENT_ID") and os.getenv("AZURE_CLIENT_SECRET"):
        return "azure-ad-sp"
    if os.getenv("DATABRICKS_HOST") and os.getenv("DATABRICKS_TOKEN"):
        return "env-pat"
    if profile:
        return f"cli-profile:{profile}"
    if os.path.exists(os.path.expanduser("~/.databrickscfg")):
        return "cli-profile:DEFAULT"
    return "notebook-native"


def get_auth_status() -> dict:
    """Get current authentication status without making API calls.

    Returns:
        dict with cached auth info (authenticated, auth_method, host).
    """
    if _cached_client and _client_verified:
        return {
            "authenticated": True,
            "auth_method": _detect_auth_method(),
            "host": _cached_client.config.host or "unknown",
            "cached": True,
            "verified_at": _client_verify_time,
        }
    return {
        "authenticated": False,
        "auth_method": None,
        "host": None,
        "cached": False,
    }


def list_warehouses(client: WorkspaceClient) -> list[dict]:
    """List available SQL warehouses in the workspace.

    Returns:
        List of dicts with warehouse id, name, size, state, and type.
    """
    warehouses = []
    try:
        for wh in client.warehouses.list():
            warehouses.append({
                "id": wh.id,
                "name": wh.name or "",
                "size": getattr(wh, "cluster_size", "") or "",
                "state": str(getattr(wh, "state", "")).split(".")[-1] if wh.state else "UNKNOWN",
                "type": "SERVERLESS" if getattr(wh, "enable_serverless_compute", False)
                    else "PRO" if getattr(wh, "warehouse_type", None) and "PRO" in str(wh.warehouse_type)
                    else "CLASSIC",
            })
    except Exception as e:
        logger.debug("Failed to list warehouses: %s", e)
    return warehouses


def select_warehouse(client: WorkspaceClient) -> str:
    """Interactively list SQL warehouses and let the user pick one.

    Returns:
        The selected warehouse ID.
    """
    warehouses = list_warehouses(client)
    if not warehouses:
        print("  No SQL warehouses found in this workspace.")
        wid = input("  Enter warehouse ID manually: ").strip()
        if not wid:
            raise SystemExit(1)
        return wid

    if len(warehouses) == 1:
        wh = warehouses[0]
        print(f"  SQL warehouse: {wh['name']} ({wh['id']}) [{wh['state']}]")
        return wh["id"]

    print(f"\n  SQL warehouses ({len(warehouses)}):")
    for i, wh in enumerate(warehouses, 1):
        state_icon = "*" if wh["state"] == "RUNNING" else " "
        print(f"    {i}. {wh['name']:<30} {wh['size']:<12} {wh['state']:<10} {wh['type']}{state_icon}")
        print(f"       {wh['id']}")
    pick = input(f"\n  Select warehouse [1-{len(warehouses)}] (default: 1): ").strip()
    idx = int(pick) - 1 if pick.isdigit() and 1 <= int(pick) <= len(warehouses) else 0
    selected = warehouses[idx]
    print(f"  Warehouse: {selected['name']} ({selected['id']})")

    # Save to session for subsequent commands
    session = _load_session()
    _save_session(session.get("host", ""), selected["id"])

    return selected["id"]


def add_auth_args(parser) -> None:
    """Add authentication arguments to an argparse parser.

    Adds --host, --token, --auth-profile, --login flags grouped under 'Authentication'.
    """
    auth_group = parser.add_argument_group("Authentication")
    auth_group.add_argument(
        "--host",
        help="Databricks workspace URL (or set DATABRICKS_HOST)",
    )
    auth_group.add_argument(
        "--token",
        help="Databricks personal access token (or set DATABRICKS_TOKEN)",
    )
    auth_group.add_argument(
        "--auth-profile",
        help="Databricks CLI profile from ~/.databrickscfg",
    )
    auth_group.add_argument(
        "--verify-auth",
        action="store_true",
        help="Verify authentication before running commands",
    )
    auth_group.add_argument(
        "--login",
        action="store_true",
        help="Interactive browser login via Databricks OAuth (like az login)",
    )
