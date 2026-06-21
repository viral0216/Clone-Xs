"""SAT Scanner — Azure AD login flows."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from typing import Any

from .checks import _WORKSPACE_ACCOUNT_IDS, _WORKSPACE_REGIONS, _WORKSPACE_ARM_INFO, _AZURE_MGMT_TOKEN
from .api import _fetch_account_id

import sat_scanner.checks as _checks_mod


def _activate_terminal() -> None:
    """Bring the terminal window back to the foreground after a browser-based login."""
    if sys.platform != "darwin":
        return
    try:
        # Determine which app owns our terminal session
        prev = subprocess.run(
            ["osascript", "-e",
             'tell application "System Events" to get name of first application process whose frontmost is true'],
            capture_output=True, text=True, timeout=5,
        )
        front_app = prev.stdout.strip()
        # If the browser grabbed focus, reactivate the terminal/IDE
        if front_app and front_app.lower() not in ("terminal", "iterm2", "visual studio code", "code"):
            for app in ("Visual Studio Code", "Code", "Terminal", "iTerm2"):
                try:
                    subprocess.run(
                        ["osascript", "-e", f'tell application "{app}" to activate'],
                        capture_output=True, timeout=5,
                    )
                    return
                except Exception:
                    continue
        # If we're already in a terminal app, no-op
    except Exception:
        pass


_LOGIN_MODE: str = "auto"  # "auto", "browser", or "codespace" — set by CLI flags


def _is_headless() -> bool:
    """Detect headless environments where a browser cannot be opened (Codespaces, SSH, containers)."""
    if _LOGIN_MODE == "codespace":
        return True
    if _LOGIN_MODE == "browser":
        return False
    # auto-detect
    if os.environ.get("CODESPACES") or os.environ.get("REMOTE_CONTAINERS"):
        return True
    if sys.platform in ("darwin", "win32"):
        return False
    # Linux: need DISPLAY or WAYLAND_DISPLAY for a browser
    return not (os.environ.get("DISPLAY") or os.environ.get("WAYLAND_DISPLAY"))


def _run_az(args: list[str]) -> dict | list | str:
    """Run an az CLI command and return parsed JSON output."""
    cmd = ["az"] + args + ["-o", "json"]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    if proc.returncode != 0:
        raise RuntimeError(f"az CLI error: {proc.stderr.strip()}")
    return json.loads(proc.stdout) if proc.stdout.strip() else {}


_DATABRICKS_AAD_RESOURCE = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"
_ARM_RESOURCE = "https://management.azure.com"


def fetch_tokens_from_existing_session() -> tuple[str, str]:
    """Use the EXISTING ``az`` CLI login to mint tokens for BOTH Databricks and ARM.

    Performs NO interactive ``az login`` — it relies on whatever session the user
    already established.  Returns ``(databricks_aad_token, management_token)`` and
    sets the module-level ARM token used by Azure infra discovery.

    Raises ``RuntimeError`` / ``FileNotFoundError`` if ``az`` is missing or no
    session exists, so callers can degrade gracefully.
    """
    _run_az(["account", "show"])  # raises if not logged in / az missing
    dbx = _run_az(["account", "get-access-token", "--resource", _DATABRICKS_AAD_RESOURCE])
    dbx_token = dbx.get("accessToken", "") if isinstance(dbx, dict) else ""
    mgmt = _run_az(["account", "get-access-token", "--resource", _ARM_RESOURCE])
    mgmt_token = mgmt.get("accessToken", "") if isinstance(mgmt, dict) else ""
    _checks_mod._AZURE_MGMT_TOKEN = mgmt_token
    return dbx_token, mgmt_token


def _az_login(extra_args: list[str] | None = None) -> None:
    """Run az login, using device-code flow in headless environments."""
    args = ["login"]
    if extra_args:
        args.extend(extra_args)
    if _is_headless():
        args.append("--use-device-code")
        # Capture stderr so we can fix the wrong URL that some az CLI versions print
        cmd = ["az"] + args + ["-o", "json"]
        proc = subprocess.Popen(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        # Stream stderr line-by-line, replacing the wrong URL
        import threading
        def _stream_stderr():
            for line in proc.stderr:
                fixed = line.replace("https://login.microsoft.com/device", "https://microsoft.com/devicelogin")
                sys.stderr.write(fixed)
                sys.stderr.flush()
        t = threading.Thread(target=_stream_stderr, daemon=True)
        t.start()
        proc.wait(timeout=300)
        t.join(timeout=5)
        if proc.returncode != 0:
            raise RuntimeError("az login failed. Please try again.")
    else:
        _run_az(args)
        _activate_terminal()



def azure_login_flow(scan_all: bool = False) -> list[tuple[str, str, str]]:
    """Interactive Azure login -> tenant -> subscription -> workspace -> token.

    If scan_all is True, returns credentials for ALL workspaces in the
    selected subscription. Otherwise prompts the user to pick one.

    Returns a list of (host, token, workspace_name) tuples.
    """
    print("\n  Azure Login Flow")
    print("  " + "─" * 40)

    # Step 1: az login
    print(f"  Step 1: Signing in to Azure ({'device code' if _is_headless() else 'browser'} flow)...")
    _az_login()
    account = _run_az(["account", "show"])
    print(f"  ✓ Signed in as: {account.get('user', {}).get('name', 'unknown')}")

    # Step 2: Select tenant
    print("\n  Step 2: Select tenant")
    tenants = _run_az(["account", "tenant", "list"])
    if not tenants:
        raise RuntimeError("No Azure tenants found.")
    for i, t in enumerate(tenants):
        marker = " ← active" if t.get("tenantId") == account.get("tenantId") else ""
        print(f"    [{i+1}] {t.get('displayName', t.get('tenantId', '?'))}{marker}")
    choice = input(f"  Enter tenant number [1-{len(tenants)}] (default: active): ").strip()
    if choice:
        idx = int(choice) - 1
        tenant_id = tenants[idx]["tenantId"]
        print(f"  Switching to tenant {tenant_id}...")
        _az_login(["--tenant", tenant_id])
    else:
        tenant_id = account.get("tenantId", "")

    # Step 3: Select subscription
    print("\n  Step 3: Select subscription")
    subs = _run_az(["account", "list", "--query", f"[?tenantId=='{tenant_id}']"])
    if not subs:
        raise RuntimeError("No subscriptions found for this tenant.")
    for i, s in enumerate(subs):
        default = " ← default" if s.get("isDefault") else ""
        print(f"    [{i+1}] {s.get('name', '?')} ({s.get('id', '?')}){default}")
    choice = input(f"  Enter subscription number [1-{len(subs)}] (default: 1): ").strip()
    idx = (int(choice) - 1) if choice else 0
    sub_id = subs[idx]["id"]
    _run_az(["account", "set", "--subscription", sub_id])

    # Step 4: Select Databricks workspace
    print("\n  Step 4: Select Databricks workspace")
    workspaces = _run_az(["resource", "list", "--resource-type", "Microsoft.Databricks/workspaces",
        "--query", "[].{name:name, id:id, location:location, resourceGroup:resourceGroup}"])
    if not workspaces:
        raise RuntimeError("No Databricks workspaces found in this subscription.")
    for i, w in enumerate(workspaces):
        print(f"    [{i+1}] {w.get('name', '?')} ({w.get('location', '?')}) — {w.get('resourceGroup', '?')}")
    if len(workspaces) > 1:
        print(f"    [A] All workspaces")

    if scan_all:
        selected_indices = list(range(len(workspaces)))
        print("  → Scanning all workspaces (--azure-all)")
    else:
        choice = input(f"  Enter workspace number [1-{len(workspaces)}], or A for all (default: 1): ").strip()
        if choice.lower() == "a":
            selected_indices = list(range(len(workspaces)))
        else:
            idx = (int(choice) - 1) if choice else 0
            selected_indices = [idx]

    # Get Databricks-scoped token (shared across workspaces in same tenant)
    print("  Fetching Azure AD token for Databricks...")
    token_data = _run_az(["account", "get-access-token", "--resource", "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"])
    token = token_data.get("accessToken", "")
    if not token:
        raise RuntimeError("Failed to get Azure AD token for Databricks.")

    # Get Azure Management token for ARM API calls (diagnostic settings, etc.)
    print("  Fetching Azure Management token...")
    mgmt_data = _run_az(["account", "get-access-token", "--resource", "https://management.azure.com"])
    _checks_mod._AZURE_MGMT_TOKEN = mgmt_data.get("accessToken", "")

    results: list[tuple[str, str, str]] = []
    for idx in selected_indices:
        resource_id = workspaces[idx]["id"]
        ws_name = workspaces[idx].get("name", "")

        # Get workspace URL from ARM
        print(f"  Fetching workspace URL for {ws_name}...")
        resource = _run_az(["resource", "show", "--ids", resource_id])
        workspace_url = resource.get("properties", {}).get("workspaceUrl", "")
        if workspace_url:
            workspace_url = f"https://{workspace_url}"
        else:
            print(f"  ⚠ Could not determine URL for {ws_name}, skipping.")
            continue

        print(f"  ✓ Connected to {ws_name} ({workspace_url})")
        _WORKSPACE_REGIONS[workspace_url] = workspaces[idx].get("location", "")
        _WORKSPACE_ARM_INFO[workspace_url.rstrip("/")] = {"resource_id": resource_id, "tenant": tenant_id}
        # Fetch Databricks account_id from Unity Catalog metastores
        _fetch_account_id(workspace_url, token)
        results.append((workspace_url, token, ws_name))

    if not results:
        raise RuntimeError("No workspaces could be resolved.")

    return results


def azure_tenant_flow() -> list[tuple[str, str, str]]:
    """Azure login -> tenant -> ALL subscriptions -> ALL workspaces.

    Scans every Databricks workspace across every subscription in the
    selected tenant.  Returns a list of (host, token, workspace_name) tuples.
    """
    print("\n  Azure Tenant-Wide Scan")
    print("  " + "─" * 40)

    # Step 1: az login
    print(f"  Step 1: Signing in to Azure ({'device code' if _is_headless() else 'browser'} flow)...")
    _az_login()
    account = _run_az(["account", "show"])
    print(f"  ✓ Signed in as: {account.get('user', {}).get('name', 'unknown')}")

    # Step 2: Select tenant
    print("\n  Step 2: Select tenant")
    tenants = _run_az(["account", "tenant", "list"])
    if not tenants:
        raise RuntimeError("No Azure tenants found.")
    for i, t in enumerate(tenants):
        marker = " ← active" if t.get("tenantId") == account.get("tenantId") else ""
        print(f"    [{i+1}] {t.get('displayName', t.get('tenantId', '?'))}{marker}")
    choice = input(f"  Enter tenant number [1-{len(tenants)}] (default: active): ").strip()
    if choice:
        idx = int(choice) - 1
        tenant_id = tenants[idx]["tenantId"]
        print(f"  Switching to tenant {tenant_id}...")
        _az_login(["--tenant", tenant_id])
    else:
        tenant_id = account.get("tenantId", "")

    # Step 3: Enumerate ALL subscriptions in this tenant
    print("\n  Step 3: Discovering subscriptions in tenant...")
    subs = _run_az(["account", "list", "--query", f"[?tenantId=='{tenant_id}']"])
    if not subs:
        raise RuntimeError("No subscriptions found for this tenant.")
    print(f"  Found {len(subs)} subscription(s):")
    for i, s in enumerate(subs):
        print(f"    • {s.get('name', '?')} ({s.get('id', '?')})")

    # Step 4: For each subscription, discover Databricks workspaces
    print(f"\n  Step 4: Discovering Databricks workspaces across all subscriptions...")
    all_workspaces: list[tuple[str, dict]] = []  # (sub_name, workspace_resource)
    for s in subs:
        sub_id = s["id"]
        sub_name = s.get("name", sub_id)
        _run_az(["account", "set", "--subscription", sub_id])
        try:
            workspaces = _run_az([
                "resource", "list",
                "--resource-type", "Microsoft.Databricks/workspaces",
                "--query", "[].{name:name, id:id, location:location, resourceGroup:resourceGroup}",
            ])
        except RuntimeError:
            print(f"    ⚠ Could not list resources in '{sub_name}', skipping.")
            continue
        if workspaces:
            for w in workspaces:
                all_workspaces.append((sub_name, w))
            print(f"    {sub_name}: {len(workspaces)} workspace(s)")
        else:
            print(f"    {sub_name}: no Databricks workspaces")

    if not all_workspaces:
        raise RuntimeError("No Databricks workspaces found across any subscription in this tenant.")

    print(f"\n  Total: {len(all_workspaces)} Databricks workspace(s) across {len(subs)} subscription(s)")

    # Step 5: Get Databricks-scoped token and resolve workspace URLs
    print("  Fetching Azure AD token for Databricks...")
    token_data = _run_az(["account", "get-access-token", "--resource", "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"])
    token = token_data.get("accessToken", "")
    if not token:
        raise RuntimeError("Failed to get Azure AD token for Databricks.")

    # Get Azure Management token for ARM API calls (diagnostic settings, etc.)
    print("  Fetching Azure Management token...")
    mgmt_data = _run_az(["account", "get-access-token", "--resource", "https://management.azure.com"])
    _checks_mod._AZURE_MGMT_TOKEN = mgmt_data.get("accessToken", "")

    results: list[tuple[str, str, str]] = []
    for sub_name, ws in all_workspaces:
        resource_id = ws["id"]
        ws_name = ws.get("name", "")
        display_name = f"{ws_name} ({sub_name})"

        print(f"  Resolving {ws_name} in {sub_name}...")
        try:
            resource = _run_az(["resource", "show", "--ids", resource_id])
        except RuntimeError:
            print(f"    ⚠ Could not resolve {ws_name}, skipping.")
            continue
        workspace_url = resource.get("properties", {}).get("workspaceUrl", "")
        if workspace_url:
            workspace_url = f"https://{workspace_url}"
        else:
            print(f"    ⚠ Could not determine URL for {ws_name}, skipping.")
            continue

        print(f"    ✓ {ws_name} → {workspace_url}")
        _WORKSPACE_REGIONS[workspace_url] = ws.get("location", "")
        _WORKSPACE_ARM_INFO[workspace_url.rstrip("/")] = {"resource_id": resource_id, "tenant": tenant_id}
        # Fetch Databricks account_id from Unity Catalog metastores
        _fetch_account_id(workspace_url, token)
        results.append((workspace_url, token, display_name))

    if not results:
        raise RuntimeError("No workspaces could be resolved.")

    print(f"\n  ✓ Ready to scan {len(results)} workspace(s) across the tenant.")
    return results
