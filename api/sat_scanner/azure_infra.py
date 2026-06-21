"""SAT Scanner — Azure infrastructure discovery + UC↔Azure mapping.

Discovers the Azure resources backing a Databricks / Unity Catalog deployment and
maps UC external locations & storage credentials to the concrete Azure storage
account / container and the identity (access connector / managed identity) that
grants access.

Reuses the *existing* approach — the ``az`` CLI for login (already wired in
``azure_auth.py``) and the Azure Resource Manager (ARM) REST API called via
``httpx`` with a bearer token.  No ``azure-mgmt-*`` SDK dependency.

The ARM management token and workspace ARM info are read **live** from the
``checks`` module on every call (``_checks._AZURE_MGMT_TOKEN`` /
``_checks._WORKSPACE_ARM_INFO``).  This is deliberate: ``azure_auth`` reassigns
the module attribute, so importing the scalar by name would capture a stale
empty string (the same latent bug that silently degrades ``SAT-LOG-DIAG``).
"""

from __future__ import annotations

import asyncio
import re
from urllib.parse import urlparse
from typing import Any

import httpx

import sat_scanner.checks as _checks
from .inventory_models import _Slotted

# ── ARM API versions (centralised for easy bumps) ──────────────────────────
_API_WORKSPACE = "2024-05-01"
_API_WORKSPACE_FALLBACK = "2023-02-01"
_API_STORAGE = "2023-05-01"
_API_CONNECTOR = "2024-05-01"
_API_MSI = "2023-01-31"
_API_ROLE_ASSIGN = "2022-04-01"
_API_ROLE_DEF = "2022-04-01"
_API_KEYVAULT = "2023-07-01"
_API_RESOURCE_GRAPH = "2024-04-01"

_ARM_BASE = "https://management.azure.com"

_MAX_RETRIES = 3
_RETRY_BACKOFF = [1, 2, 4]

# Well-known Azure RBAC role definition GUIDs → human names (offline labelling).
ROLE_DEF_NAMES: dict[str, str] = {
    "ba92f5b4-2d11-453d-a403-e96b0029c9fe": "Storage Blob Data Contributor",
    "2a2b9908-6ea1-4ae2-8e65-a410df84e7d1": "Storage Blob Data Reader",
    "b7e6dc6d-f1e8-4753-8033-0f276bb0955b": "Storage Blob Data Owner",
    "17d1049b-9a84-46fb-8f53-869881c3d3ab": "Storage Account Contributor",
    "acdd72a7-3385-48ef-bd42-f606fba81ae7": "Reader",
    "8e3af657-a8ff-443c-a75c-2fe8c4bcb635": "Owner",
    "b24988ac-6180-42a0-ab88-20f7382dd24c": "Contributor",
}

_ARM_ID_RE = re.compile(
    r"/subscriptions/(?P<sub>[^/]+)/resourceGroups/(?P<rg>[^/]+)"
    r"/providers/(?P<provider>[^/]+)/(?P<type>[^/]+)/(?P<name>[^/]+)",
    re.IGNORECASE,
)


# ─────────────────────────────────────────────────────────────────────────────
# Pure parsing helpers
# ─────────────────────────────────────────────────────────────────────────────

def parse_arm_id(resource_id: str) -> dict | None:
    """Parse an ARM resource id into {sub, rg, provider, type, name}."""
    if not resource_id:
        return None
    m = _ARM_ID_RE.search(resource_id)
    if not m:
        return None
    return {
        "sub": m.group("sub"),
        "rg": m.group("rg"),
        "provider": m.group("provider"),
        "type": m.group("type"),
        "name": m.group("name"),
    }


def parse_storage_url(url: str) -> dict | None:
    """Parse a UC storage location URL into Azure storage components.

    Handles ``abfss://container@account.dfs.core.windows.net/path``,
    ``wasbs://container@account.blob.core.windows.net/path`` and
    ``https://account.dfs.core.windows.net/container/path`` forms (and sovereign
    cloud suffixes).  Returns None for non-Azure (s3://, gs://, dbfs:) or
    unparseable URLs so callers can skip them cleanly.
    """
    if not url:
        return None
    try:
        parsed = urlparse(url)
    except Exception:
        return None
    scheme = (parsed.scheme or "").lower()
    netloc = parsed.netloc

    if scheme in ("abfss", "abfs", "wasbs", "wasb"):
        # netloc = container@account.dfs.core.windows.net
        if "@" not in netloc:
            return None
        container, _, host = netloc.partition("@")
        host_parts = host.split(".", 1)
        account = host_parts[0].lower()
        endpoint_suffix = host_parts[1] if len(host_parts) > 1 else ""
        path = parsed.path.lstrip("/")
    elif scheme in ("https", "http"):
        host = netloc.split("@")[-1]
        if ".dfs.core" not in host and ".blob.core" not in host and ".core.windows" not in host \
           and ".core.usgov" not in host and ".core.chinacloud" not in host:
            return None
        host_parts = host.split(".", 1)
        account = host_parts[0].lower()
        endpoint_suffix = host_parts[1] if len(host_parts) > 1 else ""
        segs = parsed.path.lstrip("/").split("/", 1)
        container = segs[0] if segs and segs[0] else ""
        path = segs[1] if len(segs) > 1 else ""
    else:
        return None

    if not account:
        return None
    return {
        "scheme": scheme,
        "account": account,
        "container": container,
        "path": path,
        "endpoint_suffix": endpoint_suffix,
    }


def _role_name(role_definition_id: str) -> str:
    """Map a roleDefinition id (.../roleDefinitions/<guid>) to a known name (offline)."""
    guid = (role_definition_id or "").rstrip("/").split("/")[-1].lower()
    return ROLE_DEF_NAMES.get(guid, guid)


# Cache of resolved custom role-definition names (guid → roleName)
_ROLE_DEF_CACHE: dict[str, str] = {}


async def _resolve_role_name(client: httpx.AsyncClient, token: str,
                             role_definition_id: str, arm_errors: list[str]) -> str:
    """Resolve a roleDefinition id to its display name, fetching custom roles via ARM (cached)."""
    guid = (role_definition_id or "").rstrip("/").split("/")[-1].lower()
    if guid in ROLE_DEF_NAMES:
        return ROLE_DEF_NAMES[guid]
    if guid in _ROLE_DEF_CACHE:
        return _ROLE_DEF_CACHE[guid]
    if not role_definition_id:
        return guid
    data, status, err = await _arm_get(
        client, f"{_ARM_BASE}{role_definition_id}", token, {"api-version": _API_ROLE_DEF})
    name = guid
    if status == 200:
        name = (data.get("properties", {}) or {}).get("roleName", guid)
    elif status not in (0,):
        arm_errors.append(f"roleDefinition {guid} {status}: {err}")
    _ROLE_DEF_CACHE[guid] = name
    return name


# ─────────────────────────────────────────────────────────────────────────────
# Data models
# ─────────────────────────────────────────────────────────────────────────────

class AzureRoleAssignment(_Slotted):
    __slots__ = ("principal_id", "principal_type", "role_name", "role_definition_id", "scope_level")

    def __init__(self, principal_id="", principal_type="", role_name="",
                 role_definition_id="", scope_level="account"):
        self.principal_id = principal_id
        self.principal_type = principal_type
        self.role_name = role_name
        self.role_definition_id = role_definition_id
        self.scope_level = scope_level


class AzureStorageAccount(_Slotted):
    __slots__ = ("name", "resource_id", "subscription_id", "resource_group",
                 "location", "sku", "kind", "hns_enabled", "public_network_access",
                 "allow_blob_public_access", "network_default_action", "vnet_rules",
                 "ip_rules", "min_tls_version", "private_endpoints",
                 "role_assignments", "resolved", "resolve_error")

    def __init__(self, name="", resource_id="", subscription_id="", resource_group="",
                 location="", sku="", kind="", hns_enabled=None, public_network_access="",
                 allow_blob_public_access=None, network_default_action="", vnet_rules=None,
                 ip_rules=None, min_tls_version="", private_endpoints=None,
                 role_assignments=None, resolved=True, resolve_error=""):
        self.name = name
        self.resource_id = resource_id
        self.subscription_id = subscription_id
        self.resource_group = resource_group
        self.location = location
        self.sku = sku
        self.kind = kind
        self.hns_enabled = hns_enabled
        self.public_network_access = public_network_access
        self.allow_blob_public_access = allow_blob_public_access
        self.network_default_action = network_default_action
        self.vnet_rules = vnet_rules or []
        self.ip_rules = ip_rules or []
        self.min_tls_version = min_tls_version
        self.private_endpoints = private_endpoints or []
        self.role_assignments = role_assignments or []   # list[AzureRoleAssignment]
        self.resolved = resolved
        self.resolve_error = resolve_error


class AzureIdentity(_Slotted):
    __slots__ = ("kind", "resource_id", "name", "identity_type", "principal_id",
                 "client_id", "location")

    def __init__(self, kind="", resource_id="", name="", identity_type="",
                 principal_id="", client_id="", location=""):
        self.kind = kind                  # "access_connector" | "user_assigned_mi"
        self.resource_id = resource_id
        self.name = name
        self.identity_type = identity_type
        self.principal_id = principal_id
        self.client_id = client_id
        self.location = location


class AzureWorkspaceInfra(_Slotted):
    __slots__ = ("resource_id", "subscription_id", "resource_group",
                 "managed_resource_group_id", "location", "geo", "sku",
                 "vnet_injected", "custom_vnet_id", "public_subnet", "private_subnet",
                 "no_public_ip", "infra_encryption", "private_endpoints")

    def __init__(self, resource_id="", subscription_id="", resource_group="",
                 managed_resource_group_id="", location="", geo="", sku="",
                 vnet_injected=False, custom_vnet_id="", public_subnet="",
                 private_subnet="", no_public_ip=None, infra_encryption=None,
                 private_endpoints=None):
        self.resource_id = resource_id
        self.subscription_id = subscription_id
        self.resource_group = resource_group
        self.managed_resource_group_id = managed_resource_group_id
        self.location = location
        self.geo = geo
        self.sku = sku
        self.vnet_injected = vnet_injected
        self.custom_vnet_id = custom_vnet_id
        self.public_subnet = public_subnet
        self.private_subnet = private_subnet
        self.no_public_ip = no_public_ip
        self.infra_encryption = infra_encryption
        self.private_endpoints = private_endpoints or []


class AzureKeyVault(_Slotted):
    __slots__ = ("name", "resource_id", "vault_uri", "rbac_authorization",
                 "location", "backing_scopes")

    def __init__(self, name="", resource_id="", vault_uri="", rbac_authorization=None,
                 location="", backing_scopes=None):
        self.name = name
        self.resource_id = resource_id
        self.vault_uri = vault_uri
        self.rbac_authorization = rbac_authorization
        self.location = location
        self.backing_scopes = backing_scopes or []


class UCAzureMapping(_Slotted):
    __slots__ = ("uc_object_type", "uc_name", "url", "read_only", "storage_account",
                 "container", "storage_account_resolved", "credential_name",
                 "identity", "granting_roles", "notes")

    def __init__(self, uc_object_type="", uc_name="", url="", read_only=None,
                 storage_account="", container="", storage_account_resolved=False,
                 credential_name="", identity=None, granting_roles=None, notes=None):
        self.uc_object_type = uc_object_type
        self.uc_name = uc_name
        self.url = url
        self.read_only = read_only
        self.storage_account = storage_account
        self.container = container
        self.storage_account_resolved = storage_account_resolved
        self.credential_name = credential_name
        self.identity = identity          # AzureIdentity | None
        self.granting_roles = granting_roles or []
        self.notes = notes or []


class AzureInventory(_Slotted):
    __slots__ = ("available", "reason", "workspace", "storage_accounts",
                 "identities", "key_vaults", "mappings", "arm_errors")

    def __init__(self, available=False, reason="", workspace=None, storage_accounts=None,
                 identities=None, key_vaults=None, mappings=None, arm_errors=None):
        self.available = available
        self.reason = reason
        self.workspace = workspace        # AzureWorkspaceInfra | None
        self.storage_accounts = storage_accounts or []
        self.identities = identities or []
        self.key_vaults = key_vaults or []
        self.mappings = mappings or []
        self.arm_errors = arm_errors or []


# ─────────────────────────────────────────────────────────────────────────────
# ARM HTTP helpers (own 429 backoff, mirrors api._dbx_get)
# ─────────────────────────────────────────────────────────────────────────────

async def _arm_get(client: httpx.AsyncClient, url: str, token: str,
                   params: dict | None = None) -> tuple[Any, int, Any]:
    for attempt in range(_MAX_RETRIES + 1):
        try:
            resp = await client.get(url, headers={"Authorization": f"Bearer {token}"},
                                    params=params or {})
            if resp.status_code == 200:
                return resp.json(), 200, None
            if resp.status_code == 429 and attempt < _MAX_RETRIES:
                await asyncio.sleep(_retry_after(resp, attempt))
                continue
            return None, resp.status_code, _err_body(resp)
        except httpx.TimeoutException:
            if attempt < _MAX_RETRIES:
                await asyncio.sleep(_RETRY_BACKOFF[min(attempt, len(_RETRY_BACKOFF) - 1)])
                continue
            return None, 0, "Request timed out"
        except Exception as exc:
            return None, 0, str(exc)
    return None, 0, "Max retries exceeded"


async def _arm_post(client: httpx.AsyncClient, url: str, token: str,
                    body: dict) -> tuple[Any, int, Any]:
    for attempt in range(_MAX_RETRIES + 1):
        try:
            resp = await client.post(url, headers={"Authorization": f"Bearer {token}"}, json=body)
            if resp.status_code == 200:
                return resp.json(), 200, None
            if resp.status_code == 429 and attempt < _MAX_RETRIES:
                await asyncio.sleep(_retry_after(resp, attempt))
                continue
            return None, resp.status_code, _err_body(resp)
        except httpx.TimeoutException:
            if attempt < _MAX_RETRIES:
                await asyncio.sleep(_RETRY_BACKOFF[min(attempt, len(_RETRY_BACKOFF) - 1)])
                continue
            return None, 0, "Request timed out"
        except Exception as exc:
            return None, 0, str(exc)
    return None, 0, "Max retries exceeded"


def _retry_after(resp: Any, attempt: int) -> float:
    ra = resp.headers.get("Retry-After", "")
    if ra and ra.isdigit():
        return float(ra)
    return float(_RETRY_BACKOFF[min(attempt, len(_RETRY_BACKOFF) - 1)])


def _err_body(resp: Any) -> Any:
    try:
        return resp.json()
    except Exception:
        return (resp.text[:300].strip() if resp.text else f"HTTP {resp.status_code}")


# ─────────────────────────────────────────────────────────────────────────────
# Auth
# ─────────────────────────────────────────────────────────────────────────────

def _ensure_mgmt_token() -> str:
    """Return the ARM management token, re-fetching via ``az`` if not already set."""
    tok = _checks._AZURE_MGMT_TOKEN
    if tok:
        return tok
    try:
        from .azure_auth import _run_az
        data = _run_az(["account", "get-access-token", "--resource", "https://management.azure.com"])
        tok = data.get("accessToken", "") if isinstance(data, dict) else ""
        _checks._AZURE_MGMT_TOKEN = tok
        return tok
    except Exception:
        return ""


def _scoped_subscription_ids(tenant: str, fallback_sub: str = "") -> list[str]:
    """Return subscription ids in scope for resource resolution.

    Uses the EXISTING ``az`` CLI session (``az account list``) filtered by tenant;
    falls back to the workspace's own subscription when ``az`` is unavailable.
    No interactive login is performed.
    """
    subs: list[str] = []
    try:
        from .azure_auth import _run_az
        query = f"[?tenantId=='{tenant}'].id" if tenant else "[].id"
        data = _run_az(["account", "list", "--all", "--query", query])
        if isinstance(data, list):
            subs = [s for s in data if isinstance(s, str)]
    except Exception:
        subs = []
    if not subs and fallback_sub:
        subs = [fallback_sub]
    # ensure the workspace subscription is always included
    if fallback_sub and fallback_sub not in subs:
        subs.append(fallback_sub)
    return subs


async def _resolve_workspace_via_graph(client: httpx.AsyncClient, token: str, host_key: str,
                                       sub_ids: list[str], arm_errors: list[str]) -> dict | None:
    """Locate this workspace's Azure resource using the EXISTING az login (no interactive flow).

    Queries Azure Resource Graph for all Databricks workspaces visible to the
    current ``az`` session and matches on ``properties.workspaceUrl``.  On success
    populates ``_checks._WORKSPACE_ARM_INFO`` / ``_WORKSPACE_REGIONS`` and returns
    the arm info dict.
    """
    if not sub_ids:
        arm_errors.append("no subscriptions visible to the current az login")
        return None
    target = host_key.split("://")[-1].rstrip("/").lower()
    query = (
        "Resources | where type =~ 'microsoft.databricks/workspaces' "
        "| project id, name, location, tenantId, "
        "workspaceUrl=tostring(properties.workspaceUrl)"
    )
    url = f"{_ARM_BASE}/providers/Microsoft.ResourceGraph/resources?api-version={_API_RESOURCE_GRAPH}"
    skip_token = None
    while True:
        body = {"subscriptions": sub_ids, "query": query,
                "options": {"resultFormat": "objectArray", "top": 1000}}
        if skip_token:
            body["options"]["$skipToken"] = skip_token
        data, status, err = await _arm_post(client, url, token, body)
        if status != 200:
            arm_errors.append(f"workspace Resource Graph {status}: {err}")
            return None
        for row in data.get("data", []) or []:
            wsurl = (row.get("workspaceUrl") or "").lower()
            if wsurl and wsurl == target:
                info = {"resource_id": row.get("id", ""), "tenant": row.get("tenantId", "")}
                _checks._WORKSPACE_ARM_INFO[host_key] = info
                if row.get("location"):
                    _checks._WORKSPACE_REGIONS[host_key] = row["location"]
                return info
        skip_token = data.get("$skipToken") or data.get("skipToken")
        if not skip_token:
            break
    arm_errors.append(f"workspace '{target}' not found via Resource Graph (check az tenant / Reader access)")
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Discovery
# ─────────────────────────────────────────────────────────────────────────────

async def discover_workspace_infra(client: httpx.AsyncClient, token: str, resource_id: str,
                                   region_hint: str, arm_errors: list[str]) -> AzureWorkspaceInfra:
    ids = parse_arm_id(resource_id) or {}
    url = f"{_ARM_BASE}{resource_id}"
    data, status, err = await _arm_get(client, url, token, {"api-version": _API_WORKSPACE})
    if status != 200:
        data, status, err = await _arm_get(client, url, token, {"api-version": _API_WORKSPACE_FALLBACK})
    if status != 200:
        arm_errors.append(f"workspace GET {status}: {err}")
        return AzureWorkspaceInfra(
            resource_id=resource_id, subscription_id=ids.get("sub", ""),
            resource_group=ids.get("rg", ""), location=region_hint,
            geo=_checks._resolve_geo(region_hint) if region_hint else "",
        )

    props = data.get("properties", {}) or {}
    params = props.get("parameters", {}) or {}

    def _pv(key: str) -> Any:
        v = params.get(key)
        return v.get("value") if isinstance(v, dict) else v

    location = data.get("location", region_hint)
    custom_vnet = _pv("customVirtualNetworkId") or ""
    pe = [
        {"name": c.get("name", ""),
         "state": (c.get("properties", {}) or {}).get("privateLinkServiceConnectionState", {}).get("status", ""),
         "id": (c.get("properties", {}) or {}).get("privateEndpoint", {}).get("id", "")}
        for c in (props.get("privateEndpointConnections", []) or [])
    ]
    return AzureWorkspaceInfra(
        resource_id=resource_id,
        subscription_id=ids.get("sub", ""),
        resource_group=ids.get("rg", ""),
        managed_resource_group_id=props.get("managedResourceGroupId", ""),
        location=location,
        geo=_checks._resolve_geo(location) if location else "",
        sku=(data.get("sku", {}) or {}).get("name", ""),
        vnet_injected=bool(custom_vnet),
        custom_vnet_id=custom_vnet,
        public_subnet=_pv("customPublicSubnetName") or "",
        private_subnet=_pv("customPrivateSubnetName") or "",
        no_public_ip=_pv("enableNoPublicIp"),
        infra_encryption=_pv("requireInfrastructureEncryption"),
        private_endpoints=pe,
    )


async def resolve_storage_accounts(client: httpx.AsyncClient, token: str,
                                   account_names: list[str], sub_ids: list[str],
                                   arm_errors: list[str]) -> dict[str, AzureStorageAccount]:
    """Resolve storage account DNS names → AzureStorageAccount via Resource Graph."""
    out: dict[str, AzureStorageAccount] = {}
    names = sorted({n for n in account_names if n})
    if not names or not sub_ids:
        return out

    name_list = ", ".join("'" + n.replace("'", "") + "'" for n in names)
    query = (
        "Resources | where type =~ 'microsoft.storage/storageAccounts' "
        f"and name in~ ({name_list}) "
        "| project id, name, location, resourceGroup, subscriptionId, kind, sku, properties"
    )
    url = f"{_ARM_BASE}/providers/Microsoft.ResourceGraph/resources"
    skip_token = None
    while True:
        body = {
            "subscriptions": sub_ids,
            "query": query,
            "options": {"resultFormat": "objectArray", "top": 1000},
        }
        if skip_token:
            body["options"]["$skipToken"] = skip_token
        data, status, err = await _arm_post(
            client, f"{url}?api-version={_API_RESOURCE_GRAPH}", token, body)
        if status != 200:
            arm_errors.append(f"Resource Graph storage query {status}: {err}")
            return out
        for row in data.get("data", []) or []:
            acct = _storage_account_from_graph(row)
            if acct.name:
                out[acct.name.lower()] = acct
        skip_token = data.get("$skipToken") or data.get("skipToken")
        if not skip_token:
            break
    return out


def _storage_account_from_graph(row: dict) -> AzureStorageAccount:
    props = row.get("properties", {}) or {}
    net = props.get("networkAcls", {}) or {}
    pe = [
        (c.get("properties", {}) or {}).get("privateEndpoint", {}).get("id", "")
        for c in (props.get("privateEndpointConnections", []) or [])
    ]
    return AzureStorageAccount(
        name=row.get("name", ""),
        resource_id=row.get("id", ""),
        subscription_id=row.get("subscriptionId", ""),
        resource_group=row.get("resourceGroup", ""),
        location=row.get("location", ""),
        sku=(row.get("sku", {}) or {}).get("name", ""),
        kind=row.get("kind", ""),
        hns_enabled=props.get("isHnsEnabled"),
        public_network_access=props.get("publicNetworkAccess", ""),
        allow_blob_public_access=props.get("allowBlobPublicAccess"),
        network_default_action=net.get("defaultAction", ""),
        vnet_rules=[r.get("id", "") for r in (net.get("virtualNetworkRules", []) or [])],
        ip_rules=[r.get("value", "") for r in (net.get("ipRules", []) or [])],
        min_tls_version=props.get("minimumTlsVersion", ""),
        private_endpoints=[p for p in pe if p],
    )


async def fetch_role_assignments(client: httpx.AsyncClient, token: str,
                                 storage_account_id: str, arm_errors: list[str]) -> list[AzureRoleAssignment]:
    if not storage_account_id:
        return []
    url = f"{_ARM_BASE}{storage_account_id}/providers/Microsoft.Authorization/roleAssignments"
    data, status, err = await _arm_get(
        client, url, token,
        {"api-version": _API_ROLE_ASSIGN, "$filter": "atScope()"})
    if status != 200:
        arm_errors.append(f"roleAssignments {storage_account_id.split('/')[-1]} {status}: {err}")
        return []
    out: list[AzureRoleAssignment] = []
    for item in data.get("value", []) or []:
        p = item.get("properties", {}) or {}
        rdid = p.get("roleDefinitionId", "")
        out.append(AzureRoleAssignment(
            principal_id=p.get("principalId", ""),
            principal_type=p.get("principalType", ""),
            role_name=await _resolve_role_name(client, token, rdid, arm_errors),
            role_definition_id=rdid,
            scope_level="account",
        ))
    return out


async def resolve_identity(client: httpx.AsyncClient, token: str,
                           azure_managed_identity: dict, arm_errors: list[str]) -> AzureIdentity | None:
    if not azure_managed_identity:
        return None
    connector_id = azure_managed_identity.get("access_connector_id", "")
    msi_id = azure_managed_identity.get("managed_identity_id", "")

    if connector_id:
        data, status, err = await _arm_get(
            client, f"{_ARM_BASE}{connector_id}", token, {"api-version": _API_CONNECTOR})
        if status == 200:
            ident = data.get("identity", {}) or {}
            principal_id = ident.get("principalId", "")
            client_id = ""
            # If user-assigned, pull the first UAMI's principal/client id
            uami = ident.get("userAssignedIdentities", {}) or {}
            if not principal_id and uami:
                first = next(iter(uami.values()), {}) or {}
                principal_id = first.get("principalId", "")
                client_id = first.get("clientId", "")
            ids = parse_arm_id(connector_id) or {}
            return AzureIdentity(
                kind="access_connector", resource_id=connector_id,
                name=ids.get("name", ""), identity_type=ident.get("type", ""),
                principal_id=principal_id, client_id=client_id,
                location=data.get("location", ""))
        arm_errors.append(f"accessConnector {status}: {err}")

    if msi_id:
        data, status, err = await _arm_get(
            client, f"{_ARM_BASE}{msi_id}", token, {"api-version": _API_MSI})
        if status == 200:
            props = data.get("properties", {}) or {}
            ids = parse_arm_id(msi_id) or {}
            return AzureIdentity(
                kind="user_assigned_mi", resource_id=msi_id, name=ids.get("name", ""),
                identity_type="UserAssigned", principal_id=props.get("principalId", ""),
                client_id=props.get("clientId", ""), location=data.get("location", ""))
        arm_errors.append(f"userAssignedIdentity {status}: {err}")

    return None


async def discover_key_vaults(client: httpx.AsyncClient, token: str,
                              akv_scopes: list[dict], sub_ids: list[str],
                              arm_errors: list[str]) -> list[AzureKeyVault]:
    """Best-effort Key Vault discovery for AZURE_KEYVAULT secret scopes."""
    out: dict[str, AzureKeyVault] = {}
    by_name: dict[str, list[str]] = {}
    for scope in akv_scopes or []:
        meta = scope.get("keyvault_metadata", {}) or {}
        rid = meta.get("resource_id", "")
        scope_name = scope.get("name", "")
        if rid:
            data, status, _ = await _arm_get(
                client, f"{_ARM_BASE}{rid}", token, {"api-version": _API_KEYVAULT})
            if status == 200:
                props = data.get("properties", {}) or {}
                kv = out.get(rid) or AzureKeyVault(
                    name=data.get("name", parse_arm_id(rid).get("name", "") if parse_arm_id(rid) else ""),
                    resource_id=rid, vault_uri=props.get("vaultUri", ""),
                    rbac_authorization=props.get("enableRbacAuthorization"),
                    location=data.get("location", ""))
                if scope_name not in kv.backing_scopes:
                    kv.backing_scopes.append(scope_name)
                out[rid] = kv
        else:
            dns = meta.get("dns_name", "")
            name = dns.split("//")[-1].split(".")[0] if dns else ""
            if name:
                by_name.setdefault(name.lower(), []).append(scope_name)

    # Resolve name-only vaults via Resource Graph
    if by_name and sub_ids:
        names = ", ".join("'" + n.replace("'", "") + "'" for n in by_name)
        query = (
            "Resources | where type =~ 'microsoft.keyvault/vaults' "
            f"and name in~ ({names}) | project id, name, location, properties"
        )
        url = f"{_ARM_BASE}/providers/Microsoft.ResourceGraph/resources?api-version={_API_RESOURCE_GRAPH}"
        data, status, err = await _arm_post(
            client, url, token,
            {"subscriptions": sub_ids, "query": query, "options": {"resultFormat": "objectArray"}})
        if status == 200:
            for row in data.get("data", []) or []:
                props = row.get("properties", {}) or {}
                rid = row.get("id", "")
                kv = AzureKeyVault(
                    name=row.get("name", ""), resource_id=rid,
                    vault_uri=props.get("vaultUri", ""),
                    rbac_authorization=props.get("enableRbacAuthorization"),
                    location=row.get("location", ""),
                    backing_scopes=by_name.get(row.get("name", "").lower(), []))
                out[rid or row.get("name", "")] = kv
        else:
            arm_errors.append(f"keyvault Resource Graph {status}: {err}")
    return list(out.values())


# ─────────────────────────────────────────────────────────────────────────────
# Orchestrator
# ─────────────────────────────────────────────────────────────────────────────

async def build_azure_inventory(
    client: httpx.AsyncClient,
    host: str,
    external_locations: list[dict],
    storage_credentials: list[dict],
    metastores: list[dict],
    secret_scopes: list[dict] | None = None,
) -> AzureInventory:
    """Discover Azure infra for ``host`` and map it to UC objects.

    Reads ARM context live from ``checks``.  Degrades gracefully to
    ``available=False`` when there is no Azure context (PAT-only) or ``az`` is
    unavailable — the caller still renders the full UC inventory.
    """
    host_key = host.rstrip("/")
    arm_errors: list[str] = []

    # Mint the ARM token from the EXISTING az CLI session (no interactive login).
    token = _ensure_mgmt_token()
    if not token:
        return AzureInventory(
            available=False,
            reason=("Azure Management token unavailable — the az CLI is not installed or not "
                    "logged in. Run 'az login' (the existing session is reused) or use --azure."))

    # Resolve this workspace's Azure resource. If an --azure flow already populated
    # the ARM info, reuse it; otherwise locate it via the existing az login.
    arm = _checks._WORKSPACE_ARM_INFO.get(host_key)
    if not arm or not arm.get("resource_id"):
        all_subs = _scoped_subscription_ids("", "")
        arm = await _resolve_workspace_via_graph(client, token, host_key, all_subs, arm_errors)
        if not arm or not arm.get("resource_id"):
            return AzureInventory(
                available=False,
                reason=("Could not locate this workspace's Azure resource via the existing az "
                        "login. Check 'az account show' is logged into the correct tenant with "
                        "Reader access, or use --azure to select it interactively."),
                arm_errors=arm_errors)

    resource_id = arm["resource_id"]
    tenant = arm.get("tenant", "")
    region_hint = _checks._WORKSPACE_REGIONS.get(host_key, "")
    ws_ids = parse_arm_id(resource_id) or {}
    sub_ids = _scoped_subscription_ids(tenant, ws_ids.get("sub", ""))

    sem = asyncio.Semaphore(5)

    async def _guard(coro):
        async with sem:
            return await coro

    # 1. Workspace infra
    workspace = await discover_workspace_infra(client, token, resource_id, region_hint, arm_errors)

    # 2. Collect candidate storage account names from locations + credentials + metastore roots
    parsed_locations: list[tuple[dict, dict | None]] = []
    account_names: set[str] = set()
    for loc in external_locations or []:
        purl = parse_storage_url(loc.get("url", ""))
        parsed_locations.append((loc, purl))
        if purl:
            account_names.add(purl["account"])
    metastore_roots: list[tuple[str, str]] = []   # (label, url)
    for ms in metastores or []:
        root = ms.get("storage_root", "") or ms.get("default_data_access_config_id", "")
        if isinstance(root, str) and "://" in root:
            metastore_roots.append((ms.get("name", "metastore"), root))
            purl = parse_storage_url(root)
            if purl:
                account_names.add(purl["account"])

    # 3. Resolve storage accounts (one Resource Graph call)
    accounts = await resolve_storage_accounts(client, token, list(account_names), sub_ids, arm_errors)

    # 4. Role assignments per resolved account (concurrent, capped)
    role_tasks = {
        name: asyncio.create_task(_guard(fetch_role_assignments(client, token, acct.resource_id, arm_errors)))
        for name, acct in accounts.items() if acct.resource_id
    }
    for name, task in role_tasks.items():
        accounts[name].role_assignments = await task

    # 5. Resolve credential identities (dedup by credential name)
    cred_by_name: dict[str, dict] = {c.get("name", ""): c for c in (storage_credentials or [])}
    identity_by_cred: dict[str, AzureIdentity | None] = {}
    id_tasks = {}
    for cname, cred in cred_by_name.items():
        ami = cred.get("azure_managed_identity") or {}
        if ami:
            id_tasks[cname] = asyncio.create_task(_guard(resolve_identity(client, token, ami, arm_errors)))
    for cname, task in id_tasks.items():
        identity_by_cred[cname] = await task

    identities = [i for i in identity_by_cred.values() if i is not None]

    # 6. Build UC↔Azure mappings
    mappings: list[UCAzureMapping] = []
    for loc, purl in parsed_locations:
        mappings.append(_build_mapping(
            "external_location", loc.get("name", ""), loc.get("url", ""),
            loc.get("read_only"), loc.get("credential_name", ""), purl,
            accounts, identity_by_cred, workspace))
    for label, root in metastore_roots:
        purl = parse_storage_url(root)
        mappings.append(_build_mapping(
            "metastore_root", label, root, None, "", purl,
            accounts, identity_by_cred, workspace))

    # 7. Key vaults (best-effort)
    akv_scopes = [s for s in (secret_scopes or []) if s.get("backend_type") == "AZURE_KEYVAULT"]
    key_vaults = await discover_key_vaults(client, token, akv_scopes, sub_ids, arm_errors) if akv_scopes else []

    return AzureInventory(
        available=True, reason="", workspace=workspace,
        storage_accounts=list(accounts.values()), identities=identities,
        key_vaults=key_vaults, mappings=mappings, arm_errors=arm_errors)


def _build_mapping(uc_object_type: str, uc_name: str, url: str, read_only,
                   credential_name: str, purl: dict | None,
                   accounts: dict[str, AzureStorageAccount],
                   identity_by_cred: dict[str, AzureIdentity | None],
                   workspace: AzureWorkspaceInfra) -> UCAzureMapping:
    notes: list[str] = []
    if purl is None:
        notes.append("non-Azure or unparseable storage URL")
        return UCAzureMapping(
            uc_object_type=uc_object_type, uc_name=uc_name, url=url, read_only=read_only,
            credential_name=credential_name, notes=notes)

    account = purl["account"]
    acct = accounts.get(account)
    identity = identity_by_cred.get(credential_name) if credential_name else None
    granting_roles: list[str] = []
    if acct is None:
        notes.append("storage account not resolvable (different tenant, deleted, or no ARM read access)")
    else:
        if workspace and workspace.geo and acct.location and \
           _checks._resolve_geo(acct.location) != workspace.geo:
            notes.append(f"storage geo ({acct.location}) differs from workspace geo ({workspace.geo})")
        if identity and identity.principal_id:
            granting_roles = [
                ra.role_name for ra in acct.role_assignments
                if ra.principal_id == identity.principal_id
            ]
            if not granting_roles and acct.role_assignments:
                notes.append("identity has no direct role assignment on this account (may be inherited)")
            elif not acct.role_assignments:
                notes.append("role assignments unreadable (insufficient RBAC) — cannot confirm access")

    return UCAzureMapping(
        uc_object_type=uc_object_type, uc_name=uc_name, url=url, read_only=read_only,
        storage_account=account, container=purl.get("container", ""),
        storage_account_resolved=acct is not None, credential_name=credential_name,
        identity=identity, granting_roles=granting_roles, notes=notes)
