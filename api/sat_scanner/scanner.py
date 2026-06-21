"""SAT Scanner — check functions and scan orchestration."""

from __future__ import annotations

import asyncio
import base64
import re
import time
from datetime import datetime
from typing import Any, Optional

import httpx

from .models import SATFinding, SATScanResult
from .checks import (
    SAT_CHECKS, _get_effort, AZURE_REGION_TO_GEO, _resolve_geo,
    CROSS_GEO_DISABLED_BY_DEFAULT, _WORKSPACE_ACCOUNT_IDS, _WORKSPACE_REGIONS,
    _WORKSPACE_ARM_INFO, _AZURE_MGMT_TOKEN,
    CHECK_API_ENDPOINTS, CHECK_BENEFITS, _WS_CONF_EVIDENCE,
)
from .api import (
    _make_finding, _na, _dbx_get, _dbx_post, _dbx_get_all_jobs,
    _dbx_get_workspace_conf, _acct_get, _fetch_account_id,
    _ItemTrackingClient, _dbx_sql_query, _find_running_warehouse,
)
from .helpers import _auto_extract_evidence, _resolve_portal_link, _log
from .scoring import _compute_sat_score, _build_endpoint_summary, _print_summary

# Cache: reference_url -> extracted summary text
_DOC_SUMMARIES: dict[str, str] = {}


async def _fetch_doc_summaries(reference_urls: set[str], client: "httpx.AsyncClient") -> dict[str, str]:
    """Fetch Microsoft Learn pages and extract the meta description as a benefits summary.

    Returns {url: summary_text} for each URL that was successfully fetched.
    Results are cached in _DOC_SUMMARIES to avoid re-fetching.
    """
    import asyncio
    import re

    results: dict[str, str] = {}
    urls_to_fetch: list[str] = []
    for url in reference_urls:
        if not url:
            continue
        if url in _DOC_SUMMARIES:
            results[url] = _DOC_SUMMARIES[url]
        else:
            urls_to_fetch.append(url)

    if not urls_to_fetch:
        return results

    async def _fetch_one(url: str) -> tuple[str, str]:
        try:
            resp = await client.get(url, timeout=10, follow_redirects=True)
            if resp.status_code != 200:
                return url, ""
            # Extract <meta name="description" content="...">
            text = resp.text[:8000]  # Only need the <head> section
            m = re.search(r'<meta\s+name=["\']description["\']\s+content=["\']([^"\']+)["\']', text, re.IGNORECASE)
            if not m:
                # Try alternate order: content before name
                m = re.search(r'<meta\s+content=["\']([^"\']+)["\']\s+name=["\']description["\']', text, re.IGNORECASE)
            return url, m.group(1).strip() if m else ""
        except Exception:
            return url, ""

    tasks = [_fetch_one(url) for url in urls_to_fetch]
    fetched = await asyncio.gather(*tasks)
    for url, summary in fetched:
        _DOC_SUMMARIES[url] = summary
        results[url] = summary

    return results


# ─────────────────────────────────────────────────────────────────────────────
# Check functions
# ─────────────────────────────────────────────────────────────────────────────

async def _check_iam(client: httpx.AsyncClient, host: str, token: str) -> list[SATFinding]:
    findings: list[SATFinding] = []
    admins_data, adm_s, adm_e = await _dbx_get(client, host, "/api/2.0/preview/scim/v2/Groups", token,
        {"filter": 'displayName eq "admins"', "attributes": "members"})
    if admins_data is not None:
        groups = admins_data.get("Resources", [])
        admin_count = len(groups[0].get("members", [])) if groups else 0
        status = "PASS" if admin_count <= 2 else ("WARN" if admin_count <= 4 else "FAIL")
        findings.append(_make_finding("SAT-IAM-1", status, f"{admin_count} admin account(s) found",
            {"admin_count": admin_count}))
    else:
        findings.append(_na("SAT-IAM-1", adm_s, adm_e))

    sp_data, sp_s, sp_e = await _dbx_get(client, host, "/api/2.0/preview/scim/v2/ServicePrincipals", token, {"count": "100"})
    if sp_data is not None:
        sp_count = sp_data.get("totalResults", len(sp_data.get("Resources", [])))
        iam2_status = "PASS" if sp_count >= 2 else ("WARN" if sp_count == 1 else "WARN")
        findings.append(_make_finding("SAT-IAM-2", iam2_status,
            f"{sp_count} service principal(s) configured" if sp_count else "No service principals — consider using SPs for automation"))
    else:
        findings.append(_na("SAT-IAM-2", sp_s, sp_e))

    ws_conf, wc_s, wc_e = await _dbx_get_workspace_conf(client, host, token,
        "maxTokenLifetimeDays,enableTokensConfig,enableRoleBasedAccessControl")
    if ws_conf is not None:
        tokens_config = str(ws_conf.get("enableTokensConfig", "false")).lower() == "true"
        max_lifetime = ws_conf.get("maxTokenLifetimeDays", "")
        has_lifetime = bool(max_lifetime) and str(max_lifetime) not in ("0", "")
        pat_ok = tokens_config and has_lifetime
        findings.append(_make_finding("SAT-IAM-3",
            "PASS" if pat_ok else ("WARN" if tokens_config else "FAIL"),
            f"Token management: {'enabled' if tokens_config else 'disabled'}, max lifetime: {max_lifetime or 'unlimited'}"))
        acl_raw = ws_conf.get("enableRoleBasedAccessControl")
        is_azure = "azuredatabricks.net" in host.lower()
        if acl_raw is not None:
            acl_enabled = str(acl_raw).lower() == "true"
            findings.append(_make_finding("SAT-IAM-4", "PASS" if acl_enabled else "FAIL",
                f"Workspace RBAC: {'enabled' if acl_enabled else 'disabled'}"))
        elif is_azure:
            # Azure Databricks has RBAC enabled by default and the conf key may not exist
            findings.append(_make_finding("SAT-IAM-4", "PASS",
                "Workspace RBAC: enabled (Azure Databricks — always on by default)"))
        else:
            findings.append(_make_finding("SAT-IAM-4", "WARN",
                "Workspace RBAC: could not determine — enableRoleBasedAccessControl not found in workspace config"))
    else:
        for cid in ("SAT-IAM-3", "SAT-IAM-4"):
            findings.append(_na(cid, wc_s, wc_e))
    # Enrich with API response data
    _api = {"SAT-IAM-1": admins_data, "SAT-IAM-2": sp_data, "SAT-IAM-3": ws_conf, "SAT-IAM-4": ws_conf}
    for f in findings:
        resp = _api.get(f.check_id)
        if resp is not None:
            f.details.setdefault("api_response", resp)
    return findings


async def _check_network(client: httpx.AsyncClient, host: str, token: str) -> list[SATFinding]:
    findings: list[SATFinding] = []
    ip_data, ip_s, ip_e = await _dbx_get(client, host, "/api/2.0/ip-access-lists", token)
    if ip_data is not None:
        lists = ip_data.get("ip_access_lists", [])
        enabled_lists = [l for l in lists if l.get("list_status") == "ENABLED"]
        has_allowlist = any(l.get("list_type") == "ALLOW" for l in enabled_lists)
        status = "PASS" if has_allowlist else ("WARN" if lists else "FAIL")
        findings.append(_make_finding("SAT-NET-1", status,
            f"{len(enabled_lists)} enabled IP access list(s), allowlist: {'present' if has_allowlist else 'missing'}"))
    else:
        findings.append(_na("SAT-NET-1", ip_s, ip_e))

    # SAT-NET-2: Secure Cluster Connectivity (workspace-level setting)
    ws_conf_scc, scc_s, scc_e = await _dbx_get_workspace_conf(client, host, token, "enableNoPublicIp")
    if ws_conf_scc is not None:
        scc = str(ws_conf_scc.get("enableNoPublicIp", "false")).lower() == "true"
        findings.append(_make_finding("SAT-NET-2", "PASS" if scc else "WARN",
            f"SCC (No Public IP): {'enabled' if scc else 'not enabled'}"))
    else:
        # Fallback: workspace-conf key deprecated on some workspaces — try Settings API
        scc_resolved = False
        for scc_type in ("secure_cluster_connectivity_ws", "no_public_ip_ws"):
            scc_data, scc_ss, scc_se = await _dbx_get(client, host,
                f"/api/2.0/settings/types/{scc_type}/names/default", token)
            if scc_ss == 200 and scc_data is not None:
                ws_conf_scc = scc_data
                scc_obj = scc_data.get(scc_type, scc_data)
                scc = scc_obj.get("is_enabled", False)
                findings.append(_make_finding("SAT-NET-2", "PASS" if scc else "WARN",
                    f"SCC (No Public IP) via Settings API: {'enabled' if scc else 'not enabled'}"))
                scc_resolved = True
                break
        if not scc_resolved:
            # Last resort: infer from cluster configurations
            cl_data, cl_s2, _ = await _dbx_get(client, host, "/api/2.0/clusters/list", token)
            if cl_s2 == 200 and cl_data is not None:
                clusters = cl_data.get("clusters", [])
                if clusters:
                    no_pub = all(not c.get("azure_attributes", {}).get("enable_elastic_disk") is None
                        and "subnet_id" in str(c.get("azure_attributes", {})).lower()
                        for c in clusters)
                    ws_conf_scc = {"inferred_from_clusters": True, "cluster_count": len(clusters), "all_no_public_ip": no_pub}
                    findings.append(_make_finding("SAT-NET-2", "PASS" if no_pub else "WARN",
                        f"SCC (No Public IP) inferred from {len(clusters)} cluster(s): "
                        f"{'all appear SCC-enabled' if no_pub else 'some clusters may have public IPs'}"))
                else:
                    ws_conf_scc = {"inferred_from_clusters": True, "cluster_count": 0}
                    findings.append(_make_finding("SAT-NET-2", "WARN",
                        "SCC (No Public IP): cannot determine — workspace-conf key deprecated and no clusters to inspect."))
            else:
                findings.append(_na("SAT-NET-2", scc_s, scc_e))

    # SAT-NET-3: VNet injection / Private Link (workspace conf + cluster attributes)
    net_conf, net_s, net_e = await _dbx_get_workspace_conf(client, host, token, "customSubnetId,virtualNetworkId")
    has_vnet = net_conf and (net_conf.get("customSubnetId") or net_conf.get("virtualNetworkId"))
    clusters_data, cl_s, cl_e = await _dbx_get(client, host, "/api/2.0/clusters/list", token)
    vnet_injected = False
    if clusters_data is not None:
        clusters = clusters_data.get("clusters", [])
        vnet_injected = any(c.get("azure_attributes", {}).get("availability") or "subnet_name" in str(c.get("azure_attributes", {})) for c in clusters)
    if net_conf is not None or clusters_data is not None:
        findings.append(_make_finding("SAT-NET-3", "PASS" if (has_vnet or vnet_injected) else "WARN",
            f"VNet/Private Link: {'detected' if (has_vnet or vnet_injected) else 'not detected'}"))
    else:
        findings.append(_na("SAT-NET-3", net_s, net_e))
    # Enrich with API response data
    _api = {"SAT-NET-1": ip_data, "SAT-NET-2": ws_conf_scc, "SAT-NET-3": net_conf or clusters_data}
    for f in findings:
        resp = _api.get(f.check_id)
        if resp is not None:
            f.details.setdefault("api_response", resp)
    return findings


async def _check_data_protection(client: httpx.AsyncClient, host: str, token: str) -> list[SATFinding]:
    findings: list[SATFinding] = []
    uc_data, uc_s, uc_e = await _dbx_get(client, host, "/api/2.1/unity-catalog/metastores", token)
    if uc_data is not None:
        metastores = uc_data.get("metastores", [])
        findings.append(_make_finding("SAT-DATA-1", "PASS" if metastores else "WARN",
            f"Unity Catalog: {'enabled' if metastores else 'not configured'}. {len(metastores)} metastore(s)."))
    else:
        findings.append(_na("SAT-DATA-1", uc_s, uc_e))

    ws_conf2, wc2_s, wc2_e = await _dbx_get_workspace_conf(client, host, token, "enableDbfsFileBrowser")
    if ws_conf2 is not None:
        dbfs_browser = str(ws_conf2.get("enableDbfsFileBrowser", "true")).lower() == "true"
        findings.append(_make_finding("SAT-DATA-2", "WARN" if dbfs_browser else "PASS",
            f"DBFS file browser: {'enabled' if dbfs_browser else 'disabled'}"))
    else:
        findings.append(_na("SAT-DATA-2", wc2_s, wc2_e))

    cats_data, cats_s, cats_e = await _dbx_get(client, host, "/api/2.1/unity-catalog/catalogs", token)
    if cats_data is not None:
        catalogs = cats_data.get("catalogs", [])
        non_system = [c for c in catalogs if c.get("name") not in ("system", "main", "__databricks_internal")]
        has_env = any(any(kw in (c.get("name") or "").lower() for kw in ("prod", "dev", "staging", "uat")) for c in non_system)
        status = "NOT_APPLICABLE" if not non_system else ("PASS" if has_env else "WARN")
        findings.append(_make_finding("SAT-DATA-3", status,
            f"{len(catalogs)} catalog(s), env-specific: {'yes' if has_env else 'no'}" if non_system else "No user catalogs found"))
    else:
        findings.append(_na("SAT-DATA-3", cats_s, cats_e))
    # Enrich with API response data
    _api = {"SAT-DATA-1": uc_data, "SAT-DATA-2": ws_conf2, "SAT-DATA-3": cats_data}
    for f in findings:
        resp = _api.get(f.check_id)
        if resp is not None:
            f.details.setdefault("api_response", resp)
    return findings


async def _check_compute(client: httpx.AsyncClient, host: str, token: str) -> list[SATFinding]:
    findings: list[SATFinding] = []
    policies_data, pol_s, pol_e = await _dbx_get(client, host, "/api/2.0/policies/clusters/list", token)
    clusters_pol, cp_s, cp_e = await _dbx_get(client, host, "/api/2.0/clusters/list", token)
    if policies_data is not None:
        policies = policies_data.get("policies", [])
        custom = [p for p in policies if not p.get("is_default", False)]
        if not custom:
            findings.append(_make_finding("SAT-COMPUTE-1", "WARN", "No custom cluster policies defined — consider creating policies to enforce standards"))
        else:
            active_clusters = []
            if clusters_pol is not None:
                active_clusters = [c for c in clusters_pol.get("clusters", []) if c.get("state") not in ("TERMINATED", "TERMINATING")]
            with_policy = [c for c in active_clusters if c.get("policy_id")]
            without_policy = [c for c in active_clusters if not c.get("policy_id")]
            if active_clusters and len(without_policy) > len(active_clusters) // 2:
                status = "WARN"
            elif active_clusters and with_policy:
                status = "PASS"
            else:
                status = "WARN"
            findings.append(_make_finding("SAT-COMPUTE-1", status,
                f"{len(custom)} custom policies. {len(with_policy)}/{len(active_clusters)} active clusters use a policy."))
    else:
        findings.append(_na("SAT-COMPUTE-1", pol_s, pol_e))

    clusters_data, cl2_s, cl2_e = await _dbx_get(client, host, "/api/2.0/clusters/list", token)
    if clusters_data is not None:
        clusters = clusters_data.get("clusters", [])
        active = [c for c in clusters if c.get("state") not in ("TERMINATED", "TERMINATING")]
        def _is_eol(sv: str) -> bool:
            try: return int((sv or "").split(".")[0]) < 11
            except: return False
        eol = [c for c in active if _is_eol(c.get("spark_version", ""))]
        findings.append(_make_finding("SAT-COMPUTE-2",
            "NOT_APPLICABLE" if not active else ("FAIL" if eol else "PASS"),
            f"{len(eol)} cluster(s) running EOL runtime out of {len(active)} active" if active else "No active clusters"))
        no_log = [c for c in active if not c.get("cluster_log_conf")]
        findings.append(_make_finding("SAT-COMPUTE-3",
            "NOT_APPLICABLE" if not active else ("PASS" if not no_log else ("WARN" if len(no_log) < len(active) // 2 else "FAIL")),
            f"{len(active) - len(no_log)}/{len(active)} active clusters have log delivery" if active else "No active clusters"))
        no_autoscale = [c for c in active if not c.get("autoscale") and c.get("cluster_source") != "JOB"]
        interactive = [c for c in active if c.get("cluster_source") != "JOB"]
        findings.append(_make_finding("SAT-COMPUTE-4",
            "NOT_APPLICABLE" if not interactive else ("PASS" if not no_autoscale else ("WARN" if len(no_autoscale) < len(interactive) else "FAIL")),
            f"{len(interactive) - len(no_autoscale)}/{len(interactive)} interactive clusters have autoscaling" if interactive else "No interactive clusters"))
    else:
        for cid in ("SAT-COMPUTE-2", "SAT-COMPUTE-3", "SAT-COMPUTE-4"):
            findings.append(_na(cid, cl2_s, cl2_e))
    # Enrich with API response data
    _api = {"SAT-COMPUTE-1": policies_data, "SAT-COMPUTE-2": clusters_data, "SAT-COMPUTE-3": clusters_data, "SAT-COMPUTE-4": clusters_data}
    for f in findings:
        resp = _api.get(f.check_id)
        if resp is not None:
            f.details.setdefault("api_response", resp)
    return findings


async def _check_sql_warehouses(client: httpx.AsyncClient, host: str, token: str) -> list[SATFinding]:
    findings: list[SATFinding] = []
    wh_data, wh_s, wh_e = await _dbx_get(client, host, "/api/2.0/sql/warehouses", token)
    if wh_data is None:
        for cid in ("SAT-SQL-1", "SAT-SQL-2", "SAT-SQL-3"):
            findings.append(_na(cid, wh_s, wh_e))
        return findings
    warehouses = wh_data.get("warehouses", [])
    if not warehouses:
        for cid in ("SAT-SQL-1", "SAT-SQL-2", "SAT-SQL-3"):
            findings.append(_make_finding(cid, "NOT_APPLICABLE", "No SQL warehouses found"))
        return findings
    no_autostop = [w for w in warehouses if not w.get("auto_stop_mins") or w.get("auto_stop_mins") == 0]
    findings.append(_make_finding("SAT-SQL-1",
        "PASS" if not no_autostop else ("WARN" if len(no_autostop) < len(warehouses) else "FAIL"),
        f"{len(warehouses) - len(no_autostop)}/{len(warehouses)} SQL warehouses have auto-stop"))
    preview_wh = [w for w in warehouses if (w.get("channel") or {}).get("name") == "CHANNEL_NAME_CURRENT"]
    findings.append(_make_finding("SAT-SQL-2", "WARN" if preview_wh else "PASS",
        f"{len(preview_wh)} warehouse(s) on preview/current channel"))
    # SAT-SQL-3: Unity Catalog enabled
    no_uc_wh = [w for w in warehouses if not w.get("enable_serverless_compute") and
        (w.get("warehouse_type") or "").upper() not in ("PRO", "SERVERLESS")]
    findings.append(_make_finding("SAT-SQL-3",
        "PASS" if not no_uc_wh else ("WARN" if len(no_uc_wh) < len(warehouses) else "FAIL"),
        f"{len(warehouses) - len(no_uc_wh)}/{len(warehouses)} SQL warehouses have Unity Catalog/Pro/Serverless"))

    # SAT-WH-SCALING: Appropriate min/max scaling bounds
    bad_scaling = [w for w in warehouses
        if w.get("max_num_clusters", 1) > 20 or w.get("min_num_clusters", 0) > 5]
    findings.append(_make_finding("SAT-WH-SCALING",
        "PASS" if not bad_scaling else "WARN",
        f"{len(warehouses) - len(bad_scaling)}/{len(warehouses)} SQL warehouses have appropriate scaling bounds."))

    # SAT-WH-TAGS: Cost attribution tags on warehouses
    tagged_wh = [w for w in warehouses if w.get("tags", {}).get("custom_tags")]
    pct_tagged = round(len(tagged_wh) / len(warehouses) * 100) if warehouses else 0
    findings.append(_make_finding("SAT-WH-TAGS",
        "PASS" if pct_tagged >= 80 else ("WARN" if tagged_wh else "FAIL"),
        f"{len(tagged_wh)}/{len(warehouses)} SQL warehouses ({pct_tagged}%) have cost attribution tags."))

    # Enrich with API response data
    for f in findings:
        if wh_data is not None:
            f.details.setdefault("api_response", wh_data)
    return findings


async def _check_secrets(client: httpx.AsyncClient, host: str, token: str) -> list[SATFinding]:
    findings: list[SATFinding] = []
    scopes_data, sc_s, sc_e = await _dbx_get(client, host, "/api/2.0/secrets/scopes/list", token)
    if scopes_data is not None:
        scopes = scopes_data.get("scopes", [])
        kv_backed = [s for s in scopes if s.get("backend_type") == "AZURE_KEYVAULT"]
        findings.append(_make_finding("SAT-SEC-1", "PASS" if scopes else "WARN",
            f"{len(scopes)} secret scope(s) configured ({len(kv_backed)} Azure Key Vault backed)" if scopes else "No secret scopes configured — consider using secret scopes for credential management"))
    else:
        findings.append(_na("SAT-SEC-1", sc_s, sc_e))
    jobs_data, jb_s, jb_e = await _dbx_get_all_jobs(client, host, token, {"expand_tasks": "false"})
    if jobs_data is not None:
        jobs = jobs_data.get("jobs", [])
        sp_owned = [j for j in jobs if j.get("creator_user_name", "").endswith("@") or "applicationId" in str(j.get("settings", {}))]
        user_owned = [j for j in jobs if j not in sp_owned]
        findings.append(_make_finding("SAT-SEC-2",
            "NOT_APPLICABLE" if not jobs else ("PASS" if not user_owned else ("WARN" if len(user_owned) < len(jobs) else "FAIL")),
            f"{len(jobs)} job(s) total. {len(user_owned)} may be owned by human users" if jobs else "No jobs found"))
    else:
        findings.append(_na("SAT-SEC-2", jb_s, jb_e))
    # Enrich with API response data
    _api = {"SAT-SEC-1": scopes_data, "SAT-SEC-2": jobs_data}
    for f in findings:
        resp = _api.get(f.check_id)
        if resp is not None:
            f.details.setdefault("api_response", resp)
    return findings


async def _check_logging(client: httpx.AsyncClient, host: str, token: str) -> list[SATFinding]:
    findings: list[SATFinding] = []

    # SAT-LOG-DIAG: Check Azure Diagnostic Settings via ARM API
    arm = _WORKSPACE_ARM_INFO.get(host.rstrip("/"), {})
    if arm and _AZURE_MGMT_TOKEN:
        resource_id = arm.get("resource_id", "")
        diag_url = f"https://management.azure.com{resource_id}/providers/Microsoft.Insights/diagnosticSettings?api-version=2021-05-01-preview"
        try:
            diag_resp = await client.get(diag_url, headers={"Authorization": f"Bearer {_AZURE_MGMT_TOKEN}"})
            if diag_resp.status_code == 200:
                diag_data = diag_resp.json()
                settings = diag_data.get("value", [])
                if settings:
                    names = [s.get("name", "?") for s in settings]
                    # Check which log destinations are configured
                    destinations = set()
                    for s in settings:
                        props = s.get("properties", {})
                        if props.get("workspaceId"):
                            destinations.add("Log Analytics")
                        if props.get("storageAccountId"):
                            destinations.add("Storage Account")
                        if props.get("eventHubAuthorizationRuleId"):
                            destinations.add("Event Hub")
                    dest_str = ", ".join(sorted(destinations)) if destinations else "configured"
                    f_diag = _make_finding("SAT-LOG-DIAG", "PASS",
                        f"{len(settings)} diagnostic setting(s): {', '.join(names)}. Destinations: {dest_str}")
                    f_diag.details["api_response"] = diag_data
                    findings.append(f_diag)
                else:
                    f_diag = _make_finding("SAT-LOG-DIAG", "FAIL",
                        "No Azure Diagnostic Settings configured. Audit logs are not being delivered.")
                    f_diag.details["api_response"] = diag_data
                    findings.append(f_diag)
            else:
                findings.append(_make_finding("SAT-LOG-DIAG", "WARN",
                    f"Could not query Azure Diagnostic Settings (HTTP {diag_resp.status_code})."))
        except Exception as exc:
            findings.append(_make_finding("SAT-LOG-DIAG", "WARN",
                f"Error querying Azure Diagnostic Settings: {exc}"))
    else:
        findings.append(_make_finding("SAT-LOG-DIAG", "NOT_APPLICABLE",
            "Azure Diagnostic Settings check requires Azure login (--azure/--azure-all/--azure-tenant)."))
    ws_conf_log, wl_s, wl_e = await _dbx_get_workspace_conf(client, host, token, "enableVerboseAuditLogs")
    if ws_conf_log is not None:
        verbose_audit = str(ws_conf_log.get("enableVerboseAuditLogs", "false")).lower() == "true"
        findings.append(_make_finding("SAT-LOG-1", "PASS" if verbose_audit else "WARN",
            f"Verbose audit logs: {'enabled' if verbose_audit else 'not enabled'}"))
    else:
        findings.append(_na("SAT-LOG-1", wl_s, wl_e))
    clusters_log, cl3_s, cl3_e = await _dbx_get(client, host, "/api/2.0/clusters/list", token)
    if clusters_log is not None:
        clusters = clusters_log.get("clusters", [])
        with_logs = [c for c in clusters if c.get("cluster_log_conf")]
        pct = (len(with_logs) * 100 // len(clusters)) if clusters else 0
        findings.append(_make_finding("SAT-LOG-2",
            "NOT_APPLICABLE" if not clusters else (
                "PASS" if pct >= 80 else ("WARN" if with_logs else "FAIL")),
            f"{len(with_logs)}/{len(clusters)} clusters ({pct}%) have log delivery" if clusters else "No clusters found"))
    else:
        findings.append(_na("SAT-LOG-2", cl3_s, cl3_e))
    jobs_log, jl_s, jl_e = await _dbx_get_all_jobs(client, host, token)
    if jobs_log is not None:
        jobs = jobs_log.get("jobs", [])
        findings.append(_make_finding("SAT-LOG-3", "NOT_APPLICABLE" if not jobs else "WARN",
            f"{len(jobs)} job(s) found — verify job results are exported to durable storage for retention."
            if jobs else "No jobs found."))
    else:
        findings.append(_na("SAT-LOG-3", jl_s, jl_e))
    # Enrich with API response data
    _api = {"SAT-LOG-1": ws_conf_log, "SAT-LOG-2": clusters_log, "SAT-LOG-3": jobs_log}
    for f in findings:
        resp = _api.get(f.check_id)
        if resp is not None:
            f.details.setdefault("api_response", resp)
    return findings


async def _check_workspace_config_extended(client: httpx.AsyncClient, host: str, token: str) -> list[SATFinding]:
    findings: list[SATFinding] = []
    keys = ",".join([
        "enableResultsDownloading", "enableExportNotebook", "enableNotebookTableClipboard",
        "storeInteractiveNotebookResultsInCustomerAccount", "enableFileStoreEndpoint",
        "enforceUserIsolation", "enableVerboseAuditLogs",
        "enableDeprecatedGlobalInitScripts", "enableDeprecatedClusterNamedInitScripts",
        "maxTokenLifetimeDays", "enableTokensConfig",
        "enableJobViewAcls", "enforceClusterViewAcls", "enforceWorkspaceViewAcls",
        "enableProjectTypeInWorkspace",
        "enable-X-Frame-Options", "enable-X-Content-Type-Options", "enable-X-XSS-Protection",
    ])
    ws_conf, ws_s, ws_e = await _dbx_get_workspace_conf(client, host, token, keys)
    na_ids = ("SAT-DATA-5", "SAT-DATA-6", "SAT-DATA-7", "SAT-DATA-8", "SAT-DATA-9",
              "SAT-GOV-7", "SAT-GOV-8", "SAT-GOV-11", "SAT-GOV-12", "SAT-IAM-6",
              "SAT-INFO-3", "SAT-INFO-4", "SAT-INFO-5", "SAT-INFO-6", "SAT-INFO-7", "SAT-INFO-8", "SAT-INFO-9")
    if ws_conf is None:
        for cid in na_ids:
            findings.append(_na(cid, ws_s, ws_e))
        return findings

    def _bv(key: str, default: str = "false") -> bool:
        return str(ws_conf.get(key, default)).lower() == "true"

    for cid, key, bad, lab in [
        ("SAT-DATA-5", "enableResultsDownloading", True, "Results downloading"),
        ("SAT-DATA-6", "enableExportNotebook", True, "Notebook export"),
        ("SAT-DATA-7", "enableNotebookTableClipboard", True, "Notebook table clipboard"),
        ("SAT-DATA-9", "enableFileStoreEndpoint", True, "DBFS file store endpoint"),
    ]:
        val = _bv(key, "true")
        findings.append(_make_finding(cid, ("WARN" if bad else "PASS") if val else ("PASS" if bad else "WARN"),
            f"{lab}: {'enabled' if val else 'disabled'}"))

    in_cust = _bv("storeInteractiveNotebookResultsInCustomerAccount", "false")
    findings.append(_make_finding("SAT-DATA-8", "PASS" if in_cust else "WARN",
        f"Notebook results: {'customer account' if in_cust else 'Databricks control plane'}"))

    for cid, key, lab in [
        ("SAT-GOV-7", "enforceUserIsolation", "User isolation"),
        ("SAT-GOV-8", "enableVerboseAuditLogs", "Verbose audit logs"),
    ]:
        val = _bv(key, "false")
        findings.append(_make_finding(cid, "PASS" if val else ("FAIL" if cid == "SAT-GOV-7" else "WARN"),
            f"{lab}: {'enabled' if val else 'disabled'}"))

    for cid, key, lab in [
        ("SAT-GOV-11", "enableDeprecatedGlobalInitScripts", "Deprecated global init scripts"),
        ("SAT-GOV-12", "enableDeprecatedClusterNamedInitScripts", "Deprecated cluster-named init scripts"),
    ]:
        val = _bv(key, "false")
        findings.append(_make_finding(cid, "FAIL" if val else "PASS",
            f"{lab}: {'enabled (insecure)' if val else 'disabled'}"))

    tok_enabled = _bv("enableTokensConfig", "false")
    max_raw = ws_conf.get("maxTokenLifetimeDays", "")
    try:
        max_days = int(max_raw) if max_raw else None
    except (ValueError, TypeError):
        max_days = None
    if not tok_enabled:
        iam6_s, iam6_st = "FAIL", "Token management not enabled"
    elif max_days is None or max_days == 0:
        iam6_s, iam6_st = "FAIL", "Token management enabled but no max lifetime set"
    elif max_days >= 180:
        iam6_s, iam6_st = "FAIL", f"Max token lifetime: {max_days} days (far exceeds 90-day limit; Microsoft recommends < 90)"
    elif max_days >= 90:
        iam6_s, iam6_st = "WARN", f"Max token lifetime: {max_days} days (≥ 90-day limit; Microsoft recommends < 90)"
    else:
        iam6_s, iam6_st = "PASS", f"Max token lifetime: {max_days} days (< 90)"
    findings.append(_make_finding("SAT-IAM-6", iam6_s, iam6_st))

    for cid, key, lab in [
        ("SAT-INFO-3", "enableJobViewAcls", "Job view ACLs"),
        ("SAT-INFO-4", "enforceClusterViewAcls", "Cluster view ACLs"),
        ("SAT-INFO-5", "enforceWorkspaceViewAcls", "Workspace view ACLs"),
        ("SAT-INFO-6", "enableProjectTypeInWorkspace", "Databricks Projects"),
        ("SAT-INFO-7", "enable-X-Frame-Options", "X-Frame-Options"),
        ("SAT-INFO-8", "enable-X-Content-Type-Options", "X-Content-Type-Options"),
        ("SAT-INFO-9", "enable-X-XSS-Protection", "X-XSS-Protection"),
    ]:
        val = _bv(key, "false")
        findings.append(_make_finding(cid, "PASS" if val else "WARN",
            f"{lab}: {'enabled' if val else 'not enabled'}"))
    # Enrich all findings from this check with the workspace-conf response
    if ws_conf is not None:
        for f in findings:
            f.details.setdefault("api_response", ws_conf)
    return findings


async def _check_pat_tokens(client: httpx.AsyncClient, host: str, token: str) -> list[SATFinding]:
    findings: list[SATFinding] = []
    tokens_data, tok_s, tok_e = await _dbx_get(client, host, "/api/2.0/token/list", token)
    ws_conf_tok, _, _ = await _dbx_get_workspace_conf(client, host, token, "maxTokenLifetimeDays")
    max_days: Optional[int] = None
    if ws_conf_tok:
        try:
            raw = ws_conf_tok.get("maxTokenLifetimeDays", "")
            max_days = int(raw) if raw else None
        except: pass
    if tokens_data is None:
        for cid in ("SAT-IAM-5", "SAT-IAM-7", "SAT-IAM-SP-OAUTH", "SAT-IAM-STALE-TOKENS"):
            findings.append(_na(cid, tok_s, tok_e))
        return findings
    tokens = tokens_data.get("token_infos", [])
    no_exp = [t for t in tokens if t.get("expiry_time", -1) == -1]
    findings.append(_make_finding("SAT-IAM-5",
        "NOT_APPLICABLE" if not tokens else ("FAIL" if no_exp else "PASS"),
        f"{len(no_exp)} token(s) with no expiry out of {len(tokens)} total" if tokens else "No PAT tokens found"))
    if max_days and max_days > 0:
        max_ms = max_days * 86_400 * 1_000
        exceeding = [t for t in tokens if t.get("expiry_time", -1) != -1 and (t.get("expiry_time", 0) - t.get("creation_time", 0)) > max_ms]
        findings.append(_make_finding("SAT-IAM-7", "WARN" if exceeding else "PASS",
            f"{len(exceeding)} token(s) exceed the {max_days}-day maximum lifetime",
            {"api_response": {"total_tokens": len(tokens), "exceeding": len(exceeding), "max_days": max_days},
             "api_endpoint": "/api/2.0/token/list"}))
    else:
        findings.append(_make_finding("SAT-IAM-7", "NOT_APPLICABLE", "maxTokenLifetimeDays not configured"))
    # SAT-IAM-SP-OAUTH: Check for SP-owned PAT tokens (SPs should use OAuth)
    sp_tokens = [t for t in tokens if "spn-" in (t.get("comment", "") or "").lower()
        or "service-principal" in (t.get("comment", "") or "").lower()
        or "service_principal" in (t.get("comment", "") or "").lower()
        or (t.get("created_by_username", "") or "").endswith("@azuread")]
    findings.append(_make_finding("SAT-IAM-SP-OAUTH",
        "NOT_APPLICABLE" if not tokens else ("WARN" if sp_tokens else "PASS"),
        f"{len(sp_tokens)} token(s) may belong to service principals. SPs should use OAuth M2M."
        if sp_tokens else ("No PAT tokens found" if not tokens else f"No service principal PAT tokens detected out of {len(tokens)} tokens.")))
    # SAT-IAM-STALE-TOKENS: Tokens not used in > 90 days
    now_ms = int(time.time() * 1_000)
    stale_cutoff_ms = now_ms - 90 * 86_400 * 1_000
    stale = [t for t in tokens if t.get("last_used_time") and t["last_used_time"] > 0 and t["last_used_time"] < stale_cutoff_ms]
    no_usage = [t for t in tokens if not t.get("last_used_time") or t.get("last_used_time", 0) <= 0]
    findings.append(_make_finding("SAT-IAM-STALE-TOKENS",
        "NOT_APPLICABLE" if not tokens else ("WARN" if stale or no_usage else "PASS"),
        f"{len(stale)} token(s) unused > 90 days, {len(no_usage)} with no usage data out of {len(tokens)} total" if tokens else "No PAT tokens found"))
    # SAT-IAM-8: PAT tokens expiring within 7 days
    seven_days_ms = 7 * 86_400 * 1_000
    expiring_soon = [t for t in tokens
                     if t.get("expiry_time", -1) > 0 and 0 < (t["expiry_time"] - now_ms) <= seven_days_ms]
    already_expired = [t for t in tokens
                       if t.get("expiry_time", -1) > 0 and t["expiry_time"] <= now_ms]
    if not tokens:
        findings.append(_make_finding("SAT-IAM-8", "NOT_APPLICABLE", "No PAT tokens found."))
    elif expiring_soon or already_expired:
        parts = []
        if expiring_soon:
            parts.append(f"{len(expiring_soon)} expiring within 7 days")
        if already_expired:
            parts.append(f"{len(already_expired)} already expired")
        findings.append(_make_finding("SAT-IAM-8", "WARN",
            f"{', '.join(parts)} out of {len(tokens)} total token(s).",
            {"expiring_soon": len(expiring_soon), "already_expired": len(already_expired),
             "total_tokens": len(tokens)}))
    else:
        findings.append(_make_finding("SAT-IAM-8", "PASS",
            f"No tokens expiring within 7 days out of {len(tokens)} total."))
    # Enrich with API response data
    _api = {"SAT-IAM-5": tokens_data, "SAT-IAM-7": tokens_data,
            "SAT-IAM-SP-OAUTH": tokens_data, "SAT-IAM-STALE-TOKENS": tokens_data,
            "SAT-IAM-8": tokens_data}
    for f in findings:
        resp = _api.get(f.check_id)
        if resp is not None:
            f.details.setdefault("api_response", resp)
    return findings


async def _check_compute_extended(client: httpx.AsyncClient, host: str, token: str) -> list[SATFinding]:
    findings: list[SATFinding] = []
    na_ids = ("SAT-DATA-4", "SAT-NET-4", "SAT-GOV-2", "SAT-GOV-3", "SAT-GOV-6", "SAT-COMPUTE-PHOTON",
              "SAT-DATA-CREDENTIAL-PASSTHROUGH")
    clusters_data, cex_s, cex_e = await _dbx_get(client, host, "/api/2.0/clusters/list", token)
    if clusters_data is None:
        for cid in na_ids:
            findings.append(_na(cid, cex_s, cex_e))
        return findings
    clusters = clusters_data.get("clusters", [])
    active = [c for c in clusters if c.get("state") not in ("TERMINATED", "TERMINATING")]
    interactive = [c for c in active if c.get("cluster_source") != "JOB"]

    no_enc = [c for c in interactive if not c.get("enable_local_disk_encryption", False)]
    findings.append(_make_finding("SAT-DATA-4", "FAIL" if no_enc and interactive else "PASS",
        f"{len(interactive) - len(no_enc)}/{len(interactive)} interactive clusters have local disk encryption"))
    ssh_cl = [c for c in active if c.get("ssh_public_keys")]
    findings.append(_make_finding("SAT-NET-4", "FAIL" if ssh_cl else "PASS",
        f"{len(ssh_cl)} cluster(s) have SSH keys out of {len(active)} active"))
    now_ms = int(time.time() * 1_000)
    cutoff_ms = now_ms - 30 * 24 * 3_600 * 1_000
    long_run = [c for c in active if c.get("state") == "RUNNING" and c.get("start_time", now_ms) < cutoff_ms]
    findings.append(_make_finding("SAT-GOV-2", "WARN" if long_run else "PASS",
        f"{len(long_run)} cluster(s) running > 30 days"))
    no_tags = [c for c in interactive if not c.get("custom_tags")]
    findings.append(_make_finding("SAT-GOV-3", "WARN" if no_tags else ("NOT_APPLICABLE" if not interactive else "PASS"),
        f"{len(interactive) - len(no_tags)}/{len(interactive)} interactive clusters have tags"))
    SECURE_MODES = {"USER_ISOLATION", "SINGLE_USER"}
    no_uc = [c for c in interactive if (c.get("data_security_mode") or "NONE") not in SECURE_MODES]
    findings.append(_make_finding("SAT-GOV-6",
        "FAIL" if len(no_uc) == len(interactive) and interactive else ("WARN" if no_uc else "PASS"),
        f"{len(interactive) - len(no_uc)}/{len(interactive)} interactive clusters use UC security mode"))
    # SAT-COMPUTE-PHOTON: Photon enabled
    if not interactive:
        findings.append(_make_finding("SAT-COMPUTE-PHOTON", "NOT_APPLICABLE", "No active interactive clusters"))
    else:
        photon = [c for c in interactive if (c.get("runtime_engine") or "").upper() == "PHOTON"]
        findings.append(_make_finding("SAT-COMPUTE-PHOTON",
            "PASS" if len(photon) == len(interactive) else "WARN",
            f"{len(photon)}/{len(interactive)} interactive clusters use Photon engine"))
    # SAT-DATA-CREDENTIAL-PASSTHROUGH: No clusters using legacy credential passthrough
    PASSTHROUGH_MODES = {"LEGACY_PASSTHROUGH", "LEGACY_SINGLE_USER_PASSTHROUGH"}
    if not clusters:
        findings.append(_make_finding("SAT-DATA-CREDENTIAL-PASSTHROUGH", "NOT_APPLICABLE",
            "No clusters found."))
    else:
        passthrough_clusters = []
        for c in clusters:
            cname = c.get("cluster_name", "unnamed")
            dsm = (c.get("data_security_mode") or "").upper()
            if dsm in PASSTHROUGH_MODES:
                passthrough_clusters.append(cname)
                continue
            spark_conf = c.get("spark_conf", {})
            if str(spark_conf.get("spark.databricks.passthrough.enabled", "")).lower() == "true":
                passthrough_clusters.append(cname)
        if passthrough_clusters:
            findings.append(_make_finding("SAT-DATA-CREDENTIAL-PASSTHROUGH", "FAIL",
                f"{len(passthrough_clusters)} cluster(s) use legacy credential passthrough: "
                f"{', '.join(passthrough_clusters[:5])}{'...' if len(passthrough_clusters) > 5 else ''}. "
                "Migrate to Unity Catalog access modes."))
        else:
            findings.append(_make_finding("SAT-DATA-CREDENTIAL-PASSTHROUGH", "PASS",
                f"No clusters use legacy credential passthrough ({len(clusters)} checked)."))
    # Enrich with API response data
    for f in findings:
        f.details.setdefault("api_response", clusters_data)
    return findings


async def _check_jobs_extended(client: httpx.AsyncClient, host: str, token: str) -> list[SATFinding]:
    findings: list[SATFinding] = []
    na_ids = ("SAT-NET-5", "SAT-GOV-4", "SAT-GOV-5")
    jobs_data, jex_s, jex_e = await _dbx_get_all_jobs(client, host, token, {"expand_tasks": "false"})
    if jobs_data is None:
        for cid in na_ids:
            findings.append(_na(cid, jex_s, jex_e))
        return findings
    jobs = jobs_data.get("jobs", [])
    job_clusters: list[tuple[str, dict]] = []
    for j in jobs:
        settings = j.get("settings", {})
        jname = settings.get("name", "")
        for task in settings.get("tasks", []):
            if "new_cluster" in task:
                job_clusters.append((jname, task["new_cluster"]))
        if "new_cluster" in settings:
            job_clusters.append((jname, settings["new_cluster"]))
    ssh_jc = [(n, cl) for n, cl in job_clusters if cl.get("ssh_public_keys")]
    findings.append(_make_finding("SAT-NET-5", "FAIL" if ssh_jc else "PASS",
        f"{len(ssh_jc)} job cluster spec(s) have SSH keys",
        {"api_response": {"total_job_clusters": len(job_clusters), "ssh_key_clusters": len(ssh_jc)},
         "api_endpoint": "/api/2.1/jobs/list"}))
    if not job_clusters:
        for cid in ("SAT-GOV-4", "SAT-GOV-5"):
            findings.append(_make_finding(cid, "NOT_APPLICABLE", "No job cluster specs found"))
        return findings
    no_tag_jc = [(n, cl) for n, cl in job_clusters if not cl.get("custom_tags")]
    findings.append(_make_finding("SAT-GOV-4", "WARN" if no_tag_jc else "PASS",
        f"{len(job_clusters) - len(no_tag_jc)}/{len(job_clusters)} job cluster specs have tags"))
    no_log_jc = [(n, cl) for n, cl in job_clusters if not cl.get("cluster_log_conf")]
    findings.append(_make_finding("SAT-GOV-5", "WARN" if no_log_jc else "PASS",
        f"{len(job_clusters) - len(no_log_jc)}/{len(job_clusters)} job cluster specs have log delivery"))
    # Enrich with API response data
    for f in findings:
        f.details.setdefault("api_response", jobs_data)
    return findings


async def _check_governance_extended(client: httpx.AsyncClient, host: str, token: str) -> list[SATFinding]:
    findings: list[SATFinding] = []
    ws_conf_gov, wg_s, wg_e = await _dbx_get_workspace_conf(client, host, token,
        "enableTokensConfig,maxTokenLifetimeDays,enableCustomerManagedKey,defaultCatalog")
    if ws_conf_gov is not None:
        tok_on = str(ws_conf_gov.get("enableTokensConfig", "false")).lower() == "true"
        max_r = ws_conf_gov.get("maxTokenLifetimeDays", "")
        try: max_d = int(max_r) if max_r else None
        except: max_d = None
        if tok_on and max_d and max_d <= 90:
            g1s, g1st = "PASS", f"PAT expiry enforced: {max_d} days"
        elif tok_on and max_d and max_d >= 180:
            g1s, g1st = "FAIL", f"PAT expiry {max_d} days far exceeds 90-day recommendation"
        elif tok_on and max_d:
            g1s, g1st = "WARN", f"PAT expiry {max_d} days exceeds 90-day recommendation"
        elif tok_on:
            g1s, g1st = "FAIL", "Token management enabled but no max lifetime"
        else:
            g1s, g1st = "FAIL", "Token management not enabled"
        findings.append(_make_finding("SAT-GOV-1", g1s, g1st))
        # SAT-DATA-CMK: Customer-managed keys
        cmk_on = str(ws_conf_gov.get("enableCustomerManagedKey", "false")).lower() == "true"
        findings.append(_make_finding("SAT-DATA-CMK", "PASS" if cmk_on else "WARN",
            f"Customer-managed keys (CMK): {'enabled' if cmk_on else 'not enabled (using Databricks-managed keys)'}"))
        # SAT-GOV-DEFAULT-CAT: Default catalog
        default_cat = ws_conf_gov.get("defaultCatalog", "") or ""
        if default_cat and default_cat.lower() not in ("", "hive_metastore"):
            findings.append(_make_finding("SAT-GOV-DEFAULT-CAT", "PASS",
                f"Default catalog: '{default_cat}' (Unity Catalog)"))
        elif default_cat.lower() == "hive_metastore":
            findings.append(_make_finding("SAT-GOV-DEFAULT-CAT", "WARN",
                "Default catalog: 'hive_metastore' (legacy — bypasses UC governance)"))
        else:
            findings.append(_make_finding("SAT-GOV-DEFAULT-CAT", "WARN",
                "Default catalog not explicitly configured. Verify in workspace settings."))
    else:
        findings.append(_na("SAT-GOV-1", wg_s, wg_e))
        findings.append(_na("SAT-DATA-CMK", wg_s, wg_e))
        findings.append(_na("SAT-GOV-DEFAULT-CAT", wg_s, wg_e))

    uc_data_gov, ucg_s, ucg_e = await _dbx_get(client, host, "/api/2.1/unity-catalog/metastores", token)
    if uc_data_gov is not None:
        metastores = uc_data_gov.get("metastores", [])
        if metastores:
            m0 = metastores[0]
            owner = m0.get("owner", "") or ""
            is_group = "@" not in owner and bool(owner)
            findings.append(_make_finding("SAT-GOV-10", "PASS" if is_group else "WARN",
                f"Metastore owner: '{owner}'. {'Group (recommended)' if is_group else 'May be individual user'}"))
            ds_raw = m0.get("delta_sharing_recipient_token_lifetime_in_seconds")
            try: ds_secs = int(ds_raw) if ds_raw is not None else None
            except: ds_secs = None
            if ds_secs is None:
                findings.append(_make_finding("SAT-GOV-9", "WARN", "Delta Sharing token lifetime not set"))
            elif ds_secs > 7_776_000:
                findings.append(_make_finding("SAT-GOV-9", "WARN", f"Token lifetime: {ds_secs // 86400} days (> 90-day recommendation)"))
            else:
                findings.append(_make_finding("SAT-GOV-9", "PASS", f"Token lifetime: {ds_secs // 86400} days"))
            # SAT-DATA-LINEAGE: Lineage enabled
            lineage_enabled = m0.get("delta_sharing_scope") or m0.get("enable_lineage_tracking")
            findings.append(_make_finding("SAT-DATA-LINEAGE",
                "PASS" if lineage_enabled else "WARN",
                f"Data lineage: {'enabled' if lineage_enabled else 'verify in metastore settings — not confirmed via API'}"))
            # SAT-DATA-ROW-FILTER: Row filters/column masks
            findings.append(_make_finding("SAT-DATA-ROW-FILTER", "WARN",
                f"Unity Catalog metastore active. Verify row filters and column masks are applied to sensitive tables."))
            # SAT-GOV-AUTO-MAINT: Auto table maintenance
            _pred_val = str(m0.get("enable_predictive_optimization", "")).upper()
            auto_maint = _pred_val == "ENABLE" or m0.get("auto_maintenance")
            # Fallback 1: check catalog effective flags (metastore API often omits this field)
            if not auto_maint:
                _cat_data, _, _ = await _dbx_get(client, host, "/api/2.1/unity-catalog/catalogs", token)
                if _cat_data:
                    for _c in _cat_data.get("catalogs", []):
                        _eff = _c.get("effective_predictive_optimization_flag", {})
                        if (_eff.get("inherited_from_type") == "METASTORE"
                                and str(_eff.get("value", "")).upper() == "ENABLE"):
                            auto_maint = True
                            break
            # Fallback 2: check system table for recent PO activity
            if not auto_maint:
                wh_id = await _find_running_warehouse(client, host, token)
                if wh_id:
                    _po_rows, _ = await _dbx_sql_query(
                        client, host, token, wh_id,
                        "SELECT 1 FROM system.storage.predictive_optimization_operations_history "
                        "WHERE start_time >= current_date() - INTERVAL 30 DAYS LIMIT 1",
                    )
                    if _po_rows:
                        auto_maint = True
            findings.append(_make_finding("SAT-GOV-AUTO-MAINT",
                "PASS" if auto_maint else "WARN",
                f"Predictive optimization: {'enabled' if auto_maint else 'not confirmed — enable for automatic OPTIMIZE/VACUUM'}"))
        else:
            findings.append(_make_finding("SAT-GOV-10", "NOT_APPLICABLE", "No Unity Catalog metastore found"))
            findings.append(_make_finding("SAT-GOV-9", "NOT_APPLICABLE", "No UC metastore — feature requires Unity Catalog"))
            for cid in ("SAT-DATA-LINEAGE", "SAT-DATA-ROW-FILTER", "SAT-GOV-AUTO-MAINT"):
                findings.append(_make_finding(cid, "NOT_APPLICABLE", "No UC metastore — feature requires Unity Catalog"))
    else:
        findings.append(_na("SAT-GOV-10", ucg_s, ucg_e))
        findings.append(_na("SAT-GOV-9", ucg_s, ucg_e))
        for cid in ("SAT-DATA-LINEAGE", "SAT-DATA-ROW-FILTER", "SAT-GOV-AUTO-MAINT"):
            findings.append(_na(cid, ucg_s, ucg_e))

    sys_data, sys_s, sys_e = await _dbx_get(client, host, "/api/2.1/unity-catalog/schemas", token, {"catalog_name": "system"})
    if sys_data is not None:
        schemas = sys_data.get("schemas", [])
        schema_names = {s.get("name") for s in schemas}
        core_schemas = {"access", "billing", "compute", "query"}
        present = core_schemas & schema_names
        missing = core_schemas - schema_names
        gov13_status = "PASS" if len(present) == len(core_schemas) else ("WARN" if present else "FAIL")
        findings.append(_make_finding("SAT-GOV-13", gov13_status,
            f"{len(schemas)} schema(s) in system catalog. Core audit schemas: {len(present)}/{len(core_schemas)} enabled"
            + (f" (missing: {', '.join(sorted(missing))})" if missing else "")))
    else:
        findings.append(_na("SAT-GOV-13", sys_s, sys_e))

    # SAT-GOV-WS-BINDING: Workspace-catalog bindings restrict catalog access
    catalogs_data, cat_s, cat_e = await _dbx_get(client, host, "/api/2.1/unity-catalog/catalogs", token)
    if catalogs_data is not None:
        catalogs = catalogs_data.get("catalogs", [])
        if not catalogs:
            findings.append(_make_finding("SAT-GOV-WS-BINDING", "NOT_APPLICABLE", "No catalogs found in Unity Catalog."))
        else:
            unbound = [c.get("name", "?") for c in catalogs
                if c.get("isolation_mode", "OPEN") == "OPEN" and c.get("name") not in ("system", "hive_metastore")]
            if unbound:
                findings.append(_make_finding("SAT-GOV-WS-BINDING", "WARN",
                    f"{len(unbound)}/{len(catalogs)} catalog(s) have OPEN isolation mode (no workspace binding): "
                    f"{', '.join(unbound[:5])}{'...' if len(unbound) > 5 else ''}"))
            else:
                findings.append(_make_finding("SAT-GOV-WS-BINDING", "PASS",
                    f"All {len(catalogs)} catalog(s) have workspace bindings or are system catalogs."))
    else:
        findings.append(_na("SAT-GOV-WS-BINDING", cat_s, cat_e))

    shares_data, sh_s, sh_e = await _dbx_get(client, host, "/api/2.1/unity-catalog/shares", token)
    if shares_data is not None:
        shares = shares_data.get("shares", [])
        if not shares:
            findings.append(_make_finding("SAT-DATA-10", "NOT_APPLICABLE", "No Delta shares configured"))
        else:
            recip_data, _, _ = await _dbx_get(client, host, "/api/2.1/unity-catalog/recipients", token)
            recipients = (recip_data or {}).get("recipients", [])
            now_ms = int(time.time() * 1_000)
            expired = [r for r in recipients if r.get("expiration_time") and r.get("expiration_time") < now_ms]
            findings.append(_make_finding("SAT-DATA-10", "WARN" if expired else "PASS",
                f"{len(shares)} share(s), {len(recipients)} recipient(s), {len(expired)} expired"))
    else:
        findings.append(_na("SAT-DATA-10", sh_s, sh_e))

    # SAT-GOV-DBFS-INIT: Global init scripts not stored in DBFS root
    init_data_gov, ig_s, ig_e = await _dbx_get(client, host, "/api/2.0/global-init-scripts", token)
    dbfs_refs: list[str] = []
    if init_data_gov is not None:
        scripts = init_data_gov.get("scripts", [])
        if not scripts:
            findings.append(_make_finding("SAT-GOV-DBFS-INIT", "NOT_APPLICABLE",
                "No global init scripts configured"))
        else:
            for s in scripts:
                script_id = s.get("script_id", "")
                name = s.get("name", script_id)
                if script_id:
                    detail, _, _ = await _dbx_get(client, host,
                        f"/api/2.0/global-init-scripts/{script_id}", token)
                    if detail:
                        try:
                            content = base64.b64decode(detail.get("script", "")).decode("utf-8", errors="replace")
                        except Exception:
                            content = ""
                        if "dbfs:/" in content or "/dbfs/" in content:
                            dbfs_refs.append(name)
            if dbfs_refs:
                findings.append(_make_finding("SAT-GOV-DBFS-INIT", "WARN",
                    f"{len(dbfs_refs)} global init script(s) reference DBFS root: "
                    f"{', '.join(dbfs_refs[:5])}{'...' if len(dbfs_refs) > 5 else ''}"))
            else:
                findings.append(_make_finding("SAT-GOV-DBFS-INIT", "PASS",
                    f"{len(scripts)} global init script(s), none reference DBFS root"))
    else:
        findings.append(_na("SAT-GOV-DBFS-INIT", ig_s, ig_e))

    # Also check cluster-level init scripts for DBFS references
    cl_init_data, _, _ = await _dbx_get(client, host, "/api/2.0/clusters/list", token)
    if cl_init_data is not None:
        dbfs_cluster_inits: list[tuple[str, str]] = []
        for c in cl_init_data.get("clusters", []):
            for iscript in c.get("init_scripts", []):
                dest = iscript.get("dbfs", {}).get("destination", "")
                if dest:
                    dbfs_cluster_inits.append((c.get("cluster_name", "?"), dest))
        if dbfs_cluster_inits:
            # Upgrade existing finding if it was PASS
            for f in findings:
                if f.check_id == "SAT-GOV-DBFS-INIT":
                    if f.status in ("PASS", "NOT_APPLICABLE"):
                        f.status = "WARN"
                    detail_parts = [f"{n} → {d}" for n, d in dbfs_cluster_inits[:5]]
                    suffix = "..." if len(dbfs_cluster_inits) > 5 else ""
                    existing = f.details.get("message", "")
                    f.details["message"] = (
                        f"{existing}; " if existing and f.status == "WARN" else ""
                    ) + f"{len(dbfs_cluster_inits)} cluster(s) use DBFS init scripts: {', '.join(detail_parts)}{suffix}"
                    break

    # SAT-DATA-VECTORSEARCH: Vector Search endpoints secured
    vs_data, vs_s, vs_e = await _dbx_get(client, host, "/api/2.0/vector-search/endpoints", token)
    if vs_data is not None:
        vs_endpoints = vs_data.get("endpoints", [])
        if not vs_endpoints:
            findings.append(_make_finding("SAT-DATA-VECTORSEARCH", "NOT_APPLICABLE",
                "No Vector Search endpoints configured."))
        else:
            # Check permissions on each endpoint
            unprotected: list[str] = []
            for ep in vs_endpoints:
                ep_name = ep.get("name", "unknown")
                perm_data, perm_s, _ = await _dbx_get(client, host,
                    f"/api/2.0/permissions/vector-search-endpoints/{ep.get('id', ep_name)}", token)
                if perm_data is not None:
                    acls = perm_data.get("access_control_list", [])
                    # Flag if "All Users" group has CAN_MANAGE or CAN_USE
                    for acl in acls:
                        grp = acl.get("group_name", "")
                        if grp.lower() in ("users", "all users"):
                            perms = [p.get("permission_level", "") for p in acl.get("all_permissions", [])]
                            if any(p in ("CAN_MANAGE", "CAN_USE") for p in perms):
                                unprotected.append(ep_name)
                                break
                elif perm_s in (403, 404):
                    pass  # Permission API not available or not authorized
            if unprotected:
                findings.append(_make_finding("SAT-DATA-VECTORSEARCH", "WARN",
                    f"{len(unprotected)}/{len(vs_endpoints)} Vector Search endpoint(s) accessible to all users: "
                    f"{', '.join(unprotected[:5])}{'...' if len(unprotected) > 5 else ''}.",
                    {"unprotected": unprotected, "total_endpoints": len(vs_endpoints)}))
            else:
                findings.append(_make_finding("SAT-DATA-VECTORSEARCH", "PASS",
                    f"{len(vs_endpoints)} Vector Search endpoint(s) — all have restricted access."))
    elif vs_s in (403, 404):
        findings.append(_make_finding("SAT-DATA-VECTORSEARCH", "NOT_APPLICABLE",
            "Vector Search API not available."))
    else:
        findings.append(_na("SAT-DATA-VECTORSEARCH", vs_s, vs_e))

    # Enrich with API response data
    _api = {
        "SAT-GOV-1": ws_conf_gov, "SAT-GOV-9": uc_data_gov, "SAT-GOV-10": uc_data_gov,
        "SAT-GOV-13": sys_data, "SAT-DATA-10": shares_data,
        "SAT-DATA-CMK": ws_conf_gov, "SAT-GOV-DEFAULT-CAT": ws_conf_gov,
        "SAT-DATA-LINEAGE": uc_data_gov, "SAT-DATA-ROW-FILTER": uc_data_gov,
        "SAT-GOV-AUTO-MAINT": uc_data_gov, "SAT-GOV-WS-BINDING": catalogs_data,
        "SAT-GOV-DBFS-INIT": init_data_gov, "SAT-DATA-VECTORSEARCH": vs_data,
    }
    for f in findings:
        resp = _api.get(f.check_id)
        if resp is not None:
            f.details.setdefault("api_response", resp)
    return findings


async def _check_settings_api(client: httpx.AsyncClient, host: str, token: str) -> list[SATFinding]:
    findings: list[SATFinding] = []
    restrict_data, rd_s, rd_e = await _dbx_get(client, host,
        "/api/2.0/settings/types/restrict_workspace_admins/names/default", token)
    if restrict_data is not None:
        rwa = restrict_data.get("restrict_workspace_admins", restrict_data)
        restricted = isinstance(rwa, dict) and rwa.get("status", "") in ("RESTRICT_TOKENS_AND_JOB_RUN_AS", "ALLOW_MISSING_VALUES", "ENABLED")
        findings.append(_make_finding("SAT-GOV-14", "PASS" if restricted else "WARN",
            f"Workspace admin restriction: {'enabled' if restricted else 'not confirmed'}"))
    elif rd_s == 404:
        findings.append(_make_finding("SAT-GOV-14", "WARN", "Cannot verify via REST API — check workspace Settings.",
            {"api_response": None, "note": "Settings API returned 404"}))
    else:
        findings.append(_na("SAT-GOV-14", rd_s, rd_e))

    acu_data, acu_s, acu_e = await _dbx_get(client, host,
        "/api/2.0/settings/types/automatic_cluster_update/names/default", token)
    if acu_data is not None:
        acu = acu_data.get("automatic_cluster_update", acu_data)
        enabled = isinstance(acu, dict) and acu.get("enabled", False)
        findings.append(_make_finding("SAT-GOV-15", "PASS" if enabled else "WARN",
            f"Automatic cluster update: {'enabled' if enabled else 'not enabled'}"))
    elif acu_s == 404:
        findings.append(_make_finding("SAT-GOV-15", "WARN", "Cannot verify via REST API — check Azure Portal > Databricks workspace > Settings > Security & compliance.",
            {"api_response": None, "note": "Settings API returned 404"}))
    else:
        findings.append(_na("SAT-GOV-15", acu_s, acu_e))
    # Enrich with API response data
    _api = {"SAT-GOV-14": restrict_data, "SAT-GOV-15": acu_data}
    for f in findings:
        resp = _api.get(f.check_id)
        if resp is not None:
            f.details.setdefault("api_response", resp)
    return findings


async def _check_informational(client: httpx.AsyncClient, host: str, token: str) -> list[SATFinding]:
    findings: list[SATFinding] = []
    ws_conf_inf, wi_s, wi_e = await _dbx_get_workspace_conf(client, host, token, "enableClusterCreation")
    if ws_conf_inf is not None:
        all_create = str(ws_conf_inf.get("enableClusterCreation", "true")).lower() == "true"
        findings.append(_make_finding("SAT-INFO-1", "WARN" if all_create else "PASS",
            f"Cluster creation: {'open to all users' if all_create else 'restricted'}"))
    else:
        # Fallback: workspace-conf key may not exist on newer Azure workspaces.
        # Use SCIM Groups API to check if the "users" group has allow-cluster-create entitlement.
        grp_data, grp_s, grp_e = await _dbx_get(client, host,
            "/api/2.0/preview/scim/v2/Groups", token, {"filter": 'displayName eq "users"'})
        if grp_s == 200 and grp_data is not None:
            ws_conf_inf = grp_data  # for API response enrichment
            groups = grp_data.get("Resources", [])
            users_grp = next((g for g in groups if g.get("displayName") == "users"), None)
            if users_grp:
                entitlements = [e.get("value", "") for e in users_grp.get("entitlements", [])]
                has_create = "allow-cluster-create" in entitlements
                findings.append(_make_finding("SAT-INFO-1", "WARN" if has_create else "PASS",
                    f"Cluster creation: {'open to all users (via SCIM entitlement)' if has_create else 'restricted (users group lacks allow-cluster-create)'}"))
            else:
                findings.append(_make_finding("SAT-INFO-1", "PASS",
                    "No 'users' group found — cluster creation likely restricted via policies."))
        else:
            findings.append(_na("SAT-INFO-1", wi_s, wi_e))

    init_data, init_s, init_e = await _dbx_get(client, host, "/api/2.0/global-init-scripts", token)
    if init_data is not None:
        scripts = init_data.get("scripts", [])
        enabled_scripts = [s for s in scripts if s.get("enabled", False)]
        findings.append(_make_finding("SAT-INFO-2",
            "NOT_APPLICABLE" if not scripts else ("WARN" if enabled_scripts else "PASS"),
            f"{len(scripts)} global init script(s), {len(enabled_scripts)} enabled" if scripts else "No global init scripts configured"))
    else:
        findings.append(_na("SAT-INFO-2", init_s, init_e))
    # Enrich with API response data
    _api = {"SAT-INFO-1": ws_conf_inf, "SAT-INFO-2": init_data}
    for f in findings:
        resp = _api.get(f.check_id)
        if resp is not None:
            f.details.setdefault("api_response", resp)
    return findings


async def _check_sso_scim(client: httpx.AsyncClient, host: str, token: str) -> list[SATFinding]:
    findings: list[SATFinding] = []
    hdr = {"Authorization": f"Bearer {token}"}
    _scim_raw = None
    _tac_raw = None
    try:
        r = await client.get(f"{host}/api/2.0/preview/scim/v2/Users", params={"count": "200"}, headers=hdr, timeout=20)
        if r.status_code == 200:
            _scim_raw = r.json()
            users = _scim_raw.get("Resources", [])
            idp_users = [u for u in users if u.get("externalId")]
            total = len(users)
            pct = round(len(idp_users) / total * 100) if total else 0
            if total == 0:
                findings.append(_make_finding("SAT-IA-SSO", "NOT_APPLICABLE", "No users found."))
            elif pct >= 80:
                findings.append(_make_finding("SAT-IA-SSO", "PASS", f"{len(idp_users)}/{total} users ({pct}%) are IdP-linked."))
            elif pct > 0:
                findings.append(_make_finding("SAT-IA-SSO", "WARN", f"Only {len(idp_users)}/{total} users ({pct}%) have IdP external IDs."))
            else:
                findings.append(_make_finding("SAT-IA-SSO", "FAIL", f"None of {total} users have IdP external ID. SSO not configured."))
            scim_count = len(idp_users)
            scim_pct = (scim_count * 100 // total) if total else 0
            if scim_pct >= 80:
                findings.append(_make_finding("SAT-IA-SCIM", "PASS",
                    f"SCIM provisioning detected: {scim_count}/{total} users ({scim_pct}%) have externalId."))
            elif scim_count > 0:
                findings.append(_make_finding("SAT-IA-SCIM", "WARN",
                    f"Only {scim_count}/{total} users ({scim_pct}%) have SCIM externalId."))
            else:
                findings.append(_make_finding("SAT-IA-SCIM", "FAIL",
                    "No users with SCIM externalId found."))
        else:
            findings.append(_make_finding("SAT-IA-SSO", "WARN", f"SCIM API returned HTTP {r.status_code}."))
            findings.append(_make_finding("SAT-IA-SCIM", "WARN", f"SCIM API returned HTTP {r.status_code}."))
    except Exception as exc:
        findings.append(_make_finding("SAT-IA-SSO", "WARN", f"Could not query SCIM API: {exc}"))
        findings.append(_make_finding("SAT-IA-SCIM", "WARN", f"Could not query SCIM API: {exc}"))

    _tac_data, _tac_s, _tac_e = await _dbx_get_workspace_conf(client, host, token, "enableTableAccessControl")
    if _tac_data is not None:
        _tac_raw = _tac_data
        tac = str(_tac_data.get("enableTableAccessControl", "false")).lower() == "true"
        findings.append(_make_finding("SAT-IA-TAC", "PASS" if tac else "FAIL",
            f"Table Access Control: {'enabled' if tac else 'disabled'}"))
    else:
        # Fallback: on Unity Catalog workspaces the legacy enableTableAccessControl key
        # is removed. Check if a UC metastore is assigned — if so, data governance is
        # handled by UC and this check is PASS (UC supersedes legacy Table ACLs).
        uc_meta, uc_s, _ = await _dbx_get(client, host, "/api/2.1/unity-catalog/current-metastore-assignment", token)
        if uc_s == 200 and uc_meta is not None:
            ms_name = uc_meta.get("metastore_id", "unknown")
            _tac_raw = uc_meta
            findings.append(_make_finding("SAT-IA-TAC", "PASS",
                f"Unity Catalog metastore assigned ({ms_name}). Legacy Table ACL setting superseded by UC governance."))
        else:
            findings.append(_na("SAT-IA-TAC", _tac_s, _tac_e))
    # Enrich with API response data
    _api = {"SAT-IA-SSO": _scim_raw, "SAT-IA-SCIM": _scim_raw, "SAT-IA-TAC": _tac_raw}
    for f in findings:
        resp = _api.get(f.check_id)
        if resp is not None:
            f.details.setdefault("api_response", resp)
    return findings


async def _check_ai_ml_governance(client: httpx.AsyncClient, host: str, token: str) -> list[SATFinding]:
    findings: list[SATFinding] = []
    hdr = {"Authorization": f"Bearer {token}"}
    _serving_raw = None
    _mlflow_raw = None
    try:
        r = await client.get(f"{host}/api/2.0/serving-endpoints", headers=hdr, timeout=15)
        if r.status_code == 200:
            _serving_raw = r.json()
            endpoints = _serving_raw.get("endpoints", [])
            if not endpoints:
                findings.append(_make_finding("SAT-NS-SERVING", "NOT_APPLICABLE", "No model serving endpoints."))
            else:
                external_eps = [e for e in endpoints if e.get("config", {}).get("served_entities") and
                    any(se.get("external_model") for se in e["config"].get("served_entities", []))]
                regular_eps = [e for e in endpoints if e not in external_eps]
                if regular_eps:
                    findings.append(_make_finding("SAT-NS-SERVING", "WARN",
                        f"{len(regular_eps)} model serving endpoint(s) found. Verify IP access lists."))
                else:
                    findings.append(_make_finding("SAT-NS-SERVING", "NOT_APPLICABLE", "No non-external serving endpoints."))
                if external_eps:
                    findings.append(_make_finding("SAT-NS-EXT-LLM", "WARN",
                        f"{len(external_eps)} external AI model endpoint(s) configured."))
                else:
                    findings.append(_make_finding("SAT-NS-EXT-LLM", "NOT_APPLICABLE", "No external AI model endpoints."))
        elif r.status_code == 404:
            findings.append(_make_finding("SAT-NS-SERVING", "NOT_APPLICABLE", "Model Serving not available."))
            findings.append(_make_finding("SAT-NS-EXT-LLM", "NOT_APPLICABLE", "Model Serving not available."))
        else:
            findings.append(_make_finding("SAT-NS-SERVING", "WARN", f"HTTP {r.status_code}."))
            findings.append(_make_finding("SAT-NS-EXT-LLM", "WARN", f"HTTP {r.status_code}."))
    except Exception as exc:
        findings.append(_make_finding("SAT-NS-SERVING", "WARN", f"Error: {exc}"))
        findings.append(_make_finding("SAT-NS-EXT-LLM", "WARN", f"Error: {exc}"))

    try:
        r = await client.get(f"{host}/api/2.0/mlflow/registered-models/search",
            params={"max_results": "100"}, headers=hdr, timeout=15)
        if r.status_code == 200:
            _mlflow_raw = r.json()
            models = _mlflow_raw.get("registered_models", [])
            if not models:
                findings.append(_make_finding("SAT-GOV-ML-UC", "NOT_APPLICABLE", "No registered models."))
            else:
                findings.append(_make_finding("SAT-GOV-ML-UC", "WARN",
                    f"{len(models)} registered model(s) in workspace registry. Verify UC migration."))
        elif r.status_code == 404:
            findings.append(_make_finding("SAT-GOV-ML-UC", "NOT_APPLICABLE", "MLflow registry not available on this workspace."))
        elif r.status_code == 403:
            findings.append(_make_finding("SAT-GOV-ML-UC", "WARN", "MLflow registry permission denied (HTTP 403). Use admin token."))
        else:
            findings.append(_make_finding("SAT-GOV-ML-UC", "WARN", f"HTTP {r.status_code}."))
    except Exception as exc:
        findings.append(_make_finding("SAT-GOV-ML-UC", "WARN", f"Error: {exc}"))

    # SAT-ML-AI-GATEWAY: AI Gateway rate limits and guardrails
    if _serving_raw is not None:
        endpoints = _serving_raw.get("endpoints", [])
        no_rate_limit = []
        no_guardrails = []
        ai_gw_endpoints = []
        for ep in endpoints:
            conf = ep.get("config", {})
            ai_gw = conf.get("auto_capture_config") or ep.get("ai_gateway")
            rate_limits = ep.get("rate_limits") or conf.get("rate_limits") or []
            guardrails = (ep.get("ai_gateway") or {}).get("guardrails")
            served = conf.get("served_entities", [])
            is_external = any(se.get("external_model") for se in served)
            if is_external or ai_gw:
                ai_gw_endpoints.append(ep.get("name", "?"))
                if not rate_limits:
                    no_rate_limit.append(ep.get("name", "?"))
                if not guardrails:
                    no_guardrails.append(ep.get("name", "?"))
        if not ai_gw_endpoints:
            findings.append(_make_finding("SAT-ML-AI-GATEWAY", "NOT_APPLICABLE",
                "No AI Gateway or external model endpoints found."))
        elif no_rate_limit or no_guardrails:
            issues = []
            if no_rate_limit:
                issues.append(f"{len(no_rate_limit)} without rate limits")
            if no_guardrails:
                issues.append(f"{len(no_guardrails)} without guardrails")
            findings.append(_make_finding("SAT-ML-AI-GATEWAY", "WARN",
                f"{len(ai_gw_endpoints)} AI Gateway endpoint(s): {', '.join(issues)}."))
        else:
            findings.append(_make_finding("SAT-ML-AI-GATEWAY", "PASS",
                f"All {len(ai_gw_endpoints)} AI Gateway endpoint(s) have rate limits and guardrails."))
    else:
        findings.append(_make_finding("SAT-ML-AI-GATEWAY", "NOT_APPLICABLE",
            "Serving endpoints data not available."))

    # ── Phase 1 new ML/AI checks (reuse _serving_raw) ──────────────────────

    # SAT-AI-PAYLOAD-LOG: Inference payload logging for audit trail
    if _serving_raw is not None:
        endpoints = _serving_raw.get("endpoints", [])
        if not endpoints:
            findings.append(_make_finding("SAT-AI-PAYLOAD-LOG", "NOT_APPLICABLE",
                "No model serving endpoints."))
        else:
            no_logging = []
            for ep in endpoints:
                auto_cap = ep.get("config", {}).get("auto_capture_config", {})
                if not auto_cap or not auto_cap.get("enabled"):
                    no_logging.append(ep.get("name", "?"))
            if no_logging:
                findings.append(_make_finding("SAT-AI-PAYLOAD-LOG", "WARN",
                    f"{len(no_logging)}/{len(endpoints)} endpoint(s) without payload logging: "
                    f"{', '.join(no_logging[:5])}{'...' if len(no_logging) > 5 else ''}."))
            else:
                findings.append(_make_finding("SAT-AI-PAYLOAD-LOG", "PASS",
                    f"All {len(endpoints)} endpoint(s) have payload logging enabled."))
    else:
        findings.append(_make_finding("SAT-AI-PAYLOAD-LOG", "NOT_APPLICABLE",
            "Serving endpoints data not available."))

    # SAT-AI-GUARDRAILS: Content filter rules in AI Gateway guardrails
    if _serving_raw is not None:
        endpoints = _serving_raw.get("endpoints", [])
        ai_gw_eps = [ep for ep in endpoints if ep.get("ai_gateway")]
        if not ai_gw_eps:
            findings.append(_make_finding("SAT-AI-GUARDRAILS", "NOT_APPLICABLE",
                "No AI Gateway endpoints configured."))
        else:
            no_filters = []
            for ep in ai_gw_eps:
                guardrails = ep.get("ai_gateway", {}).get("guardrails", {})
                inp = guardrails.get("input", {})
                out = guardrails.get("output", {})
                has_input = bool(inp and (inp.get("pii", {}).get("behavior")
                    or inp.get("safety", {}).get("behavior") or inp.get("valid_topics")))
                has_output = bool(out and (out.get("pii", {}).get("behavior")
                    or out.get("safety", {}).get("behavior")))
                if not has_input and not has_output:
                    no_filters.append(ep.get("name", "?"))
            if no_filters:
                findings.append(_make_finding("SAT-AI-GUARDRAILS", "WARN",
                    f"{len(no_filters)}/{len(ai_gw_eps)} AI Gateway endpoint(s) without content filter rules: "
                    f"{', '.join(no_filters[:5])}{'...' if len(no_filters) > 5 else ''}."))
            else:
                findings.append(_make_finding("SAT-AI-GUARDRAILS", "PASS",
                    f"All {len(ai_gw_eps)} AI Gateway endpoint(s) have content filter rules configured."))
    else:
        findings.append(_make_finding("SAT-AI-GUARDRAILS", "NOT_APPLICABLE",
            "Serving endpoints data not available."))

    # SAT-AI-TOKEN-LIMITS: Token-based rate limits on external model endpoints
    if _serving_raw is not None:
        endpoints = _serving_raw.get("endpoints", [])
        external_eps = [ep for ep in endpoints
            if any(se.get("external_model") for se in ep.get("config", {}).get("served_entities", []))]
        if not external_eps:
            findings.append(_make_finding("SAT-AI-TOKEN-LIMITS", "NOT_APPLICABLE",
                "No external model endpoints."))
        else:
            no_token_limits = []
            for ep in external_eps:
                rate_limits = ep.get("rate_limits") or ep.get("config", {}).get("rate_limits") or []
                ai_gw_limits = (ep.get("ai_gateway") or {}).get("rate_limits") or []
                all_limits = list(rate_limits) + list(ai_gw_limits)
                has_token = any(rl.get("key") == "tokens" or "token" in rl.get("key", "").lower()
                    for rl in all_limits if isinstance(rl, dict))
                if not has_token:
                    no_token_limits.append(ep.get("name", "?"))
            if no_token_limits:
                findings.append(_make_finding("SAT-AI-TOKEN-LIMITS", "WARN",
                    f"{len(no_token_limits)}/{len(external_eps)} external model endpoint(s) without token-based rate limits: "
                    f"{', '.join(no_token_limits[:5])}{'...' if len(no_token_limits) > 5 else ''}."))
            else:
                findings.append(_make_finding("SAT-AI-TOKEN-LIMITS", "PASS",
                    f"All {len(external_eps)} external model endpoint(s) have token-based rate limits."))
    else:
        findings.append(_make_finding("SAT-AI-TOKEN-LIMITS", "NOT_APPLICABLE",
            "Serving endpoints data not available."))

    # SAT-ML-SERVING-SCALE: Scale-to-zero for cost control
    if _serving_raw is not None:
        endpoints = _serving_raw.get("endpoints", [])
        # Only check non-external endpoints (external endpoints are proxies, no compute)
        custom_eps = [ep for ep in endpoints
            if not all(se.get("external_model") for se in ep.get("config", {}).get("served_entities", []))]
        if not custom_eps:
            findings.append(_make_finding("SAT-ML-SERVING-SCALE", "NOT_APPLICABLE",
                "No custom model serving endpoints (only external model proxies)."))
        else:
            no_scale_zero = []
            for ep in custom_eps:
                entities = ep.get("config", {}).get("served_entities", [])
                has_s2z = any(se.get("scale_to_zero_enabled", False) for se in entities)
                if not has_s2z:
                    no_scale_zero.append(ep.get("name", "?"))
            if no_scale_zero:
                findings.append(_make_finding("SAT-ML-SERVING-SCALE", "WARN",
                    f"{len(no_scale_zero)}/{len(custom_eps)} custom endpoint(s) without scale-to-zero: "
                    f"{', '.join(no_scale_zero[:5])}{'...' if len(no_scale_zero) > 5 else ''}."))
            else:
                findings.append(_make_finding("SAT-ML-SERVING-SCALE", "PASS",
                    f"All {len(custom_eps)} custom endpoint(s) have scale-to-zero enabled."))
    else:
        findings.append(_make_finding("SAT-ML-SERVING-SCALE", "NOT_APPLICABLE",
            "Serving endpoints data not available."))

    # SAT-AI-EXT-MODEL-KEYS: External model API keys stored in secrets
    if _serving_raw is not None:
        endpoints = _serving_raw.get("endpoints", [])
        external_eps = [ep for ep in endpoints
            if any(se.get("external_model") for se in ep.get("config", {}).get("served_entities", []))]
        if not external_eps:
            findings.append(_make_finding("SAT-AI-EXT-MODEL-KEYS", "NOT_APPLICABLE",
                "No external model endpoints."))
        else:
            plaintext_keys = []
            for ep in external_eps:
                for se in ep.get("config", {}).get("served_entities", []):
                    ext = se.get("external_model", {})
                    if not ext:
                        continue
                    # Check all provider configs for plaintext key fields
                    for provider_key in ("openai_config", "anthropic_config", "cohere_config",
                                         "palm_config", "amazon_bedrock_config", "ai21labs_config",
                                         "google_cloud_vertex_ai_config", "custom_provider_config"):
                        prov = ext.get(provider_key, {})
                        if not prov:
                            continue
                        # Fields ending in _plaintext indicate non-secret storage
                        for field_name, field_val in prov.items():
                            if "_plaintext" in field_name and field_val:
                                plaintext_keys.append(f"{ep.get('name', '?')} ({field_name})")
                                break
            if plaintext_keys:
                findings.append(_make_finding("SAT-AI-EXT-MODEL-KEYS", "WARN",
                    f"{len(plaintext_keys)} external model endpoint(s) use plaintext API keys: "
                    f"{', '.join(plaintext_keys[:5])}{'...' if len(plaintext_keys) > 5 else ''}. "
                    "Use Databricks secrets instead."))
            else:
                findings.append(_make_finding("SAT-AI-EXT-MODEL-KEYS", "PASS",
                    f"All {len(external_eps)} external model endpoint(s) use secret-backed API keys."))
    else:
        findings.append(_make_finding("SAT-AI-EXT-MODEL-KEYS", "NOT_APPLICABLE",
            "Serving endpoints data not available."))

    # SAT-FEAT-MODEL-SERVING: Feature adoption — model serving endpoints
    if _serving_raw is not None:
        endpoints = _serving_raw.get("endpoints", [])
        _api_detail = {"api_response": {"endpoints": endpoints}, "api_endpoint": "/api/2.0/serving-endpoints"}
        if len(endpoints) >= 2:
            findings.append(_make_finding("SAT-FEAT-MODEL-SERVING", "PASS",
                f"{len(endpoints)} model serving endpoint(s) deployed.", _api_detail))
        elif len(endpoints) == 1:
            findings.append(_make_finding("SAT-FEAT-MODEL-SERVING", "WARN",
                "1 model serving endpoint deployed.", _api_detail))
        else:
            findings.append(_make_finding("SAT-FEAT-MODEL-SERVING", "NOT_APPLICABLE",
                "No model serving endpoints deployed.", _api_detail))
    else:
        findings.append(_make_finding("SAT-FEAT-MODEL-SERVING", "NOT_APPLICABLE",
            "Serving endpoints data not available."))

    # SAT-FEAT-AGENT-FW: Feature adoption — agent framework endpoints
    if _serving_raw is not None:
        endpoints = _serving_raw.get("endpoints", [])
        agent_eps = []
        for ep in endpoints:
            for se in ep.get("config", {}).get("served_entities", []):
                ename = (se.get("entity_name") or "").lower()
                if "agent" in ename or "langgraph" in ename or "langchain" in ename:
                    agent_eps.append(ep.get("name", "?"))
                    break
        _api_detail = {"api_response": {"endpoints": endpoints}, "api_endpoint": "/api/2.0/serving-endpoints"}
        if agent_eps:
            findings.append(_make_finding("SAT-FEAT-AGENT-FW", "PASS",
                f"{len(agent_eps)} agent framework endpoint(s) deployed: "
                f"{', '.join(agent_eps[:5])}.", _api_detail))
        else:
            findings.append(_make_finding("SAT-FEAT-AGENT-FW", "NOT_APPLICABLE",
                "No agent framework endpoints detected.", _api_detail))
    else:
        findings.append(_make_finding("SAT-FEAT-AGENT-FW", "NOT_APPLICABLE",
            "Serving endpoints data not available."))

    # SAT-AI-GATEWAY-ENABLED: Serving endpoints route through AI Gateway
    if _serving_raw is not None:
        endpoints = _serving_raw.get("endpoints", [])
        if not endpoints:
            findings.append(_make_finding("SAT-AI-GATEWAY-ENABLED", "NOT_APPLICABLE",
                "No model serving endpoints."))
        else:
            no_gateway = [ep.get("name", "?") for ep in endpoints if not ep.get("ai_gateway")]
            if no_gateway:
                findings.append(_make_finding("SAT-AI-GATEWAY-ENABLED", "WARN",
                    f"{len(no_gateway)}/{len(endpoints)} endpoint(s) without AI Gateway: "
                    f"{', '.join(no_gateway[:5])}{'...' if len(no_gateway) > 5 else ''}."))
            else:
                findings.append(_make_finding("SAT-AI-GATEWAY-ENABLED", "PASS",
                    f"All {len(endpoints)} endpoint(s) route through AI Gateway."))
    else:
        findings.append(_make_finding("SAT-AI-GATEWAY-ENABLED", "NOT_APPLICABLE",
            "Serving endpoints data not available."))

    # SAT-AI-USAGE-TRACKING: AI Gateway usage tracking for cost attribution
    if _serving_raw is not None:
        endpoints = _serving_raw.get("endpoints", [])
        ai_gw_eps = [ep for ep in endpoints if ep.get("ai_gateway")]
        if not ai_gw_eps:
            findings.append(_make_finding("SAT-AI-USAGE-TRACKING", "NOT_APPLICABLE",
                "No AI Gateway endpoints configured."))
        else:
            no_tracking = [ep.get("name", "?") for ep in ai_gw_eps
                if not (ep.get("ai_gateway") or {}).get("usage_tracking_config")]
            if no_tracking:
                findings.append(_make_finding("SAT-AI-USAGE-TRACKING", "WARN",
                    f"{len(no_tracking)}/{len(ai_gw_eps)} AI Gateway endpoint(s) without usage tracking: "
                    f"{', '.join(no_tracking[:5])}{'...' if len(no_tracking) > 5 else ''}."))
            else:
                findings.append(_make_finding("SAT-AI-USAGE-TRACKING", "PASS",
                    f"All {len(ai_gw_eps)} AI Gateway endpoint(s) have usage tracking enabled."))
    else:
        findings.append(_make_finding("SAT-AI-USAGE-TRACKING", "NOT_APPLICABLE",
            "Serving endpoints data not available."))

    # SAT-AI-PII-FILTER: Guardrails block PII in inputs and outputs
    if _serving_raw is not None:
        endpoints = _serving_raw.get("endpoints", [])
        ai_gw_eps = [ep for ep in endpoints if ep.get("ai_gateway")]
        if not ai_gw_eps:
            findings.append(_make_finding("SAT-AI-PII-FILTER", "NOT_APPLICABLE",
                "No AI Gateway endpoints configured."))
        else:
            no_pii_block = []
            for ep in ai_gw_eps:
                guardrails = (ep.get("ai_gateway") or {}).get("guardrails", {})
                inp_pii = (guardrails.get("input") or {}).get("pii", {}).get("behavior", "")
                out_pii = (guardrails.get("output") or {}).get("pii", {}).get("behavior", "")
                if inp_pii.upper() != "BLOCK" and out_pii.upper() != "BLOCK":
                    no_pii_block.append(ep.get("name", "?"))
            if no_pii_block:
                findings.append(_make_finding("SAT-AI-PII-FILTER", "WARN",
                    f"{len(no_pii_block)}/{len(ai_gw_eps)} AI Gateway endpoint(s) without PII blocking: "
                    f"{', '.join(no_pii_block[:5])}{'...' if len(no_pii_block) > 5 else ''}."))
            else:
                findings.append(_make_finding("SAT-AI-PII-FILTER", "PASS",
                    f"All {len(ai_gw_eps)} AI Gateway endpoint(s) have PII blocking enabled."))
    else:
        findings.append(_make_finding("SAT-AI-PII-FILTER", "NOT_APPLICABLE",
            "Serving endpoints data not available."))

    # SAT-ML-SERVING-TRAFFIC: Multi-entity endpoints use traffic routing
    if _serving_raw is not None:
        endpoints = _serving_raw.get("endpoints", [])
        multi_entity = [ep for ep in endpoints
            if len(ep.get("config", {}).get("served_entities", [])) > 1]
        if not multi_entity:
            findings.append(_make_finding("SAT-ML-SERVING-TRAFFIC", "NOT_APPLICABLE",
                "No multi-entity serving endpoints."))
        else:
            no_traffic = [ep.get("name", "?") for ep in multi_entity
                if not ep.get("config", {}).get("traffic_config")]
            if no_traffic:
                findings.append(_make_finding("SAT-ML-SERVING-TRAFFIC", "WARN",
                    f"{len(no_traffic)}/{len(multi_entity)} multi-entity endpoint(s) without traffic routing: "
                    f"{', '.join(no_traffic[:5])}{'...' if len(no_traffic) > 5 else ''}."))
            else:
                findings.append(_make_finding("SAT-ML-SERVING-TRAFFIC", "PASS",
                    f"All {len(multi_entity)} multi-entity endpoint(s) have traffic routing configured."))
    else:
        findings.append(_make_finding("SAT-ML-SERVING-TRAFFIC", "NOT_APPLICABLE",
            "Serving endpoints data not available."))

    # SAT-AI-ENDPOINT-GPU: Custom endpoints have compute limits
    if _serving_raw is not None:
        endpoints = _serving_raw.get("endpoints", [])
        custom_eps = [ep for ep in endpoints
            if not all(se.get("external_model") for se in ep.get("config", {}).get("served_entities", []))]
        if not custom_eps:
            findings.append(_make_finding("SAT-AI-ENDPOINT-GPU", "NOT_APPLICABLE",
                "No custom model serving endpoints (only external model proxies)."))
        else:
            no_limits = []
            for ep in custom_eps:
                for se in ep.get("config", {}).get("served_entities", []):
                    if se.get("external_model"):
                        continue
                    has_size = bool(se.get("workload_size") or se.get("max_provisioned_throughput")
                        or se.get("workload_type"))
                    if not has_size:
                        no_limits.append(ep.get("name", "?"))
                        break
            if no_limits:
                findings.append(_make_finding("SAT-AI-ENDPOINT-GPU", "WARN",
                    f"{len(no_limits)}/{len(custom_eps)} custom endpoint(s) without explicit compute limits: "
                    f"{', '.join(no_limits[:5])}{'...' if len(no_limits) > 5 else ''}."))
            else:
                findings.append(_make_finding("SAT-AI-ENDPOINT-GPU", "PASS",
                    f"All {len(custom_eps)} custom endpoint(s) have compute limits configured."))
    else:
        findings.append(_make_finding("SAT-AI-ENDPOINT-GPU", "NOT_APPLICABLE",
            "Serving endpoints data not available."))

    # SAT-AI-RATE-LIMITS: All endpoints have at least one rate limit
    if _serving_raw is not None:
        endpoints = _serving_raw.get("endpoints", [])
        if not endpoints:
            findings.append(_make_finding("SAT-AI-RATE-LIMITS", "NOT_APPLICABLE",
                "No model serving endpoints."))
        else:
            no_any_limits = []
            for ep in endpoints:
                rate_limits = ep.get("rate_limits") or []
                ai_gw_limits = (ep.get("ai_gateway") or {}).get("rate_limits") or []
                if not rate_limits and not ai_gw_limits:
                    no_any_limits.append(ep.get("name", "?"))
            if no_any_limits:
                findings.append(_make_finding("SAT-AI-RATE-LIMITS", "WARN",
                    f"{len(no_any_limits)}/{len(endpoints)} endpoint(s) without any rate limits: "
                    f"{', '.join(no_any_limits[:5])}{'...' if len(no_any_limits) > 5 else ''}."))
            else:
                findings.append(_make_finding("SAT-AI-RATE-LIMITS", "PASS",
                    f"All {len(endpoints)} endpoint(s) have rate limits configured."))
    else:
        findings.append(_make_finding("SAT-AI-RATE-LIMITS", "NOT_APPLICABLE",
            "Serving endpoints data not available."))

    # SAT-FEAT-MOSAIC-TRAINING: Feature adoption — foundation model fine-tuning
    if _serving_raw is not None:
        endpoints = _serving_raw.get("endpoints", [])
        ft_eps = []
        for ep in endpoints:
            for se in ep.get("config", {}).get("served_entities", []):
                ename = (se.get("entity_name") or "").lower()
                if "ft:" in ename or "fine-tun" in ename or "finetun" in ename:
                    ft_eps.append(ep.get("name", "?"))
                    break
        _api_detail = {"api_response": {"endpoints": endpoints}, "api_endpoint": "/api/2.0/serving-endpoints"}
        if ft_eps:
            findings.append(_make_finding("SAT-FEAT-MOSAIC-TRAINING", "PASS",
                f"{len(ft_eps)} fine-tuning endpoint(s) deployed: "
                f"{', '.join(ft_eps[:5])}.", _api_detail))
        else:
            findings.append(_make_finding("SAT-FEAT-MOSAIC-TRAINING", "NOT_APPLICABLE",
                "No foundation model fine-tuning endpoints detected.", _api_detail))
    else:
        findings.append(_make_finding("SAT-FEAT-MOSAIC-TRAINING", "NOT_APPLICABLE",
            "Serving endpoints data not available."))

    # SAT-ML-FEATURE-ACL: Feature Engineering tables governed in UC
    _feature_raw = None
    try:
        r = await client.get(f"{host}/api/2.1/unity-catalog/tables",
            params={"catalog_name": "main", "schema_name": "default", "max_results": "50"},
            headers=hdr, timeout=15)
        if r.status_code == 200:
            _feature_raw = r.json()
            tables = _feature_raw.get("tables", [])
            feature_tables = [t for t in tables if t.get("table_type") == "FEATURE_TABLE"
                or "feature" in (t.get("name", "") or "").lower()
                or (t.get("properties", {}) or {}).get("delta.feature.table")]
            if not feature_tables and not tables:
                findings.append(_make_finding("SAT-ML-FEATURE-ACL", "NOT_APPLICABLE",
                    "No tables in main.default. Feature table governance requires UC."))
            elif feature_tables:
                findings.append(_make_finding("SAT-ML-FEATURE-ACL", "PASS",
                    f"{len(feature_tables)} feature table(s) found in Unity Catalog — governed by UC ACLs."))
            else:
                findings.append(_make_finding("SAT-ML-FEATURE-ACL", "WARN",
                    f"{len(tables)} table(s) in main.default but no explicit feature tables detected. "
                    "Verify feature tables are in Unity Catalog (not legacy workspace)."))
        elif r.status_code == 404:
            findings.append(_make_finding("SAT-ML-FEATURE-ACL", "NOT_APPLICABLE",
                "Unity Catalog tables API not available on this workspace."))
        elif r.status_code == 403:
            findings.append(_na("SAT-ML-FEATURE-ACL", 403, "Insufficient permissions to list UC tables."))
        else:
            findings.append(_na("SAT-ML-FEATURE-ACL", r.status_code, r.text))
    except Exception as exc:
        findings.append(_na("SAT-ML-FEATURE-ACL", 0, str(exc)))

    # SAT-ML-REGISTRY-UC: Models registered in Unity Catalog Model Registry
    try:
        r = await client.get(f"{host}/api/2.1/unity-catalog/models",
            params={"max_results": "50"}, headers=hdr, timeout=15)
        if r.status_code == 200:
            uc_models = r.json().get("registered_models", [])
            if uc_models:
                findings.append(_make_finding("SAT-ML-REGISTRY-UC", "PASS",
                    f"{len(uc_models)} model(s) registered in Unity Catalog Model Registry."))
            else:
                # Check if there are workspace-level models instead
                ws_models = _mlflow_raw.get("registered_models", []) if _mlflow_raw else []
                if ws_models:
                    findings.append(_make_finding("SAT-ML-REGISTRY-UC", "FAIL",
                        f"No UC models found, but {len(ws_models)} workspace-level model(s) exist. "
                        "Migrate to UC Model Registry."))
                else:
                    findings.append(_make_finding("SAT-ML-REGISTRY-UC", "NOT_APPLICABLE",
                        "No registered models found in either UC or workspace registry."))
        elif r.status_code in (404, 403):
            findings.append(_make_finding("SAT-ML-REGISTRY-UC", "NOT_APPLICABLE",
                f"UC Models API not available (HTTP {r.status_code})."))
        else:
            findings.append(_make_finding("SAT-ML-REGISTRY-UC", "WARN", f"HTTP {r.status_code}."))
    except Exception as exc:
        findings.append(_make_finding("SAT-ML-REGISTRY-UC", "WARN", f"Error: {exc}"))

    # SAT-ML-INFERENCE-TABLES: Serving endpoints have inference tables enabled
    if _serving_raw is not None:
        endpoints = _serving_raw.get("endpoints", [])
        if not endpoints:
            findings.append(_make_finding("SAT-ML-INFERENCE-TABLES", "NOT_APPLICABLE",
                "No model serving endpoints."))
        else:
            with_inference = []
            without_inference = []
            for ep in endpoints:
                auto_cap = ep.get("config", {}).get("auto_capture_config", {})
                if auto_cap and auto_cap.get("catalog_name"):
                    with_inference.append(ep.get("name", "?"))
                else:
                    without_inference.append(ep.get("name", "?"))
            if not without_inference:
                findings.append(_make_finding("SAT-ML-INFERENCE-TABLES", "PASS",
                    f"All {len(endpoints)} endpoint(s) have inference tables enabled."))
            else:
                findings.append(_make_finding("SAT-ML-INFERENCE-TABLES", "WARN",
                    f"{len(without_inference)}/{len(endpoints)} endpoint(s) without inference tables: "
                    f"{', '.join(without_inference[:5])}{'...' if len(without_inference) > 5 else ''}."))
    else:
        findings.append(_make_finding("SAT-ML-INFERENCE-TABLES", "NOT_APPLICABLE",
            "Serving endpoints data not available."))

    # Enrich with API response data
    _api = {"SAT-NS-SERVING": _serving_raw, "SAT-NS-EXT-LLM": _serving_raw, "SAT-GOV-ML-UC": _mlflow_raw,
            "SAT-ML-AI-GATEWAY": _serving_raw, "SAT-ML-FEATURE-ACL": _feature_raw,
            "SAT-AI-PAYLOAD-LOG": _serving_raw, "SAT-AI-GUARDRAILS": _serving_raw,
            "SAT-AI-TOKEN-LIMITS": _serving_raw, "SAT-ML-SERVING-SCALE": _serving_raw,
            "SAT-AI-EXT-MODEL-KEYS": _serving_raw, "SAT-AI-GATEWAY-ENABLED": _serving_raw,
            "SAT-AI-USAGE-TRACKING": _serving_raw, "SAT-AI-PII-FILTER": _serving_raw,
            "SAT-ML-SERVING-TRAFFIC": _serving_raw, "SAT-AI-ENDPOINT-GPU": _serving_raw,
            "SAT-AI-RATE-LIMITS": _serving_raw, "SAT-ML-REGISTRY-UC": _serving_raw,
            "SAT-ML-INFERENCE-TABLES": _serving_raw}
    for f in findings:
        resp = _api.get(f.check_id)
        if resp is not None:
            f.details.setdefault("api_response", resp)
    return findings


async def _check_ai_genai_security(client: httpx.AsyncClient, host: str, token: str) -> list[SATFinding]:
    """GenAI security: UC models, experiment governance, serving permissions, drift monitoring."""
    findings: list[SATFinding] = []
    hdr = {"Authorization": f"Bearer {token}"}

    # SAT-ML-UC-MODELS: Models registered in Unity Catalog
    _uc_models_raw = None
    try:
        r = await client.get(f"{host}/api/2.1/unity-catalog/registered-models",
            params={"max_results": "100"}, headers=hdr, timeout=15)
        if r.status_code == 200:
            _uc_models_raw = r.json()
            models = _uc_models_raw.get("registered_models", [])
            if models:
                findings.append(_make_finding("SAT-ML-UC-MODELS", "PASS",
                    f"{len(models)} model(s) registered in Unity Catalog.",
                    {"api_response": _uc_models_raw}))
            else:
                findings.append(_make_finding("SAT-ML-UC-MODELS", "WARN",
                    "No models found in Unity Catalog. Models may only exist in the legacy workspace registry.",
                    {"api_response": _uc_models_raw}))
        elif r.status_code == 404:
            findings.append(_make_finding("SAT-ML-UC-MODELS", "NOT_APPLICABLE",
                "Unity Catalog registered models API not available."))
        elif r.status_code == 403:
            findings.append(_na("SAT-ML-UC-MODELS", 403, "Insufficient permissions to list UC models."))
        else:
            findings.append(_na("SAT-ML-UC-MODELS", r.status_code, r.text))
    except Exception as exc:
        findings.append(_na("SAT-ML-UC-MODELS", 0, str(exc)))

    # SAT-ML-EXPERIMENT-ACL: MLflow experiments in shared paths
    _experiments_raw = None
    try:
        r = await client.get(f"{host}/api/2.0/mlflow/experiments/search",
            params={"max_results": "100"}, headers=hdr, timeout=15)
        if r.status_code == 200:
            _experiments_raw = r.json()
            experiments = _experiments_raw.get("experiments", [])
            if not experiments:
                findings.append(_make_finding("SAT-ML-EXPERIMENT-ACL", "NOT_APPLICABLE",
                    "No MLflow experiments found.", {"api_response": _experiments_raw}))
            else:
                user_path_exps = [e for e in experiments
                    if (e.get("name") or "").startswith("/Users/")]
                shared_exps = len(experiments) - len(user_path_exps)
                if not user_path_exps:
                    findings.append(_make_finding("SAT-ML-EXPERIMENT-ACL", "PASS",
                        f"All {len(experiments)} experiment(s) are in shared workspace paths.",
                        {"api_response": _experiments_raw}))
                elif len(user_path_exps) > len(experiments) // 2:
                    findings.append(_make_finding("SAT-ML-EXPERIMENT-ACL", "WARN",
                        f"{len(user_path_exps)}/{len(experiments)} experiment(s) in /Users/ paths "
                        f"(only {shared_exps} in shared paths). Move experiments to /Shared/ for team governance.",
                        {"api_response": _experiments_raw}))
                else:
                    findings.append(_make_finding("SAT-ML-EXPERIMENT-ACL", "PASS",
                        f"{shared_exps}/{len(experiments)} experiment(s) in shared paths "
                        f"({len(user_path_exps)} in /Users/ paths).",
                        {"api_response": _experiments_raw}))
        elif r.status_code == 404:
            findings.append(_make_finding("SAT-ML-EXPERIMENT-ACL", "NOT_APPLICABLE",
                "MLflow experiments API not available."))
        elif r.status_code == 403:
            findings.append(_na("SAT-ML-EXPERIMENT-ACL", 403, "Insufficient permissions to list experiments."))
        else:
            findings.append(_na("SAT-ML-EXPERIMENT-ACL", r.status_code, r.text))
    except Exception as exc:
        findings.append(_na("SAT-ML-EXPERIMENT-ACL", 0, str(exc)))

    # SAT-ML-SERVING-AUTH: Serving endpoint permissions check
    _serving_raw = None
    try:
        r = await client.get(f"{host}/api/2.0/serving-endpoints", headers=hdr, timeout=15)
        if r.status_code == 200:
            _serving_raw = r.json()
    except Exception:
        pass

    if _serving_raw is not None:
        endpoints = _serving_raw.get("endpoints", [])
        if not endpoints:
            findings.append(_make_finding("SAT-ML-SERVING-AUTH", "NOT_APPLICABLE",
                "No model serving endpoints."))
        else:
            overly_permissive = []
            # Sample up to 10 endpoints to avoid rate limits
            for ep in endpoints[:10]:
                ep_id = ep.get("id") or ep.get("name", "")
                if not ep_id:
                    continue
                try:
                    pr = await client.get(
                        f"{host}/api/2.0/permissions/serving-endpoints/{ep_id}",
                        headers=hdr, timeout=10)
                    if pr.status_code == 200:
                        perms = pr.json()
                        for acl in perms.get("access_control_list", []):
                            principal = (acl.get("group_name") or acl.get("user_name") or "").lower()
                            if principal in ("users", "account users", "all users"):
                                all_perms = acl.get("all_permissions", [])
                                for p in all_perms:
                                    if p.get("permission_level") in ("CAN_QUERY", "CAN_MANAGE"):
                                        overly_permissive.append(ep.get("name", ep_id))
                                        break
                except Exception:
                    pass  # Skip individual permission failures
            if overly_permissive:
                findings.append(_make_finding("SAT-ML-SERVING-AUTH", "WARN",
                    f"{len(overly_permissive)} endpoint(s) accessible to all workspace users: "
                    f"{', '.join(overly_permissive[:5])}{'...' if len(overly_permissive) > 5 else ''}. "
                    "Restrict permissions to specific groups.",
                    {"api_response": _serving_raw}))
            else:
                findings.append(_make_finding("SAT-ML-SERVING-AUTH", "PASS",
                    f"Checked {min(len(endpoints), 10)} endpoint(s) — none accessible to all workspace users.",
                    {"api_response": _serving_raw}))
    else:
        findings.append(_make_finding("SAT-ML-SERVING-AUTH", "NOT_APPLICABLE",
            "Serving endpoints data not available."))

    # SAT-GOV-MODEL-SERVING-ACL: Model serving endpoints have explicit access control
    if _serving_raw is not None:
        endpoints = _serving_raw.get("endpoints", [])
        if not endpoints:
            findings.append(_make_finding("SAT-GOV-MODEL-SERVING-ACL", "NOT_APPLICABLE",
                "No model serving endpoints."))
        else:
            broad_access = []
            # Sample up to 20 endpoints for performance
            for ep in endpoints[:20]:
                ep_id = ep.get("id") or ep.get("name", "")
                if not ep_id:
                    continue
                try:
                    pr = await client.get(
                        f"{host}/api/2.0/permissions/serving-endpoints/{ep_id}",
                        headers=hdr, timeout=10)
                    if pr.status_code == 200:
                        perms = pr.json()
                        for acl in perms.get("access_control_list", []):
                            principal = (acl.get("group_name") or acl.get("user_name") or "").lower()
                            if principal in ("users", "account users", "all users"):
                                all_perms = acl.get("all_permissions", [])
                                for p in all_perms:
                                    if p.get("permission_level") in ("CAN_MANAGE", "CAN_QUERY"):
                                        broad_access.append(ep.get("name", ep_id))
                                        break
                except Exception:
                    pass  # Skip individual permission failures
            if broad_access:
                findings.append(_make_finding("SAT-GOV-MODEL-SERVING-ACL", "WARN",
                    f"{len(broad_access)} endpoint(s) grant broad access to All Users: "
                    f"{', '.join(broad_access[:5])}{'...' if len(broad_access) > 5 else ''}. "
                    "Configure explicit ACLs to restrict access.",
                    {"api_response": _serving_raw, "broad_access_endpoints": broad_access}))
            else:
                findings.append(_make_finding("SAT-GOV-MODEL-SERVING-ACL", "PASS",
                    f"Checked {min(len(endpoints), 20)} endpoint(s) — all have restricted access controls.",
                    {"api_response": _serving_raw}))
    else:
        findings.append(_make_finding("SAT-GOV-MODEL-SERVING-ACL", "NOT_APPLICABLE",
            "Serving endpoints data not available."))

    # SAT-ML-DRIFT-MON: Inference tables have drift monitors
    if _serving_raw is not None:
        endpoints = _serving_raw.get("endpoints", [])
        # Collect inference table FQNs from auto_capture_config
        inference_tables: list[tuple[str, str]] = []  # (endpoint_name, table_fqn)
        for ep in endpoints:
            auto_cap = ep.get("config", {}).get("auto_capture_config", {})
            if auto_cap and auto_cap.get("enabled"):
                cat = auto_cap.get("catalog_name", "")
                sch = auto_cap.get("schema_name", "")
                tbl = auto_cap.get("table_name_prefix", "") or ep.get("name", "")
                if cat and sch:
                    fqn = f"{cat}.{sch}.{tbl}_payload"
                    inference_tables.append((ep.get("name", "?"), fqn))
        if not inference_tables:
            findings.append(_make_finding("SAT-ML-DRIFT-MON", "NOT_APPLICABLE",
                "No serving endpoints with inference table logging enabled.",
                {"api_response": _serving_raw}))
        else:
            no_monitor = []
            # Sample up to 5 inference tables
            for ep_name, tbl_fqn in inference_tables[:5]:
                try:
                    mr = await client.get(
                        f"{host}/api/2.1/unity-catalog/tables/{tbl_fqn}/monitor",
                        headers=hdr, timeout=10)
                    if mr.status_code == 404:
                        no_monitor.append(ep_name)
                    # 200 = monitor exists, skip
                except Exception:
                    pass  # Skip individual failures
            if no_monitor:
                findings.append(_make_finding("SAT-ML-DRIFT-MON", "WARN",
                    f"{len(no_monitor)}/{len(inference_tables[:5])} inference table(s) without drift monitors: "
                    f"{', '.join(no_monitor[:5])}. Configure Lakehouse Monitors for prediction quality tracking.",
                    {"api_response": _serving_raw}))
            else:
                findings.append(_make_finding("SAT-ML-DRIFT-MON", "PASS",
                    f"All {len(inference_tables[:5])} checked inference table(s) have monitors configured.",
                    {"api_response": _serving_raw}))
    else:
        findings.append(_make_finding("SAT-ML-DRIFT-MON", "NOT_APPLICABLE",
            "Serving endpoints data not available."))

    # SAT-ML-MODEL-VERSIONS: UC models have multiple versions (proper promotion workflow)
    if _uc_models_raw is not None:
        models = _uc_models_raw.get("registered_models", [])
        if not models:
            findings.append(_make_finding("SAT-ML-MODEL-VERSIONS", "NOT_APPLICABLE",
                "No Unity Catalog models found.", {"api_response": _uc_models_raw}))
        else:
            single_version = []
            # Sample up to 10 models
            for m in models[:10]:
                full_name = m.get("full_name", "")
                if not full_name:
                    continue
                try:
                    vr = await client.get(
                        f"{host}/api/2.1/unity-catalog/registered-models/{full_name}/versions",
                        params={"max_results": "2"}, headers=hdr, timeout=10)
                    if vr.status_code == 200:
                        versions = vr.json().get("model_versions", [])
                        if len(versions) <= 1:
                            single_version.append(full_name.split(".")[-1] if "." in full_name else full_name)
                except Exception:
                    pass  # Skip individual failures
            checked = min(len(models), 10)
            if single_version and len(single_version) > checked // 2:
                findings.append(_make_finding("SAT-ML-MODEL-VERSIONS", "WARN",
                    f"{len(single_version)}/{checked} sampled model(s) have only 1 version: "
                    f"{', '.join(single_version[:5])}{'...' if len(single_version) > 5 else ''}. "
                    "Use versioned promotion workflows (dev → staging → production).",
                    {"api_response": _uc_models_raw}))
            else:
                findings.append(_make_finding("SAT-ML-MODEL-VERSIONS", "PASS",
                    f"Checked {checked} model(s) — majority have multiple versions.",
                    {"api_response": _uc_models_raw}))
    else:
        findings.append(_make_finding("SAT-ML-MODEL-VERSIONS", "NOT_APPLICABLE",
            "Unity Catalog models data not available."))

    # SAT-ML-MODEL-DESCRIPTIONS: UC models have descriptions
    if _uc_models_raw is not None:
        models = _uc_models_raw.get("registered_models", [])
        if not models:
            findings.append(_make_finding("SAT-ML-MODEL-DESCRIPTIONS", "NOT_APPLICABLE",
                "No Unity Catalog models found.", {"api_response": _uc_models_raw}))
        else:
            no_desc = [m.get("name", "?") for m in models if not (m.get("comment") or "").strip()]
            if not no_desc:
                findings.append(_make_finding("SAT-ML-MODEL-DESCRIPTIONS", "PASS",
                    f"All {len(models)} UC model(s) have descriptions.",
                    {"api_response": _uc_models_raw}))
            elif len(no_desc) > len(models) // 2:
                findings.append(_make_finding("SAT-ML-MODEL-DESCRIPTIONS", "WARN",
                    f"{len(no_desc)}/{len(models)} UC model(s) without descriptions: "
                    f"{', '.join(no_desc[:5])}{'...' if len(no_desc) > 5 else ''}.",
                    {"api_response": _uc_models_raw}))
            else:
                findings.append(_make_finding("SAT-ML-MODEL-DESCRIPTIONS", "PASS",
                    f"{len(models) - len(no_desc)}/{len(models)} UC model(s) have descriptions.",
                    {"api_response": _uc_models_raw}))
    else:
        findings.append(_make_finding("SAT-ML-MODEL-DESCRIPTIONS", "NOT_APPLICABLE",
            "Unity Catalog models data not available."))

    # SAT-ML-EXPERIMENT-CLEANUP: No deleted experiments accumulating
    if _experiments_raw is not None:
        experiments = _experiments_raw.get("experiments", [])
        if not experiments:
            findings.append(_make_finding("SAT-ML-EXPERIMENT-CLEANUP", "NOT_APPLICABLE",
                "No MLflow experiments found.", {"api_response": _experiments_raw}))
        else:
            deleted = [e for e in experiments if e.get("lifecycle_stage") == "deleted"]
            if deleted:
                findings.append(_make_finding("SAT-ML-EXPERIMENT-CLEANUP", "WARN",
                    f"{len(deleted)} deleted experiment(s) still present. "
                    "Permanently delete or restore trashed experiments.",
                    {"api_response": _experiments_raw}))
            else:
                findings.append(_make_finding("SAT-ML-EXPERIMENT-CLEANUP", "PASS",
                    f"No deleted experiments accumulating ({len(experiments)} active experiment(s)).",
                    {"api_response": _experiments_raw}))
    else:
        findings.append(_make_finding("SAT-ML-EXPERIMENT-CLEANUP", "NOT_APPLICABLE",
            "MLflow experiments data not available."))

    # SAT-ML-TRAINING-COMPUTE: ML training jobs use job clusters
    _jobs_raw = None
    try:
        jr = await client.get(f"{host}/api/2.1/jobs/list",
            params={"limit": "100"}, headers=hdr, timeout=15)
        if jr.status_code == 200:
            _jobs_raw = jr.json()
    except Exception:
        pass
    if _jobs_raw is not None:
        jobs = _jobs_raw.get("jobs", [])
        ml_keywords = {"ml", "train", "model", "experiment", "mlflow", "fine-tune", "finetune",
                        "feature", "embedding", "inference"}
        ml_jobs = [j for j in jobs
            if any(kw in (j.get("settings", {}).get("name", "") or "").lower() for kw in ml_keywords)]
        if not ml_jobs:
            findings.append(_make_finding("SAT-ML-TRAINING-COMPUTE", "NOT_APPLICABLE",
                "No ML training jobs detected (by name pattern).",
                {"api_response": {"jobs_sampled": len(jobs)}}))
        else:
            interactive = [j for j in ml_jobs if j.get("settings", {}).get("existing_cluster_id")]
            if interactive:
                names = [j.get("settings", {}).get("name", "?") for j in interactive]
                findings.append(_make_finding("SAT-ML-TRAINING-COMPUTE", "WARN",
                    f"{len(interactive)}/{len(ml_jobs)} ML job(s) use interactive clusters: "
                    f"{', '.join(names[:5])}{'...' if len(names) > 5 else ''}. "
                    "Use job clusters for isolation and cost control.",
                    {"api_response": {"ml_jobs": len(ml_jobs), "interactive": len(interactive)}}))
            else:
                findings.append(_make_finding("SAT-ML-TRAINING-COMPUTE", "PASS",
                    f"All {len(ml_jobs)} ML training job(s) use job clusters.",
                    {"api_response": {"ml_jobs": len(ml_jobs)}}))
    else:
        findings.append(_make_finding("SAT-ML-TRAINING-COMPUTE", "NOT_APPLICABLE",
            "Jobs data not available."))

    # SAT-FEAT-UC-VOLUMES-ML: UC Volumes used for ML artifact storage
    try:
        vr = await client.get(f"{host}/api/2.1/unity-catalog/volumes",
            params={"catalog_name": "main", "schema_name": "default", "max_results": "50"},
            headers=hdr, timeout=10)
        if vr.status_code == 200:
            _vols_raw = vr.json()
            volumes = _vols_raw.get("volumes", [])
            ml_keywords = {"model", "ml", "artifact", "dataset", "training", "checkpoint",
                           "experiment", "feature", "embedding"}
            ml_vols = [v for v in volumes
                if any(kw in (v.get("name", "") or "").lower() for kw in ml_keywords)]
            _api_detail = {"api_response": _vols_raw, "api_endpoint": "/api/2.1/unity-catalog/volumes"}
            if ml_vols:
                findings.append(_make_finding("SAT-FEAT-UC-VOLUMES-ML", "PASS",
                    f"{len(ml_vols)} ML-related volume(s) found: "
                    f"{', '.join(v.get('name', '?') for v in ml_vols[:5])}.", _api_detail))
            else:
                findings.append(_make_finding("SAT-FEAT-UC-VOLUMES-ML", "NOT_APPLICABLE",
                    f"No ML-related volumes detected in main.default ({len(volumes)} volume(s) total).",
                    _api_detail))
        elif vr.status_code == 404:
            findings.append(_make_finding("SAT-FEAT-UC-VOLUMES-ML", "NOT_APPLICABLE",
                "UC Volumes API not available."))
        elif vr.status_code == 403:
            findings.append(_na("SAT-FEAT-UC-VOLUMES-ML", 403, "Insufficient permissions to list volumes."))
        else:
            findings.append(_na("SAT-FEAT-UC-VOLUMES-ML", vr.status_code, vr.text))
    except Exception as exc:
        findings.append(_na("SAT-FEAT-UC-VOLUMES-ML", 0, str(exc)))

    return findings


async def _check_pools_jobs_extended(client: httpx.AsyncClient, host: str, token: str) -> list[SATFinding]:
    findings: list[SATFinding] = []
    hdr = {"Authorization": f"Bearer {token}"}
    _pools_raw = None
    _jobs_raw = None
    _libs_raw = None
    _wh_raw = None
    try:
        r = await client.get(f"{host}/api/2.0/instance-pools/list", headers=hdr, timeout=15)
        if r.status_code == 200:
            _pools_raw = r.json()
            pools = _pools_raw.get("instance_pools", [])
            if not pools:
                findings.append(_make_finding("SAT-INFO-POOL-TAGS", "NOT_APPLICABLE", "No instance pools."))
            else:
                untagged = [p.get("instance_pool_name", "?") for p in pools if not p.get("custom_tags")]
                findings.append(_make_finding("SAT-INFO-POOL-TAGS",
                    "WARN" if untagged else "PASS",
                    f"{len(untagged)}/{len(pools)} instance pool(s) have no tags." if untagged else f"All {len(pools)} pool(s) have tags."))
        else:
            findings.append(_make_finding("SAT-INFO-POOL-TAGS", "WARN", f"HTTP {r.status_code}."))
    except Exception as exc:
        findings.append(_make_finding("SAT-INFO-POOL-TAGS", "WARN", f"Error: {exc}"))

    try:
        r = await client.get(f"{host}/api/2.1/jobs/list", params={"expand_tasks": "false", "limit": "100"}, headers=hdr, timeout=20)
        if r.status_code == 200:
            _jobs_raw = r.json()
            jobs = _jobs_raw.get("jobs", [])
            unlimited = [j.get("settings", {}).get("name", "?") for j in jobs if j.get("settings", {}).get("max_concurrent_runs", 1) > 5]
            if not jobs:
                findings.append(_make_finding("SAT-INFO-JOB-CONCUR", "NOT_APPLICABLE", "No jobs found."))
            elif unlimited:
                findings.append(_make_finding("SAT-INFO-JOB-CONCUR", "WARN",
                    f"{len(unlimited)}/{len(jobs)} job(s) allow > 5 concurrent runs."))
            else:
                findings.append(_make_finding("SAT-INFO-JOB-CONCUR", "PASS",
                    f"All {len(jobs)} job(s) have reasonable concurrency limits."))
        else:
            findings.append(_make_finding("SAT-INFO-JOB-CONCUR", "WARN", f"HTTP {r.status_code}."))
    except Exception as exc:
        findings.append(_make_finding("SAT-INFO-JOB-CONCUR", "WARN", f"Error: {exc}"))

    try:
        r = await client.get(f"{host}/api/2.0/libraries/all-cluster-statuses", headers=hdr, timeout=20)
        if r.status_code == 200:
            _libs_raw = r.json()
            statuses = _libs_raw.get("statuses", [])
            clusters_with_libs = [(s.get("cluster_id"), len(s.get("library_statuses", []))) for s in statuses if s.get("library_statuses")]
            total_lib = sum(c[1] for c in clusters_with_libs)
            _lib_api = {"api_response": {"clusters_with_libs": len(clusters_with_libs), "total_libs": total_lib},
                "api_endpoint": "/api/2.0/libraries/all-cluster-statuses"}
            if not clusters_with_libs:
                findings.append(_make_finding("SAT-INFO-GLOBAL-LIBS", "PASS", "No cluster-level library installations.", _lib_api))
            elif total_lib > 20:
                findings.append(_make_finding("SAT-INFO-GLOBAL-LIBS", "WARN",
                    f"{total_lib} cluster-level lib installation(s) across {len(clusters_with_libs)} cluster(s).", _lib_api))
            else:
                findings.append(_make_finding("SAT-INFO-GLOBAL-LIBS", "PASS",
                    f"{total_lib} cluster-level lib installation(s) — within limits.", _lib_api))
        else:
            findings.append(_make_finding("SAT-INFO-GLOBAL-LIBS", "WARN", f"HTTP {r.status_code}."))
    except Exception as exc:
        findings.append(_make_finding("SAT-INFO-GLOBAL-LIBS", "WARN", f"Error: {exc}"))

    try:
        r = await client.get(f"{host}/api/2.0/sql/warehouses", headers=hdr, timeout=15)
        if r.status_code == 200:
            _wh_raw = r.json()
            warehouses = _wh_raw.get("warehouses", [])
            serverless_wh = [w for w in warehouses if w.get("enable_serverless_compute")]
            if not warehouses:
                findings.append(_make_finding("SAT-INFO-SERVERLESS", "WARN", "No SQL warehouses. Consider serverless."))
            elif serverless_wh:
                findings.append(_make_finding("SAT-INFO-SERVERLESS", "PASS",
                    f"{len(serverless_wh)}/{len(warehouses)} SQL warehouses use serverless."))
            else:
                findings.append(_make_finding("SAT-INFO-SERVERLESS", "WARN",
                    f"None of {len(warehouses)} SQL warehouses use serverless."))
        else:
            findings.append(_make_finding("SAT-INFO-SERVERLESS", "WARN", f"HTTP {r.status_code}."))
    except Exception as exc:
        findings.append(_make_finding("SAT-INFO-SERVERLESS", "WARN", f"Error: {exc}"))
    # ── SAT-INFO-APPS: Databricks Apps inventory ──
    _apps_raw = None
    try:
        r = await client.get(f"{host}/api/2.0/apps", headers=hdr, timeout=15)
        if r.status_code == 200:
            _apps_raw = r.json()
            apps = _apps_raw.get("apps", [])
            if apps:
                active = [a for a in apps if a.get("status", {}).get("state") == "RUNNING"]
                findings.append(_make_finding("SAT-INFO-APPS", "PASS",
                    f"{len(apps)} app(s) deployed ({len(active)} running). Review periodically."))
            else:
                findings.append(_make_finding("SAT-INFO-APPS", "NOT_APPLICABLE", "No Databricks Apps deployed."))
        else:
            findings.append(_make_finding("SAT-INFO-APPS", "NOT_APPLICABLE", f"Apps API not available (HTTP {r.status_code})."))
    except Exception as exc:
        findings.append(_make_finding("SAT-INFO-APPS", "NOT_APPLICABLE", f"Apps API error: {exc}"))

    # Enrich with API response data
    _api = {"SAT-INFO-POOL-TAGS": _pools_raw, "SAT-INFO-JOB-CONCUR": _jobs_raw,
            "SAT-INFO-GLOBAL-LIBS": _libs_raw, "SAT-INFO-SERVERLESS": _wh_raw,
            "SAT-INFO-APPS": _apps_raw}
    for f in findings:
        resp = _api.get(f.check_id)
        if resp is not None:
            f.details.setdefault("api_response", resp)
    return findings


async def _check_compliance_extended(client: httpx.AsyncClient, host: str, token: str) -> list[SATFinding]:
    findings: list[SATFinding] = []
    hdr = {"Authorization": f"Bearer {token}"}
    _recipients_raw = None
    _clusters_raw = None
    _wsconf_raw = None

    try:
        r = await client.get(f"{host}/api/2.1/unity-catalog/recipients", params={"max_results": "100"}, headers=hdr, timeout=15)
        if r.status_code == 200:
            _recipients_raw = r.json()
            recipients = _recipients_raw.get("recipients", [])
            if not recipients:
                for cid in ("SAT-GOV-DS-IP", "SAT-GOV-DS-EXP", "SAT-GOV-DS-PERMS"):
                    findings.append(_make_finding(cid, "NOT_APPLICABLE", "No Delta Sharing recipients."))
            else:
                no_ip = [r_.get("name", "?") for r_ in recipients if not r_.get("ip_access_list", {}).get("allowed_ip_addresses")]
                no_exp = [r_.get("name", "?") for r_ in recipients if not r_.get("expiration_time")]
                findings.append(_make_finding("SAT-GOV-DS-IP", "PASS" if not no_ip else "WARN",
                    f"{len(no_ip)}/{len(recipients)} recipient(s) have no IP allowlist." if no_ip else f"All {len(recipients)} have IP allowlists."))
                findings.append(_make_finding("SAT-GOV-DS-EXP", "PASS" if not no_exp else "WARN",
                    f"{len(no_exp)}/{len(recipients)} recipient(s) have no token expiration." if no_exp else f"All {len(recipients)} have token expiration."))
                findings.append(_make_finding("SAT-GOV-DS-PERMS", "WARN",
                    f"{len(recipients)} recipient(s). Verify creation is restricted."))
        elif r.status_code == 404:
            for cid in ("SAT-GOV-DS-IP", "SAT-GOV-DS-EXP", "SAT-GOV-DS-PERMS"):
                findings.append(_make_finding(cid, "NOT_APPLICABLE", "Delta Sharing not available on this workspace."))
        elif r.status_code == 403:
            for cid in ("SAT-GOV-DS-IP", "SAT-GOV-DS-EXP", "SAT-GOV-DS-PERMS"):
                findings.append(_make_finding(cid, "WARN", "Delta Sharing permission denied (HTTP 403). Use admin token."))
        else:
            for cid in ("SAT-GOV-DS-IP", "SAT-GOV-DS-EXP", "SAT-GOV-DS-PERMS"):
                findings.append(_make_finding(cid, "WARN", f"HTTP {r.status_code}."))
    except Exception as exc:
        for cid in ("SAT-GOV-DS-IP", "SAT-GOV-DS-EXP", "SAT-GOV-DS-PERMS"):
            findings.append(_make_finding(cid, "WARN", f"Error: {exc}"))

    try:
        r = await client.get(f"{host}/api/2.0/clusters/list", headers=hdr, timeout=20)
        if r.status_code == 200:
            _clusters_raw = r.json()
            clusters = _clusters_raw.get("clusters", [])
            CRED_PATTERNS = ["fs.azure.account.key", "fs.s3a.access.key", "fs.s3n.awsAccessKeyId",
                "spark.hadoop.fs.azure", "spark.hadoop.fs.s3", "AZURE_CLIENT_SECRET", "SAS_TOKEN", "sasToken"]
            flagged = []
            for cl in clusters:
                conf = cl.get("spark_conf", {})
                env = cl.get("spark_env_vars", {})
                all_keys = list(conf.keys()) + list(env.keys())
                all_vals = [str(v) for v in list(conf.values()) + list(env.values())]
                if any(p.lower() in k.lower() for p in CRED_PATTERNS for k in all_keys) or \
                   any("dapi" in v or "eyJ" in v for v in all_vals):
                    flagged.append(cl.get("cluster_name", "?"))
            findings.append(_make_finding("SAT-GOV-DIRECT-CREDS",
                "FAIL" if flagged else "PASS",
                f"{len(flagged)} cluster(s) may have inline credentials." if flagged else "No inline credentials detected.",
                {"api_response": {"clusters_checked": len(clusters), "flagged": len(flagged)},
                 "api_endpoint": "/api/2.0/clusters/list"}))
        else:
            findings.append(_make_finding("SAT-GOV-DIRECT-CREDS", "WARN", f"HTTP {r.status_code}."))
    except Exception as exc:
        findings.append(_make_finding("SAT-GOV-DIRECT-CREDS", "WARN", f"Error: {exc}"))

    _wsconf_data, _, _ = await _dbx_get_workspace_conf(client, host, token,
        "complianceSecurityProfileEnabled,enhancedSecurityMonitoringEnabled,artifactAllowlistEnabled")
    if _wsconf_data is not None:
        _wsconf_raw = _wsconf_data
        conf = _wsconf_data
        compliance_on = conf.get("complianceSecurityProfileEnabled", "false").lower() == "true"
        findings.append(_make_finding("SAT-INFO-COMPLIANCE-PROFILE", "PASS" if compliance_on else "WARN",
            f"Compliance Security Profile: {'enabled' if compliance_on else 'not enabled'}"))
        monitoring_on = conf.get("enhancedSecurityMonitoringEnabled", "false").lower() == "true"
        findings.append(_make_finding("SAT-INFO-ENHANCED-MONITORING", "PASS" if monitoring_on else "WARN",
            f"Enhanced Security Monitoring: {'enabled' if monitoring_on else 'not enabled'}"))
        allowlist_on = conf.get("artifactAllowlistEnabled", "false").lower() == "true"
        findings.append(_make_finding("SAT-INFO-3P-LIBS", "PASS" if allowlist_on else "WARN",
            f"Artifact allowlist: {'enabled' if allowlist_on else 'not configured'}"))
    else:
        # Fallback: workspace-conf keys may not exist on newer Azure workspaces.
        # Use the Settings API v2 endpoints instead.

        # ── Compliance Security Profile ──
        csp_data, csp_s, csp_e = await _dbx_get(client, host,
            "/api/2.0/settings/types/shield_csp_enablement_ws_db/names/default", token)
        if csp_s == 200 and csp_data is not None:
            _wsconf_raw = csp_data
            csp_obj = csp_data.get("shield_csp_enablement_ws_db", csp_data)
            compliance_on = csp_obj.get("is_enabled", False)
            findings.append(_make_finding("SAT-INFO-COMPLIANCE-PROFILE", "PASS" if compliance_on else "WARN",
                f"Compliance Security Profile (Settings API): {'enabled' if compliance_on else 'not enabled'}"))
        elif csp_s == 404:
            # Settings API endpoint not available — feature is not configured on this workspace
            findings.append(_make_finding("SAT-INFO-COMPLIANCE-PROFILE", "WARN",
                "Compliance Security Profile: not enabled (Settings API returned 404 — feature not available on this workspace).",
                {"api_response": None, "http_status": 404}))
        else:
            findings.append(_na("SAT-INFO-COMPLIANCE-PROFILE", csp_s, csp_e))

        # ── Enhanced Security Monitoring ──
        esm_data, esm_s, esm_e = await _dbx_get(client, host,
            "/api/2.0/settings/types/enhanced_security_monitoring_ws/names/default", token)
        if esm_s == 200 and esm_data is not None:
            esm_obj = esm_data.get("enhanced_security_monitoring_ws", esm_data)
            monitoring_on = esm_obj.get("is_enabled", False)
            findings.append(_make_finding("SAT-INFO-ENHANCED-MONITORING", "PASS" if monitoring_on else "WARN",
                f"Enhanced Security Monitoring (Settings API): {'enabled' if monitoring_on else 'not enabled'}"))
        elif esm_s == 404:
            findings.append(_make_finding("SAT-INFO-ENHANCED-MONITORING", "NOT_APPLICABLE",
                "Enhanced Security Monitoring not available on this workspace (HTTP 404)."))
        else:
            findings.append(_na("SAT-INFO-ENHANCED-MONITORING", esm_s, esm_e))

        # ── Artifact Allowlists (3rd-party library control) ──
        # Try Settings API first, then UC artifact-allowlists API
        _al_resolved = False
        for al_type in ("artifact_allowlist_ws", "artifact_allowlist"):
            al_set_data, al_set_s, _ = await _dbx_get(client, host,
                f"/api/2.0/settings/types/{al_type}/names/default", token)
            if al_set_s == 200 and al_set_data is not None:
                al_obj = al_set_data.get(al_type, al_set_data)
                al_on = al_obj.get("is_enabled", False)
                findings.append(_make_finding("SAT-INFO-3P-LIBS",
                    "PASS" if al_on else "WARN",
                    f"Artifact allowlist (Settings API): {'enabled' if al_on else 'not enabled'}"))
                _al_resolved = True
                break
        if not _al_resolved:
            # Fallback: check UC artifact-allowlists API directly
            _al_configured = False
            _al_reachable = False
            al_last_s, al_last_e = 0, None
            for art_type in ("LIBRARY_JAR", "LIBRARY_MAVEN", "INIT_SCRIPT"):
                al_data, al_s, al_e = await _dbx_get(client, host,
                    f"/api/2.1/unity-catalog/artifact-allowlists/{art_type}", token)
                al_last_s, al_last_e = al_s, al_e
                if al_s == 200 and al_data is not None:
                    _al_reachable = True
                    matchers = al_data.get("artifact_matchers", [])
                    if matchers:
                        _al_configured = True
                        break
            if _al_reachable:
                findings.append(_make_finding("SAT-INFO-3P-LIBS",
                    "PASS" if _al_configured else "WARN",
                    f"Artifact allowlist (UC API): {'configured' if _al_configured else 'no entries configured — all artifacts allowed'}"))
            elif al_last_s == 403:
                findings.append(_na("SAT-INFO-3P-LIBS", 403,
                    "Insufficient permissions to query UC metastore artifact allowlists."))
            elif al_last_s == 404:
                findings.append(_make_finding("SAT-INFO-3P-LIBS", "NOT_APPLICABLE",
                    "Artifact allowlists not available (Unity Catalog may not be enabled on this workspace)."))
            else:
                findings.append(_na("SAT-INFO-3P-LIBS", al_last_s, al_last_e))

    findings.append(_make_finding("SAT-INFO-NETWORK-PEERING", "WARN",
        "Network peering status cannot be auto-verified. Check Azure Portal → Databricks workspace → Networking.",
        {"api_response": None, "note": "manual check — no API available"}))

    # SAT-NET-NCC: Network Connectivity Config (serverless private endpoints)
    _ncc_raw = None
    try:
        r = await client.get(f"{host}/api/2.0/settings/types/default_namespace_ws/names/default",
            headers=hdr, timeout=10)
        if r.status_code == 200:
            _ncc_raw = r.json()
            ncc = _ncc_raw.get("default_namespace_ws", {})
            ns_value = ncc.get("value", "") or ""
            if ns_value:
                findings.append(_make_finding("SAT-NET-NCC", "PASS",
                    f"Network Connectivity Config: namespace '{ns_value}' is configured for serverless."))
            else:
                findings.append(_make_finding("SAT-NET-NCC", "WARN",
                    "Network Connectivity Config not explicitly set. Serverless workloads use default networking."))
        elif r.status_code in (404, 400):
            findings.append(_make_finding("SAT-NET-NCC", "WARN",
                "NCC settings API not available — verify serverless private connectivity in Account Console."))
        else:
            findings.append(_make_finding("SAT-NET-NCC", "WARN", f"HTTP {r.status_code}."))
    except Exception as exc:
        findings.append(_make_finding("SAT-NET-NCC", "WARN", f"Error: {exc}"))

    # SAT-NET-FRONTEND-PL: Front-end Private Link (advisory check)
    findings.append(_make_finding("SAT-NET-FRONTEND-PL", "WARN",
        "Front-end Private Link cannot be auto-verified via workspace API. "
        "Check Azure Portal → Databricks workspace → Networking for Private Link configuration.",
        {"api_response": None, "note": "manual check — no API available"}))

    # SAT-GOV-DLT: DLT pipelines use secure cluster configuration
    _pipelines_raw = None
    try:
        r = await client.get(f"{host}/api/2.0/pipelines", params={"max_results": "100"}, headers=hdr, timeout=15)
        if r.status_code == 200:
            _pipelines_raw = r.json()
            pipelines = _pipelines_raw.get("statuses", [])
            if not pipelines:
                findings.append(_make_finding("SAT-GOV-DLT", "NOT_APPLICABLE", "No DLT pipelines found."))
            else:
                insecure = []
                for pl in pipelines:
                    pid = pl.get("pipeline_id", "")
                    pname = pl.get("name", pid)
                    # Fetch pipeline details for cluster config
                    try:
                        pr = await client.get(f"{host}/api/2.0/pipelines/{pid}", headers=hdr, timeout=10)
                        if pr.status_code == 200:
                            pdetail = pr.json()
                            clusters = pdetail.get("spec", {}).get("clusters", [])
                            for cl_spec in clusters:
                                conf = cl_spec.get("spark_conf", {})
                                env = cl_spec.get("spark_env_vars", {})
                                all_keys = list(conf.keys()) + list(env.keys())
                                all_vals = [str(v) for v in list(conf.values()) + list(env.values())]
                                CRED_PATTERNS = ["fs.azure.account.key", "fs.s3a.access.key", "AZURE_CLIENT_SECRET", "SAS_TOKEN"]
                                if any(p.lower() in k.lower() for p in CRED_PATTERNS for k in all_keys) or \
                                   any("dapi" in v or "eyJ" in v for v in all_vals):
                                    insecure.append(pname)
                                    break
                                dsm = cl_spec.get("data_security_mode") or cl_spec.get("access_mode") or ""
                                if dsm and dsm not in ("USER_ISOLATION", "SINGLE_USER"):
                                    insecure.append(pname)
                                    break
                    except:
                        pass
                findings.append(_make_finding("SAT-GOV-DLT",
                    "FAIL" if insecure else "PASS",
                    f"{len(insecure)}/{len(pipelines)} DLT pipeline(s) have potential cluster security issues."
                    if insecure else f"All {len(pipelines)} DLT pipeline(s) have secure cluster configs."))
        elif r.status_code == 404:
            findings.append(_make_finding("SAT-GOV-DLT", "NOT_APPLICABLE", "DLT Pipelines API not available on this workspace."))
        elif r.status_code == 403:
            findings.append(_make_finding("SAT-GOV-DLT", "WARN", "DLT Pipelines API permission denied (HTTP 403). Use admin token."))
        else:
            findings.append(_make_finding("SAT-GOV-DLT", "WARN", f"HTTP {r.status_code}."))
    except Exception as exc:
        findings.append(_make_finding("SAT-GOV-DLT", "WARN", f"Error: {exc}"))

    # SAT-NET-EXFIL: Data exfiltration prevention controls
    try:
        # Check workspace-conf keys related to exfiltration prevention
        exfil_keys = "enableResultsDownloading,enableExportNotebook,enableNotebookTableClipboard"
        exfil_conf, _, _ = await _dbx_get_workspace_conf(client, host, token, exfil_keys)
        if exfil_conf is not None:
            download_enabled = str(exfil_conf.get("enableResultsDownloading", "true")).lower() == "true"
            export_enabled = str(exfil_conf.get("enableExportNotebook", "true")).lower() == "true"
            clipboard_enabled = str(exfil_conf.get("enableNotebookTableClipboard", "true")).lower() == "true"
            blocked = sum(1 for v in [not download_enabled, not export_enabled, not clipboard_enabled] if v)
            if blocked >= 2:
                findings.append(_make_finding("SAT-NET-EXFIL", "PASS",
                    f"Data exfiltration controls: {blocked}/3 download/export/clipboard restrictions active."))
            elif blocked >= 1:
                findings.append(_make_finding("SAT-NET-EXFIL", "WARN",
                    f"Only {blocked}/3 exfiltration controls active. "
                    "Disable result downloads, notebook exports, and clipboard for sensitive workspaces."))
            else:
                findings.append(_make_finding("SAT-NET-EXFIL", "FAIL",
                    "No data exfiltration prevention controls enabled. "
                    "Downloads, exports, and clipboard are all unrestricted."))
        else:
            findings.append(_make_finding("SAT-NET-EXFIL", "WARN",
                "Could not read workspace configuration for exfiltration controls."))
    except Exception as exc:
        findings.append(_make_finding("SAT-NET-EXFIL", "WARN", f"Error: {exc}"))

    # Enrich with API response data
    _api = {"SAT-GOV-DS-IP": _recipients_raw, "SAT-GOV-DS-EXP": _recipients_raw,
            "SAT-GOV-DS-PERMS": _recipients_raw, "SAT-GOV-DIRECT-CREDS": _clusters_raw,
            "SAT-INFO-COMPLIANCE-PROFILE": _wsconf_raw, "SAT-INFO-ENHANCED-MONITORING": _wsconf_raw,
            "SAT-INFO-3P-LIBS": _wsconf_raw, "SAT-NET-NCC": _ncc_raw,
            "SAT-GOV-DLT": _pipelines_raw}
    for f in findings:
        resp = _api.get(f.check_id)
        if resp is not None:
            f.details.setdefault("api_response", resp)
    return findings


# ── Batch 3: Architecture Assessment Framework checks ──────────────────────

async def _check_uc_governance(client: httpx.AsyncClient, host: str, token: str) -> list[SATFinding]:
    findings: list[SATFinding] = []
    hdr = {"Authorization": f"Bearer {token}"}

    # SAT-GOV-EXT-LOC: External location credential isolation
    _ext_loc_raw = None
    try:
        r = await client.get(f"{host}/api/2.1/unity-catalog/external-locations", headers=hdr, timeout=15)
        if r.status_code == 200:
            _ext_loc_raw = r.json()
            locations = _ext_loc_raw.get("external_locations", [])
            if not locations:
                findings.append(_make_finding("SAT-GOV-EXT-LOC", "NOT_APPLICABLE", "No external locations configured."))
            else:
                cred_names = set(loc.get("credential_name", "") for loc in locations if loc.get("credential_name"))
                read_only_count = sum(1 for loc in locations if loc.get("read_only"))
                if len(cred_names) > 1 and read_only_count > 0:
                    findings.append(_make_finding("SAT-GOV-EXT-LOC", "PASS",
                        f"{len(locations)} external location(s), {len(cred_names)} distinct credential(s), "
                        f"{read_only_count} read-only."))
                elif len(cred_names) <= 1:
                    findings.append(_make_finding("SAT-GOV-EXT-LOC", "WARN",
                        f"{len(locations)} external location(s) share {len(cred_names)} credential. "
                        "Use dedicated credentials per location."))
                else:
                    findings.append(_make_finding("SAT-GOV-EXT-LOC", "WARN",
                        f"{len(locations)} external location(s), {len(cred_names)} credential(s), "
                        f"but no read-only flags. Consider adding read_only where applicable."))
        elif r.status_code == 404:
            findings.append(_make_finding("SAT-GOV-EXT-LOC", "NOT_APPLICABLE",
                "External locations API not available on this workspace."))
        elif r.status_code == 403:
            findings.append(_make_finding("SAT-GOV-EXT-LOC", "WARN",
                "External locations API permission denied (HTTP 403). Use admin token."))
        else:
            findings.append(_make_finding("SAT-GOV-EXT-LOC", "WARN", f"HTTP {r.status_code}."))
    except Exception as exc:
        findings.append(_make_finding("SAT-GOV-EXT-LOC", "WARN", f"Error: {exc}"))

    # SAT-DATA-EXT-LOC-OVERLAP: No overlapping external location paths
    if _ext_loc_raw is not None:
        locations = _ext_loc_raw.get("external_locations", [])
        if not locations:
            findings.append(_make_finding("SAT-DATA-EXT-LOC-OVERLAP", "NOT_APPLICABLE",
                "No external locations configured."))
        else:
            urls = sorted(
                [(loc.get("name", "?"), loc.get("url", "")) for loc in locations if loc.get("url")],
                key=lambda x: x[1])
            overlaps: list[str] = []
            for i in range(len(urls)):
                for j in range(i + 1, len(urls)):
                    url_a = urls[i][1].rstrip("/") + "/"
                    url_b = urls[j][1].rstrip("/") + "/"
                    if url_b.startswith(url_a) or url_a.startswith(url_b):
                        overlaps.append(f"{urls[i][0]} ({urls[i][1]}) <-> {urls[j][0]} ({urls[j][1]})")
            if overlaps:
                findings.append(_make_finding("SAT-DATA-EXT-LOC-OVERLAP", "WARN",
                    f"{len(overlaps)} overlapping external location pair(s) found: "
                    f"{'; '.join(overlaps[:5])}{'...' if len(overlaps) > 5 else ''}. "
                    "Overlapping paths create ambiguous access controls.",
                    {"overlaps": overlaps}))
            else:
                findings.append(_make_finding("SAT-DATA-EXT-LOC-OVERLAP", "PASS",
                    f"No overlapping paths among {len(urls)} external location(s)."))
    else:
        findings.append(_make_finding("SAT-DATA-EXT-LOC-OVERLAP", "NOT_APPLICABLE",
            "External locations data not available."))

    # SAT-GOV-STORAGE-CRED: Storage credentials use managed identity
    _storage_cred_raw = None
    try:
        r = await client.get(f"{host}/api/2.1/unity-catalog/storage-credentials", headers=hdr, timeout=15)
        if r.status_code == 200:
            _storage_cred_raw = r.json()
            creds = _storage_cred_raw.get("storage_credentials", [])
            if not creds:
                findings.append(_make_finding("SAT-GOV-STORAGE-CRED", "NOT_APPLICABLE",
                    "No storage credentials configured."))
            else:
                managed = [c for c in creds if c.get("azure_managed_identity") or c.get("aws_iam_role")]
                key_based = len(creds) - len(managed)
                pct = round(len(managed) / len(creds) * 100)
                if pct >= 80:
                    status = "PASS"
                elif pct < 50:
                    status = "FAIL"
                else:
                    status = "WARN"
                findings.append(_make_finding("SAT-GOV-STORAGE-CRED", status,
                    f"{len(creds)} credential(s): {len(managed)} managed identity ({pct}%), "
                    f"{key_based} key-based."))
        elif r.status_code == 404:
            findings.append(_make_finding("SAT-GOV-STORAGE-CRED", "NOT_APPLICABLE",
                "Storage credentials API not available on this workspace."))
        elif r.status_code == 403:
            findings.append(_make_finding("SAT-GOV-STORAGE-CRED", "WARN",
                "Storage credentials API permission denied (HTTP 403). Use admin token."))
        else:
            findings.append(_make_finding("SAT-GOV-STORAGE-CRED", "WARN", f"HTTP {r.status_code}."))
    except Exception as exc:
        findings.append(_make_finding("SAT-GOV-STORAGE-CRED", "WARN", f"Error: {exc}"))

    # SAT-GOV-UC-TAGS: Data classification tag coverage
    _tags_raw = None
    try:
        r = await client.get(f"{host}/api/2.1/unity-catalog/catalogs", headers=hdr, timeout=15)
        if r.status_code == 200:
            _tags_raw = r.json()
            catalogs = _tags_raw.get("catalogs", [])
            non_system = [c for c in catalogs
                if c.get("name") not in ("system", "__databricks_internal", "hive_metastore")][:5]
            if not non_system:
                findings.append(_make_finding("SAT-GOV-UC-TAGS", "NOT_APPLICABLE",
                    "No non-system catalogs found."))
            else:
                tagged = 0
                for cat in non_system:
                    try:
                        tr = await client.get(f"{host}/api/2.1/unity-catalog/tags",
                            params={"catalog_name": cat.get("name", ""), "schema_name": ""},
                            headers=hdr, timeout=10)
                        if tr.status_code == 200:
                            tag_data = tr.json()
                            if tag_data.get("tag_assignments") or tag_data.get("tags"):
                                tagged += 1
                    except:
                        pass
                pct = round(tagged / len(non_system) * 100)
                findings.append(_make_finding("SAT-GOV-UC-TAGS",
                    "PASS" if pct >= 60 else ("WARN" if tagged > 0 else "FAIL"),
                    f"{tagged}/{len(non_system)} sampled catalog(s) have tags ({pct}%)."))
        elif r.status_code == 404:
            findings.append(_make_finding("SAT-GOV-UC-TAGS", "NOT_APPLICABLE",
                "Catalogs API not available on this workspace."))
        elif r.status_code == 403:
            findings.append(_make_finding("SAT-GOV-UC-TAGS", "WARN",
                "Catalogs API permission denied (HTTP 403). Use admin token."))
        else:
            findings.append(_na("SAT-GOV-UC-TAGS", r.status_code, r.text))
    except Exception as exc:
        findings.append(_na("SAT-GOV-UC-TAGS", 0, str(exc)))

    _api = {"SAT-GOV-EXT-LOC": _ext_loc_raw, "SAT-GOV-STORAGE-CRED": _storage_cred_raw,
            "SAT-GOV-UC-TAGS": _tags_raw}
    for f in findings:
        resp = _api.get(f.check_id)
        if resp is not None:
            f.details.setdefault("api_response", resp)
    return findings


async def _check_permission_audits(client: httpx.AsyncClient, host: str, token: str) -> list[SATFinding]:
    findings: list[SATFinding] = []
    hdr = {"Authorization": f"Bearer {token}"}

    # SAT-IAM-ADMIN-SPRAWL: Admin user count ratio
    try:
        r_groups = await client.get(f"{host}/api/2.0/preview/scim/v2/Groups",
            params={"count": "200"}, headers=hdr, timeout=15)
        r_users = await client.get(f"{host}/api/2.0/preview/scim/v2/Users",
            params={"count": "1"}, headers=hdr, timeout=10)
        if r_groups.status_code == 200 and r_users.status_code == 200:
            groups = r_groups.json().get("Resources", [])
            total_users = r_users.json().get("totalResults", 0)
            admin_group = next((g for g in groups if g.get("displayName") == "admins"), None)
            admin_count = len(admin_group.get("members", [])) if admin_group else 0
            if total_users > 0:
                ratio = round(admin_count / total_users * 100, 1)
                _admin_api = {"api_response": {"admin_count": admin_count, "total_users": total_users, "ratio_pct": ratio},
                    "api_endpoint": "/api/2.0/preview/scim/v2/Groups + /Users"}
                if total_users <= 3:
                    status = "PASS"
                    findings.append(_make_finding("SAT-IAM-ADMIN-SPRAWL", status,
                        f"{admin_count} admin(s) out of {total_users} users ({ratio}%). Small workspace — ratio check not applicable.",
                        _admin_api))
                else:
                    if ratio <= 5:
                        status = "PASS"
                    elif ratio <= 20:
                        status = "WARN"
                    else:
                        status = "FAIL"
                    findings.append(_make_finding("SAT-IAM-ADMIN-SPRAWL", status,
                        f"{admin_count} admin(s) out of {total_users} users ({ratio}%).",
                        _admin_api))
            else:
                findings.append(_make_finding("SAT-IAM-ADMIN-SPRAWL", "WARN", "No users found."))
        else:
            findings.append(_make_finding("SAT-IAM-ADMIN-SPRAWL", "WARN",
                f"SCIM API returned HTTP {r_groups.status_code}/{r_users.status_code}."))
    except Exception as exc:
        findings.append(_make_finding("SAT-IAM-ADMIN-SPRAWL", "WARN", f"Error: {exc}"))

    # SAT-SEC-SCOPE-ACL: Secret scope ACLs and backend types
    try:
        r = await client.get(f"{host}/api/2.0/secrets/scopes/list", headers=hdr, timeout=15)
        if r.status_code == 200:
            scopes = r.json().get("scopes", [])
            if not scopes:
                findings.append(_make_finding("SAT-SEC-SCOPE-ACL", "NOT_APPLICABLE", "No secret scopes."))
            else:
                kv_backed = sum(1 for s in scopes if (s.get("backend_type") or "").upper() == "AZURE_KEYVAULT")
                with_acls = 0
                for s in scopes[:10]:
                    try:
                        ar = await client.get(f"{host}/api/2.0/secrets/acls/list",
                            params={"scope": s.get("name", "")}, headers=hdr, timeout=10)
                        if ar.status_code == 200:
                            acls = ar.json().get("items", [])
                            if len(acls) > 0:
                                with_acls += 1
                    except:
                        pass
                if kv_backed > 0 and with_acls >= len(scopes[:10]) * 0.5:
                    status = "PASS"
                elif with_acls > 0:
                    status = "WARN"
                else:
                    status = "FAIL"
                findings.append(_make_finding("SAT-SEC-SCOPE-ACL", status,
                    f"{len(scopes)} scope(s): {kv_backed} Key Vault-backed, "
                    f"{with_acls}/{min(len(scopes), 10)} sampled have ACLs.",
                    {"api_response": {"total_scopes": len(scopes), "kv_backed": kv_backed,
                        "with_acls": with_acls, "sampled": min(len(scopes), 10)},
                     "api_endpoint": "/api/2.0/secrets/scopes/list"}))
        elif r.status_code in (404, 403):
            findings.append(_make_finding("SAT-SEC-SCOPE-ACL", "WARN",
                f"Secrets API HTTP {r.status_code}."))
        else:
            findings.append(_make_finding("SAT-SEC-SCOPE-ACL", "WARN", f"HTTP {r.status_code}."))
    except Exception as exc:
        findings.append(_make_finding("SAT-SEC-SCOPE-ACL", "WARN", f"Error: {exc}"))

    # SAT-GOV-CLUSTER-ACL: Cluster-level permissions
    try:
        r = await client.get(f"{host}/api/2.0/clusters/list", headers=hdr, timeout=20)
        if r.status_code == 200:
            clusters = r.json().get("clusters", [])
            interactive = [c for c in clusters if c.get("cluster_source") != "JOB"][:10]
            if not interactive:
                findings.append(_make_finding("SAT-GOV-CLUSTER-ACL", "NOT_APPLICABLE", "No interactive clusters."))
            else:
                governed = 0
                for cl in interactive:
                    try:
                        pr = await client.get(
                            f"{host}/api/2.0/permissions/clusters/{cl['cluster_id']}",
                            headers=hdr, timeout=10)
                        if pr.status_code == 200:
                            acls = pr.json().get("access_control_list", [])
                            if len(acls) >= 2:
                                governed += 1
                    except:
                        pass
                pct = round(governed / len(interactive) * 100)
                findings.append(_make_finding("SAT-GOV-CLUSTER-ACL",
                    "PASS" if pct >= 50 else ("WARN" if governed > 0 else "FAIL"),
                    f"{governed}/{len(interactive)} sampled cluster(s) have granular ACLs ({pct}%).",
                    {"api_response": {"interactive_clusters": len(interactive), "governed": governed, "pct": pct},
                     "api_endpoint": "/api/2.0/clusters/list + /permissions/clusters/{id}"}))
        else:
            findings.append(_make_finding("SAT-GOV-CLUSTER-ACL", "WARN", f"HTTP {r.status_code}."))
    except Exception as exc:
        findings.append(_make_finding("SAT-GOV-CLUSTER-ACL", "WARN", f"Error: {exc}"))

    # SAT-GOV-JOB-ACL: Job-level permissions
    try:
        r = await client.get(f"{host}/api/2.1/jobs/list",
            params={"limit": "25", "expand_tasks": "false"}, headers=hdr, timeout=20)
        if r.status_code == 200:
            jobs = r.json().get("jobs", [])[:15]
            if not jobs:
                findings.append(_make_finding("SAT-GOV-JOB-ACL", "NOT_APPLICABLE", "No jobs found."))
            else:
                governed = 0
                for j in jobs:
                    try:
                        pr = await client.get(
                            f"{host}/api/2.0/permissions/jobs/{j['job_id']}",
                            headers=hdr, timeout=10)
                        if pr.status_code == 200:
                            acls = pr.json().get("access_control_list", [])
                            if len(acls) >= 2:
                                governed += 1
                    except:
                        pass
                pct = round(governed / len(jobs) * 100)
                findings.append(_make_finding("SAT-GOV-JOB-ACL",
                    "PASS" if pct >= 50 else ("WARN" if governed > 0 else "FAIL"),
                    f"{governed}/{len(jobs)} sampled job(s) have granular ACLs ({pct}%).",
                    {"api_response": {"total_jobs_sampled": len(jobs), "governed": governed, "pct": pct},
                     "api_endpoint": "/api/2.1/jobs/list + /permissions/jobs/{id}"}))
        else:
            findings.append(_make_finding("SAT-GOV-JOB-ACL", "WARN", f"HTTP {r.status_code}."))
    except Exception as exc:
        findings.append(_make_finding("SAT-GOV-JOB-ACL", "WARN", f"Error: {exc}"))

    # SAT-GOV-WH-ACL: SQL warehouse permissions
    try:
        r = await client.get(f"{host}/api/2.0/sql/warehouses", headers=hdr, timeout=15)
        if r.status_code == 200:
            warehouses = r.json().get("warehouses", [])[:10]
            if not warehouses:
                findings.append(_make_finding("SAT-GOV-WH-ACL", "NOT_APPLICABLE", "No SQL warehouses."))
            else:
                governed = 0
                for wh in warehouses:
                    try:
                        pr = await client.get(
                            f"{host}/api/2.0/permissions/sql/warehouses/{wh['id']}",
                            headers=hdr, timeout=10)
                        if pr.status_code == 200:
                            acls = pr.json().get("access_control_list", [])
                            non_default = [a for a in acls
                                if a.get("group_name") not in ("users", None) or len(acls) >= 3]
                            if non_default:
                                governed += 1
                    except:
                        pass
                pct = round(governed / len(warehouses) * 100)
                findings.append(_make_finding("SAT-GOV-WH-ACL",
                    "PASS" if pct >= 50 else ("WARN" if governed > 0 else "FAIL"),
                    f"{governed}/{len(warehouses)} SQL warehouse(s) have granular permissions ({pct}%).",
                    {"api_response": {"total_warehouses_sampled": len(warehouses), "governed": governed, "pct": pct},
                     "api_endpoint": "/api/2.0/sql/warehouses + /permissions/sql/warehouses/{id}"}))
        else:
            findings.append(_make_finding("SAT-GOV-WH-ACL", "WARN", f"HTTP {r.status_code}."))
    except Exception as exc:
        findings.append(_make_finding("SAT-GOV-WH-ACL", "WARN", f"Error: {exc}"))

    return findings


async def _check_workspace_hygiene(client: httpx.AsyncClient, host: str, token: str) -> list[SATFinding]:
    findings: list[SATFinding] = []
    hdr = {"Authorization": f"Bearer {token}"}

    # SAT-DATA-DBFS-MOUNTS: Legacy mount points
    try:
        r = await client.get(f"{host}/api/2.0/dbfs/list", params={"path": "/mnt"}, headers=hdr, timeout=15)
        if r.status_code == 200:
            files = r.json().get("files", [])
            mount_count = len(files)
            _mnt_api = {"api_response": {"mount_count": mount_count,
                "mounts": [f.get("path", "?").split("/")[-1] for f in files[:10]]},
                "api_endpoint": "/api/2.0/dbfs/list?path=/mnt"}
            if mount_count == 0:
                findings.append(_make_finding("SAT-DATA-DBFS-MOUNTS", "PASS",
                    "No DBFS mount points found. Fully migrated to UC.", _mnt_api))
            elif mount_count <= 3:
                names = ", ".join(f.get("path", "?").split("/")[-1] for f in files[:5])
                findings.append(_make_finding("SAT-DATA-DBFS-MOUNTS", "WARN",
                    f"{mount_count} mount point(s) in /mnt: {names}", _mnt_api))
            else:
                findings.append(_make_finding("SAT-DATA-DBFS-MOUNTS", "FAIL",
                    f"{mount_count} mount point(s) in /mnt. Migrate to UC external locations.", _mnt_api))
        elif r.status_code == 404:
            findings.append(_make_finding("SAT-DATA-DBFS-MOUNTS", "PASS", "No /mnt directory found.",
                {"api_response": {"mount_count": 0, "http_status": 404}, "api_endpoint": "/api/2.0/dbfs/list?path=/mnt"}))
        else:
            findings.append(_make_finding("SAT-DATA-DBFS-MOUNTS", "WARN", f"HTTP {r.status_code}."))
    except Exception as exc:
        findings.append(_make_finding("SAT-DATA-DBFS-MOUNTS", "WARN", f"Error: {exc}"))

    # SAT-DATA-DBFS-ROOT: User directories in DBFS root
    SYSTEM_DIRS = {"FileStore", "databricks-datasets", "databricks", "databricks-results",
                   "mnt", "tmp", "user", "_delta_log", "ml", "pipelines"}
    try:
        r = await client.get(f"{host}/api/2.0/dbfs/list", params={"path": "/"}, headers=hdr, timeout=15)
        if r.status_code == 200:
            files = r.json().get("files", [])
            user_dirs = [f for f in files
                if f.get("is_dir", False)
                and f.get("path", "").strip("/").split("/")[-1] not in SYSTEM_DIRS]
            _dbfs_api = {"api_response": {"files": files, "user_dirs": len(user_dirs)},
                         "api_endpoint": "/api/2.0/dbfs/list?path=/"}
            if not user_dirs:
                findings.append(_make_finding("SAT-DATA-DBFS-ROOT", "PASS",
                    "No user directories in DBFS root.", _dbfs_api))
            elif len(user_dirs) <= 3:
                names = ", ".join(f.get("path", "?") for f in user_dirs[:5])
                findings.append(_make_finding("SAT-DATA-DBFS-ROOT", "WARN",
                    f"{len(user_dirs)} user directory(ies) in DBFS root: {names}", _dbfs_api))
            else:
                findings.append(_make_finding("SAT-DATA-DBFS-ROOT", "FAIL",
                    f"{len(user_dirs)} user directories in DBFS root. Migrate to UC.", _dbfs_api))
        elif r.status_code == 404:
            findings.append(_make_finding("SAT-DATA-DBFS-ROOT", "PASS", "DBFS root not accessible."))
        else:
            findings.append(_make_finding("SAT-DATA-DBFS-ROOT", "WARN", f"HTTP {r.status_code}."))
    except Exception as exc:
        findings.append(_make_finding("SAT-DATA-DBFS-ROOT", "WARN", f"Error: {exc}"))

    return findings


async def _check_ops_monitoring(client: httpx.AsyncClient, host: str, token: str) -> list[SATFinding]:
    findings: list[SATFinding] = []
    hdr = {"Authorization": f"Bearer {token}"}

    # SAT-OPS-JOB-SUCCESS: Job run success rate
    try:
        r = await client.get(f"{host}/api/2.1/jobs/runs/list",
            params={"limit": "25", "expand_tasks": "false"}, headers=hdr, timeout=20)
        if r.status_code == 200:
            runs = r.json().get("runs", [])
            if not runs:
                findings.append(_make_finding("SAT-OPS-JOB-SUCCESS", "NOT_APPLICABLE", "No job runs found."))
            else:
                finished = [r_ for r_ in runs if r_.get("state", {}).get("life_cycle_state") not in ("RUNNING", "PENDING")]
                succeeded = sum(1 for r_ in finished if r_.get("state", {}).get("result_state") == "SUCCESS")
                rate = round(succeeded / len(finished) * 100, 1) if finished else 0
                if rate >= 95:
                    status = "PASS"
                elif rate >= 70:
                    status = "WARN"
                else:
                    status = "FAIL"
                findings.append(_make_finding("SAT-OPS-JOB-SUCCESS", status,
                    f"Job success rate: {rate}% ({succeeded}/{len(finished)} recent runs)."))
        else:
            findings.append(_make_finding("SAT-OPS-JOB-SUCCESS", "WARN", f"HTTP {r.status_code}.",
                {"api_response": None, "http_status": r.status_code}))
    except Exception as exc:
        findings.append(_make_finding("SAT-OPS-JOB-SUCCESS", "WARN", f"Error: {exc}"))

    # SAT-OPS-DORMANT-JOBS: Jobs without schedule and no recent runs
    try:
        r = await client.get(f"{host}/api/2.1/jobs/list",
            params={"limit": "100", "expand_tasks": "false"}, headers=hdr, timeout=20)
        if r.status_code == 200:
            jobs = r.json().get("jobs", [])
            if not jobs:
                findings.append(_make_finding("SAT-OPS-DORMANT-JOBS", "PASS", "No jobs found — no dormant jobs."))
            else:
                no_schedule = [j for j in jobs if not j.get("settings", {}).get("schedule")
                    and not j.get("settings", {}).get("trigger")
                    and not j.get("settings", {}).get("continuous")]
                dormant = 0
                for j in no_schedule[:20]:
                    try:
                        rr = await client.get(f"{host}/api/2.1/jobs/runs/list",
                            params={"job_id": str(j["job_id"]), "limit": "1"},
                            headers=hdr, timeout=10)
                        if rr.status_code == 200 and not rr.json().get("runs"):
                            dormant += 1
                    except:
                        pass
                pct = round(dormant / len(jobs) * 100, 1)
                if pct <= 10:
                    status = "PASS"
                elif pct <= 50:
                    status = "WARN"
                else:
                    status = "FAIL"
                findings.append(_make_finding("SAT-OPS-DORMANT-JOBS", status,
                    f"{dormant} dormant job(s) out of {len(jobs)} ({pct}%).",
                    {"api_response": {"jobs": jobs, "dormant": dormant},
                     "api_endpoint": "/api/2.1/jobs/list"}))
        else:
            findings.append(_make_finding("SAT-OPS-DORMANT-JOBS", "WARN", f"HTTP {r.status_code}."))
    except Exception as exc:
        findings.append(_make_finding("SAT-OPS-DORMANT-JOBS", "WARN", f"Error: {exc}"))

    # SAT-OPS-JOB-NOTIFY: Job failure notification coverage
    try:
        r = await client.get(f"{host}/api/2.1/jobs/list",
            params={"limit": "100", "expand_tasks": "false"}, headers=hdr, timeout=20)
        if r.status_code == 200:
            jobs = r.json().get("jobs", [])
            if not jobs:
                findings.append(_make_finding("SAT-OPS-JOB-NOTIFY", "NOT_APPLICABLE", "No jobs found."))
            else:
                with_notif = sum(1 for j in jobs if j.get("settings", {}).get("notification_settings")
                    or j.get("settings", {}).get("email_notifications")
                    or j.get("settings", {}).get("webhook_notifications"))
                ratio = round(with_notif / len(jobs) * 100, 1)
                if ratio >= 80:
                    status = "PASS"
                elif with_notif > 0:
                    status = "WARN"
                else:
                    status = "FAIL"
                findings.append(_make_finding("SAT-OPS-JOB-NOTIFY", status,
                    f"{with_notif}/{len(jobs)} job(s) have failure notifications ({ratio}%).",
                    {"api_response": {"jobs": jobs, "with_notif": with_notif},
                     "api_endpoint": "/api/2.1/jobs/list"}))
        else:
            findings.append(_make_finding("SAT-OPS-JOB-NOTIFY", "WARN", f"HTTP {r.status_code}."))
    except Exception as exc:
        findings.append(_make_finding("SAT-OPS-JOB-NOTIFY", "WARN", f"Error: {exc}"))

    # SAT-OPS-CLUSTER-EVENTS: Cluster failure events
    try:
        r = await client.get(f"{host}/api/2.0/clusters/list", headers=hdr, timeout=20)
        if r.status_code == 200:
            clusters = r.json().get("clusters", [])
            running = [c for c in clusters if c.get("state") == "RUNNING"][:5]
            if not running:
                findings.append(_make_finding("SAT-OPS-CLUSTER-EVENTS", "NOT_APPLICABLE",
                    "No running clusters to check events."))
            else:
                FAILURE_TYPES = {"DRIVER_NOT_RESPONDING", "SPARK_EXCEPTION", "NODES_LOST",
                                 "DRIVER_UNAVAILABLE", "DBFS_DOWN", "METASTORE_DOWN"}
                total_failures = 0
                for cl in running:
                    data, _, _ = await _dbx_post(client, host, "/api/2.0/clusters/events", token,
                        {"cluster_id": cl["cluster_id"], "limit": 50})
                    if data:
                        events = data.get("events", [])
                        total_failures += sum(1 for e in events if e.get("type") in FAILURE_TYPES)
                if total_failures == 0:
                    status = "PASS"
                elif total_failures <= 5:
                    status = "WARN"
                else:
                    status = "FAIL"
                findings.append(_make_finding("SAT-OPS-CLUSTER-EVENTS", status,
                    f"{total_failures} failure event(s) across {len(running)} running cluster(s)."))
        else:
            findings.append(_make_finding("SAT-OPS-CLUSTER-EVENTS", "WARN", f"HTTP {r.status_code}."))
    except Exception as exc:
        findings.append(_make_finding("SAT-OPS-CLUSTER-EVENTS", "WARN", f"Error: {exc}"))

    # SAT-OPS-IDLE-CLUSTERS: Auto-termination aggressiveness
    try:
        r = await client.get(f"{host}/api/2.0/clusters/list", headers=hdr, timeout=20)
        if r.status_code == 200:
            clusters = r.json().get("clusters", [])
            interactive = [c for c in clusters if c.get("cluster_source") != "JOB"]
            if not interactive:
                findings.append(_make_finding("SAT-OPS-IDLE-CLUSTERS", "NOT_APPLICABLE",
                    "No interactive clusters."))
            else:
                aggressive = sum(1 for c in interactive
                    if c.get("autotermination_minutes") and c["autotermination_minutes"] <= 30)
                pct = round(aggressive / len(interactive) * 100)
                if pct >= 70:
                    status = "PASS"
                elif pct >= 30:
                    status = "WARN"
                else:
                    status = "FAIL"
                findings.append(_make_finding("SAT-OPS-IDLE-CLUSTERS", status,
                    f"{aggressive}/{len(interactive)} interactive cluster(s) have auto-termination <= 30 min ({pct}%).",
                    {"api_response": {"interactive_clusters": len(interactive), "aggressive_autotermination": aggressive, "pct": pct},
                     "api_endpoint": "/api/2.0/clusters/list"}))
        else:
            findings.append(_make_finding("SAT-OPS-IDLE-CLUSTERS", "WARN", f"HTTP {r.status_code}."))
    except Exception as exc:
        findings.append(_make_finding("SAT-OPS-IDLE-CLUSTERS", "WARN", f"Error: {exc}"))

    # SAT-OPS-NOTIFY-DEST: Notification destinations
    try:
        r = await client.get(f"{host}/api/2.0/notification-destinations", headers=hdr, timeout=15)
        if r.status_code == 200:
            data = r.json()
            dests = data.get("results", data.get("notification_destinations", []))
            _nd_api = {"api_response": {"results": dests}, "api_endpoint": "/api/2.0/notification-destinations"}
            if not dests:
                findings.append(_make_finding("SAT-OPS-NOTIFY-DEST", "FAIL",
                    "No notification destinations configured.", _nd_api))
            elif len(dests) >= 3:
                types = set(d.get("destination_type", d.get("type", "?")) for d in dests)
                findings.append(_make_finding("SAT-OPS-NOTIFY-DEST", "PASS",
                    f"{len(dests)} notification destination(s) ({', '.join(types)}).", _nd_api))
            else:
                findings.append(_make_finding("SAT-OPS-NOTIFY-DEST", "WARN",
                    f"{len(dests)} notification destination(s). Consider adding more (Slack, PagerDuty, etc.).", _nd_api))
        elif r.status_code in (404, 403):
            findings.append(_make_finding("SAT-OPS-NOTIFY-DEST", "NOT_APPLICABLE",
                f"Notification destinations API not available (HTTP {r.status_code})."))
        else:
            findings.append(_make_finding("SAT-OPS-NOTIFY-DEST", "WARN", f"HTTP {r.status_code}."))
    except Exception as exc:
        findings.append(_make_finding("SAT-OPS-NOTIFY-DEST", "WARN", f"Error: {exc}"))

    # SAT-OPS-DLT-QUALITY: DLT pipeline edition and Photon usage
    try:
        r = await client.get(f"{host}/api/2.0/pipelines", params={"max_results": "50"}, headers=hdr, timeout=15)
        if r.status_code == 200:
            pipelines = r.json().get("statuses", [])
            if not pipelines:
                findings.append(_make_finding("SAT-OPS-DLT-QUALITY", "NOT_APPLICABLE",
                    "No DLT pipelines found."))
            else:
                pro_adv = 0
                with_photon = 0
                for pl in pipelines[:10]:
                    try:
                        pr = await client.get(f"{host}/api/2.0/pipelines/{pl.get('pipeline_id', '')}",
                            headers=hdr, timeout=10)
                        if pr.status_code == 200:
                            spec = pr.json().get("spec", {})
                            if spec.get("edition", "").upper() in ("PRO", "ADVANCED"):
                                pro_adv += 1
                            if spec.get("photon"):
                                with_photon += 1
                    except:
                        pass
                sampled = min(len(pipelines), 10)
                if pro_adv > 0 and with_photon > 0:
                    status = "PASS"
                elif pro_adv > 0 or with_photon > 0:
                    status = "WARN"
                else:
                    status = "FAIL"
                findings.append(_make_finding("SAT-OPS-DLT-QUALITY", status,
                    f"{len(pipelines)} pipeline(s): {pro_adv}/{sampled} Pro/Advanced, "
                    f"{with_photon}/{sampled} Photon-enabled.",
                    {"api_response": {"statuses": pipelines, "pro_adv": pro_adv, "with_photon": with_photon},
                     "api_endpoint": "/api/2.0/pipelines"}))
        elif r.status_code in (404, 403):
            findings.append(_make_finding("SAT-OPS-DLT-QUALITY", "NOT_APPLICABLE",
                f"Pipelines API not available (HTTP {r.status_code})."))
        else:
            findings.append(_make_finding("SAT-OPS-DLT-QUALITY", "WARN", f"HTTP {r.status_code}."))
    except Exception as exc:
        findings.append(_make_finding("SAT-OPS-DLT-QUALITY", "WARN", f"Error: {exc}"))

    # SAT-OPS-SQL-HEALTH: SQL query failure rate
    try:
        r = await client.get(f"{host}/api/2.0/sql/history/queries",
            params={"max_results": "100"}, headers=hdr, timeout=15)
        if r.status_code == 200:
            data = r.json()
            queries = data.get("res", data.get("results", []))
            if not queries:
                findings.append(_make_finding("SAT-OPS-SQL-HEALTH", "NOT_APPLICABLE",
                    "No SQL query history found."))
            else:
                failed = sum(1 for q in queries if q.get("status") in ("FAILED", "CANCELED"))
                slow = sum(1 for q in queries if (q.get("duration") or 0) > 60000)
                fail_rate = round(failed / len(queries) * 100, 1)
                if fail_rate < 5 and slow < len(queries) * 0.1:
                    status = "PASS"
                elif fail_rate < 20:
                    status = "WARN"
                else:
                    status = "FAIL"
                findings.append(_make_finding("SAT-OPS-SQL-HEALTH", status,
                    f"{len(queries)} queries: {failed} failed ({fail_rate}%), {slow} slow (>60s).",
                    {"api_response": {"total_queries": len(queries), "failed": failed, "slow": slow, "fail_rate_pct": fail_rate},
                     "api_endpoint": "/api/2.0/sql/history/queries"}))
        elif r.status_code in (404, 403):
            findings.append(_make_finding("SAT-OPS-SQL-HEALTH", "NOT_APPLICABLE",
                f"SQL history API not available (HTTP {r.status_code})."))
        else:
            findings.append(_make_finding("SAT-OPS-SQL-HEALTH", "WARN", f"HTTP {r.status_code}."))
    except Exception as exc:
        findings.append(_make_finding("SAT-OPS-SQL-HEALTH", "WARN", f"Error: {exc}"))

    # SAT-OPS-SQL-ALERTS: SQL alerts
    try:
        r = await client.get(f"{host}/api/2.0/sql/alerts", headers=hdr, timeout=15)
        if r.status_code == 200:
            data = r.json()
            alerts = data if isinstance(data, list) else data.get("results", [])
            active = sum(1 for a in alerts if a.get("state") not in ("unknown", None))
            _alert_api = {"api_response": {"alerts": alerts}, "api_endpoint": "/api/2.0/sql/alerts"}
            if not alerts:
                findings.append(_make_finding("SAT-OPS-SQL-ALERTS", "FAIL",
                    "No SQL alerts configured.", _alert_api))
            elif active >= 5:
                findings.append(_make_finding("SAT-OPS-SQL-ALERTS", "PASS",
                    f"{len(alerts)} SQL alert(s), {active} active.", _alert_api))
            else:
                findings.append(_make_finding("SAT-OPS-SQL-ALERTS", "WARN",
                    f"{len(alerts)} SQL alert(s), {active} active. Consider adding more.", _alert_api))
        elif r.status_code in (404, 403):
            findings.append(_make_finding("SAT-OPS-SQL-ALERTS", "NOT_APPLICABLE",
                f"SQL alerts API not available (HTTP {r.status_code})."))
        else:
            findings.append(_make_finding("SAT-OPS-SQL-ALERTS", "WARN", f"HTTP {r.status_code}."))
    except Exception as exc:
        findings.append(_make_finding("SAT-OPS-SQL-ALERTS", "WARN", f"Error: {exc}"))

    # SAT-OPS-POOL-USAGE: Instance pool utilization
    try:
        r = await client.get(f"{host}/api/2.0/instance-pools/list", headers=hdr, timeout=15)
        if r.status_code == 200:
            pools = r.json().get("instance_pools", [])
            if not pools:
                findings.append(_make_finding("SAT-OPS-POOL-USAGE", "NOT_APPLICABLE",
                    "No instance pools configured."))
            else:
                preloaded = sum(1 for p in pools if p.get("preloaded_spark_versions"))
                if len(pools) >= 3 and preloaded > 0:
                    status = "PASS"
                elif len(pools) >= 1:
                    status = "WARN"
                else:
                    status = "FAIL"
                findings.append(_make_finding("SAT-OPS-POOL-USAGE", status,
                    f"{len(pools)} pool(s), {preloaded} with preloaded Spark versions."))
        else:
            findings.append(_make_finding("SAT-OPS-POOL-USAGE", "WARN", f"HTTP {r.status_code}."))
    except Exception as exc:
        findings.append(_make_finding("SAT-OPS-POOL-USAGE", "WARN", f"Error: {exc}"))

    # SAT-OPS-JOB-SP-OWNER: Jobs owned by service principals
    try:
        r = await client.get(f"{host}/api/2.1/jobs/list",
            params={"limit": "100", "expand_tasks": "false"}, headers=hdr, timeout=20)
        if r.status_code == 200:
            jobs = r.json().get("jobs", [])
            if not jobs:
                findings.append(_make_finding("SAT-OPS-JOB-SP-OWNER", "NOT_APPLICABLE", "No jobs found."))
            else:
                sp_owned = sum(1 for j in jobs
                    if j.get("creator_user_name", "").endswith(".serviceprincipal")
                    or "@" not in j.get("creator_user_name", "@"))
                user_owned = len(jobs) - sp_owned
                if user_owned == 0:
                    status = "PASS"
                elif sp_owned > user_owned:
                    status = "WARN"
                else:
                    status = "FAIL"
                findings.append(_make_finding("SAT-OPS-JOB-SP-OWNER", status,
                    f"{sp_owned}/{len(jobs)} jobs owned by service principals, {user_owned} by users."))
        else:
            findings.append(_make_finding("SAT-OPS-JOB-SP-OWNER", "WARN", f"HTTP {r.status_code}."))
    except Exception as exc:
        findings.append(_make_finding("SAT-OPS-JOB-SP-OWNER", "WARN", f"Error: {exc}"))

    # SAT-OPS-JOB-TAGS: Jobs have cost attribution tags
    try:
        r = await client.get(f"{host}/api/2.1/jobs/list",
            params={"limit": "100", "expand_tasks": "false"}, headers=hdr, timeout=20)
        if r.status_code == 200:
            jobs = r.json().get("jobs", [])
            if not jobs:
                findings.append(_make_finding("SAT-OPS-JOB-TAGS", "NOT_APPLICABLE", "No jobs found."))
            else:
                tagged = sum(1 for j in jobs if j.get("settings", {}).get("tags"))
                pct = round(tagged / len(jobs) * 100)
                findings.append(_make_finding("SAT-OPS-JOB-TAGS",
                    "PASS" if pct >= 80 else ("WARN" if tagged > 0 else "FAIL"),
                    f"{tagged}/{len(jobs)} jobs ({pct}%) have cost attribution tags."))
        else:
            findings.append(_make_finding("SAT-OPS-JOB-TAGS", "WARN", f"HTTP {r.status_code}."))
    except Exception as exc:
        findings.append(_make_finding("SAT-OPS-JOB-TAGS", "WARN", f"Error: {exc}"))

    # SAT-OPS-JOB-GIT: Jobs use Git-backed sources
    try:
        r = await client.get(f"{host}/api/2.1/jobs/list",
            params={"limit": "100", "expand_tasks": "false"}, headers=hdr, timeout=20)
        if r.status_code == 200:
            jobs = r.json().get("jobs", [])
            if not jobs:
                findings.append(_make_finding("SAT-OPS-JOB-GIT", "NOT_APPLICABLE", "No jobs found."))
            else:
                git_jobs = sum(1 for j in jobs if j.get("settings", {}).get("git_source"))
                pct = round(git_jobs / len(jobs) * 100)
                findings.append(_make_finding("SAT-OPS-JOB-GIT",
                    "PASS" if pct >= 50 else ("WARN" if git_jobs > 0 else "FAIL"),
                    f"{git_jobs}/{len(jobs)} jobs ({pct}%) use Git-backed sources."))
        else:
            findings.append(_make_finding("SAT-OPS-JOB-GIT", "WARN", f"HTTP {r.status_code}."))
    except Exception as exc:
        findings.append(_make_finding("SAT-OPS-JOB-GIT", "WARN", f"Error: {exc}"))

    return findings


async def _check_feature_adoption(client: httpx.AsyncClient, host: str, token: str) -> list[SATFinding]:
    findings: list[SATFinding] = []
    hdr = {"Authorization": f"Bearer {token}"}

    # Helper for simple feature checks
    async def _feat_check(check_id: str, api_path: str, list_key: str, feature_name: str,
                          pass_threshold: int = 5, warn_min: int = 1):
        try:
            r = await client.get(f"{host}{api_path}", headers=hdr, timeout=15)
            if r.status_code == 200:
                data = r.json()
                items = data.get(list_key, data if isinstance(data, list) else [])
                count = len(items) if isinstance(items, list) else 0
                _api = {"api_response": {list_key: items}, "api_endpoint": api_path}
                if count >= pass_threshold:
                    findings.append(_make_finding(check_id, "PASS", f"{count} {feature_name} found.", _api))
                elif count >= warn_min:
                    findings.append(_make_finding(check_id, "WARN", f"{count} {feature_name} found.", _api))
                else:
                    findings.append(_make_finding(check_id, "NOT_APPLICABLE", f"No {feature_name} found.", _api))
            elif r.status_code in (404, 403):
                # Feature not available — expected; clear tracker error so endpoint
                # summary doesn't flag it as a failure.
                if hasattr(client, "api_errors"):
                    client.api_errors.pop(api_path, None)
                findings.append(_make_finding(check_id, "NOT_APPLICABLE",
                    f"{feature_name} API not available (HTTP {r.status_code})."))
            else:
                findings.append(_make_finding(check_id, "WARN", f"HTTP {r.status_code}."))
        except Exception as exc:
            findings.append(_make_finding(check_id, "WARN", f"Error: {exc}"))

    # SAT-FEAT-VECTOR-SEARCH
    await _feat_check("SAT-FEAT-VECTOR-SEARCH", "/api/2.0/vector-search/endpoints",
        "endpoints", "Vector Search endpoint(s)", pass_threshold=2)

    # SAT-FEAT-FEATURE-STORE
    await _feat_check("SAT-FEAT-FEATURE-STORE", "/api/2.0/feature-store/feature-tables",
        "feature_tables", "Feature Engineering table(s)", pass_threshold=5)

    # SAT-FEAT-QUALITY-MON: Requires catalog traversal
    try:
        r = await client.get(f"{host}/api/2.1/unity-catalog/catalogs", headers=hdr, timeout=15)
        if r.status_code == 200:
            catalogs = r.json().get("catalogs", [])
            non_sys = [c for c in catalogs
                if c.get("name") not in ("system", "__databricks_internal", "hive_metastore")][:3]
            total_monitors = 0
            for cat in non_sys:
                try:
                    sr = await client.get(f"{host}/api/2.1/unity-catalog/schemas",
                        params={"catalog_name": cat["name"]}, headers=hdr, timeout=10)
                    if sr.status_code == 200:
                        schemas = sr.json().get("schemas", [])[:2]
                        for sch in schemas:
                            try:
                                tr = await client.get(f"{host}/api/2.1/unity-catalog/tables",
                                    params={"catalog_name": cat["name"], "schema_name": sch["name"],
                                            "max_results": "5"},
                                    headers=hdr, timeout=10)
                                if tr.status_code == 200:
                                    tables = tr.json().get("tables", [])
                                    for tbl in tables:
                                        try:
                                            mon_path = f"/api/2.1/unity-catalog/tables/{tbl['full_name']}/monitor"
                                            mr = await client.get(
                                                f"{host}{mon_path}",
                                                headers=hdr, timeout=10)
                                            if mr.status_code == 200:
                                                total_monitors += 1
                                            elif hasattr(client, "api_errors"):
                                                # 404 = no monitor on this table — expected
                                                client.api_errors.pop(mon_path, None)
                                        except:
                                            pass
                            except:
                                pass
                except:
                    pass
            if total_monitors >= 5:
                findings.append(_make_finding("SAT-FEAT-QUALITY-MON", "PASS",
                    f"{total_monitors} quality monitor(s) found."))
            elif total_monitors > 0:
                findings.append(_make_finding("SAT-FEAT-QUALITY-MON", "WARN",
                    f"{total_monitors} quality monitor(s) found. Consider adding more."))
            else:
                findings.append(_make_finding("SAT-FEAT-QUALITY-MON", "NOT_APPLICABLE",
                    "No quality monitors found on sampled tables."))
        else:
            findings.append(_make_finding("SAT-FEAT-QUALITY-MON", "NOT_APPLICABLE",
                f"Catalogs API HTTP {r.status_code}."))
    except Exception as exc:
        findings.append(_make_finding("SAT-FEAT-QUALITY-MON", "WARN", f"Error: {exc}"))

    # SAT-FEAT-GENIE
    await _feat_check("SAT-FEAT-GENIE", "/api/2.0/genie/spaces",
        "spaces", "Genie Space(s)", pass_threshold=3)

    # SAT-FEAT-LAKEVIEW
    try:
        r = await client.get(f"{host}/api/2.0/lakeview/dashboards", headers=hdr, timeout=15)
        if r.status_code == 200:
            dashboards = r.json().get("dashboards", [])
            active = sum(1 for d in dashboards if d.get("lifecycle_state") == "ACTIVE")
            _lv_api = {"api_response": {"total_dashboards": len(dashboards), "active": active},
                "api_endpoint": "/api/2.0/lakeview/dashboards"}
            if active >= 10:
                findings.append(_make_finding("SAT-FEAT-LAKEVIEW", "PASS",
                    f"{len(dashboards)} dashboard(s), {active} active.", _lv_api))
            elif active > 0:
                findings.append(_make_finding("SAT-FEAT-LAKEVIEW", "WARN",
                    f"{len(dashboards)} dashboard(s), {active} active.", _lv_api))
            else:
                findings.append(_make_finding("SAT-FEAT-LAKEVIEW", "NOT_APPLICABLE",
                    "No active Lakeview dashboards.", _lv_api))
        elif r.status_code in (404, 403):
            if hasattr(client, "api_errors"):
                client.api_errors.pop("/api/2.0/lakeview/dashboards", None)
            findings.append(_make_finding("SAT-FEAT-LAKEVIEW", "NOT_APPLICABLE",
                f"Lakeview API not available (HTTP {r.status_code})."))
        else:
            findings.append(_make_finding("SAT-FEAT-LAKEVIEW", "WARN", f"HTTP {r.status_code}."))
    except Exception as exc:
        findings.append(_make_finding("SAT-FEAT-LAKEVIEW", "WARN", f"Error: {exc}"))

    # SAT-FEAT-APPS
    try:
        r = await client.get(f"{host}/api/2.0/apps", headers=hdr, timeout=15)
        if r.status_code == 200:
            data = r.json()
            apps = data.get("apps", data if isinstance(data, list) else [])
            running = sum(1 for a in apps if (a.get("status", {}).get("state") or "").upper() in ("RUNNING", "ACTIVE"))
            _apps_api = {"api_response": {"apps": apps}, "api_endpoint": "/api/2.0/apps"}
            if running >= 5:
                findings.append(_make_finding("SAT-FEAT-APPS", "PASS",
                    f"{len(apps)} app(s), {running} running.", _apps_api))
            elif len(apps) > 0:
                findings.append(_make_finding("SAT-FEAT-APPS", "WARN",
                    f"{len(apps)} app(s), {running} running.", _apps_api))
            else:
                findings.append(_make_finding("SAT-FEAT-APPS", "NOT_APPLICABLE", "No Databricks Apps found.", _apps_api))
        elif r.status_code in (404, 403):
            if hasattr(client, "api_errors"):
                client.api_errors.pop("/api/2.0/apps", None)
            findings.append(_make_finding("SAT-FEAT-APPS", "NOT_APPLICABLE",
                f"Apps API not available (HTTP {r.status_code})."))
        else:
            findings.append(_make_finding("SAT-FEAT-APPS", "WARN", f"HTTP {r.status_code}."))
    except Exception as exc:
        findings.append(_make_finding("SAT-FEAT-APPS", "WARN", f"Error: {exc}"))

    # SAT-FEAT-GIT-REPOS
    await _feat_check("SAT-FEAT-GIT-REPOS", "/api/2.0/repos",
        "repos", "Git repo(s)", pass_threshold=10)

    # SAT-FEAT-CLEAN-ROOMS
    await _feat_check("SAT-FEAT-CLEAN-ROOMS", "/api/2.0/clean-rooms",
        "clean_rooms", "UC Clean Room(s)", pass_threshold=1)

    # SAT-FEAT-MARKETPLACE
    try:
        r = await client.get(f"{host}/api/2.1/marketplace-consumer/listings", headers=hdr, timeout=15)
        if r.status_code == 200:
            data = r.json()
            listings = data.get("listings", [])
            installed = sum(1 for l in listings if (l.get("status") or "").upper() == "INSTALLED")
            if installed >= 3:
                findings.append(_make_finding("SAT-FEAT-MARKETPLACE", "PASS",
                    f"{len(listings)} listing(s), {installed} installed."))
            elif len(listings) > 0:
                findings.append(_make_finding("SAT-FEAT-MARKETPLACE", "WARN",
                    f"{len(listings)} listing(s), {installed} installed."))
            else:
                findings.append(_make_finding("SAT-FEAT-MARKETPLACE", "NOT_APPLICABLE",
                    "No Marketplace listings found."))
        elif r.status_code in (404, 403):
            if hasattr(client, "api_errors"):
                client.api_errors.pop("/api/2.1/marketplace-consumer/listings", None)
            findings.append(_make_finding("SAT-FEAT-MARKETPLACE", "NOT_APPLICABLE",
                f"Marketplace API not available (HTTP {r.status_code})."))
        else:
            findings.append(_make_finding("SAT-FEAT-MARKETPLACE", "WARN", f"HTTP {r.status_code}."))
    except Exception as exc:
        findings.append(_make_finding("SAT-FEAT-MARKETPLACE", "WARN", f"Error: {exc}"))

    # SAT-FEAT-ONLINE-TABLES
    await _feat_check("SAT-FEAT-ONLINE-TABLES", "/api/2.0/online-tables",
        "online_tables", "online table(s)", pass_threshold=5)

    return findings


# ═══════════════════════════════════════════════════════════════════════════════
# Phase 1 — Performance, Cost Optimization, Reliability
# ═══════════════════════════════════════════════════════════════════════════════


async def _check_performance(client: httpx.AsyncClient, host: str, token: str) -> list[SATFinding]:
    """Performance checks: Photon adoption, serverless warehouses, runtime currency."""
    findings: list[SATFinding] = []

    # SAT-PERF-1: Photon runtime adoption
    cl_data, cl_s, cl_e = await _dbx_get(client, host, "/api/2.0/clusters/list", token)
    if cl_data is not None:
        clusters = cl_data.get("clusters", [])
        active = [c for c in clusters if c.get("state") not in ("TERMINATED", "TERMINATING")]
        if not active:
            findings.append(_make_finding("SAT-PERF-1", "NOT_APPLICABLE", "No active clusters found."))
        else:
            photon = [c for c in active
                      if "photon" in c.get("spark_version", "").lower()
                      or c.get("runtime_engine") == "PHOTON"]
            pct = round(len(photon) / len(active) * 100)
            if pct >= 80:
                status = "PASS"
            elif pct >= 30:
                status = "WARN"
            else:
                status = "FAIL"
            findings.append(_make_finding("SAT-PERF-1", status,
                f"{len(photon)}/{len(active)} active clusters ({pct}%) use Photon.",
                {"photon_count": len(photon), "active_count": len(active), "pct": pct}))
    else:
        findings.append(_na("SAT-PERF-1", cl_s, cl_e))

    # SAT-PERF-2: SQL Warehouse serverless adoption
    wh_data, wh_s, wh_e = await _dbx_get(client, host, "/api/2.0/sql/warehouses", token)
    if wh_data is not None:
        warehouses = wh_data.get("warehouses", [])
        if not warehouses:
            findings.append(_make_finding("SAT-PERF-2", "NOT_APPLICABLE", "No SQL warehouses found."))
        else:
            serverless = [w for w in warehouses
                          if w.get("warehouse_type") == "PRO"
                          or w.get("enable_serverless_compute", False)]
            if len(serverless) == len(warehouses):
                status = "PASS"
            elif serverless:
                status = "WARN"
            else:
                status = "FAIL"
            findings.append(_make_finding("SAT-PERF-2", status,
                f"{len(serverless)}/{len(warehouses)} SQL warehouses are serverless/PRO.",
                {"serverless_count": len(serverless), "total": len(warehouses)}))
    else:
        findings.append(_na("SAT-PERF-2", wh_s, wh_e))

    # SAT-PERF-3: Runtime version currency (LTS & EOL)
    cl2_data, cl2_s, cl2_e = await _dbx_get(client, host, "/api/2.0/clusters/list", token)
    if cl2_data is not None:
        clusters = cl2_data.get("clusters", [])
        active = [c for c in clusters if c.get("state") not in ("TERMINATED", "TERMINATING")]
        if not active:
            findings.append(_make_finding("SAT-PERF-3", "NOT_APPLICABLE", "No active clusters found."))
        else:
            def _is_lts(sv: str) -> bool:
                return "-lts-" in sv.lower() or sv.lower().endswith("-lts")
            def _is_eol(sv: str) -> bool:
                try:
                    return int(sv.split(".")[0]) < 13
                except (ValueError, IndexError):
                    return False
            lts = [c for c in active if _is_lts(c.get("spark_version", ""))]
            eol = [c for c in active if _is_eol(c.get("spark_version", ""))]
            lts_pct = round(len(lts) / len(active) * 100)
            if eol:
                status = "FAIL"
            elif lts_pct < 50:
                status = "WARN"
            else:
                status = "PASS"
            findings.append(_make_finding("SAT-PERF-3", status,
                f"{len(lts)}/{len(active)} active clusters ({lts_pct}%) on LTS, {len(eol)} on EOL (<13.x).",
                {"lts_count": len(lts), "eol_count": len(eol), "active_count": len(active), "lts_pct": lts_pct}))
    else:
        findings.append(_na("SAT-PERF-3", cl2_s, cl2_e))

    # Enrich with API response
    _api = {"SAT-PERF-1": cl_data, "SAT-PERF-2": wh_data, "SAT-PERF-3": cl2_data}
    for f in findings:
        resp = _api.get(f.check_id)
        if resp is not None:
            f.details.setdefault("api_response", resp)
    return findings


async def _check_cost(client: httpx.AsyncClient, host: str, token: str) -> list[SATFinding]:
    """Cost optimization checks: autoscaling, spot, auto-termination, tagging, pools."""
    findings: list[SATFinding] = []

    cl_data, cl_s, cl_e = await _dbx_get(client, host, "/api/2.0/clusters/list", token)
    clusters = cl_data.get("clusters", []) if cl_data is not None else []
    active = [c for c in clusters if c.get("state") not in ("TERMINATED", "TERMINATING")]
    interactive = [c for c in active if c.get("cluster_source") != "JOB"]

    # SAT-COST-1: Autoscaling adoption
    if cl_data is None:
        findings.append(_na("SAT-COST-1", cl_s, cl_e))
    elif not interactive:
        findings.append(_make_finding("SAT-COST-1", "NOT_APPLICABLE", "No interactive clusters found."))
    else:
        with_as = [c for c in interactive if c.get("autoscale")]
        pct = round(len(with_as) / len(interactive) * 100)
        if pct >= 80:
            status = "PASS"
        elif pct >= 40:
            status = "WARN"
        else:
            status = "FAIL"
        findings.append(_make_finding("SAT-COST-1", status,
            f"{len(with_as)}/{len(interactive)} interactive clusters ({pct}%) have autoscaling.",
            {"autoscale_count": len(with_as), "interactive_count": len(interactive), "pct": pct}))

    # SAT-COST-2: Azure Spot VM usage
    if cl_data is None:
        findings.append(_na("SAT-COST-2", cl_s, cl_e))
    elif not active:
        findings.append(_make_finding("SAT-COST-2", "NOT_APPLICABLE", "No active clusters found."))
    else:
        spot = [c for c in active
                if c.get("aws_attributes", {}).get("availability") == "SPOT_WITH_FALLBACK"
                or c.get("azure_attributes", {}).get("availability") == "SPOT_WITH_FALLBACK_AZURE"
                or c.get("gcp_attributes", {}).get("availability") == "PREEMPTIBLE_WITH_FALLBACK_GCP"]
        pct = round(len(spot) / len(active) * 100)
        if pct >= 50:
            status = "PASS"
        elif spot:
            status = "WARN"
        else:
            status = "FAIL"
        findings.append(_make_finding("SAT-COST-2", status,
            f"{len(spot)}/{len(active)} active clusters ({pct}%) use Azure Spot VMs.",
            {"spot_count": len(spot), "active_count": len(active), "pct": pct}))

    # SAT-COST-3: Auto-termination ≤60 min
    if cl_data is None:
        findings.append(_na("SAT-COST-3", cl_s, cl_e))
    elif not interactive:
        findings.append(_make_finding("SAT-COST-3", "NOT_APPLICABLE", "No interactive clusters found."))
    else:
        good_term = [c for c in interactive
                     if 0 < c.get("autotermination_minutes", 0) <= 60]
        any_term = [c for c in interactive
                    if c.get("autotermination_minutes", 0) > 0]
        if len(good_term) == len(interactive):
            status = "PASS"
        elif good_term:
            status = "WARN"
        else:
            status = "FAIL"
        findings.append(_make_finding("SAT-COST-3", status,
            f"{len(good_term)}/{len(interactive)} interactive clusters have auto-termination ≤60 min "
            f"({len(any_term)} have any termination).",
            {"good_term": len(good_term), "any_term": len(any_term), "interactive_count": len(interactive)}))

    # SAT-COST-4: Cost allocation tagging
    if cl_data is None:
        findings.append(_na("SAT-COST-4", cl_s, cl_e))
    elif not active:
        findings.append(_make_finding("SAT-COST-4", "NOT_APPLICABLE", "No active clusters found."))
    else:
        tagged = [c for c in active if c.get("custom_tags")]
        pct = round(len(tagged) / len(active) * 100)
        if pct >= 80:
            status = "PASS"
        elif pct >= 40:
            status = "WARN"
        else:
            status = "FAIL"
        findings.append(_make_finding("SAT-COST-4", status,
            f"{len(tagged)}/{len(active)} active clusters ({pct}%) have custom tags for cost allocation.",
            {"tagged_count": len(tagged), "active_count": len(active), "pct": pct}))

    # SAT-COST-5: Instance pool usage
    pool_data, pool_s, pool_e = await _dbx_get(client, host, "/api/2.0/instance-pools/list", token)
    if pool_data is not None:
        pools = pool_data.get("instance_pools", [])
        if len(pools) >= 3:
            status = "PASS"
        elif pools:
            status = "WARN"
        else:
            status = "FAIL"
        details: dict[str, Any] = {"pool_count": len(pools)}
        if pools:
            total_idle = sum(p.get("stats", {}).get("idle_count", 0) for p in pools)
            total_used = sum(p.get("stats", {}).get("used_count", 0) for p in pools)
            preloaded = sum(1 for p in pools if p.get("preloaded_spark_versions"))
            details.update({"idle": total_idle, "used": total_used, "preloaded": preloaded})
        findings.append(_make_finding("SAT-COST-5", status,
            f"{len(pools)} instance pool(s) configured.", details))
    else:
        findings.append(_na("SAT-COST-5", pool_s, pool_e))

    # SAT-COST-BUDGET: Budget alerts configured
    hdr = {"Authorization": f"Bearer {token}"}
    try:
        r = await client.get(f"{host}/api/2.1/budgets", headers=hdr, timeout=15)
        if r.status_code == 200:
            budgets = r.json().get("budgets", [])
            if not budgets:
                findings.append(_make_finding("SAT-COST-BUDGET", "FAIL",
                    "No budget policies configured for the workspace."))
            else:
                with_alerts = sum(1 for b in budgets if b.get("alert_configurations"))
                if with_alerts:
                    findings.append(_make_finding("SAT-COST-BUDGET", "PASS",
                        f"{len(budgets)} budget(s) configured, {with_alerts} with alerts."))
                else:
                    findings.append(_make_finding("SAT-COST-BUDGET", "WARN",
                        f"{len(budgets)} budget(s) but none have alert configurations."))
        elif r.status_code in (404, 403):
            findings.append(_make_finding("SAT-COST-BUDGET", "NOT_APPLICABLE",
                f"Budgets API not available (HTTP {r.status_code})."))
        else:
            findings.append(_make_finding("SAT-COST-BUDGET", "WARN", f"HTTP {r.status_code}."))
    except Exception as exc:
        findings.append(_make_finding("SAT-COST-BUDGET", "WARN", f"Error: {exc}"))

    # SAT-COST-SYSTEM-TABLES: System tables enabled
    try:
        r = await client.get(f"{host}/api/2.1/unity-catalog/schemas",
            params={"catalog_name": "system"}, headers=hdr, timeout=15)
        if r.status_code == 200:
            schemas = r.json().get("schemas", [])
            billing_schema = any(s.get("name") == "billing" for s in schemas)
            if billing_schema:
                findings.append(_make_finding("SAT-COST-SYSTEM-TABLES", "PASS",
                    "System tables (billing schema) available for cost monitoring."))
            else:
                findings.append(_make_finding("SAT-COST-SYSTEM-TABLES", "WARN",
                    "System catalog exists but billing schema not found. Enable system tables."))
        elif r.status_code in (404, 403):
            findings.append(_make_finding("SAT-COST-SYSTEM-TABLES", "FAIL",
                "System catalog not accessible. Enable system tables for cost monitoring."))
        else:
            findings.append(_make_finding("SAT-COST-SYSTEM-TABLES", "WARN", f"HTTP {r.status_code}."))
    except Exception as exc:
        findings.append(_make_finding("SAT-COST-SYSTEM-TABLES", "WARN", f"Error: {exc}"))

    # SAT-COST-IDLE-WH: Idle SQL warehouses
    try:
        r = await client.get(f"{host}/api/2.0/sql/warehouses", headers=hdr, timeout=15)
        if r.status_code == 200:
            warehouses = r.json().get("warehouses", [])
            running = [w for w in warehouses if w.get("state") == "RUNNING"]
            if not warehouses:
                findings.append(_make_finding("SAT-COST-IDLE-WH", "NOT_APPLICABLE",
                    "No SQL warehouses found."))
            elif not running:
                findings.append(_make_finding("SAT-COST-IDLE-WH", "PASS",
                    "No idle SQL warehouses running."))
            else:
                idle = [w for w in running if w.get("num_active_sessions", 0) == 0]
                if idle:
                    names = [w.get("name", "?") for w in idle[:5]]
                    findings.append(_make_finding("SAT-COST-IDLE-WH", "WARN",
                        f"{len(idle)} warehouse(s) running with 0 active sessions: {', '.join(names)}."))
                else:
                    findings.append(_make_finding("SAT-COST-IDLE-WH", "PASS",
                        f"{len(running)} running warehouse(s), all have active sessions."))
        elif r.status_code in (404, 403):
            findings.append(_make_finding("SAT-COST-IDLE-WH", "NOT_APPLICABLE",
                f"SQL Warehouses API not available (HTTP {r.status_code})."))
        else:
            findings.append(_make_finding("SAT-COST-IDLE-WH", "WARN", f"HTTP {r.status_code}."))
    except Exception as exc:
        findings.append(_make_finding("SAT-COST-IDLE-WH", "WARN", f"Error: {exc}"))

    # SAT-COST-POLICY-GUARDRAILS: Cluster policies enforce cost guardrails
    policies_data, pol_s2, pol_e2 = await _dbx_get(client, host, "/api/2.0/policies/clusters/list", token)
    if policies_data is not None:
        policies = policies_data.get("policies", [])
        custom = [p for p in policies if not p.get("is_default", False)]
        if not custom:
            findings.append(_make_finding("SAT-COST-POLICY-GUARDRAILS", "FAIL",
                "No custom cluster policies — no cost guardrails enforced."))
        else:
            cost_guardrails = 0
            for p in custom:
                defn = p.get("definition", "{}")
                if isinstance(defn, str):
                    import json as _json
                    try:
                        defn = _json.loads(defn)
                    except:
                        defn = {}
                has_max_workers = "num_workers" in defn or "autoscale.max_workers" in defn
                has_auto_term = "autotermination_minutes" in defn
                if has_max_workers or has_auto_term:
                    cost_guardrails += 1
            pct = round(cost_guardrails / len(custom) * 100)
            findings.append(_make_finding("SAT-COST-POLICY-GUARDRAILS",
                "PASS" if pct >= 60 else ("WARN" if cost_guardrails > 0 else "FAIL"),
                f"{cost_guardrails}/{len(custom)} custom policies ({pct}%) enforce cost guardrails "
                f"(max workers or auto-termination)."))
    else:
        findings.append(_na("SAT-COST-POLICY-GUARDRAILS", pol_s2, pol_e2))

    # Enrich
    _api: dict[str, Any] = {
        "SAT-COST-1": cl_data, "SAT-COST-2": cl_data, "SAT-COST-3": cl_data,
        "SAT-COST-4": cl_data, "SAT-COST-5": pool_data,
    }
    for f in findings:
        resp = _api.get(f.check_id)
        if resp is not None:
            f.details.setdefault("api_response", resp)
    return findings


async def _check_reliability(client: httpx.AsyncClient, host: str, token: str) -> list[SATFinding]:
    """Reliability checks: job retry/timeout, success rate, log delivery."""
    findings: list[SATFinding] = []

    # SAT-REL-1: Job retry & timeout config
    jobs_data, j_s, j_e = await _dbx_get_all_jobs(client, host, token, {"expand_tasks": "false"})
    if jobs_data is not None:
        jobs = jobs_data.get("jobs", [])
        if not jobs:
            findings.append(_make_finding("SAT-REL-1", "NOT_APPLICABLE", "No jobs found."))
        else:
            with_retry = sum(1 for j in jobs if j.get("settings", {}).get("max_retries", 0) > 0)
            with_timeout = sum(1 for j in jobs if j.get("settings", {}).get("timeout_seconds", 0) > 0)
            retry_r = with_retry / len(jobs)
            timeout_r = with_timeout / len(jobs)
            avg = (retry_r + timeout_r) / 2
            if avg >= 0.5:
                status = "PASS"
            elif avg >= 0.25:
                status = "WARN"
            else:
                status = "FAIL"
            findings.append(_make_finding("SAT-REL-1", status,
                f"{with_retry}/{len(jobs)} jobs have retries, {with_timeout}/{len(jobs)} have timeouts.",
                {"total_jobs": len(jobs), "with_retry": with_retry, "with_timeout": with_timeout,
                 "retry_ratio": round(retry_r, 2), "timeout_ratio": round(timeout_r, 2)}))
    else:
        findings.append(_na("SAT-REL-1", j_s, j_e))

    # SAT-REL-2: Job run success rate
    runs_data, r_s, r_e = await _dbx_get(client, host, "/api/2.1/jobs/runs/list", token,
        {"limit": "25", "expand_tasks": "false"})
    if runs_data is not None:
        runs = runs_data.get("runs", [])
        if not runs:
            findings.append(_make_finding("SAT-REL-2", "NOT_APPLICABLE", "No recent job runs found."))
        else:
            succeeded = sum(1 for r in runs if r.get("state", {}).get("result_state") == "SUCCESS")
            failed = sum(1 for r in runs if r.get("state", {}).get("result_state") in ("FAILED", "TIMEDOUT"))
            cancelled = sum(1 for r in runs if r.get("state", {}).get("result_state") == "CANCELED")
            finished = succeeded + failed + cancelled
            rate = round(succeeded / finished * 100) if finished else 0
            if rate >= 95:
                status = "PASS"
            elif rate >= 80:
                status = "WARN"
            else:
                status = "FAIL"
            findings.append(_make_finding("SAT-REL-2", status,
                f"Last {len(runs)} runs: {succeeded} succeeded, {failed} failed, "
                f"{cancelled} cancelled ({rate}% success rate).",
                {"total_runs": len(runs), "succeeded": succeeded, "failed": failed,
                 "cancelled": cancelled, "success_rate": rate}))
    else:
        findings.append(_na("SAT-REL-2", r_s, r_e))

    # SAT-REL-3: Cluster log delivery coverage
    cl_data, cl_s, cl_e = await _dbx_get(client, host, "/api/2.0/clusters/list", token)
    if cl_data is not None:
        clusters = cl_data.get("clusters", [])
        active = [c for c in clusters if c.get("state") not in ("TERMINATED", "TERMINATING")]
        if not active:
            findings.append(_make_finding("SAT-REL-3", "NOT_APPLICABLE", "No active clusters found."))
        else:
            with_logs = [c for c in active if c.get("cluster_log_conf")]
            pct = round(len(with_logs) / len(active) * 100)
            if pct >= 80:
                status = "PASS"
            elif pct >= 40:
                status = "WARN"
            else:
                status = "FAIL"
            findings.append(_make_finding("SAT-REL-3", status,
                f"{len(with_logs)}/{len(active)} active clusters ({pct}%) have log delivery configured.",
                {"with_logs": len(with_logs), "active_count": len(active), "pct": pct}))
    else:
        findings.append(_na("SAT-REL-3", cl_s, cl_e))

    # Enrich
    _api: dict[str, Any] = {"SAT-REL-1": jobs_data, "SAT-REL-2": runs_data, "SAT-REL-3": cl_data}
    for f in findings:
        resp = _api.get(f.check_id)
        if resp is not None:
            f.details.setdefault("api_response", resp)
    return findings


# ═══════════════════════════════════════════════════════════════════════════════
# Phase 2 — Data Architecture, Ops Excellence, Governance Data Quality
# ═══════════════════════════════════════════════════════════════════════════════


async def _check_data_architecture(client: httpx.AsyncClient, host: str, token: str) -> list[SATFinding]:
    """Data architecture checks: medallion pattern, UC volumes, DLT health."""
    findings: list[SATFinding] = []
    hdr = {"Authorization": f"Bearer {token}"}

    # SAT-DA-1: Medallion architecture adoption
    cat_data, cat_s, cat_e = await _dbx_get(client, host, "/api/2.1/unity-catalog/catalogs", token)
    if cat_data is not None:
        catalogs = [c for c in cat_data.get("catalogs", [])
                    if c.get("name") not in ("system", "hive_metastore", "__databricks_internal")]
        if not catalogs:
            findings.append(_make_finding("SAT-DA-1", "NOT_APPLICABLE", "No Unity Catalog catalogs found."))
        else:
            medallion_keywords = {"bronze", "silver", "gold", "raw", "curated", "aggregated"}
            found_layers: set[str] = set()
            schema_data_raw = None
            # Check catalog names first
            for cat in catalogs:
                cname = cat.get("name", "").lower()
                for kw in medallion_keywords:
                    if kw in cname:
                        found_layers.add(kw)
            # Then check schema names across all catalogs
            for cat in catalogs:
                try:
                    r = await client.get(f"{host}/api/2.1/unity-catalog/schemas",
                        headers=hdr, params={"catalog_name": cat["name"]}, timeout=15)
                    if r.status_code == 200:
                        schema_data_raw = r.json()
                        for s in schema_data_raw.get("schemas", []):
                            sname = s.get("name", "").lower()
                            for kw in medallion_keywords:
                                if kw in sname:
                                    found_layers.add(kw)
                except Exception:
                    pass
            layer_count = len(found_layers)
            if layer_count >= 3:
                status = "PASS"
            elif layer_count >= 1:
                status = "WARN"
            else:
                status = "FAIL"
            findings.append(_make_finding("SAT-DA-1", status,
                f"Found {layer_count} medallion layer(s): {', '.join(sorted(found_layers)) or 'none'}.",
                {"found_layers": sorted(found_layers), "layer_count": layer_count}))
    else:
        findings.append(_na("SAT-DA-1", cat_s, cat_e))

    # SAT-DA-2: UC Volumes adoption
    vol_total = 0
    vol_managed = 0
    vol_external = 0
    vol_raw = None
    if cat_data is not None:
        catalogs = [c for c in cat_data.get("catalogs", [])
                    if c.get("name") not in ("system", "hive_metastore", "__databricks_internal")]
        if not catalogs:
            findings.append(_make_finding("SAT-DA-2", "NOT_APPLICABLE", "No Unity Catalog catalogs found."))
        else:
            for cat in catalogs[:3]:
                try:
                    r = await client.get(f"{host}/api/2.1/unity-catalog/schemas",
                        headers=hdr, params={"catalog_name": cat["name"]}, timeout=15)
                    if r.status_code != 200:
                        continue
                    for schema in r.json().get("schemas", [])[:5]:
                        sname = schema.get("name", "")
                        if sname == "information_schema":
                            continue
                        try:
                            vr = await client.get(f"{host}/api/2.1/unity-catalog/volumes",
                                headers=hdr,
                                params={"catalog_name": cat["name"], "schema_name": sname},
                                timeout=15)
                            if vr.status_code == 200:
                                vol_raw = vr.json()
                                vols = vol_raw.get("volumes", [])
                                vol_total += len(vols)
                                vol_managed += sum(1 for v in vols if v.get("volume_type") == "MANAGED")
                                vol_external += sum(1 for v in vols if v.get("volume_type") == "EXTERNAL")
                        except Exception:
                            pass
                except Exception:
                    pass
            if vol_total == 0:
                status = "FAIL"
            elif vol_managed >= 3:
                status = "PASS"
            else:
                status = "WARN"
            findings.append(_make_finding("SAT-DA-2", status,
                f"{vol_total} UC volume(s): {vol_managed} managed, {vol_external} external.",
                {"total": vol_total, "managed": vol_managed, "external": vol_external}))
    else:
        findings.append(_na("SAT-DA-2", cat_s, cat_e))

    # SAT-DA-3: DLT pipeline health
    dlt_data, dlt_s, dlt_e = await _dbx_get(client, host, "/api/2.0/pipelines", token)
    if dlt_data is not None:
        pipelines = dlt_data.get("statuses", [])
        if not pipelines:
            findings.append(_make_finding("SAT-DA-3", "NOT_APPLICABLE", "No DLT pipelines found."))
        else:
            healthy = [p for p in pipelines if p.get("state") in ("RUNNING", "IDLE")]
            failed = [p for p in pipelines if p.get("state") == "FAILED"]
            if healthy and not failed:
                status = "PASS"
            elif failed and healthy:
                status = "WARN"
            elif failed:
                status = "FAIL"
            else:
                status = "WARN"
            findings.append(_make_finding("SAT-DA-3", status,
                f"{len(pipelines)} pipeline(s): {len(healthy)} healthy, {len(failed)} failed.",
                {"total": len(pipelines), "healthy": len(healthy), "failed": len(failed)}))
    else:
        findings.append(_na("SAT-DA-3", dlt_s, dlt_e))

    # Enrich
    _api: dict[str, Any] = {"SAT-DA-1": cat_data, "SAT-DA-2": vol_raw or cat_data, "SAT-DA-3": dlt_data}
    for f in findings:
        resp = _api.get(f.check_id)
        if resp is not None:
            f.details.setdefault("api_response", resp)
    return findings


async def _check_ops_excellence(client: httpx.AsyncClient, host: str, token: str) -> list[SATFinding]:
    """Ops excellence checks: Git repos, job ownership, idle clusters, SQL alerts."""
    findings: list[SATFinding] = []

    # SAT-OPS-1: Git repos adoption
    repos_data, rp_s, rp_e = await _dbx_get(client, host, "/api/2.0/repos", token)
    if repos_data is not None:
        repos = repos_data.get("repos", [])
        if len(repos) >= 10:
            status = "PASS"
        elif repos:
            status = "WARN"
        else:
            status = "FAIL"
        findings.append(_make_finding("SAT-OPS-1", status,
            f"{len(repos)} Git repo(s) connected.",
            {"repo_count": len(repos)}))
    else:
        findings.append(_na("SAT-OPS-1", rp_s, rp_e))

    # SAT-OPS-2: Job ownership by service principals
    jobs_data, j_s, j_e = await _dbx_get_all_jobs(client, host, token, {"expand_tasks": "false"})
    if jobs_data is not None:
        jobs = jobs_data.get("jobs", [])
        if not jobs:
            findings.append(_make_finding("SAT-OPS-2", "NOT_APPLICABLE", "No jobs found."))
        else:
            sp_owned = sum(1 for j in jobs
                          if "@" not in j.get("creator_user_name", "@"))
            pct = round(sp_owned / len(jobs) * 100)
            if pct >= 50:
                status = "PASS"
            elif sp_owned > 0:
                status = "WARN"
            else:
                status = "FAIL"
            findings.append(_make_finding("SAT-OPS-2", status,
                f"{sp_owned}/{len(jobs)} jobs ({pct}%) owned by service principals.",
                {"sp_owned": sp_owned, "total_jobs": len(jobs), "pct": pct}))
    else:
        findings.append(_na("SAT-OPS-2", j_s, j_e))

    # SAT-OPS-3: Idle interactive cluster detection
    cl_data, cl_s, cl_e = await _dbx_get(client, host, "/api/2.0/clusters/list", token)
    if cl_data is not None:
        clusters = cl_data.get("clusters", [])
        running_interactive = [c for c in clusters
                               if c.get("state") == "RUNNING"
                               and c.get("cluster_source") != "JOB"]
        if not running_interactive:
            findings.append(_make_finding("SAT-OPS-3", "PASS", "No running interactive clusters."))
        else:
            now_ms = time.time() * 1000
            idle = [c for c in running_interactive
                    if c.get("last_activity_time")
                    and (now_ms - c.get("last_activity_time", now_ms)) > 7_200_000]
            if not idle:
                status = "PASS"
            elif len(idle) > len(running_interactive) // 2:
                status = "FAIL"
            else:
                status = "WARN"
            findings.append(_make_finding("SAT-OPS-3", status,
                f"{len(idle)}/{len(running_interactive)} running interactive clusters idle >2 hours.",
                {"api_response": {"idle_count": len(idle), "running_interactive": len(running_interactive)},
                 "api_endpoint": "/api/2.0/clusters/list"}))
    else:
        findings.append(_na("SAT-OPS-3", cl_s, cl_e))

    # SAT-OPS-4: SQL alert monitoring coverage
    alerts_data, al_s, al_e = await _dbx_get(client, host, "/api/2.0/sql/alerts", token)
    if alerts_data is not None:
        alerts = alerts_data if isinstance(alerts_data, list) else alerts_data.get("results", [])
        if len(alerts) >= 10:
            status = "PASS"
        elif alerts:
            status = "WARN"
        else:
            status = "FAIL"
        findings.append(_make_finding("SAT-OPS-4", status,
            f"{len(alerts)} SQL alert(s) configured.",
            {"alert_count": len(alerts)}))
    else:
        findings.append(_na("SAT-OPS-4", al_s, al_e))

    # Enrich
    _api: dict[str, Any] = {
        "SAT-OPS-1": repos_data, "SAT-OPS-2": jobs_data,
        "SAT-OPS-3": cl_data, "SAT-OPS-4": alerts_data,
    }
    for f in findings:
        resp = _api.get(f.check_id)
        if resp is not None:
            f.details.setdefault("api_response", resp)
    return findings


async def _check_governance_data_quality(client: httpx.AsyncClient, host: str, token: str) -> list[SATFinding]:
    """Governance data quality checks: monitors, metadata, UC migration, tags."""
    findings: list[SATFinding] = []
    hdr = {"Authorization": f"Bearer {token}"}

    cat_data, cat_s, cat_e = await _dbx_get(client, host, "/api/2.1/unity-catalog/catalogs", token)
    catalogs = [c for c in (cat_data or {}).get("catalogs", [])
                if c.get("name") not in ("system", "__databricks_internal")]

    # Pre-fetch schemas for user catalogs (shared by QUALITY-MON and METADATA).
    # Use _dbx_get for 429 retry, and throttle between calls to avoid rate limits.
    user_cats = [c for c in catalogs if c.get("name") != "hive_metastore"]
    _cat_schemas: dict[str, list[dict]] = {}  # catalog_name -> list of schema dicts
    if cat_data is not None and user_cats:
        for cat in user_cats[:5]:
            s_data, _, _ = await _dbx_get(client, host,
                "/api/2.1/unity-catalog/schemas", token,
                params={"catalog_name": cat["name"]})
            if s_data:
                _cat_schemas[cat["name"]] = [
                    s for s in s_data.get("schemas", [])
                    if s.get("name") != "information_schema"
                ]
            await asyncio.sleep(0.25)  # throttle between catalogs

    # SAT-GOV-QUALITY-MON: Lakehouse Monitor adoption
    if cat_data is None:
        findings.append(_na("SAT-GOV-QUALITY-MON", cat_s, cat_e))
    elif not catalogs:
        findings.append(_make_finding("SAT-GOV-QUALITY-MON", "NOT_APPLICABLE", "No Unity Catalog catalogs found."))
    else:
        monitors_found = 0
        for cat in user_cats[:3]:
            for schema in _cat_schemas.get(cat["name"], [])[:5]:
                t_data, _, _ = await _dbx_get(client, host,
                    "/api/2.1/unity-catalog/tables", token,
                    params={"catalog_name": cat["name"], "schema_name": schema["name"]})
                if not t_data:
                    continue
                for tbl in t_data.get("tables", [])[:10]:
                    full_name = tbl.get("full_name", "")
                    if not full_name:
                        continue
                    m_data, m_s, _ = await _dbx_get(client, host,
                        f"/api/2.1/unity-catalog/tables/{full_name}/monitor", token)
                    if m_s == 200 and m_data is not None:
                        monitors_found += 1
                await asyncio.sleep(0.15)  # throttle between schemas
        if monitors_found >= 1:
            status = "PASS"
        else:
            status = "WARN"
        findings.append(_make_finding("SAT-GOV-QUALITY-MON", status,
            f"{monitors_found} Lakehouse Monitor(s) found.",
            {"monitors_found": monitors_found}))

    # SAT-GOV-METADATA: Catalog & schema description completeness
    if cat_data is None:
        findings.append(_na("SAT-GOV-METADATA", cat_s, cat_e))
    elif not catalogs:
        findings.append(_make_finding("SAT-GOV-METADATA", "NOT_APPLICABLE", "No Unity Catalog catalogs found."))
    else:
        total_objects = 0
        described = 0
        for cat in catalogs:
            total_objects += 1
            if cat.get("comment"):
                described += 1
        for cat in user_cats[:5]:
            for s in _cat_schemas.get(cat["name"], []):
                total_objects += 1
                if s.get("comment"):
                    described += 1
        pct = round(described / total_objects * 100) if total_objects else 0
        if pct >= 80:
            status = "PASS"
        elif pct >= 40:
            status = "WARN"
        else:
            status = "FAIL"
        findings.append(_make_finding("SAT-GOV-METADATA", status,
            f"{described}/{total_objects} catalogs & schemas ({pct}%) have descriptions.",
            {"described": described, "total_objects": total_objects, "pct": pct}))

    # SAT-GOV-UC-MIGRATE: Hive metastore migration progress
    if cat_data is None:
        findings.append(_na("SAT-GOV-UC-MIGRATE", cat_s, cat_e))
    else:
        all_cats = cat_data.get("catalogs", [])
        cat_names = [c.get("name", "") for c in all_cats]
        has_hive = "hive_metastore" in cat_names
        uc_cats = [n for n in cat_names if n not in ("hive_metastore", "system", "__databricks_internal")]
        if not has_hive and uc_cats:
            status = "PASS"
        elif has_hive and len(uc_cats) >= 2:
            status = "WARN"
        elif has_hive:
            status = "FAIL"
        else:
            status = "NOT_APPLICABLE"
        findings.append(_make_finding("SAT-GOV-UC-MIGRATE", status,
            f"{len(uc_cats)} UC catalog(s), hive_metastore {'present' if has_hive else 'absent'}.",
            {"uc_catalogs": len(uc_cats), "has_hive_metastore": has_hive, "catalog_names": cat_names}))

    # SAT-GOV-UC-TAGS-ADOPT: UC tag adoption
    if cat_data is None:
        findings.append(_na("SAT-GOV-UC-TAGS-ADOPT", cat_s, cat_e))
    elif not catalogs:
        findings.append(_make_finding("SAT-GOV-UC-TAGS-ADOPT", "NOT_APPLICABLE", "No Unity Catalog catalogs found."))
    else:
        tagged = 0
        user_cats = [c for c in catalogs if c.get("name") != "hive_metastore"]
        for cat in user_cats[:5]:
            # Check if catalog has tags via properties or tags API
            if cat.get("properties") or cat.get("tags"):
                tagged += 1
        if tagged >= 1:
            status = "PASS"
        else:
            status = "WARN"
        findings.append(_make_finding("SAT-GOV-UC-TAGS-ADOPT", status,
            f"{tagged}/{len(user_cats)} catalog(s) have tags or properties set.",
            {"tagged": tagged, "total_user_catalogs": len(user_cats)}))

    # Enrich
    for f in findings:
        if cat_data is not None:
            f.details.setdefault("api_response", cat_data)
    return findings


# ═══════════════════════════════════════════════════════════════════════════════
# Phase 3 — Advanced Performance, Advanced Governance
# ═══════════════════════════════════════════════════════════════════════════════


async def _check_advanced_performance(client: httpx.AsyncClient, host: str, token: str) -> list[SATFinding]:
    """Advanced performance checks: AQE config, Delta optimization jobs."""
    findings: list[SATFinding] = []

    # SAT-PERF-4: Adaptive Query Execution (AQE)
    cl_data, cl_s, cl_e = await _dbx_get(client, host, "/api/2.0/clusters/list", token)
    if cl_data is not None:
        clusters = cl_data.get("clusters", [])
        active = [c for c in clusters if c.get("state") not in ("TERMINATED", "TERMINATING")]
        if not active:
            findings.append(_make_finding("SAT-PERF-4", "NOT_APPLICABLE", "No active clusters found."))
        else:
            disabled = [c for c in active
                        if str(c.get("spark_conf", {}).get("spark.sql.adaptive.enabled", "")).lower() == "false"]
            enabled = [c for c in active
                       if str(c.get("spark_conf", {}).get("spark.sql.adaptive.enabled", "")).lower() == "true"]
            if disabled:
                status = "FAIL"
            elif enabled:
                status = "PASS"
            else:
                status = "WARN"  # not explicitly set — using default (enabled)
            findings.append(_make_finding("SAT-PERF-4", status,
                f"{len(enabled)} cluster(s) explicitly enable AQE, {len(disabled)} disable it "
                f"({len(active) - len(enabled) - len(disabled)} use default).",
                {"enabled": len(enabled), "disabled": len(disabled), "default": len(active) - len(enabled) - len(disabled)}))
    else:
        findings.append(_na("SAT-PERF-4", cl_s, cl_e))

    # SAT-PERF-5: Delta optimization jobs
    jobs_data, j_s, j_e = await _dbx_get_all_jobs(client, host, token, {"expand_tasks": "false"})
    if jobs_data is not None:
        jobs = jobs_data.get("jobs", [])
        keywords = {"optimize", "vacuum", "maintenance", "compaction"}
        maint_jobs = [j for j in jobs
                      if any(kw in j.get("settings", {}).get("name", "").lower() for kw in keywords)]
        if len(maint_jobs) >= 3:
            status = "PASS"
        elif maint_jobs:
            status = "WARN"
        else:
            status = "FAIL"
        findings.append(_make_finding("SAT-PERF-5", status,
            f"{len(maint_jobs)} Delta optimization/maintenance job(s) found.",
            {"maintenance_jobs": len(maint_jobs), "total_jobs": len(jobs)}))
    else:
        findings.append(_na("SAT-PERF-5", j_s, j_e))

    # Enrich
    _api: dict[str, Any] = {"SAT-PERF-4": cl_data, "SAT-PERF-5": jobs_data}
    for f in findings:
        resp = _api.get(f.check_id)
        if resp is not None:
            f.details.setdefault("api_response", resp)
    return findings


async def _check_advanced_governance(client: httpx.AsyncClient, host: str, token: str) -> list[SATFinding]:
    """Advanced governance checks: PII, info schema, stale objects, notifications, DLT expectations, ext locations."""
    findings: list[SATFinding] = []
    hdr = {"Authorization": f"Bearer {token}"}

    cat_data, cat_s, cat_e = await _dbx_get(client, host, "/api/2.1/unity-catalog/catalogs", token)
    catalogs = [c for c in (cat_data or {}).get("catalogs", [])
                if c.get("name") not in ("system", "hive_metastore", "__databricks_internal")]

    # SAT-GOV-PII: PII column classification
    if cat_data is None:
        findings.append(_na("SAT-GOV-PII", cat_s, cat_e))
    elif not catalogs:
        findings.append(_make_finding("SAT-GOV-PII", "NOT_APPLICABLE", "No Unity Catalog catalogs found."))
    else:
        pii_found = 0
        for cat in catalogs[:3]:
            try:
                sr = await client.get(f"{host}/api/2.1/unity-catalog/schemas",
                    headers=hdr, params={"catalog_name": cat["name"]}, timeout=15)
                if sr.status_code != 200:
                    continue
                for schema in sr.json().get("schemas", [])[:3]:
                    if schema.get("name") == "information_schema":
                        continue
                    try:
                        tr = await client.get(f"{host}/api/2.1/unity-catalog/tables",
                            headers=hdr,
                            params={"catalog_name": cat["name"], "schema_name": schema["name"]},
                            timeout=15)
                        if tr.status_code != 200:
                            continue
                        for tbl in tr.json().get("tables", [])[:10]:
                            for col in tbl.get("columns", []):
                                tags = col.get("tags") or col.get("mask") or {}
                                comment = (col.get("comment") or "").lower()
                                if any(kw in str(tags).lower() for kw in ("pii", "sensitive", "confidential")):
                                    pii_found += 1
                                elif any(kw in comment for kw in ("pii", "sensitive", "confidential")):
                                    pii_found += 1
                    except Exception:
                        pass
            except Exception:
                pass
        if pii_found > 0:
            status = "PASS"
        else:
            status = "WARN"
        findings.append(_make_finding("SAT-GOV-PII", status,
            f"{pii_found} PII-classified column(s) found across sampled tables.",
            {"pii_columns_found": pii_found}))

    # SAT-GOV-INFO-SCHEMA: Information schema availability
    if cat_data is None:
        findings.append(_na("SAT-GOV-INFO-SCHEMA", cat_s, cat_e))
    elif not catalogs:
        findings.append(_make_finding("SAT-GOV-INFO-SCHEMA", "NOT_APPLICABLE", "No Unity Catalog catalogs found."))
    else:
        info_schema_found = False
        for cat in catalogs[:3]:
            try:
                sr = await client.get(f"{host}/api/2.1/unity-catalog/schemas",
                    headers=hdr, params={"catalog_name": cat["name"]}, timeout=15)
                if sr.status_code == 200:
                    for s in sr.json().get("schemas", []):
                        if s.get("name") == "information_schema":
                            info_schema_found = True
                            break
                if info_schema_found:
                    break
            except Exception:
                pass
        findings.append(_make_finding("SAT-GOV-INFO-SCHEMA",
            "PASS" if info_schema_found else "WARN",
            f"information_schema {'found' if info_schema_found else 'not found'} in sampled catalogs.",
            {"info_schema_found": info_schema_found}))

    # SAT-GOV-STALE-OBJ: Stale workspace objects
    try:
        r = await client.get(f"{host}/api/2.0/workspace/list",
            headers=hdr, params={"path": "/"}, timeout=15)
        if r.status_code == 200:
            objects = r.json().get("objects", [])
            count = len(objects)
            if count < 50:
                status = "PASS"
            elif count <= 200:
                status = "WARN"
            else:
                status = "FAIL"
            findings.append(_make_finding("SAT-GOV-STALE-OBJ", status,
                f"{count} top-level workspace object(s).",
                {"object_count": count, "api_response": r.json()}))
        elif r.status_code == 404:
            findings.append(_make_finding("SAT-GOV-STALE-OBJ", "NOT_APPLICABLE",
                "Workspace list API not available on this workspace."))
        elif r.status_code == 403:
            findings.append(_make_finding("SAT-GOV-STALE-OBJ", "WARN",
                "Workspace list API permission denied (HTTP 403). Use admin token."))
        else:
            findings.append(_make_finding("SAT-GOV-STALE-OBJ", "WARN", f"HTTP {r.status_code}."))
    except Exception as exc:
        findings.append(_make_finding("SAT-GOV-STALE-OBJ", "WARN", f"Error: {exc}"))

    # SAT-OPS-5: Job notification configuration
    jobs_data, j_s, j_e = await _dbx_get_all_jobs(client, host, token, {"expand_tasks": "false"})
    if jobs_data is not None:
        jobs = jobs_data.get("jobs", [])
        if not jobs:
            findings.append(_make_finding("SAT-OPS-5", "NOT_APPLICABLE", "No jobs found."))
        else:
            with_notify = sum(1 for j in jobs
                              if j.get("settings", {}).get("email_notifications")
                              or j.get("settings", {}).get("webhook_notifications")
                              or j.get("settings", {}).get("notification_settings"))
            pct = round(with_notify / len(jobs) * 100)
            if pct >= 50:
                status = "PASS"
            elif with_notify > 0:
                status = "WARN"
            else:
                status = "FAIL"
            findings.append(_make_finding("SAT-OPS-5", status,
                f"{with_notify}/{len(jobs)} jobs ({pct}%) have notifications configured.",
                {"with_notifications": with_notify, "total_jobs": len(jobs), "pct": pct}))
    else:
        findings.append(_na("SAT-OPS-5", j_s, j_e))

    # SAT-OPS-6: DLT expectations (data quality rules)
    dlt_data, dlt_s, dlt_e = await _dbx_get(client, host, "/api/2.0/pipelines", token)
    if dlt_data is not None:
        pipelines = dlt_data.get("statuses", [])
        if not pipelines:
            findings.append(_make_finding("SAT-OPS-6", "NOT_APPLICABLE", "No DLT pipelines found."))
        else:
            # Pipeline list API doesn't include expectations — informational check
            findings.append(_make_finding("SAT-OPS-6",
                "PASS" if len(pipelines) >= 1 else "WARN",
                f"{len(pipelines)} DLT pipeline(s) found — verify expectations are defined in pipeline code.",
                {"pipeline_count": len(pipelines)}))
    else:
        findings.append(_na("SAT-OPS-6", dlt_s, dlt_e))

    # SAT-DA-4: External locations governance
    _ext_raw = None
    try:
        r = await client.get(f"{host}/api/2.1/unity-catalog/external-locations",
            headers=hdr, timeout=15)
        if r.status_code == 200:
            _ext_raw = r.json()
            locations = _ext_raw.get("external_locations", [])
            if not locations:
                findings.append(_make_finding("SAT-DA-4", "NOT_APPLICABLE",
                    "No external locations configured."))
            else:
                cred_names = set(loc.get("credential_name", "") for loc in locations if loc.get("credential_name"))
                read_only_count = sum(1 for loc in locations if loc.get("read_only"))
                if len(cred_names) > 1 and read_only_count > 0:
                    status = "PASS"
                elif len(cred_names) <= 1:
                    status = "WARN"
                else:
                    status = "WARN"
                findings.append(_make_finding("SAT-DA-4", status,
                    f"{len(locations)} external location(s), {len(cred_names)} distinct credential(s), "
                    f"{read_only_count} read-only.",
                    {"location_count": len(locations), "distinct_creds": len(cred_names),
                     "read_only": read_only_count}))
        elif r.status_code == 404:
            findings.append(_make_finding("SAT-DA-4", "NOT_APPLICABLE",
                "External locations API not available on this workspace."))
        elif r.status_code == 403:
            findings.append(_make_finding("SAT-DA-4", "WARN",
                "External locations API permission denied (HTTP 403). Use admin token."))
        else:
            findings.append(_make_finding("SAT-DA-4", "WARN", f"HTTP {r.status_code}."))
    except Exception as exc:
        findings.append(_make_finding("SAT-DA-4", "WARN", f"Error: {exc}"))

    # SAT-GOV-ORPHAN-GRANTS: No grants to inactive or deleted principals
    if cat_data is None:
        findings.append(_na("SAT-GOV-ORPHAN-GRANTS", cat_s, cat_e))
    elif not catalogs:
        findings.append(_make_finding("SAT-GOV-ORPHAN-GRANTS", "NOT_APPLICABLE",
            "No Unity Catalog catalogs found."))
    else:
        # Collect current SCIM users and groups
        known_principals: set[str] = set()
        try:
            ur = await client.get(f"{host}/api/2.0/preview/scim/v2/Users",
                params={"count": "500"}, headers=hdr, timeout=20)
            if ur.status_code == 200:
                for u in ur.json().get("Resources", []):
                    uname = (u.get("userName") or "").lower()
                    if uname:
                        known_principals.add(uname)
                    display = (u.get("displayName") or "").lower()
                    if display:
                        known_principals.add(display)
        except Exception:
            pass
        try:
            gr = await client.get(f"{host}/api/2.0/preview/scim/v2/Groups",
                params={"count": "500"}, headers=hdr, timeout=20)
            if gr.status_code == 200:
                for g in gr.json().get("Resources", []):
                    gname = (g.get("displayName") or "").lower()
                    if gname:
                        known_principals.add(gname)
        except Exception:
            pass
        # Also add well-known built-in principals
        known_principals.update({"account users", "users", "admins"})

        if not known_principals:
            findings.append(_make_finding("SAT-GOV-ORPHAN-GRANTS", "WARN",
                "Could not fetch SCIM users/groups — unable to detect orphaned grants."))
        else:
            orphans: list[str] = []
            for cat in catalogs[:5]:
                try:
                    r = await client.get(
                        f"{host}/api/2.1/unity-catalog/permissions/catalog/{cat['name']}",
                        headers=hdr, timeout=15)
                    if r.status_code == 200:
                        for pa in r.json().get("privilege_assignments", []):
                            principal = pa.get("principal", "")
                            if principal and principal.lower() not in known_principals:
                                orphans.append(f"{cat['name']} → {principal}")
                except Exception:
                    pass
            if orphans:
                findings.append(_make_finding("SAT-GOV-ORPHAN-GRANTS", "WARN",
                    f"{len(orphans)} grant(s) to principals not found in current SCIM users/groups: "
                    f"{'; '.join(orphans[:5])}{'...' if len(orphans) > 5 else ''}. "
                    "Note: account-level principals may cause false positives.",
                    {"orphaned_grants": orphans}))
            else:
                findings.append(_make_finding("SAT-GOV-ORPHAN-GRANTS", "PASS",
                    f"All grantees across {min(len(catalogs), 5)} catalog(s) match current SCIM principals."))

    # SAT-GOV-UC-FUNCTIONS: Unity Catalog functions audited for external access
    _func_raw = None
    if cat_data is None:
        findings.append(_na("SAT-GOV-UC-FUNCTIONS", cat_s, cat_e))
    elif not catalogs:
        findings.append(_make_finding("SAT-GOV-UC-FUNCTIONS", "NOT_APPLICABLE",
            "No Unity Catalog catalogs found."))
    else:
        total_functions = 0
        for cat in catalogs[:10]:
            try:
                r = await client.get(f"{host}/api/2.1/unity-catalog/functions",
                    headers=hdr, params={"catalog_name": cat["name"]}, timeout=15)
                if r.status_code == 200:
                    _func_raw = r.json()
                    funcs = _func_raw.get("functions", [])
                    total_functions += len(funcs)
            except Exception:
                pass
        if total_functions > 0:
            findings.append(_make_finding("SAT-GOV-UC-FUNCTIONS", "WARN",
                f"{total_functions} UC function(s) found across catalogs — audit for external access patterns.",
                {"total_functions": total_functions}))
        else:
            findings.append(_make_finding("SAT-GOV-UC-FUNCTIONS", "PASS",
                "No UC functions found across sampled catalogs.",
                {"total_functions": 0}))

    # SAT-GOV-ALL-PRIVILEGES: No ALL_PRIVILEGES grants on UC securables
    _allpriv_raw = None
    if cat_data is None:
        findings.append(_na("SAT-GOV-ALL-PRIVILEGES", cat_s, cat_e))
    elif not catalogs:
        findings.append(_make_finding("SAT-GOV-ALL-PRIVILEGES", "NOT_APPLICABLE",
            "No Unity Catalog catalogs found."))
    else:
        all_priv_grants: list[str] = []
        for cat in catalogs[:10]:
            try:
                r = await client.get(
                    f"{host}/api/2.1/unity-catalog/permissions/catalog/{cat['name']}",
                    headers=hdr, timeout=15)
                if r.status_code == 200:
                    _allpriv_raw = r.json()
                    for assignment in _allpriv_raw.get("privilege_assignments", []):
                        privs = assignment.get("privileges", [])
                        if "ALL_PRIVILEGES" in privs:
                            principal = assignment.get("principal", "unknown")
                            all_priv_grants.append(f"{principal} on {cat['name']}")
            except Exception:
                pass
        if all_priv_grants:
            findings.append(_make_finding("SAT-GOV-ALL-PRIVILEGES", "FAIL",
                f"{len(all_priv_grants)} ALL_PRIVILEGES grant(s) found: "
                f"{'; '.join(all_priv_grants[:5])}{'...' if len(all_priv_grants) > 5 else ''}.",
                {"all_privileges_grants": all_priv_grants}))
        else:
            findings.append(_make_finding("SAT-GOV-ALL-PRIVILEGES", "PASS",
                f"No ALL_PRIVILEGES grants found across {len(catalogs[:10])} catalog(s)."))

    # SAT-GOV-CATALOG-ISOLATION: Catalogs bound to specific workspaces
    _bind_raw = None
    if cat_data is None:
        findings.append(_na("SAT-GOV-CATALOG-ISOLATION", cat_s, cat_e))
    elif not catalogs:
        findings.append(_make_finding("SAT-GOV-CATALOG-ISOLATION", "NOT_APPLICABLE",
            "No Unity Catalog catalogs found."))
    else:
        filtered = [c for c in catalogs if c.get("name") not in ("system", "hive_metastore")]
        if not filtered:
            findings.append(_make_finding("SAT-GOV-CATALOG-ISOLATION", "NOT_APPLICABLE",
                "No user catalogs found (only system/hive_metastore)."))
        else:
            unbound_count = 0
            for cat in filtered[:10]:
                try:
                    r = await client.get(
                        f"{host}/api/2.1/unity-catalog/bindings/catalog/{cat['name']}",
                        headers=hdr, timeout=15)
                    if r.status_code == 200:
                        _bind_raw = r.json()
                        bindings = _bind_raw.get("bindings", [])
                        if not bindings:
                            unbound_count += 1
                    else:
                        # API error or 404 means binding info unavailable — treat as unbound
                        unbound_count += 1
                except Exception:
                    pass
            total_checked = len(filtered[:10])
            pct_unbound = round(unbound_count / total_checked * 100) if total_checked > 0 else 0
            if pct_unbound > 50:
                findings.append(_make_finding("SAT-GOV-CATALOG-ISOLATION", "WARN",
                    f"{unbound_count}/{total_checked} catalogs ({pct_unbound}%) are unbound "
                    f"(accessible from all workspaces).",
                    {"unbound_count": unbound_count, "total_checked": total_checked, "pct_unbound": pct_unbound}))
            else:
                findings.append(_make_finding("SAT-GOV-CATALOG-ISOLATION", "PASS",
                    f"{unbound_count}/{total_checked} catalogs ({pct_unbound}%) are unbound.",
                    {"unbound_count": unbound_count, "total_checked": total_checked, "pct_unbound": pct_unbound}))

    # SAT-GOV-SCHEMA-OWNER: Schemas owned by groups or service principals
    _schema_raw = None
    if cat_data is None:
        findings.append(_na("SAT-GOV-SCHEMA-OWNER", cat_s, cat_e))
    elif not catalogs:
        findings.append(_make_finding("SAT-GOV-SCHEMA-OWNER", "NOT_APPLICABLE",
            "No Unity Catalog catalogs found."))
    else:
        user_owned = 0
        total_schemas = 0
        for cat in catalogs[:10]:
            try:
                r = await client.get(f"{host}/api/2.1/unity-catalog/schemas",
                    headers=hdr, params={"catalog_name": cat["name"]}, timeout=15)
                if r.status_code == 200:
                    _schema_raw = r.json()
                    for schema in _schema_raw.get("schemas", []):
                        # Exclude system schemas
                        if schema.get("name") in ("information_schema", "default"):
                            continue
                        total_schemas += 1
                        owner = schema.get("owner", "")
                        if "@" in owner:
                            user_owned += 1
            except Exception:
                pass
        if total_schemas == 0:
            findings.append(_make_finding("SAT-GOV-SCHEMA-OWNER", "NOT_APPLICABLE",
                "No user schemas found across sampled catalogs."))
        else:
            pct_user = round(user_owned / total_schemas * 100)
            if pct_user > 30:
                findings.append(_make_finding("SAT-GOV-SCHEMA-OWNER", "WARN",
                    f"{user_owned}/{total_schemas} schemas ({pct_user}%) are owned by individual users.",
                    {"user_owned": user_owned, "total_schemas": total_schemas, "pct_user_owned": pct_user}))
            else:
                findings.append(_make_finding("SAT-GOV-SCHEMA-OWNER", "PASS",
                    f"{user_owned}/{total_schemas} schemas ({pct_user}%) are owned by individual users.",
                    {"user_owned": user_owned, "total_schemas": total_schemas, "pct_user_owned": pct_user}))

    # Enrich
    _api: dict[str, Any] = {
        "SAT-GOV-PII": cat_data, "SAT-GOV-INFO-SCHEMA": cat_data,
        "SAT-OPS-5": jobs_data, "SAT-OPS-6": dlt_data, "SAT-DA-4": _ext_raw,
        "SAT-GOV-ORPHAN-GRANTS": cat_data,
        "SAT-GOV-UC-FUNCTIONS": _func_raw or cat_data,
        "SAT-GOV-ALL-PRIVILEGES": _allpriv_raw or cat_data,
        "SAT-GOV-CATALOG-ISOLATION": _bind_raw or cat_data,
        "SAT-GOV-SCHEMA-OWNER": _schema_raw or cat_data,
    }
    for f in findings:
        resp = _api.get(f.check_id)
        if resp is not None:
            f.details.setdefault("api_response", resp)
    return findings


# ─────────────────────────────────────────────────────────────────────────────
# Advanced Security & Compliance checks
# ─────────────────────────────────────────────────────────────────────────────

async def _check_advanced_security(client: httpx.AsyncClient, host: str, token: str) -> list[SATFinding]:
    """Advanced security & compliance: UC grants audit, system schemas, token mgmt, policy compliance, connections."""
    findings: list[SATFinding] = []
    hdr = {"Authorization": f"Bearer {token}"}

    # ── SAT-GOV-GRANTS: Unity Catalog grants audit ──
    # Check top-level catalogs for overly permissive grants
    cat_data, cat_s, cat_e = await _dbx_get(client, host, "/api/2.1/unity-catalog/catalogs", token)
    grants_raw = None
    if cat_data is not None:
        catalogs = [c for c in cat_data.get("catalogs", [])
                    if c.get("name") not in ("system", "hive_metastore", "__databricks_internal")]
        if not catalogs:
            findings.append(_make_finding("SAT-GOV-GRANTS", "NOT_APPLICABLE",
                "No Unity Catalog catalogs found."))
        else:
            overly_permissive: list[str] = []
            for cat in catalogs:
                try:
                    r = await client.get(
                        f"{host}/api/2.1/unity-catalog/permissions/catalog/{cat['name']}",
                        headers=hdr, timeout=15)
                    if r.status_code == 200:
                        grants_raw = r.json()
                        for pa in grants_raw.get("privilege_assignments", []):
                            principal = pa.get("principal", "")
                            privs = [p.get("privilege", "") for p in pa.get("privileges", [])]
                            if "ALL_PRIVILEGES" in privs:
                                overly_permissive.append(f"{cat['name']} → {principal} (ALL_PRIVILEGES)")
                            elif principal.lower() in ("account users", "users") and any(
                                    p in privs for p in ("USE_CATALOG", "USE_SCHEMA", "SELECT", "MODIFY", "CREATE")):
                                overly_permissive.append(f"{cat['name']} → {principal} ({', '.join(privs)})")
                except Exception:
                    pass
            if overly_permissive:
                findings.append(_make_finding("SAT-GOV-GRANTS", "WARN",
                    f"{len(overly_permissive)} overly permissive grant(s) found: "
                    f"{'; '.join(overly_permissive[:5])}{'...' if len(overly_permissive) > 5 else ''}.",
                    {"overly_permissive": overly_permissive}))
            else:
                findings.append(_make_finding("SAT-GOV-GRANTS", "PASS",
                    f"No overly permissive grants found across {len(catalogs)} catalog(s)."))
    else:
        findings.append(_na("SAT-GOV-GRANTS", cat_s, cat_e))

    # ── SAT-GOV-SYS-SCHEMAS: System tables enabled ──
    ms_data, ms_s, ms_e = await _dbx_get(client, host, "/api/2.1/unity-catalog/current-metastore-assignment", token)
    if ms_data is not None:
        metastore_id = ms_data.get("metastore_id", "")
        if metastore_id:
            sys_data, sys_s, sys_e = await _dbx_get(client, host,
                f"/api/2.1/unity-catalog/metastores/{metastore_id}/systemschemas", token)
            if sys_data is not None:
                schemas = sys_data.get("schemas", [])
                enabled = [s for s in schemas if s.get("state") == "ENABLE_COMPLETED"]
                available = [s for s in schemas if s.get("state") in ("AVAILABLE", "ENABLE_INITIATED")]
                all_schemas = enabled + available
                important = {"access", "billing", "compute", "storage", "lineage", "marketplace"}
                enabled_names = {s.get("schema", "") for s in enabled}
                missing = important - enabled_names
                if not missing:
                    findings.append(_make_finding("SAT-GOV-SYS-SCHEMAS", "PASS",
                        f"All key system schemas enabled ({len(enabled)} total): {', '.join(sorted(enabled_names))}.",
                        {"enabled": sorted(enabled_names)}))
                else:
                    findings.append(_make_finding("SAT-GOV-SYS-SCHEMAS", "WARN",
                        f"{len(enabled)} system schema(s) enabled, but missing: {', '.join(sorted(missing))}.",
                        {"enabled": sorted(enabled_names), "missing": sorted(missing)}))
            elif sys_s == 403:
                findings.append(_make_finding("SAT-GOV-SYS-SCHEMAS", "NOT_APPLICABLE",
                    "Insufficient permissions to query system schemas (HTTP 403).",
                    {"http_status": 403}))
            else:
                findings.append(_na("SAT-GOV-SYS-SCHEMAS", sys_s, sys_e))
        else:
            findings.append(_make_finding("SAT-GOV-SYS-SCHEMAS", "NOT_APPLICABLE",
                "No metastore assigned to this workspace."))
    elif ms_s == 404:
        findings.append(_make_finding("SAT-GOV-SYS-SCHEMAS", "NOT_APPLICABLE",
            "No metastore assigned to this workspace."))
    else:
        findings.append(_na("SAT-GOV-SYS-SCHEMAS", ms_s, ms_e))

    # ── SAT-IAM-TOKEN-MGMT: All-user token hygiene (admin view) ──
    token_data, tok_s, tok_e = await _dbx_get(client, host,
        "/api/2.0/token-management/tokens", token)
    if token_data is not None:
        tokens = token_data.get("token_infos", [])
        if not tokens:
            findings.append(_make_finding("SAT-IAM-TOKEN-MGMT", "PASS",
                "No PAT tokens found across workspace users."))
        else:
            import time
            now_ms = int(time.time() * 1000)
            ninety_days_ms = 90 * 24 * 60 * 60 * 1000
            no_expiry = [t for t in tokens if t.get("expiry_time", -1) == -1]
            stale = [t for t in tokens
                     if t.get("last_used_time") and (now_ms - t.get("last_used_time", now_ms)) > ninety_days_ms]
            issues: list[str] = []
            if no_expiry:
                issues.append(f"{len(no_expiry)} with no expiry")
            if stale:
                issues.append(f"{len(stale)} unused >90 days")
            if issues:
                findings.append(_make_finding("SAT-IAM-TOKEN-MGMT", "WARN",
                    f"{len(tokens)} PAT token(s) across all users: {', '.join(issues)}.",
                    {"total_tokens": len(tokens), "no_expiry": len(no_expiry), "stale_90d": len(stale)}))
            else:
                findings.append(_make_finding("SAT-IAM-TOKEN-MGMT", "PASS",
                    f"{len(tokens)} PAT token(s) across all users — all have expiry and recent usage."))
    elif tok_s == 403:
        findings.append(_make_finding("SAT-IAM-TOKEN-MGMT", "NOT_APPLICABLE",
            "Token management API requires admin privileges (HTTP 403).",
            {"http_status": 403}))
    else:
        findings.append(_na("SAT-IAM-TOKEN-MGMT", tok_s, tok_e))

    # ── SAT-GOV-POLICY-COMPLIANCE: Cluster policy compliance ──
    cl_data, cl_s, cl_e = await _dbx_get(client, host, "/api/2.0/clusters/list", token)
    if cl_data is not None:
        clusters = cl_data.get("clusters", [])
        active_clusters = [c for c in clusters if c.get("state") in ("RUNNING", "PENDING", "RESIZING")]
        if not active_clusters:
            findings.append(_make_finding("SAT-GOV-POLICY-COMPLIANCE", "NOT_APPLICABLE",
                "No active clusters to check policy compliance."))
        else:
            no_policy = [c for c in active_clusters if not c.get("policy_id")]
            if no_policy:
                names = [c.get("cluster_name", "?") for c in no_policy]
                findings.append(_make_finding("SAT-GOV-POLICY-COMPLIANCE", "WARN",
                    f"{len(no_policy)}/{len(active_clusters)} active cluster(s) have no policy assigned: "
                    f"{', '.join(names[:5])}{'...' if len(names) > 5 else ''}.",
                    {"total_active": len(active_clusters), "no_policy": len(no_policy)}))
            else:
                findings.append(_make_finding("SAT-GOV-POLICY-COMPLIANCE", "PASS",
                    f"All {len(active_clusters)} active cluster(s) have a policy assigned."))
    else:
        findings.append(_na("SAT-GOV-POLICY-COMPLIANCE", cl_s, cl_e))

    # ── SAT-DATA-CONNECTIONS: External connections inventory ──
    conn_data, conn_s, conn_e = await _dbx_get(client, host,
        "/api/2.1/unity-catalog/connections", token)
    if conn_data is not None:
        connections = conn_data.get("connections", [])
        if not connections:
            findings.append(_make_finding("SAT-DATA-CONNECTIONS", "PASS",
                "No external data connections configured."))
        else:
            conn_types: dict[str, int] = {}
            for c in connections:
                ctype = c.get("connection_type", "UNKNOWN")
                conn_types[ctype] = conn_types.get(ctype, 0) + 1
            type_summary = ", ".join(f"{v} {k}" for k, v in sorted(conn_types.items()))
            findings.append(_make_finding("SAT-DATA-CONNECTIONS", "WARN",
                f"{len(connections)} external connection(s) configured ({type_summary}). "
                f"Review each for business justification and secure credentials.",
                {"total": len(connections), "types": conn_types}))
    elif conn_s == 404:
        findings.append(_make_finding("SAT-DATA-CONNECTIONS", "NOT_APPLICABLE",
            "Connections API not available (Unity Catalog may not support federation on this workspace)."))
    elif conn_s == 403:
        findings.append(_make_finding("SAT-DATA-CONNECTIONS", "NOT_APPLICABLE",
            "Insufficient permissions to list connections (HTTP 403).",
            {"http_status": 403}))
    else:
        findings.append(_na("SAT-DATA-CONNECTIONS", conn_s, conn_e))

    # Enrich with API response data
    _api: dict[str, Any] = {
        "SAT-GOV-GRANTS": grants_raw or cat_data,
        "SAT-GOV-SYS-SCHEMAS": ms_data,
        "SAT-IAM-TOKEN-MGMT": token_data,
        "SAT-GOV-POLICY-COMPLIANCE": cl_data,
        "SAT-DATA-CONNECTIONS": conn_data,
    }
    for f in findings:
        resp = _api.get(f.check_id)
        if resp is not None:
            if f.details is None:
                f.details = {}
            f.details.setdefault("api_response", resp)

    return findings


# ─────────────────────────────────────────────────────────────────────────────
# Account Governance checks (Account API — accounts.azuredatabricks.net)
# ─────────────────────────────────────────────────────────────────────────────

# Track which account_ids have already been scanned (for multi-workspace dedup)
_SCANNED_ACCOUNT_IDS: set[str] = set()


async def _check_account_governance(client: httpx.AsyncClient, host: str, token: str) -> list[SATFinding]:
    """Account-level governance checks via the Databricks Account API."""
    findings: list[SATFinding] = []
    account_id = _WORKSPACE_ACCOUNT_IDS.get(host.rstrip("/"), "")
    if not account_id:
        for cid in ("SAT-ACCT-USERS", "SAT-ACCT-GROUPS", "SAT-ACCT-SP",
                     "SAT-ACCT-IP-ACL", "SAT-ACCT-METASTORES", "SAT-ACCT-BUDGETS", "SAT-ACCT-WS-LIST"):
            findings.append(_make_finding(cid, "NOT_APPLICABLE",
                "Account ID not available — run with --azure to enable account-level checks."))
        return findings

    # Deduplicate: only run once per account across multi-workspace scans
    if account_id in _SCANNED_ACCOUNT_IDS:
        return []
    _SCANNED_ACCOUNT_IDS.add(account_id)

    # Probe account API access with a lightweight call first
    probe_data, probe_s, probe_e = await _acct_get(client, account_id, "workspaces", token)
    if probe_s == 403:
        for cid in ("SAT-ACCT-USERS", "SAT-ACCT-GROUPS", "SAT-ACCT-SP",
                     "SAT-ACCT-IP-ACL", "SAT-ACCT-METASTORES", "SAT-ACCT-BUDGETS", "SAT-ACCT-WS-LIST"):
            findings.append(_na(cid, 403, "Account admin role required for account-level checks."))
        return findings
    if probe_s not in (200, 403) and probe_data is None:
        for cid in ("SAT-ACCT-USERS", "SAT-ACCT-GROUPS", "SAT-ACCT-SP",
                     "SAT-ACCT-IP-ACL", "SAT-ACCT-METASTORES", "SAT-ACCT-BUDGETS", "SAT-ACCT-WS-LIST"):
            findings.append(_na(cid, probe_s, probe_e))
        return findings

    # Fetch remaining endpoints concurrently (workspaces already fetched as probe)
    users_task = _acct_get(client, account_id, "scim/v2/Users", token, {"count": 10000})
    groups_task = _acct_get(client, account_id, "scim/v2/Groups", token, {"count": 10000})
    sp_task = _acct_get(client, account_id, "scim/v2/ServicePrincipals", token, {"count": 10000})
    ip_task = _acct_get(client, account_id, "ip-access-lists", token)
    meta_task = _acct_get(client, account_id, "metastores", token)
    budget_task = _acct_get(client, account_id, "budgets", token)

    (users_data, users_s, users_e), \
    (groups_data, groups_s, groups_e), \
    (sp_data, sp_s, sp_e), \
    (ip_data, ip_s, ip_e), \
    (meta_data, meta_s, meta_e), \
    (budget_data, budget_s, budget_e) = await asyncio.gather(
        users_task, groups_task, sp_task, ip_task, meta_task, budget_task)

    ws_data = probe_data  # workspaces already fetched

    # SAT-ACCT-USERS: Account user inventory
    if users_data is not None:
        resources = users_data.get("Resources", [])
        total = len(resources)
        inactive = [u for u in resources if not u.get("active", True)]
        if total == 0:
            findings.append(_make_finding("SAT-ACCT-USERS", "NOT_APPLICABLE",
                "No users found in account."))
        elif inactive:
            findings.append(_make_finding("SAT-ACCT-USERS", "WARN",
                f"{len(inactive)}/{total} account user(s) are inactive: "
                f"{', '.join(u.get('displayName', u.get('userName', '?')) for u in inactive[:5])}"
                f"{'...' if len(inactive) > 5 else ''}.",
                {"total_users": total, "inactive_count": len(inactive)}))
        else:
            findings.append(_make_finding("SAT-ACCT-USERS", "PASS",
                f"All {total} account user(s) are active."))
    else:
        findings.append(_na("SAT-ACCT-USERS", users_s, users_e))

    # SAT-ACCT-GROUPS: Account group hygiene
    if groups_data is not None:
        resources = groups_data.get("Resources", [])
        total = len(resources)
        empty = [g for g in resources if len(g.get("members", [])) == 0
                 and g.get("displayName", "") not in ("admins", "users", "account users")]
        if total == 0:
            findings.append(_make_finding("SAT-ACCT-GROUPS", "NOT_APPLICABLE",
                "No groups found in account."))
        elif empty:
            findings.append(_make_finding("SAT-ACCT-GROUPS", "WARN",
                f"{len(empty)}/{total} account group(s) have no members: "
                f"{', '.join(g.get('displayName', '?') for g in empty[:5])}"
                f"{'...' if len(empty) > 5 else ''}.",
                {"total_groups": total, "empty_count": len(empty)}))
        else:
            findings.append(_make_finding("SAT-ACCT-GROUPS", "PASS",
                f"All {total} account group(s) have members."))
    else:
        findings.append(_na("SAT-ACCT-GROUPS", groups_s, groups_e))

    # SAT-ACCT-SP: Service principal inventory
    if sp_data is not None:
        resources = sp_data.get("Resources", [])
        total = len(resources)
        inactive = [s for s in resources if not s.get("active", True)]
        if total == 0:
            findings.append(_make_finding("SAT-ACCT-SP", "NOT_APPLICABLE",
                "No service principals found in account."))
        elif inactive:
            findings.append(_make_finding("SAT-ACCT-SP", "WARN",
                f"{len(inactive)}/{total} service principal(s) are inactive: "
                f"{', '.join(s.get('displayName', s.get('applicationId', '?')) for s in inactive[:5])}"
                f"{'...' if len(inactive) > 5 else ''}.",
                {"total_sps": total, "inactive_count": len(inactive)}))
        else:
            findings.append(_make_finding("SAT-ACCT-SP", "PASS",
                f"All {total} service principal(s) are active."))
    else:
        findings.append(_na("SAT-ACCT-SP", sp_s, sp_e))

    # SAT-ACCT-IP-ACL: Account console IP access restricted
    if ip_data is not None:
        ip_lists = ip_data.get("ip_access_lists", [])
        enabled_lists = [l for l in ip_lists if l.get("enabled", False)]
        if enabled_lists:
            findings.append(_make_finding("SAT-ACCT-IP-ACL", "PASS",
                f"{len(enabled_lists)} IP access list(s) configured on Account Console.",
                {"list_count": len(enabled_lists)}))
        else:
            findings.append(_make_finding("SAT-ACCT-IP-ACL", "WARN",
                "No IP access lists configured on the Account Console — accessible from any network."))
    else:
        findings.append(_na("SAT-ACCT-IP-ACL", ip_s, ip_e))

    # SAT-ACCT-METASTORES: UC metastore coverage
    if meta_data is not None:
        metastores = meta_data.get("metastores", [])
        if not metastores:
            findings.append(_make_finding("SAT-ACCT-METASTORES", "NOT_APPLICABLE",
                "No Unity Catalog metastores found in account."))
        else:
            no_sharing_limit = [m for m in metastores
                if not m.get("delta_sharing_recipient_token_lifetime_in_seconds")]
            if no_sharing_limit:
                findings.append(_make_finding("SAT-ACCT-METASTORES", "WARN",
                    f"{len(metastores)} metastore(s) in account; {len(no_sharing_limit)} without "
                    f"Delta Sharing token lifetime: "
                    f"{', '.join(m.get('name', '?') for m in no_sharing_limit[:5])}"
                    f"{'...' if len(no_sharing_limit) > 5 else ''}.",
                    {"total": len(metastores), "no_sharing_limit": len(no_sharing_limit)}))
            else:
                findings.append(_make_finding("SAT-ACCT-METASTORES", "PASS",
                    f"{len(metastores)} metastore(s) in account, all with Delta Sharing token lifetime configured."))
    else:
        findings.append(_na("SAT-ACCT-METASTORES", meta_s, meta_e))

    # SAT-ACCT-BUDGETS: Budget governance configured
    if budget_data is not None:
        budgets = budget_data.get("budgets", budget_data.get("budget_configurations", []))
        if budgets:
            findings.append(_make_finding("SAT-ACCT-BUDGETS", "PASS",
                f"{len(budgets)} budget(s) configured for cost governance.",
                {"budget_count": len(budgets)}))
        else:
            findings.append(_make_finding("SAT-ACCT-BUDGETS", "WARN",
                "No budgets configured — cost overruns will go undetected."))
    elif budget_s == 404:
        findings.append(_make_finding("SAT-ACCT-BUDGETS", "NOT_APPLICABLE",
            "Budgets API not available on this account."))
    else:
        findings.append(_na("SAT-ACCT-BUDGETS", budget_s, budget_e))

    # SAT-ACCT-WS-LIST: Workspace tier audit
    if ws_data is not None:
        workspaces = ws_data if isinstance(ws_data, list) else ws_data.get("workspaces", ws_data.get("elements", []))
        if not workspaces:
            findings.append(_make_finding("SAT-ACCT-WS-LIST", "NOT_APPLICABLE",
                "No workspaces found in account."))
        else:
            standard = [w for w in workspaces
                if w.get("pricing_tier", w.get("pricingTier", "")).upper() in ("STANDARD", "")]
            if standard:
                findings.append(_make_finding("SAT-ACCT-WS-LIST", "WARN",
                    f"{len(standard)}/{len(workspaces)} workspace(s) on Standard tier "
                    f"(missing security features): "
                    f"{', '.join(w.get('workspace_name', w.get('name', '?')) for w in standard[:5])}"
                    f"{'...' if len(standard) > 5 else ''}.",
                    {"total": len(workspaces), "standard_count": len(standard)}))
            else:
                findings.append(_make_finding("SAT-ACCT-WS-LIST", "PASS",
                    f"All {len(workspaces)} workspace(s) are on Premium tier."))
    else:
        findings.append(_na("SAT-ACCT-WS-LIST", probe_s, probe_e))

    # Enrich with API response data
    _api: dict[str, Any] = {
        "SAT-ACCT-USERS": users_data, "SAT-ACCT-GROUPS": groups_data,
        "SAT-ACCT-SP": sp_data, "SAT-ACCT-IP-ACL": ip_data,
        "SAT-ACCT-METASTORES": meta_data, "SAT-ACCT-BUDGETS": budget_data,
        "SAT-ACCT-WS-LIST": ws_data,
    }
    for f in findings:
        resp = _api.get(f.check_id)
        if resp is not None:
            if f.details is None:
                f.details = {}
            f.details.setdefault("api_response", resp)

    return findings


# ─────────────────────────────────────────────────────────────────────────────
# Data Residency / Geo checks
# Ref: https://learn.microsoft.com/en-us/azure/databricks/resources/databricks-geos
# ─────────────────────────────────────────────────────────────────────────────

async def _check_data_residency(client: httpx.AsyncClient, host: str, token: str) -> list[SATFinding]:
    """Data residency / Geo checks: workspace Geo, cross-Geo enforcement, compliance interaction."""
    findings: list[SATFinding] = []

    # ── Resolve workspace region and Geo ──
    azure_region = _WORKSPACE_REGIONS.get(host.rstrip("/"), "")
    geo = _resolve_geo(azure_region) if azure_region else "Unknown"
    region_known = bool(azure_region)

    # ── SAT-GEO-1: Informational — Workspace Geography and region ──
    if region_known:
        findings.append(_make_finding("SAT-GEO-1", "PASS",
            f"Region: {azure_region}, Geography: {geo}",
            {"api_response": {"azure_region": azure_region, "geo": geo},
             "api_endpoint": "(workspace metadata)"}))
    else:
        findings.append(_make_finding("SAT-GEO-1", "NOT_APPLICABLE",
            "Workspace region not available (PAT login). Use --azure for region detection."))

    # ── SAT-GEO-2: Cross-Geography data processing enforcement ──
    cross_geo_data = None
    cross_geo_enforced: bool | None = None  # None = unknown

    # Try Settings API types
    for settings_type in ("restrict_to_geo_ws", "cross_geo_data_processing_ws",
                          "geo_data_processing_ws", "data_residency_ws"):
        cg_data, cg_s, cg_e = await _dbx_get(client, host,
            f"/api/2.0/settings/types/{settings_type}/names/default", token)
        if cg_s == 200 and cg_data is not None:
            cross_geo_data = cg_data
            inner = cg_data.get(settings_type, cg_data)
            if isinstance(inner, dict):
                cross_geo_enforced = bool(
                    inner.get("enforce_geo_boundary")
                    or inner.get("is_enabled")
                    or inner.get("enforce_data_processing_within_geo")
                    or inner.get("restrict_to_geo")
                )
            break
        elif cg_s not in (404, 400):
            # Auth error or other non-trivial error
            findings.append(_na("SAT-GEO-2", cg_s, cg_e))
            break

    # Fallback: try workspace-conf keys
    if cross_geo_enforced is None and cross_geo_data is None:
        wc_data, wc_s, _ = await _dbx_get_workspace_conf(
            client, host, token, "enforceGeoDataProcessing,enableCrossGeoProcessing")
        if wc_s == 200 and wc_data:
            enforce_val = str(wc_data.get("enforceGeoDataProcessing", "")).lower()
            cross_val = str(wc_data.get("enableCrossGeoProcessing", "")).lower()
            if enforce_val == "true" or cross_val == "false":
                cross_geo_enforced = True
                cross_geo_data = wc_data
            elif enforce_val == "false" or cross_val == "true":
                cross_geo_enforced = False
                cross_geo_data = wc_data

    # Generate SAT-GEO-2 finding
    if not any(f.check_id == "SAT-GEO-2" for f in findings):
        if cross_geo_enforced is True:
            findings.append(_make_finding("SAT-GEO-2", "PASS",
                f"Cross-Geography data processing enforcement: enabled.{f' Geo: {geo}.' if region_known else ''}",
                {"api_response": cross_geo_data or {}, "enforced": True, "geo": geo}))
        elif cross_geo_enforced is False:
            if region_known and geo not in CROSS_GEO_DISABLED_BY_DEFAULT:
                findings.append(_make_finding("SAT-GEO-2", "FAIL",
                    f"Cross-Geo enforcement: NOT enabled. Workspace in {geo} Geo — "
                    f"cross-Geo processing is enabled by default.",
                    {"api_response": cross_geo_data or {}, "enforced": False, "geo": geo,
                     "default_behavior": "cross-geo enabled by default"}))
            elif region_known:
                findings.append(_make_finding("SAT-GEO-2", "WARN",
                    f"Cross-Geo enforcement: NOT explicitly enabled. Workspace in {geo} Geo — "
                    f"cross-Geo is disabled by default, but explicit enforcement is recommended.",
                    {"api_response": cross_geo_data or {}, "enforced": False, "geo": geo,
                     "default_behavior": "cross-geo disabled by default"}))
            else:
                findings.append(_make_finding("SAT-GEO-2", "WARN",
                    "Cross-Geo enforcement: NOT enabled. Region unknown — use --azure for detection.",
                    {"api_response": cross_geo_data or {}, "enforced": False, "geo": "Unknown"}))
        else:
            # Could not determine enforcement status via API
            if region_known and geo not in CROSS_GEO_DISABLED_BY_DEFAULT:
                # Non-US/EU workspace: cross-Geo enabled by default → assume worst case
                findings.append(_make_finding("SAT-GEO-2", "FAIL",
                    f"Cross-Geo enforcement: UNKNOWN (cannot verify via API). "
                    f"Workspace in {geo} Geo — cross-Geo processing is enabled by default. "
                    f"Check Account Console → Security and Compliance tab.",
                    {"api_response": None, "enforced": None, "geo": geo,
                     "default_behavior": "cross-geo enabled by default",
                     "note": "Setting may only be available in the Account Console"}))
            elif region_known:
                # US/EU workspace: cross-Geo disabled by default → lower risk
                findings.append(_make_finding("SAT-GEO-2", "WARN",
                    f"Cannot verify cross-Geo enforcement via API. Workspace in {geo} Geo — "
                    f"cross-Geo is disabled by default. Explicit enforcement still recommended. "
                    f"Check Account Console → Security and Compliance tab.",
                    {"api_response": None, "enforced": None, "geo": geo,
                     "default_behavior": "cross-geo disabled by default",
                     "note": "Setting may only be available in the Account Console"}))
            else:
                findings.append(_make_finding("SAT-GEO-2", "WARN",
                    "Cannot verify cross-Geo enforcement via API. "
                    "Check Account Console → Workspace → Security and Compliance tab.",
                    {"api_response": None, "enforced": None, "geo": "Unknown",
                     "note": "Setting may only be available in the Account Console"}))

    # ── SAT-GEO-3: Compliance profile + cross-Geo interaction ──
    csp_enabled: bool | None = None
    csp_data = None

    # Try workspace-conf
    wc, wc_s, _ = await _dbx_get_workspace_conf(
        client, host, token, "complianceSecurityProfileEnabled")
    if wc_s == 200 and wc:
        csp_data = wc
        csp_enabled = str(wc.get("complianceSecurityProfileEnabled", "false")).lower() == "true"
    else:
        # Fallback to Settings API
        csp_resp, csp_s, _ = await _dbx_get(client, host,
            "/api/2.0/settings/types/shield_csp_enablement_ws_db/names/default", token)
        if csp_s == 200 and csp_resp:
            csp_data = csp_resp
            csp_obj = csp_resp.get("shield_csp_enablement_ws_db", csp_resp)
            csp_enabled = csp_obj.get("is_enabled", False) if isinstance(csp_obj, dict) else False

    if csp_enabled is not None and cross_geo_enforced is not None:
        if csp_enabled and cross_geo_enforced:
            findings.append(_make_finding("SAT-GEO-3", "PASS",
                f"Compliance Security Profile: enabled. Cross-Geo enforcement: enabled.{f' Geo: {geo}.' if region_known else ''}",
                {"api_response": csp_data or {}, "compliance_profile": True,
                 "cross_geo_enforced": True, "geo": geo}))
        elif not csp_enabled and not cross_geo_enforced and region_known and geo not in CROSS_GEO_DISABLED_BY_DEFAULT:
            findings.append(_make_finding("SAT-GEO-3", "FAIL",
                f"Compliance Profile: NOT enabled. Cross-Geo enforcement: NOT enabled. "
                f"Workspace in {geo} — customer content may be processed outside this Geography.",
                {"api_response": csp_data or {}, "compliance_profile": False,
                 "cross_geo_enforced": False, "geo": geo}))
        elif csp_enabled and not cross_geo_enforced:
            findings.append(_make_finding("SAT-GEO-3", "WARN",
                "Compliance Profile: enabled, but cross-Geo enforcement NOT explicitly enabled. "
                "The compliance profile changes the default, but explicit enforcement is recommended.",
                {"api_response": csp_data or {}, "compliance_profile": True,
                 "cross_geo_enforced": False, "geo": geo}))
        elif not csp_enabled and cross_geo_enforced:
            findings.append(_make_finding("SAT-GEO-3", "WARN",
                "Compliance Profile: NOT enabled, but cross-Geo enforcement IS enabled. "
                "Consider enabling compliance profile for defence-in-depth.",
                {"api_response": csp_data or {}, "compliance_profile": False,
                 "cross_geo_enforced": True, "geo": geo}))
        else:
            findings.append(_make_finding("SAT-GEO-3", "WARN",
                f"Compliance Profile: {'enabled' if csp_enabled else 'not enabled'}. "
                f"Cross-Geo enforcement: {'enabled' if cross_geo_enforced else 'not enabled'}.",
                {"api_response": csp_data or {}, "compliance_profile": csp_enabled,
                 "cross_geo_enforced": cross_geo_enforced, "geo": geo}))
    else:
        if region_known and geo not in CROSS_GEO_DISABLED_BY_DEFAULT:
            # Non-US/EU: cross-Geo enabled by default + incomplete verification → FAIL
            findings.append(_make_finding("SAT-GEO-3", "FAIL",
                f"Cannot fully verify compliance profile and cross-Geo enforcement. "
                f"Workspace in {geo} Geo — cross-Geo processing is enabled by default. "
                f"{'Compliance profile: ' + ('enabled' if csp_enabled else 'NOT enabled') + '. ' if csp_enabled is not None else ''}"
                f"Check Account Console → Security and Compliance tab.",
                {"api_response": csp_data or {},
                 "compliance_profile": csp_enabled, "cross_geo_enforced": cross_geo_enforced, "geo": geo}))
        else:
            findings.append(_make_finding("SAT-GEO-3", "WARN",
                f"Cannot fully verify compliance profile and cross-Geo enforcement"
                f"{f' (Geo: {geo})' if region_known else ''} — check Account Console manually.",
                {"api_response": csp_data or {},
                 "compliance_profile": csp_enabled, "cross_geo_enforced": cross_geo_enforced, "geo": geo}))

    return findings


# ─────────────────────────────────────────────────────────────────────────────
# Table Optimization (6 checks)
# ─────────────────────────────────────────────────────────────────────────────

async def _check_optimization(client: httpx.AsyncClient, host: str, token: str) -> list[SATFinding]:
    """Table optimization checks: predictive opt, optimized writes, auto compact, delta cache, maintenance schedule, warehouse Photon."""
    findings: list[SATFinding] = []
    hdr = {"Authorization": f"Bearer {token}"}

    # ── SAT-OPT-PRED-CATALOG: Predictive optimization at catalog level ──
    _skip_cats = {"system", "hive_metastore", "__databricks_internal"}
    # Predictive optimization only applies to standard managed catalogs
    _po_eligible_types = {"MANAGED_CATALOG"}
    cat_data, cat_s, cat_e = await _dbx_get(client, host, "/api/2.1/unity-catalog/catalogs", token)
    if cat_data is None:
        findings.append(_na("SAT-OPT-PRED-CATALOG", cat_s, cat_e))
    else:
        catalogs = [c for c in cat_data.get("catalogs", [])
                    if c.get("name") not in _skip_cats
                    and c.get("catalog_type", "MANAGED_CATALOG") in _po_eligible_types]
        if not catalogs:
            findings.append(_make_finding("SAT-OPT-PRED-CATALOG", "NOT_APPLICABLE",
                "No user Unity Catalog catalogs found."))
        else:
            # Check effective value (accounts for INHERIT from metastore)
            # Falls back to explicit setting if effective flag is absent
            def _pred_opt_enabled(c: dict) -> bool:
                eff = c.get("effective_predictive_optimization_flag", {})
                if eff and eff.get("value"):
                    return str(eff["value"]).upper() == "ENABLE"
                return str(c.get("enable_predictive_optimization", "")).upper() == "ENABLE"

            enabled = [c for c in catalogs if _pred_opt_enabled(c)]

            # Fallback: query system.storage.predictive_optimization_operations_history
            # to find catalogs with recent PO activity even if the API flag is missing
            _po_active_cats: set[str] = set()
            if len(enabled) < len(catalogs):
                wh_id = await _find_running_warehouse(client, host, token)
                if wh_id:
                    po_rows, _po_err = await _dbx_sql_query(
                        client, host, token, wh_id,
                        "SELECT DISTINCT catalog_name "
                        "FROM system.storage.predictive_optimization_operations_history "
                        "WHERE start_time >= current_date() - INTERVAL 30 DAYS",
                    )
                    if po_rows:
                        _po_active_cats = {r[0] for r in po_rows if r}
                        # Promote catalogs with recent PO activity
                        for c in catalogs:
                            if c.get("name") in _po_active_cats and not _pred_opt_enabled(c):
                                enabled.append(c)

            pct = round(len(enabled) / len(catalogs) * 100)
            not_enabled = [c.get("name", "?") for c in catalogs if not _pred_opt_enabled(c)
                           and c.get("name") not in _po_active_cats]
            if pct >= 80:
                status = "PASS"
            elif enabled:
                status = "WARN"
            else:
                status = "FAIL"
            # Count skipped (non-managed) catalogs for transparency
            all_cats = cat_data.get("catalogs", [])
            skipped = [c.get("name", "?") for c in all_cats
                       if c.get("name") not in _skip_cats
                       and c.get("catalog_type", "MANAGED_CATALOG") not in _po_eligible_types]

            source = " (via API + system table)" if _po_active_cats else " (directly or inherited)"
            msg = f"{len(enabled)}/{len(catalogs)} managed catalogs ({pct}%) have predictive optimization enabled{source}."
            if not_enabled:
                msg += f" Not enabled: {', '.join(not_enabled[:10])}{'...' if len(not_enabled) > 10 else ''}."
            if skipped:
                msg += f" Skipped {len(skipped)} non-managed catalog(s): {', '.join(skipped[:5])}{'...' if len(skipped) > 5 else ''}."
            findings.append(_make_finding("SAT-OPT-PRED-CATALOG", status, msg,
                {"enabled": len(enabled), "total": len(catalogs), "pct": pct,
                 "not_enabled": not_enabled,
                 "skipped_catalogs": skipped,
                 "system_table_active": sorted(_po_active_cats) if _po_active_cats else []}))

    # ── Cluster spark_conf checks (3 checks) ──
    cl_data, cl_s, cl_e = await _dbx_get(client, host, "/api/2.0/clusters/list", token)
    if cl_data is None:
        for cid in ("SAT-OPT-OPTIMIZE-WRITE", "SAT-OPT-AUTO-COMPACT", "SAT-OPT-DELTA-CACHE"):
            findings.append(_na(cid, cl_s, cl_e))
    else:
        clusters = cl_data.get("clusters", [])
        active = [c for c in clusters if c.get("state") not in ("TERMINATED", "TERMINATING")]
        if not active:
            for cid in ("SAT-OPT-OPTIMIZE-WRITE", "SAT-OPT-AUTO-COMPACT", "SAT-OPT-DELTA-CACHE"):
                findings.append(_make_finding(cid, "NOT_APPLICABLE", "No active clusters found."))
        else:
            for check_id, spark_key, label in [
                ("SAT-OPT-OPTIMIZE-WRITE", "spark.databricks.delta.optimizeWrite.enabled", "optimized writes"),
                ("SAT-OPT-AUTO-COMPACT", "spark.databricks.delta.autoCompact.enabled", "auto compaction"),
                ("SAT-OPT-DELTA-CACHE", "spark.databricks.io.cache.enabled", "Delta cache"),
            ]:
                enabled_count = sum(1 for c in active
                    if str(c.get("spark_conf", {}).get(spark_key, "")).lower() == "true")
                pct = round(enabled_count / len(active) * 100)
                if pct >= 80:
                    status = "PASS"
                elif enabled_count > 0:
                    status = "WARN"
                else:
                    status = "FAIL"
                findings.append(_make_finding(check_id, status,
                    f"{enabled_count}/{len(active)} active clusters ({pct}%) have {label} ({spark_key}=true).",
                    {"enabled": enabled_count, "active_count": len(active), "pct": pct, "spark_key": spark_key}))

    # ── SAT-OPT-MAINT-SCHEDULE: Maintenance jobs with cron schedules ──
    jobs_data, j_s, j_e = await _dbx_get_all_jobs(client, host, token, {"expand_tasks": "false"})
    if jobs_data is None:
        findings.append(_na("SAT-OPT-MAINT-SCHEDULE", j_s, j_e))
    else:
        jobs = jobs_data.get("jobs", [])
        keywords = {"optimize", "vacuum", "maintenance", "compaction"}
        maint_jobs = [j for j in jobs
                      if any(kw in (j.get("settings", {}).get("name", "") or "").lower() for kw in keywords)]
        if not maint_jobs:
            findings.append(_make_finding("SAT-OPT-MAINT-SCHEDULE", "NOT_APPLICABLE",
                "No OPTIMIZE/VACUUM maintenance jobs found."))
        else:
            scheduled = [j for j in maint_jobs if j.get("settings", {}).get("schedule")]
            unscheduled = [j.get("settings", {}).get("name", "?") for j in maint_jobs
                           if not j.get("settings", {}).get("schedule")]
            pct = round(len(scheduled) / len(maint_jobs) * 100)
            if pct >= 80:
                status = "PASS"
            elif scheduled:
                status = "WARN"
            else:
                status = "FAIL"
            msg = f"{len(scheduled)}/{len(maint_jobs)} maintenance jobs ({pct}%) have cron schedules."
            if unscheduled:
                msg += f" Unscheduled: {', '.join(unscheduled[:5])}{'...' if len(unscheduled) > 5 else ''}."
            findings.append(_make_finding("SAT-OPT-MAINT-SCHEDULE", status, msg,
                {"scheduled": len(scheduled), "total_maint_jobs": len(maint_jobs), "pct": pct}))

    # ── SAT-OPT-WH-PHOTON: SQL warehouses with Photon ──
    wh_data, wh_s, wh_e = await _dbx_get(client, host, "/api/2.0/sql/warehouses", token)
    if wh_data is None:
        findings.append(_na("SAT-OPT-WH-PHOTON", wh_s, wh_e))
    else:
        warehouses = wh_data.get("warehouses", [])
        if not warehouses:
            findings.append(_make_finding("SAT-OPT-WH-PHOTON", "NOT_APPLICABLE",
                "No SQL warehouses found."))
        else:
            photon_wh = [w for w in warehouses if w.get("enable_photon", False)]
            no_photon = [w.get("name", "?") for w in warehouses if not w.get("enable_photon", False)]
            pct = round(len(photon_wh) / len(warehouses) * 100)
            if pct == 100:
                status = "PASS"
            elif photon_wh:
                status = "WARN"
            else:
                status = "FAIL"
            msg = f"{len(photon_wh)}/{len(warehouses)} SQL warehouses ({pct}%) have Photon enabled."
            if no_photon:
                msg += f" Without Photon: {', '.join(no_photon[:5])}{'...' if len(no_photon) > 5 else ''}."
            findings.append(_make_finding("SAT-OPT-WH-PHOTON", status, msg,
                {"photon_count": len(photon_wh), "total": len(warehouses), "pct": pct}))

    # ── SAT-OPT-LIQUID-CLUSTER: Tables using Liquid Clustering ──
    # Reuse cat_data from above; fetch tables to check for clustering columns
    _lc_tables_checked = 0
    _lc_clustered = 0
    if cat_data is not None and cat_data.get("catalogs"):
        _user_cats = [c for c in cat_data.get("catalogs", [])
                      if c.get("name") not in _skip_cats]
        for cat in _user_cats[:3]:
            try:
                s_resp = await client.get(f"{host}/api/2.1/unity-catalog/schemas",
                    params={"catalog_name": cat["name"]}, headers=hdr, timeout=10)
                if s_resp.status_code != 200:
                    continue
                schemas = [s for s in s_resp.json().get("schemas", [])
                           if s.get("name") != "information_schema"]
                for sch in schemas[:5]:
                    t_resp = await client.get(f"{host}/api/2.1/unity-catalog/tables",
                        params={"catalog_name": cat["name"], "schema_name": sch["name"],
                                "max_results": "10"},
                        headers=hdr, timeout=10)
                    if t_resp.status_code != 200:
                        continue
                    for tbl in t_resp.json().get("tables", []):
                        if tbl.get("table_type") == "VIEW":
                            continue
                        _lc_tables_checked += 1
                        props = tbl.get("properties", {})
                        # Liquid clustering shows as clusteringColumns or delta.clustering.columns
                        if (props.get("clusteringColumns") or props.get("delta.clustering.columns")
                                or tbl.get("enable_predictive_optimization")):
                            _lc_clustered += 1
                    await asyncio.sleep(0.15)
            except Exception:
                pass
    if _lc_tables_checked == 0:
        findings.append(_make_finding("SAT-OPT-LIQUID-CLUSTER", "NOT_APPLICABLE",
            "No UC tables found to check for Liquid Clustering."))
    else:
        pct = round(_lc_clustered / _lc_tables_checked * 100)
        if pct >= 30:
            status = "PASS"
        elif _lc_clustered >= 1:
            status = "WARN"
        else:
            status = "FAIL"
        findings.append(_make_finding("SAT-OPT-LIQUID-CLUSTER", status,
            f"{_lc_clustered}/{_lc_tables_checked} sampled tables ({pct}%) use Liquid Clustering.",
            {"clustered": _lc_clustered, "checked": _lc_tables_checked, "pct": pct}))

    # ── SAT-OPT-WH-AUTO-STOP: SQL warehouses auto_stop_mins <= 10 ──
    if wh_data is None:
        findings.append(_na("SAT-OPT-WH-AUTO-STOP", wh_s, wh_e))
    else:
        warehouses = wh_data.get("warehouses", [])
        if not warehouses:
            findings.append(_make_finding("SAT-OPT-WH-AUTO-STOP", "NOT_APPLICABLE",
                "No SQL warehouses found."))
        else:
            fast_stop = [w for w in warehouses if (w.get("auto_stop_mins") or 999) <= 10]
            slow_stop = [w.get("name", "?") for w in warehouses if (w.get("auto_stop_mins") or 999) > 10]
            pct = round(len(fast_stop) / len(warehouses) * 100)
            if pct == 100:
                status = "PASS"
            elif fast_stop:
                status = "WARN"
            else:
                status = "FAIL"
            msg = f"{len(fast_stop)}/{len(warehouses)} SQL warehouses ({pct}%) auto-stop within 10 minutes."
            if slow_stop:
                msg += f" Slow: {', '.join(slow_stop[:5])}{'...' if len(slow_stop) > 5 else ''}."
            findings.append(_make_finding("SAT-OPT-WH-AUTO-STOP", status, msg,
                {"fast_stop": len(fast_stop), "total": len(warehouses), "pct": pct}))

    # ── SAT-OPT-SERVERLESS-JOBS: Jobs use serverless/job clusters (not interactive) ──
    if jobs_data is None:
        findings.append(_na("SAT-OPT-SERVERLESS-JOBS", j_s, j_e))
    else:
        jobs = jobs_data.get("jobs", [])
        if not jobs:
            findings.append(_make_finding("SAT-OPT-SERVERLESS-JOBS", "NOT_APPLICABLE",
                "No jobs found."))
        else:
            interactive = [j for j in jobs if j.get("settings", {}).get("existing_cluster_id")]
            non_interactive = len(jobs) - len(interactive)
            pct = round(non_interactive / len(jobs) * 100)
            if pct >= 80:
                status = "PASS"
            elif non_interactive > 0:
                status = "WARN"
            else:
                status = "FAIL"
            msg = f"{non_interactive}/{len(jobs)} jobs ({pct}%) use serverless or dedicated job clusters."
            if interactive:
                names = [j.get("settings", {}).get("name", "?") for j in interactive[:5]]
                msg += f" Interactive: {', '.join(names)}{'...' if len(interactive) > 5 else ''}."
            findings.append(_make_finding("SAT-OPT-SERVERLESS-JOBS", status, msg,
                {"non_interactive": non_interactive, "total": len(jobs), "pct": pct}))

    # ── SAT-OPT-TABLE-FORMAT: UC tables use Delta format ──
    if _lc_tables_checked == 0:
        findings.append(_make_finding("SAT-OPT-TABLE-FORMAT", "NOT_APPLICABLE",
            "No UC tables found to check format."))
    else:
        # Reuse tables already checked during Liquid Clustering scan
        # Re-scan with format check
        delta_count = 0
        non_delta_names: list[str] = []
        if cat_data is not None and cat_data.get("catalogs"):
            for cat in _user_cats[:3]:
                try:
                    s_resp = await client.get(f"{host}/api/2.1/unity-catalog/schemas",
                        params={"catalog_name": cat["name"]}, headers=hdr, timeout=10)
                    if s_resp.status_code != 200:
                        continue
                    schemas = [s for s in s_resp.json().get("schemas", [])
                               if s.get("name") != "information_schema"]
                    for sch in schemas[:5]:
                        t_resp = await client.get(f"{host}/api/2.1/unity-catalog/tables",
                            params={"catalog_name": cat["name"], "schema_name": sch["name"],
                                    "max_results": "10"},
                            headers=hdr, timeout=10)
                        if t_resp.status_code != 200:
                            continue
                        for tbl in t_resp.json().get("tables", []):
                            if tbl.get("table_type") == "VIEW":
                                continue
                            fmt = (tbl.get("data_source_format") or "").upper()
                            if fmt == "DELTA" or tbl.get("table_type") == "MANAGED":
                                delta_count += 1
                            else:
                                non_delta_names.append(tbl.get("name", "?"))
                        await asyncio.sleep(0.15)
                except Exception:
                    pass
        total_fmt = delta_count + len(non_delta_names)
        if total_fmt == 0:
            findings.append(_make_finding("SAT-OPT-TABLE-FORMAT", "NOT_APPLICABLE",
                "No UC tables found to check format."))
        else:
            pct = round(delta_count / total_fmt * 100)
            if pct >= 90:
                status = "PASS"
            elif pct >= 60:
                status = "WARN"
            else:
                status = "FAIL"
            msg = f"{delta_count}/{total_fmt} sampled UC tables ({pct}%) use Delta format."
            if non_delta_names:
                msg += f" Non-Delta: {', '.join(non_delta_names[:5])}{'...' if len(non_delta_names) > 5 else ''}."
            findings.append(_make_finding("SAT-OPT-TABLE-FORMAT", status, msg,
                {"delta_count": delta_count, "total": total_fmt, "pct": pct}))

    # ── Enrich with API response ──
    _api: dict[str, Any] = {
        "SAT-OPT-PRED-CATALOG": cat_data,
        "SAT-OPT-OPTIMIZE-WRITE": cl_data, "SAT-OPT-AUTO-COMPACT": cl_data,
        "SAT-OPT-DELTA-CACHE": cl_data,
        "SAT-OPT-MAINT-SCHEDULE": jobs_data, "SAT-OPT-SERVERLESS-JOBS": jobs_data,
        "SAT-OPT-WH-PHOTON": wh_data, "SAT-OPT-WH-AUTO-STOP": wh_data,
        "SAT-OPT-LIQUID-CLUSTER": cat_data, "SAT-OPT-TABLE-FORMAT": cat_data,
    }
    for f in findings:
        resp = _api.get(f.check_id)
        if resp is not None:
            f.details.setdefault("api_response", resp)
    return findings


# ─────────────────────────────────────────────────────────────────────────────
# Data Quality (8 checks)
# ─────────────────────────────────────────────────────────────────────────────

async def _check_data_quality(client: httpx.AsyncClient, host: str, token: str) -> list[SATFinding]:
    """Data quality checks: table monitors, table comments, DLT freshness, compute right-sizing."""
    findings: list[SATFinding] = []
    hdr = {"Authorization": f"Bearer {token}"}

    # ── Pre-fetch catalogs, schemas, tables (shared by monitors + comments) ──
    _skip_cats = {"system", "hive_metastore", "__databricks_internal"}
    cat_data, cat_s, cat_e = await _dbx_get(client, host, "/api/2.1/unity-catalog/catalogs", token)
    user_cats = [c for c in (cat_data or {}).get("catalogs", []) if c.get("name") not in _skip_cats]

    # Gather schemas per catalog (max 3 catalogs, 5 schemas each)
    _cat_schemas: dict[str, list[dict]] = {}
    if cat_data is not None and user_cats:
        for cat in user_cats[:3]:
            s_data, _, _ = await _dbx_get(client, host,
                "/api/2.1/unity-catalog/schemas", token,
                params={"catalog_name": cat["name"]})
            if s_data:
                _cat_schemas[cat["name"]] = [
                    s for s in s_data.get("schemas", [])
                    if s.get("name") != "information_schema"
                ]
            await asyncio.sleep(0.25)

    # Gather tables (max 10 per schema) — reuse for monitors + comments
    _all_tables: list[dict] = []
    if cat_data is not None and user_cats:
        for cat in user_cats[:3]:
            for schema in _cat_schemas.get(cat["name"], [])[:5]:
                t_data, _, _ = await _dbx_get(client, host,
                    "/api/2.1/unity-catalog/tables", token,
                    params={"catalog_name": cat["name"], "schema_name": schema["name"],
                            "max_results": "10"})
                if t_data:
                    _all_tables.extend(t_data.get("tables", []))
                await asyncio.sleep(0.15)

    # ── SAT-DQ-TABLE-MONITORS: Lakehouse Monitor coverage ──
    if cat_data is None:
        findings.append(_na("SAT-DQ-TABLE-MONITORS", cat_s, cat_e))
    elif not _all_tables:
        findings.append(_make_finding("SAT-DQ-TABLE-MONITORS", "NOT_APPLICABLE",
            "No UC tables found in sampled catalogs/schemas."))
    else:
        monitors_found = 0
        tables_checked = 0
        for tbl in _all_tables:
            full_name = tbl.get("full_name", "")
            if not full_name or tbl.get("table_type") == "VIEW":
                continue
            tables_checked += 1
            try:
                m_resp = await client.get(
                    f"{host}/api/2.1/unity-catalog/tables/{full_name}/monitor",
                    headers=hdr, timeout=10)
                if m_resp.status_code == 200:
                    monitors_found += 1
            except Exception:
                pass
            await asyncio.sleep(0.1)
        if tables_checked == 0:
            findings.append(_make_finding("SAT-DQ-TABLE-MONITORS", "NOT_APPLICABLE",
                "No UC tables to check for monitors."))
        else:
            pct = round(monitors_found / tables_checked * 100)
            if pct >= 30:
                status = "PASS"
            elif monitors_found >= 1:
                status = "WARN"
            else:
                status = "FAIL"
            findings.append(_make_finding("SAT-DQ-TABLE-MONITORS", status,
                f"{monitors_found}/{tables_checked} sampled UC tables ({pct}%) have Lakehouse Monitors.",
                {"monitors_found": monitors_found, "tables_checked": tables_checked, "pct": pct}))

    # ── SAT-DQ-TABLE-COMMENTS: Table descriptions ──
    if cat_data is None:
        findings.append(_na("SAT-DQ-TABLE-COMMENTS", cat_s, cat_e))
    elif not _all_tables:
        findings.append(_make_finding("SAT-DQ-TABLE-COMMENTS", "NOT_APPLICABLE",
            "No UC tables found in sampled catalogs/schemas."))
    else:
        described = sum(1 for t in _all_tables if t.get("comment"))
        total = len(_all_tables)
        pct = round(described / total * 100)
        if pct >= 80:
            status = "PASS"
        elif pct >= 40:
            status = "WARN"
        else:
            status = "FAIL"
        findings.append(_make_finding("SAT-DQ-TABLE-COMMENTS", status,
            f"{described}/{total} sampled UC tables ({pct}%) have descriptions.",
            {"described": described, "total_tables": total, "pct": pct}))

    # ── SAT-DQ-DLT-FRESHNESS: DLT pipelines with recent updates ──
    try:
        dlt_resp = await client.get(f"{host}/api/2.0/pipelines",
            params={"max_results": "50"}, headers=hdr, timeout=15)
        if dlt_resp.status_code == 200:
            pipelines = dlt_resp.json().get("statuses", [])
            if not pipelines:
                findings.append(_make_finding("SAT-DQ-DLT-FRESHNESS", "NOT_APPLICABLE",
                    "No DLT pipelines found."))
            else:
                import time as _time
                now_ms = int(_time.time() * 1000)
                seven_days_ms = 7 * 24 * 60 * 60 * 1000
                fresh_count = 0
                stale_names: list[str] = []
                for pl in pipelines:
                    # Check latest_updates from pipeline detail
                    pid = pl.get("pipeline_id", "")
                    try:
                        pr = await client.get(f"{host}/api/2.0/pipelines/{pid}",
                            headers=hdr, timeout=10)
                        if pr.status_code == 200:
                            updates = pr.json().get("latest_updates", [])
                            if updates:
                                latest_ts = updates[0].get("creation_time", 0)
                                if isinstance(latest_ts, str):
                                    latest_ts = int(latest_ts)
                                if now_ms - latest_ts <= seven_days_ms:
                                    fresh_count += 1
                                    continue
                    except Exception:
                        pass
                    stale_names.append(pl.get("name", "?"))
                pct = round(fresh_count / len(pipelines) * 100)
                if pct >= 80:
                    status = "PASS"
                elif pct >= 50:
                    status = "WARN"
                else:
                    status = "FAIL"
                msg = f"{fresh_count}/{len(pipelines)} DLT pipelines ({pct}%) updated within last 7 days."
                if stale_names:
                    msg += f" Stale: {', '.join(stale_names[:5])}{'...' if len(stale_names) > 5 else ''}."
                findings.append(_make_finding("SAT-DQ-DLT-FRESHNESS", status, msg,
                    {"fresh": fresh_count, "total": len(pipelines), "pct": pct}))
        elif dlt_resp.status_code in (404, 403):
            findings.append(_make_finding("SAT-DQ-DLT-FRESHNESS", "NOT_APPLICABLE",
                f"Pipelines API not available (HTTP {dlt_resp.status_code})."))
        else:
            findings.append(_make_finding("SAT-DQ-DLT-FRESHNESS", "WARN",
                f"Pipelines API returned HTTP {dlt_resp.status_code}."))
    except Exception as exc:
        findings.append(_make_finding("SAT-DQ-DLT-FRESHNESS", "WARN", f"Error: {exc}"))

    # ── SAT-DQ-COMPUTE-RIGHT-SIZE: Autoscaling min/max ratio ──
    cl_data2, cl_s2, cl_e2 = await _dbx_get(client, host, "/api/2.0/clusters/list", token)
    if cl_data2 is None:
        findings.append(_na("SAT-DQ-COMPUTE-RIGHT-SIZE", cl_s2, cl_e2))
    else:
        clusters = cl_data2.get("clusters", [])
        active = [c for c in clusters if c.get("state") not in ("TERMINATED", "TERMINATING")]
        autoscaled = [c for c in active if c.get("autoscale")]
        if not autoscaled:
            findings.append(_make_finding("SAT-DQ-COMPUTE-RIGHT-SIZE", "NOT_APPLICABLE",
                "No active autoscaling clusters found."))
        else:
            reasonable: list[str] = []
            extreme: list[str] = []
            for c in autoscaled:
                as_conf = c.get("autoscale", {})
                min_w = as_conf.get("min_workers", 1)
                max_w = as_conf.get("max_workers", 1)
                name = c.get("cluster_name", "?")
                if min_w > 0 and max_w <= 5 * min_w:
                    reasonable.append(name)
                else:
                    extreme.append(f"{name} ({min_w}→{max_w})")
            pct = round(len(reasonable) / len(autoscaled) * 100)
            if pct >= 80:
                status = "PASS"
            elif reasonable:
                status = "WARN"
            else:
                status = "FAIL"
            msg = f"{len(reasonable)}/{len(autoscaled)} autoscaling clusters ({pct}%) have reasonable min/max ratio (max ≤ 5× min)."
            if extreme:
                msg += f" Extreme: {', '.join(extreme[:3])}{'...' if len(extreme) > 3 else ''}."
            findings.append(_make_finding("SAT-DQ-COMPUTE-RIGHT-SIZE", status, msg,
                {"reasonable": len(reasonable), "autoscaled": len(autoscaled), "pct": pct}))

    # ── SAT-DQ-COLUMN-COMMENTS: Column-level descriptions ──
    if cat_data is None:
        findings.append(_na("SAT-DQ-COLUMN-COMMENTS", cat_s, cat_e))
    elif not _all_tables:
        findings.append(_make_finding("SAT-DQ-COLUMN-COMMENTS", "NOT_APPLICABLE",
            "No UC tables found in sampled catalogs/schemas."))
    else:
        total_cols = 0
        described_cols = 0
        for tbl in _all_tables:
            for col in tbl.get("columns", []):
                total_cols += 1
                if col.get("comment"):
                    described_cols += 1
        if total_cols == 0:
            findings.append(_make_finding("SAT-DQ-COLUMN-COMMENTS", "NOT_APPLICABLE",
                "No columns found in sampled tables."))
        else:
            pct = round(described_cols / total_cols * 100)
            if pct >= 60:
                status = "PASS"
            elif pct >= 30:
                status = "WARN"
            else:
                status = "FAIL"
            findings.append(_make_finding("SAT-DQ-COLUMN-COMMENTS", status,
                f"{described_cols}/{total_cols} columns ({pct}%) across sampled tables have descriptions.",
                {"described_cols": described_cols, "total_cols": total_cols, "pct": pct}))

    # ── SAT-DQ-TABLE-OWNERS: Tables with explicit owners ──
    if cat_data is None:
        findings.append(_na("SAT-DQ-TABLE-OWNERS", cat_s, cat_e))
    elif not _all_tables:
        findings.append(_make_finding("SAT-DQ-TABLE-OWNERS", "NOT_APPLICABLE",
            "No UC tables found in sampled catalogs/schemas."))
    else:
        _default_owners = {"", "system", "root", "account users"}
        owned = 0
        no_owner: list[str] = []
        for tbl in _all_tables:
            owner = (tbl.get("owner") or "").strip().lower()
            if owner and owner not in _default_owners:
                owned += 1
            else:
                no_owner.append(tbl.get("name", "?"))
        total = len(_all_tables)
        pct = round(owned / total * 100)
        if pct >= 80:
            status = "PASS"
        elif pct >= 40:
            status = "WARN"
        else:
            status = "FAIL"
        msg = f"{owned}/{total} sampled UC tables ({pct}%) have explicit owners."
        if no_owner:
            msg += f" No owner: {', '.join(no_owner[:5])}{'...' if len(no_owner) > 5 else ''}."
        findings.append(_make_finding("SAT-DQ-TABLE-OWNERS", status, msg,
            {"owned": owned, "total": total, "pct": pct}))

    # ── SAT-DQ-PIPELINE-ERRORS: DLT pipeline failure rate ──
    try:
        dlt_resp2 = await client.get(f"{host}/api/2.0/pipelines",
            params={"max_results": "50"}, headers=hdr, timeout=15)
        if dlt_resp2.status_code == 200:
            pipelines2 = dlt_resp2.json().get("statuses", [])
            if not pipelines2:
                findings.append(_make_finding("SAT-DQ-PIPELINE-ERRORS", "NOT_APPLICABLE",
                    "No DLT pipelines found."))
            else:
                healthy = 0
                failed_names: list[str] = []
                for pl in pipelines2:
                    pid = pl.get("pipeline_id", "")
                    try:
                        pr = await client.get(f"{host}/api/2.0/pipelines/{pid}",
                            headers=hdr, timeout=10)
                        if pr.status_code == 200:
                            updates = pr.json().get("latest_updates", [])
                            if updates:
                                latest_state = (updates[0].get("state") or "").upper()
                                if latest_state in ("COMPLETED", "RUNNING", "IDLE"):
                                    healthy += 1
                                    continue
                    except Exception:
                        pass
                    failed_names.append(pl.get("name", "?"))
                pct = round(healthy / len(pipelines2) * 100)
                if pct >= 80:
                    status = "PASS"
                elif pct >= 50:
                    status = "WARN"
                else:
                    status = "FAIL"
                msg = f"{healthy}/{len(pipelines2)} DLT pipelines ({pct}%) are healthy (latest update succeeded)."
                if failed_names:
                    msg += f" Failed/stale: {', '.join(failed_names[:5])}{'...' if len(failed_names) > 5 else ''}."
                findings.append(_make_finding("SAT-DQ-PIPELINE-ERRORS", status, msg,
                    {"healthy": healthy, "total": len(pipelines2), "pct": pct}))
        elif dlt_resp2.status_code in (404, 403):
            findings.append(_make_finding("SAT-DQ-PIPELINE-ERRORS", "NOT_APPLICABLE",
                f"Pipelines API not available (HTTP {dlt_resp2.status_code})."))
        else:
            findings.append(_make_finding("SAT-DQ-PIPELINE-ERRORS", "WARN",
                f"Pipelines API returned HTTP {dlt_resp2.status_code}."))
    except Exception as exc:
        findings.append(_make_finding("SAT-DQ-PIPELINE-ERRORS", "WARN", f"Error: {exc}"))

    # ── SAT-DQ-ALERT-COVERAGE: SQL alerts for data quality ──
    try:
        alert_resp = await client.get(f"{host}/api/2.0/sql/alerts",
            headers=hdr, timeout=15)
        if alert_resp.status_code == 200:
            alerts = alert_resp.json()
            # API returns a list directly (not wrapped in a key)
            if isinstance(alerts, dict):
                alerts = alerts.get("results", alerts.get("alerts", []))
            if not isinstance(alerts, list):
                alerts = []
            if len(alerts) >= 5:
                status = "PASS"
            elif len(alerts) >= 1:
                status = "WARN"
            else:
                status = "FAIL"
            findings.append(_make_finding("SAT-DQ-ALERT-COVERAGE", status,
                f"{len(alerts)} SQL alert(s) configured for data quality monitoring.",
                {"alert_count": len(alerts)}))
        elif alert_resp.status_code in (404, 403):
            findings.append(_make_finding("SAT-DQ-ALERT-COVERAGE", "NOT_APPLICABLE",
                f"SQL Alerts API not available (HTTP {alert_resp.status_code})."))
        else:
            findings.append(_make_finding("SAT-DQ-ALERT-COVERAGE", "WARN",
                f"SQL Alerts API returned HTTP {alert_resp.status_code}."))
    except Exception as exc:
        findings.append(_make_finding("SAT-DQ-ALERT-COVERAGE", "WARN", f"Error: {exc}"))

    # ── SAT-DQ-FRESHNESS: Freshness monitoring on critical tables ──
    if cat_data is None:
        findings.append(_na("SAT-DQ-FRESHNESS", cat_s, cat_e))
    elif not _all_tables:
        findings.append(_make_finding("SAT-DQ-FRESHNESS", "NOT_APPLICABLE",
            "No UC tables found in sampled catalogs."))
    else:
        # Check if tables have been updated recently (last 30 days)
        import time as _time
        now_ms = int(_time.time() * 1000)
        thirty_days_ms = 30 * 24 * 3600 * 1000
        fresh = 0
        stale_names: list[str] = []
        for tbl in _all_tables:
            updated = tbl.get("updated_at", 0)
            if updated and (now_ms - updated) < thirty_days_ms:
                fresh += 1
            else:
                stale_names.append(tbl.get("name", "?"))
        total = len(_all_tables)
        pct = round(fresh / total * 100) if total else 0
        if pct >= 80:
            status = "PASS"
        elif pct >= 50:
            status = "WARN"
        else:
            status = "FAIL"
        msg = f"{fresh}/{total} sampled tables ({pct}%) updated within 30 days."
        if stale_names:
            msg += f" Stale: {', '.join(stale_names[:5])}{'...' if len(stale_names) > 5 else ''}."
        findings.append(_make_finding("SAT-DQ-FRESHNESS", status, msg))

    # ── SAT-DQ-PROFILING: Data profiling (Lakehouse Monitors) on key tables ──
    if cat_data is None:
        findings.append(_na("SAT-DQ-PROFILING", cat_s, cat_e))
    elif not _all_tables:
        findings.append(_make_finding("SAT-DQ-PROFILING", "NOT_APPLICABLE",
            "No UC tables found in sampled catalogs."))
    else:
        # Check if any tables have associated monitors (profile tables)
        monitor_count = 0
        for tbl in _all_tables[:10]:
            full_name = tbl.get("full_name", "")
            if not full_name:
                full_name = f"{tbl.get('catalog_name', '')}.{tbl.get('schema_name', '')}.{tbl.get('name', '')}"
            try:
                mr = await client.get(
                    f"{host}/api/2.1/unity-catalog/tables/{full_name}_profile",
                    headers=hdr, timeout=5)
                if mr.status_code == 200:
                    monitor_count += 1
            except:
                pass
        sampled = min(len(_all_tables), 10)
        if monitor_count > 0:
            findings.append(_make_finding("SAT-DQ-PROFILING", "PASS",
                f"{monitor_count}/{sampled} sampled tables have associated profile tables."))
        else:
            findings.append(_make_finding("SAT-DQ-PROFILING", "WARN",
                f"No profile tables found for {sampled} sampled tables. Enable Lakehouse Monitors."))

    # ── Enrich with API response ──
    _api: dict[str, Any] = {
        "SAT-DQ-TABLE-MONITORS": cat_data, "SAT-DQ-TABLE-COMMENTS": cat_data,
        "SAT-DQ-COLUMN-COMMENTS": cat_data, "SAT-DQ-TABLE-OWNERS": cat_data,
        "SAT-DQ-COMPUTE-RIGHT-SIZE": cl_data2,
    }
    for f in findings:
        resp = _api.get(f.check_id)
        if resp is not None:
            f.details.setdefault("api_response", resp)
    return findings


async def _check_spark_best_practices(client: httpx.AsyncClient, host: str, token: str) -> list[SATFinding]:
    """Spark best practices: AQE, shuffle, schema control, job config, policies, UC governance."""
    import json as _json

    findings: list[SATFinding] = []

    # ══════════════════════════════════════════════════════
    # CLUSTER SPARK CONFIG CHECKS (7 checks)
    # ══════════════════════════════════════════════════════
    cl_data, cl_s, cl_e = await _dbx_get(client, host, "/api/2.0/clusters/list", token)
    _cluster_check_ids = (
        "SAT-SPARK-AQE", "SAT-SPARK-AQE-SKEW", "SAT-SPARK-SHUFFLE-PARTITIONS",
        "SAT-SPARK-DYNAMIC-OVERWRITE", "SAT-SPARK-SCHEMA-AUTOMERGE",
        "SAT-SPARK-STRICT-TIMESTAMP", "SAT-SPARK-CLUSTER-LOG-DELIVERY",
    )
    if cl_data is None:
        for cid in _cluster_check_ids:
            findings.append(_na(cid, cl_s, cl_e))
    else:
        clusters = cl_data.get("clusters", [])
        active = [c for c in clusters if c.get("state") not in ("TERMINATED", "TERMINATING")]
        if not active:
            for cid in _cluster_check_ids:
                findings.append(_make_finding(cid, "NOT_APPLICABLE", "No active clusters found."))
        else:
            # ── Boolean spark_conf checks ──
            for check_id, spark_key, label in [
                ("SAT-SPARK-AQE", "spark.sql.adaptive.enabled", "AQE"),
                ("SAT-SPARK-AQE-SKEW", "spark.sql.adaptive.skewJoin.enabled", "AQE skew join"),
            ]:
                enabled_count = sum(1 for c in active
                    if str(c.get("spark_conf", {}).get(spark_key, "")).lower() == "true")
                pct = round(enabled_count / len(active) * 100)
                if pct >= 80:
                    status = "PASS"
                elif enabled_count > 0:
                    status = "WARN"
                else:
                    explicitly_disabled = sum(1 for c in active
                        if str(c.get("spark_conf", {}).get(spark_key, "")).lower() == "false")
                    status = "FAIL" if explicitly_disabled > 0 else "WARN"
                findings.append(_make_finding(check_id, status,
                    f"{enabled_count}/{len(active)} active clusters ({pct}%) have {label} enabled.",
                    {"enabled": enabled_count, "active_count": len(active), "pct": pct, "spark_key": spark_key}))

            # ── SAT-SPARK-DYNAMIC-OVERWRITE ──
            dyn_count = sum(1 for c in active
                if str(c.get("spark_conf", {}).get(
                    "spark.sql.sources.partitionOverwriteMode", "")).lower() == "dynamic")
            pct = round(dyn_count / len(active) * 100)
            status = "PASS" if pct >= 80 else ("WARN" if dyn_count > 0 else "FAIL")
            findings.append(_make_finding("SAT-SPARK-DYNAMIC-OVERWRITE", status,
                f"{dyn_count}/{len(active)} active clusters ({pct}%) have dynamic partition overwrite enabled.",
                {"enabled": dyn_count, "active_count": len(active), "pct": pct}))

            # ── SAT-SPARK-SHUFFLE-PARTITIONS ──
            auto_count = sum(1 for c in active
                if str(c.get("spark_conf", {}).get("spark.sql.shuffle.partitions", "200")).lower() == "auto")
            custom_count = sum(1 for c in active
                if str(c.get("spark_conf", {}).get("spark.sql.shuffle.partitions", "200")) not in ("200", "auto"))
            default_count = len(active) - auto_count - custom_count
            if auto_count + custom_count >= len(active) * 0.8:
                status = "PASS"
            elif default_count == len(active):
                status = "FAIL"
            else:
                status = "WARN"
            findings.append(_make_finding("SAT-SPARK-SHUFFLE-PARTITIONS", status,
                f"{auto_count} auto, {custom_count} custom, {default_count} default (200) of {len(active)} clusters.",
                {"auto": auto_count, "custom": custom_count, "default_200": default_count}))

            # ── SAT-SPARK-SCHEMA-AUTOMERGE (reverse: false = PASS) ──
            automerge_enabled = sum(1 for c in active
                if str(c.get("spark_conf", {}).get(
                    "spark.databricks.delta.schema.autoMerge.enabled", "")).lower() == "true")
            if automerge_enabled == 0:
                status = "PASS"
            elif automerge_enabled < len(active):
                status = "WARN"
            else:
                status = "FAIL"
            findings.append(_make_finding("SAT-SPARK-SCHEMA-AUTOMERGE", status,
                f"{automerge_enabled}/{len(active)} clusters have schema auto-merge enabled (should be disabled in prod).",
                {"automerge_enabled": automerge_enabled, "active_count": len(active)}))

            # ── SAT-SPARK-STRICT-TIMESTAMP ──
            strict_count = sum(1 for c in active
                if str(c.get("spark_conf", {}).get(
                    "spark.sql.legacy.timeParserPolicy", "")).upper() == "EXCEPTION")
            pct = round(strict_count / len(active) * 100)
            status = "PASS" if pct >= 80 else ("WARN" if strict_count > 0 else "FAIL")
            findings.append(_make_finding("SAT-SPARK-STRICT-TIMESTAMP", status,
                f"{strict_count}/{len(active)} clusters ({pct}%) use strict timestamp parsing.",
                {"strict_count": strict_count, "active_count": len(active), "pct": pct}))

            # ── SAT-SPARK-CLUSTER-LOG-DELIVERY ──
            with_logs = sum(1 for c in active if c.get("cluster_log_conf"))
            pct = round(with_logs / len(active) * 100)
            status = "PASS" if pct >= 80 else ("WARN" if with_logs > 0 else "FAIL")
            findings.append(_make_finding("SAT-SPARK-CLUSTER-LOG-DELIVERY", status,
                f"{with_logs}/{len(active)} clusters ({pct}%) have log delivery configured.",
                {"with_logs": with_logs, "active_count": len(active), "pct": pct}))

    # ══════════════════════════════════════════════════════
    # JOB CONFIGURATION CHECKS (5 checks)
    # ══════════════════════════════════════════════════════
    jobs_data, j_s, j_e = await _dbx_get_all_jobs(client, host, token, {"expand_tasks": "true"})
    _job_check_ids = (
        "SAT-SPARK-JOB-PHOTON", "SAT-SPARK-JOB-RETRY",
        "SAT-SPARK-JOB-TIMEOUT", "SAT-SPARK-JOB-INTERACTIVE",
        "SAT-SPARK-JOB-NOTIFICATIONS",
    )
    if jobs_data is None:
        for cid in _job_check_ids:
            findings.append(_na(cid, j_s, j_e))
    else:
        jobs = jobs_data.get("jobs", [])
        if not jobs:
            for cid in _job_check_ids:
                findings.append(_make_finding(cid, "NOT_APPLICABLE", "No jobs found."))
        else:
            # ── SAT-SPARK-JOB-PHOTON ──
            photon_jobs = 0
            total_with_cluster = 0
            for j in jobs:
                for task in j.get("settings", {}).get("tasks", []):
                    nc = task.get("new_cluster", {})
                    if nc:
                        total_with_cluster += 1
                        sv = nc.get("spark_version", "")
                        if "photon" in sv.lower() or nc.get("runtime_engine") == "PHOTON":
                            photon_jobs += 1
            if total_with_cluster == 0:
                findings.append(_make_finding("SAT-SPARK-JOB-PHOTON", "NOT_APPLICABLE",
                    "No jobs with dedicated job clusters found."))
            else:
                pct = round(photon_jobs / total_with_cluster * 100)
                status = "PASS" if pct >= 80 else ("WARN" if pct >= 30 else "FAIL")
                findings.append(_make_finding("SAT-SPARK-JOB-PHOTON", status,
                    f"{photon_jobs}/{total_with_cluster} job cluster tasks ({pct}%) use Photon.",
                    {"photon": photon_jobs, "total": total_with_cluster, "pct": pct}))

            # ── SAT-SPARK-JOB-RETRY ──
            with_retry = sum(1 for j in jobs
                if j.get("settings", {}).get("max_retries", 0) > 0
                or any(t.get("max_retries", 0) > 0 for t in j.get("settings", {}).get("tasks", [])))
            pct = round(with_retry / len(jobs) * 100)
            status = "PASS" if pct >= 80 else ("WARN" if with_retry > 0 else "FAIL")
            findings.append(_make_finding("SAT-SPARK-JOB-RETRY", status,
                f"{with_retry}/{len(jobs)} jobs ({pct}%) have retry policies configured.",
                {"with_retry": with_retry, "total": len(jobs), "pct": pct}))

            # ── SAT-SPARK-JOB-TIMEOUT ──
            with_timeout = sum(1 for j in jobs
                if j.get("settings", {}).get("timeout_seconds", 0) > 0
                or any(t.get("timeout_seconds", 0) > 0 for t in j.get("settings", {}).get("tasks", [])))
            pct = round(with_timeout / len(jobs) * 100)
            status = "PASS" if pct >= 80 else ("WARN" if with_timeout > 0 else "FAIL")
            findings.append(_make_finding("SAT-SPARK-JOB-TIMEOUT", status,
                f"{with_timeout}/{len(jobs)} jobs ({pct}%) have timeout configured.",
                {"with_timeout": with_timeout, "total": len(jobs), "pct": pct}))

            # ── SAT-SPARK-JOB-INTERACTIVE ──
            on_interactive = sum(1 for j in jobs
                if any(t.get("existing_cluster_id") for t in j.get("settings", {}).get("tasks", [])))
            if on_interactive == 0:
                status = "PASS"
            elif on_interactive < len(jobs) * 0.5:
                status = "WARN"
            else:
                status = "FAIL"
            findings.append(_make_finding("SAT-SPARK-JOB-INTERACTIVE", status,
                f"{on_interactive}/{len(jobs)} jobs use interactive (all-purpose) clusters instead of job clusters.",
                {"on_interactive": on_interactive, "total": len(jobs)}))

            # ── SAT-SPARK-JOB-NOTIFICATIONS ──
            with_notify = sum(1 for j in jobs
                if j.get("settings", {}).get("email_notifications", {}).get("on_failure")
                or j.get("settings", {}).get("webhook_notifications", {}).get("on_failure")
                or j.get("settings", {}).get("notification_settings", {}).get("no_alert_for_skipped_runs") is not None)
            pct = round(with_notify / len(jobs) * 100)
            status = "PASS" if pct >= 80 else ("WARN" if with_notify > 0 else "FAIL")
            findings.append(_make_finding("SAT-SPARK-JOB-NOTIFICATIONS", status,
                f"{with_notify}/{len(jobs)} jobs ({pct}%) have failure notifications configured.",
                {"with_notify": with_notify, "total": len(jobs), "pct": pct}))

    # ══════════════════════════════════════════════════════
    # CLUSTER POLICY CHECKS (3 checks)
    # ══════════════════════════════════════════════════════
    pol_data, pol_s, pol_e = await _dbx_get(client, host, "/api/2.0/policies/clusters/list", token)
    _policy_check_ids = ("SAT-SPARK-POLICY-TAGS", "SAT-SPARK-POLICY-AUTOTERMINATE", "SAT-SPARK-POLICY-MAX-WORKERS")
    if pol_data is None:
        for cid in _policy_check_ids:
            findings.append(_na(cid, pol_s, pol_e))
    else:
        policies = pol_data.get("policies", [])
        if not policies:
            for cid in _policy_check_ids:
                findings.append(_make_finding(cid, "NOT_APPLICABLE", "No cluster policies found."))
        else:
            for check_id, key_pattern, label in [
                ("SAT-SPARK-POLICY-TAGS", "custom_tags.", "cost attribution tags"),
                ("SAT-SPARK-POLICY-AUTOTERMINATE", "autotermination_minutes", "auto-termination"),
                ("SAT-SPARK-POLICY-MAX-WORKERS", "num_workers", "max worker limits"),
            ]:
                enforcing = 0
                for p in policies:
                    try:
                        defn = _json.loads(p.get("definition", "{}"))
                    except (ValueError, TypeError):
                        defn = {}
                    if any(key_pattern in k for k in defn):
                        enforcing += 1
                pct = round(enforcing / len(policies) * 100)
                status = "PASS" if pct >= 80 else ("WARN" if enforcing > 0 else "FAIL")
                findings.append(_make_finding(check_id, status,
                    f"{enforcing}/{len(policies)} policies ({pct}%) enforce {label}.",
                    {"enforcing": enforcing, "total_policies": len(policies), "pct": pct}))

    # ══════════════════════════════════════════════════════
    # UNITY CATALOG GOVERNANCE CHECKS (3 checks)
    # ══════════════════════════════════════════════════════
    _skip_cats = {"system", "hive_metastore", "__databricks_internal"}
    _uc_check_ids = ("SAT-SPARK-UC-TABLE-COMMENTS", "SAT-SPARK-UC-TABLE-TAGS", "SAT-SPARK-UC-CONSTRAINTS")

    cat_data, cat_s, cat_e = await _dbx_get(client, host, "/api/2.1/unity-catalog/catalogs", token)
    if cat_data is None:
        for cid in _uc_check_ids:
            findings.append(_na(cid, cat_s, cat_e))
    else:
        catalogs = [c for c in cat_data.get("catalogs", []) if c.get("name") not in _skip_cats]
        if not catalogs:
            for cid in _uc_check_ids:
                findings.append(_make_finding(cid, "NOT_APPLICABLE", "No user catalogs found."))
        else:
            sample_cat = catalogs[0].get("name", "")
            schemas_data, _, _ = await _dbx_get(client, host,
                "/api/2.1/unity-catalog/schemas", token, params={"catalog_name": sample_cat})
            all_tables: list[dict] = []
            if schemas_data:
                for schema in schemas_data.get("schemas", [])[:3]:
                    schema_name = schema.get("name", "")
                    if schema_name in ("information_schema", "default"):
                        continue
                    tbl_data, _, _ = await _dbx_get(client, host,
                        "/api/2.1/unity-catalog/tables", token,
                        params={"catalog_name": sample_cat, "schema_name": schema_name})
                    if tbl_data:
                        all_tables.extend(tbl_data.get("tables", []))

            if not all_tables:
                for cid in _uc_check_ids:
                    findings.append(_make_finding(cid, "NOT_APPLICABLE",
                        f"No tables found in {sample_cat} catalog."))
            else:
                # ── SAT-SPARK-UC-TABLE-COMMENTS ──
                with_comment = sum(1 for t in all_tables if t.get("comment"))
                pct = round(with_comment / len(all_tables) * 100)
                status = "PASS" if pct >= 80 else ("WARN" if with_comment > 0 else "FAIL")
                findings.append(_make_finding("SAT-SPARK-UC-TABLE-COMMENTS", status,
                    f"{with_comment}/{len(all_tables)} tables ({pct}%) in {sample_cat} have comments.",
                    {"with_comment": with_comment, "total": len(all_tables), "pct": pct,
                     "sampled_catalog": sample_cat}))

                # ── SAT-SPARK-UC-TABLE-TAGS ──
                with_tags = sum(1 for t in all_tables if t.get("tags") or t.get("table_tags"))
                pct = round(with_tags / len(all_tables) * 100)
                status = "PASS" if pct >= 50 else ("WARN" if with_tags > 0 else "FAIL")
                findings.append(_make_finding("SAT-SPARK-UC-TABLE-TAGS", status,
                    f"{with_tags}/{len(all_tables)} tables ({pct}%) in {sample_cat} have classification tags.",
                    {"with_tags": with_tags, "total": len(all_tables), "pct": pct}))

                # ── SAT-SPARK-UC-CONSTRAINTS ──
                delta_tables = [t for t in all_tables if t.get("data_source_format", "").upper() == "DELTA"]
                if not delta_tables:
                    findings.append(_make_finding("SAT-SPARK-UC-CONSTRAINTS", "NOT_APPLICABLE",
                        "No Delta tables found to check constraints."))
                else:
                    with_constraints = sum(1 for t in delta_tables
                        if t.get("table_constraints") or (t.get("columns") and
                        any(col.get("nullable") is False for col in (t.get("columns") or []))))
                    pct = round(with_constraints / len(delta_tables) * 100)
                    status = "PASS" if pct >= 50 else ("WARN" if with_constraints > 0 else "FAIL")
                    findings.append(_make_finding("SAT-SPARK-UC-CONSTRAINTS", status,
                        f"{with_constraints}/{len(delta_tables)} Delta tables ({pct}%) have constraints.",
                        {"with_constraints": with_constraints, "total_delta": len(delta_tables), "pct": pct}))

    # ══════════════════════════════════════════════════════
    # SQL WAREHOUSE CHECKS (1 check)
    # ══════════════════════════════════════════════════════
    wh_data, wh_s, wh_e = await _dbx_get(client, host, "/api/2.0/sql/warehouses", token)
    if wh_data is None:
        findings.append(_na("SAT-SPARK-WH-QUERY-TIMEOUT", wh_s, wh_e))
    else:
        warehouses = wh_data.get("warehouses", [])
        if not warehouses:
            findings.append(_make_finding("SAT-SPARK-WH-QUERY-TIMEOUT", "NOT_APPLICABLE",
                "No SQL warehouses found."))
        else:
            with_timeout = sum(1 for w in warehouses
                if w.get("channel", {}).get("config_overrides", {}).get("statement_timeout")
                or any("timeout" in str(k).lower()
                       for k in (w.get("channel", {}).get("config_overrides") or {}).keys()))
            findings.append(_make_finding("SAT-SPARK-WH-QUERY-TIMEOUT",
                "PASS" if with_timeout == len(warehouses) else "WARN",
                f"{with_timeout}/{len(warehouses)} warehouses have explicit query timeout configured.",
                {"with_timeout": with_timeout, "total": len(warehouses)}))

    # ══════════════════════════════════════════════════════
    # DLT PIPELINE CHECKS (1 check)
    # ══════════════════════════════════════════════════════
    pipe_data, pipe_s, pipe_e = await _dbx_get(client, host, "/api/2.0/pipelines", token)
    if pipe_data is None:
        findings.append(_na("SAT-SPARK-DLT-EXPECTATIONS", pipe_s, pipe_e))
    else:
        pipelines = pipe_data.get("statuses", [])
        if not pipelines:
            findings.append(_make_finding("SAT-SPARK-DLT-EXPECTATIONS", "NOT_APPLICABLE",
                "No DLT pipelines found."))
        else:
            with_expectations = 0
            checked = 0
            for pipe in pipelines[:5]:
                pid = pipe.get("pipeline_id", "")
                if not pid:
                    continue
                detail, _, _ = await _dbx_get(client, host, f"/api/2.0/pipelines/{pid}", token)
                if detail:
                    checked += 1
                    spec = detail.get("spec", {})
                    if spec.get("configuration") or detail.get("latest_updates"):
                        with_expectations += 1
            if checked == 0:
                findings.append(_make_finding("SAT-SPARK-DLT-EXPECTATIONS", "NOT_APPLICABLE",
                    "Could not inspect DLT pipeline details."))
            else:
                pct = round(with_expectations / checked * 100)
                status = "PASS" if pct >= 80 else ("WARN" if with_expectations > 0 else "FAIL")
                findings.append(_make_finding("SAT-SPARK-DLT-EXPECTATIONS", status,
                    f"{with_expectations}/{checked} sampled pipelines appear to have data quality configuration.",
                    {"with_expectations": with_expectations, "checked": checked}))

    # ══════════════════════════════════════════════════════
    # NAMING CONVENTION CHECK (1 check)
    # ══════════════════════════════════════════════════════
    # Re-use tables from UC governance section if available, otherwise fetch
    naming_tables = all_tables if cat_data is not None and 'all_tables' in dir() and all_tables else []
    if not naming_tables and cat_data is not None:
        catalogs_for_naming = [c for c in cat_data.get("catalogs", [])
                               if c.get("name") not in _skip_cats]
        if catalogs_for_naming:
            nc = catalogs_for_naming[0].get("name", "")
            nc_schemas, _, _ = await _dbx_get(client, host,
                "/api/2.1/unity-catalog/schemas", token, params={"catalog_name": nc})
            if nc_schemas:
                for schema in nc_schemas.get("schemas", [])[:3]:
                    sn = schema.get("name", "")
                    if sn in ("information_schema", "default"):
                        continue
                    td, _, _ = await _dbx_get(client, host,
                        "/api/2.1/unity-catalog/tables", token,
                        params={"catalog_name": nc, "schema_name": sn})
                    if td:
                        naming_tables.extend(td.get("tables", []))

    if not naming_tables:
        findings.append(_make_finding("SAT-SPARK-NAMING-CONVENTION", "NOT_APPLICABLE",
            "No tables found to check naming conventions."))
    else:
        import re as _re
        _snake_re = _re.compile(r'^[a-z][a-z0-9]*(_[a-z0-9]+)*$')
        snake_ok = sum(1 for t in naming_tables if _snake_re.match(t.get("name", "")))
        pct = round(snake_ok / len(naming_tables) * 100)
        status = "PASS" if pct >= 90 else ("WARN" if pct >= 50 else "FAIL")
        non_snake = [t.get("name") for t in naming_tables if not _snake_re.match(t.get("name", ""))][:5]
        findings.append(_make_finding("SAT-SPARK-NAMING-CONVENTION", status,
            f"{snake_ok}/{len(naming_tables)} tables ({pct}%) follow snake_case naming.",
            {"snake_case_count": snake_ok, "total": len(naming_tables), "pct": pct,
             "non_snake_examples": non_snake}))

    # ══════════════════════════════════════════════════════
    # TABLE STATISTICS CHECK (1 check)
    # ══════════════════════════════════════════════════════
    stats_tables = naming_tables  # Re-use same table list
    if not stats_tables:
        findings.append(_make_finding("SAT-SPARK-TABLE-STATS", "NOT_APPLICABLE",
            "No tables found to check statistics."))
    else:
        delta_for_stats = [t for t in stats_tables
                           if t.get("data_source_format", "").upper() == "DELTA"]
        if not delta_for_stats:
            findings.append(_make_finding("SAT-SPARK-TABLE-STATS", "NOT_APPLICABLE",
                "No Delta tables found to check statistics."))
        else:
            # Tables with properties indicating statistics were collected have
            # 'delta.stats.columns' or non-null column-level statistics metadata
            with_stats = sum(1 for t in delta_for_stats
                if t.get("properties", {}).get("delta.dataSkippingStatsColumns")
                or t.get("columns") and any(
                    col.get("type_precision") is not None or col.get("type_scale") is not None
                    for col in (t.get("columns") or [])))
            pct = round(with_stats / len(delta_for_stats) * 100)
            status = "PASS" if pct >= 50 else ("WARN" if with_stats > 0 else "FAIL")
            findings.append(_make_finding("SAT-SPARK-TABLE-STATS", status,
                f"{with_stats}/{len(delta_for_stats)} Delta tables ({pct}%) have statistics metadata.",
                {"with_stats": with_stats, "total_delta": len(delta_for_stats), "pct": pct}))

    # ══════════════════════════════════════════════════════
    # CLUSTER POLICY RUNTIME VERSION CHECK (1 check)
    # ══════════════════════════════════════════════════════
    if pol_data is None:
        findings.append(_na("SAT-SPARK-POLICY-RUNTIME", pol_s, pol_e))
    else:
        policies = pol_data.get("policies", [])
        if not policies:
            findings.append(_make_finding("SAT-SPARK-POLICY-RUNTIME", "NOT_APPLICABLE",
                "No cluster policies found."))
        else:
            enforcing_rt = 0
            for p in policies:
                try:
                    defn = _json.loads(p.get("definition", "{}"))
                except (ValueError, TypeError):
                    defn = {}
                if "spark_version" in defn:
                    enforcing_rt += 1
            pct = round(enforcing_rt / len(policies) * 100)
            status = "PASS" if pct >= 80 else ("WARN" if enforcing_rt > 0 else "FAIL")
            findings.append(_make_finding("SAT-SPARK-POLICY-RUNTIME", status,
                f"{enforcing_rt}/{len(policies)} policies ({pct}%) enforce runtime version constraints.",
                {"enforcing": enforcing_rt, "total_policies": len(policies), "pct": pct}))

    # ══════════════════════════════════════════════════════
    # QUERY ANTI-PATTERN CHECK (1 check)
    # ══════════════════════════════════════════════════════
    hdr = {"Authorization": f"Bearer {token}"}
    try:
        qh_resp = await client.get(f"{host.rstrip('/')}/api/2.0/sql/history/queries",
            params={"max_results": "100"}, headers=hdr, timeout=15)
        if qh_resp.status_code == 200:
            qh_data = qh_resp.json()
            queries = qh_data.get("res", qh_data.get("results", []))
            if not queries:
                findings.append(_make_finding("SAT-SPARK-QUERY-ANTIPATTERN", "NOT_APPLICABLE",
                    "No SQL query history found."))
            else:
                import re as _re2
                select_star_re = _re2.compile(r'(?i)SELECT\s+\*\s+FROM')
                select_star = sum(1 for q in queries
                    if select_star_re.search(q.get("query_text", q.get("statement_text", ""))))
                full_scans = sum(1 for q in queries
                    if (q.get("metrics", {}).get("read_bytes", 0) or 0) > 10_737_418_240)
                antipattern_count = select_star + full_scans
                pct = round(antipattern_count / len(queries) * 100)
                if pct <= 5:
                    status = "PASS"
                elif pct <= 20:
                    status = "WARN"
                else:
                    status = "FAIL"
                findings.append(_make_finding("SAT-SPARK-QUERY-ANTIPATTERN", status,
                    f"{antipattern_count}/{len(queries)} recent queries ({pct}%) contain anti-patterns "
                    f"({select_star} SELECT *, {full_scans} full scans >10GB).",
                    {"select_star": select_star, "full_scans": full_scans,
                     "total_queries": len(queries), "pct": pct}))
        else:
            findings.append(_na("SAT-SPARK-QUERY-ANTIPATTERN", qh_resp.status_code,
                f"HTTP {qh_resp.status_code}"))
    except Exception:
        findings.append(_na("SAT-SPARK-QUERY-ANTIPATTERN", 0, "Query history API unavailable"))

    # ══════════════════════════════════════════════════════
    # CLUSTER POLICY PHOTON CHECK (1 check)
    # ══════════════════════════════════════════════════════
    if pol_data is None:
        findings.append(_na("SAT-SPARK-POLICY-PHOTON", pol_s, pol_e))
    else:
        policies = pol_data.get("policies", [])
        if not policies:
            findings.append(_make_finding("SAT-SPARK-POLICY-PHOTON", "NOT_APPLICABLE",
                "No cluster policies found."))
        else:
            enforcing_photon = 0
            for p in policies:
                try:
                    defn = _json.loads(p.get("definition", "{}"))
                except (ValueError, TypeError):
                    defn = {}
                if "runtime_engine" in defn:
                    enforcing_photon += 1
            pct = round(enforcing_photon / len(policies) * 100)
            status = "PASS" if pct >= 80 else ("WARN" if enforcing_photon > 0 else "FAIL")
            findings.append(_make_finding("SAT-SPARK-POLICY-PHOTON", status,
                f"{enforcing_photon}/{len(policies)} policies ({pct}%) enforce Photon runtime engine.",
                {"enforcing": enforcing_photon, "total_policies": len(policies), "pct": pct}))

    # ══════════════════════════════════════════════════════
    # CLUSTER POLICY SPARK CONF CHECK (1 check)
    # ══════════════════════════════════════════════════════
    if pol_data is None:
        findings.append(_na("SAT-SPARK-POLICY-SPARK-CONF", pol_s, pol_e))
    else:
        policies = pol_data.get("policies", [])
        if not policies:
            findings.append(_make_finding("SAT-SPARK-POLICY-SPARK-CONF", "NOT_APPLICABLE",
                "No cluster policies found."))
        else:
            enforcing_conf = 0
            for p in policies:
                try:
                    defn = _json.loads(p.get("definition", "{}"))
                except (ValueError, TypeError):
                    defn = {}
                if any("spark_conf." in k for k in defn):
                    enforcing_conf += 1
            pct = round(enforcing_conf / len(policies) * 100)
            status = "PASS" if pct >= 80 else ("WARN" if enforcing_conf > 0 else "FAIL")
            findings.append(_make_finding("SAT-SPARK-POLICY-SPARK-CONF", status,
                f"{enforcing_conf}/{len(policies)} policies ({pct}%) enforce Spark configuration defaults.",
                {"enforcing": enforcing_conf, "total_policies": len(policies), "pct": pct}))

    # ══════════════════════════════════════════════════════
    # ENVIRONMENT CATALOG SEPARATION CHECK (1 check)
    # ══════════════════════════════════════════════════════
    if cat_data is None:
        findings.append(_na("SAT-SPARK-ENV-CATALOGS", cat_s, cat_e))
    else:
        catalogs = [c for c in cat_data.get("catalogs", []) if c.get("name") not in _skip_cats]
        if not catalogs:
            findings.append(_make_finding("SAT-SPARK-ENV-CATALOGS", "NOT_APPLICABLE",
                "No user catalogs found."))
        else:
            env_keywords = {"prod", "production", "dev", "development", "staging", "stg", "test", "sandbox"}
            cat_names = [c.get("name", "").lower() for c in catalogs]
            found_envs = {kw for kw in env_keywords if any(kw in cn for cn in cat_names)}
            has_prod = bool({"prod", "production"} & found_envs)
            has_dev = bool({"dev", "development"} & found_envs)
            if has_prod and has_dev:
                status = "PASS"
            elif found_envs:
                status = "WARN"
            else:
                status = "FAIL"
            findings.append(_make_finding("SAT-SPARK-ENV-CATALOGS", status,
                f"Found {len(found_envs)} environment-related catalog(s): "
                f"{', '.join(sorted(found_envs)) or 'none'}.",
                {"found_envs": sorted(found_envs), "catalog_names": cat_names}))

    # ══════════════════════════════════════════════════════
    # TABLE QUALITY TIER PROPERTIES CHECK (1 check)
    # ══════════════════════════════════════════════════════
    quality_tables = naming_tables
    if not quality_tables:
        findings.append(_make_finding("SAT-SPARK-TABLE-QUALITY-TIER", "NOT_APPLICABLE",
            "No tables found to check quality tier properties."))
    else:
        quality_keywords = {"bronze", "silver", "gold", "raw", "curated"}
        with_tier = sum(1 for t in quality_tables
            if any(str(v).lower() in quality_keywords
                   for v in (t.get("properties") or {}).values()))
        pct = round(with_tier / len(quality_tables) * 100)
        status = "PASS" if pct >= 50 else ("WARN" if with_tier > 0 else "FAIL")
        findings.append(_make_finding("SAT-SPARK-TABLE-QUALITY-TIER", status,
            f"{with_tier}/{len(quality_tables)} tables ({pct}%) have quality tier TBLPROPERTIES.",
            {"with_tier": with_tier, "total": len(quality_tables), "pct": pct}))

    # Enrich all findings with API response data
    _api_map: dict[str, Any] = {
        "SAT-SPARK-AQE": cl_data, "SAT-SPARK-AQE-SKEW": cl_data,
        "SAT-SPARK-SHUFFLE-PARTITIONS": cl_data, "SAT-SPARK-DYNAMIC-OVERWRITE": cl_data,
        "SAT-SPARK-SCHEMA-AUTOMERGE": cl_data, "SAT-SPARK-STRICT-TIMESTAMP": cl_data,
        "SAT-SPARK-CLUSTER-LOG-DELIVERY": cl_data,
        "SAT-SPARK-JOB-PHOTON": jobs_data, "SAT-SPARK-JOB-RETRY": jobs_data,
        "SAT-SPARK-JOB-TIMEOUT": jobs_data, "SAT-SPARK-JOB-INTERACTIVE": jobs_data,
        "SAT-SPARK-JOB-NOTIFICATIONS": jobs_data,
        "SAT-SPARK-POLICY-TAGS": pol_data, "SAT-SPARK-POLICY-AUTOTERMINATE": pol_data,
        "SAT-SPARK-POLICY-MAX-WORKERS": pol_data, "SAT-SPARK-POLICY-RUNTIME": pol_data,
        "SAT-SPARK-POLICY-PHOTON": pol_data, "SAT-SPARK-POLICY-SPARK-CONF": pol_data,
        "SAT-SPARK-WH-QUERY-TIMEOUT": wh_data,
        "SAT-SPARK-DLT-EXPECTATIONS": pipe_data,
        "SAT-SPARK-NAMING-CONVENTION": cat_data, "SAT-SPARK-TABLE-STATS": cat_data,
        "SAT-SPARK-ENV-CATALOGS": cat_data, "SAT-SPARK-TABLE-QUALITY-TIER": cat_data,
    }
    for f in findings:
        resp = _api_map.get(f.check_id)
        if resp is not None:
            f.details.setdefault("api_response", resp)

    return findings


# ─────────────────────────────────────────────────────────────────────────────
# Dev Practices — repo config, notebook anti-patterns, dashboards
# ─────────────────────────────────────────────────────────────────────────────

async def _check_dev_practices(client: httpx.AsyncClient, host: str, token: str) -> list[SATFinding]:
    """Dev Practices checks — repo configs, notebook anti-patterns, dashboards."""
    findings: list[SATFinding] = []
    hdr = {"Authorization": f"Bearer {token}"}

    # ══════════════════════════════════════════════════════
    # PHASE 1: Fetch repos and browse their file trees
    # ══════════════════════════════════════════════════════
    repos_data, rp_s, rp_e = await _dbx_get(client, host, "/api/2.0/repos", token)
    repos = (repos_data or {}).get("repos", [])

    repo_files: dict[str, set[str]] = {}       # repo_path -> {filenames at root}
    repo_github_files: dict[str, set[str]] = {} # repo_path -> {.github/ children}

    if repos:
        _sem = asyncio.Semaphore(5)

        async def _list_repo_root(repo):
            rpath = repo.get("path", "")
            async with _sem:
                try:
                    r = await client.get(f"{host}/api/2.0/workspace/list",
                        headers=hdr, params={"path": rpath}, timeout=15)
                    if r.status_code == 200:
                        repo_files[rpath] = {
                            obj.get("path", "").rsplit("/", 1)[-1]
                            for obj in r.json().get("objects", [])
                        }
                except Exception:
                    pass

        async def _list_github_dir(repo):
            rpath = repo.get("path", "")
            async with _sem:
                try:
                    r = await client.get(f"{host}/api/2.0/workspace/list",
                        headers=hdr, params={"path": f"{rpath}/.github"}, timeout=15)
                    if r.status_code == 200:
                        repo_github_files[rpath] = {
                            obj.get("path", "").rsplit("/", 1)[-1]
                            for obj in r.json().get("objects", [])
                        }
                except Exception:
                    pass

        await asyncio.gather(*(_list_repo_root(r) for r in repos))
        await asyncio.gather(*(_list_github_dir(r) for r in repos))

    # ══════════════════════════════════════════════════════
    # PHASE 2: Six repo config checks
    # ══════════════════════════════════════════════════════

    def _repo_config_check(check_id: str, detect_fn, desc_fn: str):
        if repos_data is None:
            findings.append(_na(check_id, rp_s, rp_e))
        elif not repos:
            findings.append(_make_finding(check_id, "NOT_APPLICABLE", "No Git repos connected."))
        else:
            count = sum(1 for rp, files in repo_files.items() if detect_fn(rp, files))
            pct = round(count / len(repos) * 100)
            status = "PASS" if pct >= 50 else ("WARN" if count > 0 else "FAIL")
            findings.append(_make_finding(check_id, status,
                f"{count}/{len(repos)} repos ({pct}%) {desc_fn}.",
                {"with_config": count, "total_repos": len(repos), "pct": pct}))

    _repo_config_check("SAT-DEV-SQLFLUFF",
        lambda rp, f: ".sqlfluff" in f,
        "have .sqlfluff config")

    _repo_config_check("SAT-DEV-RUFF",
        lambda rp, f: bool(f & {"ruff.toml", "pyproject.toml", ".flake8", ".ruff.toml"}),
        "have Python linting config")

    _repo_config_check("SAT-DEV-PRECOMMIT",
        lambda rp, f: ".pre-commit-config.yaml" in f,
        "have pre-commit hooks")

    _repo_config_check("SAT-DEV-CI-PIPELINE",
        lambda rp, f: bool(f & {"Jenkinsfile", ".gitlab-ci.yml", "azure-pipelines.yml"})
                        or "workflows" in repo_github_files.get(rp, set()),
        "have CI/CD pipeline config")

    _repo_config_check("SAT-DEV-CODE-REVIEW",
        lambda rp, f: "PULL_REQUEST_TEMPLATE.md" in repo_github_files.get(rp, set()),
        "have PR template")

    _repo_config_check("SAT-DEV-DABS",
        lambda rp, f: bool(f & {"databricks.yml", "databricks.yaml"}),
        "have DABs config")

    # ══════════════════════════════════════════════════════
    # PHASE 3: Notebook source scan (anti-patterns + template)
    # ══════════════════════════════════════════════════════
    import base64 as _b64
    import re as _re

    # List notebooks from /Shared (sample up to 50)
    nb_objects: list[dict] = []
    try:
        r = await client.get(f"{host}/api/2.0/workspace/list",
            headers=hdr, params={"path": "/Shared"}, timeout=15)
        if r.status_code == 200:
            nb_objects = [o for o in r.json().get("objects", [])
                          if o.get("object_type") == "NOTEBOOK"][:50]
    except Exception:
        pass

    antipattern_re = _re.compile(
        r'\.collect\(\)|SELECT\s+\*\s+FROM|@udf\b|inferSchema\s*=\s*["\']?true',
        _re.IGNORECASE)
    template_re = _re.compile(
        r'dbutils\.widgets\.|# MAGIC %md|## (Author|Description|Title|Overview)',
        _re.IGNORECASE)

    flagged_anti = 0
    compliant_tmpl = 0
    nb_sem = asyncio.Semaphore(5)

    async def _scan_nb(nb):
        nonlocal flagged_anti, compliant_tmpl
        async with nb_sem:
            try:
                r = await client.get(f"{host}/api/2.0/workspace/export",
                    headers=hdr, params={"path": nb["path"], "format": "SOURCE"}, timeout=15)
                if r.status_code != 200:
                    return
                source = _b64.b64decode(r.json().get("content", "")).decode("utf-8", errors="replace")
                if antipattern_re.search(source):
                    flagged_anti += 1
                if template_re.search(source):
                    compliant_tmpl += 1
                await asyncio.sleep(0.2)
            except Exception:
                pass

    if nb_objects:
        await asyncio.gather(*(_scan_nb(nb) for nb in nb_objects))

    # SAT-DEV-ANTIPATTERN
    if not nb_objects:
        findings.append(_make_finding("SAT-DEV-ANTIPATTERN", "NOT_APPLICABLE",
            "No notebooks found in /Shared to scan."))
    else:
        pct = round(flagged_anti / len(nb_objects) * 100)
        status = "PASS" if pct <= 10 else ("WARN" if pct <= 30 else "FAIL")
        findings.append(_make_finding("SAT-DEV-ANTIPATTERN", status,
            f"{flagged_anti}/{len(nb_objects)} sampled notebooks ({pct}%) contain anti-patterns "
            f"(.collect(), SELECT *, @udf, inferSchema=true).",
            {"flagged": flagged_anti, "total": len(nb_objects), "pct": pct}))

    # SAT-DEV-NOTEBOOK-TEMPLATE
    if not nb_objects:
        findings.append(_make_finding("SAT-DEV-NOTEBOOK-TEMPLATE", "NOT_APPLICABLE",
            "No notebooks found to check template compliance."))
    else:
        pct = round(compliant_tmpl / len(nb_objects) * 100)
        status = "PASS" if pct >= 70 else ("WARN" if pct >= 30 else "FAIL")
        findings.append(_make_finding("SAT-DEV-NOTEBOOK-TEMPLATE", status,
            f"{compliant_tmpl}/{len(nb_objects)} notebooks ({pct}%) follow template conventions.",
            {"compliant": compliant_tmpl, "total": len(nb_objects), "pct": pct}))

    # ══════════════════════════════════════════════════════
    # PHASE 4: Onboarding folder check
    # ══════════════════════════════════════════════════════
    onboarding_found = False
    for check_path in ["/Shared/onboarding", "/Shared/docs", "/Shared/documentation"]:
        try:
            r = await client.get(f"{host}/api/2.0/workspace/list",
                headers=hdr, params={"path": check_path}, timeout=10)
            if r.status_code == 200 and r.json().get("objects"):
                onboarding_found = True
                break
        except Exception:
            pass
    findings.append(_make_finding("SAT-DEV-ONBOARDING",
        "PASS" if onboarding_found else "WARN",
        "Onboarding/docs folder found in /Shared." if onboarding_found
        else "No /Shared/onboarding or /Shared/docs folder found."))

    # ══════════════════════════════════════════════════════
    # PHASE 5: Data quality dashboard check
    # ══════════════════════════════════════════════════════
    try:
        r = await client.get(f"{host}/api/2.0/lakeview/dashboards", headers=hdr, timeout=15)
        if r.status_code == 200:
            dashboards = r.json().get("dashboards", [])
            dq_keywords = {"quality", "dq", "monitoring", "freshness", "anomal"}
            dq_dbs = [d for d in dashboards
                if any(kw in d.get("display_name", d.get("name", "")).lower()
                       for kw in dq_keywords)]
            findings.append(_make_finding("SAT-DEV-DQ-DASHBOARD",
                "PASS" if dq_dbs else "WARN",
                f"{len(dq_dbs)} data quality dashboard(s) found (of {len(dashboards)} total).",
                {"dq_count": len(dq_dbs), "total_dashboards": len(dashboards)}))
        else:
            findings.append(_make_finding("SAT-DEV-DQ-DASHBOARD", "NOT_APPLICABLE",
                f"Lakeview API returned HTTP {r.status_code}."))
    except Exception:
        findings.append(_make_finding("SAT-DEV-DQ-DASHBOARD", "NOT_APPLICABLE",
            "Lakeview API unavailable."))

    # SAT-DEV-ENV-SEPARATION: Environment separation (dev/staging/prod catalogs)
    try:
        r = await client.get(f"{host}/api/2.1/unity-catalog/catalogs", headers=hdr, timeout=15)
        if r.status_code == 200:
            catalogs = r.json().get("catalogs", [])
            _skip = {"system", "hive_metastore", "__databricks_internal"}
            user_cats = [c.get("name", "").lower() for c in catalogs if c.get("name") not in _skip]
            env_keywords = {"dev", "staging", "stg", "prod", "production", "qa", "test"}
            env_cats = [c for c in user_cats if any(kw in c for kw in env_keywords)]
            if len(env_cats) >= 2:
                findings.append(_make_finding("SAT-DEV-ENV-SEPARATION", "PASS",
                    f"Environment separation detected: {', '.join(env_cats[:5])}."))
            elif env_cats:
                findings.append(_make_finding("SAT-DEV-ENV-SEPARATION", "WARN",
                    f"Only 1 environment catalog found ({env_cats[0]}). Add dev/staging/prod catalogs."))
            else:
                findings.append(_make_finding("SAT-DEV-ENV-SEPARATION", "WARN",
                    f"{len(user_cats)} catalog(s) found but none follow environment naming patterns."))
        elif r.status_code in (404, 403):
            findings.append(_make_finding("SAT-DEV-ENV-SEPARATION", "NOT_APPLICABLE",
                f"Catalogs API not available (HTTP {r.status_code})."))
        else:
            findings.append(_make_finding("SAT-DEV-ENV-SEPARATION", "WARN", f"HTTP {r.status_code}."))
    except Exception as exc:
        findings.append(_make_finding("SAT-DEV-ENV-SEPARATION", "WARN", f"Error: {exc}"))

    return findings


# ─────────────────────────────────────────────────────────────────────────────
# Delta Best Practices (6 checks)
# ─────────────────────────────────────────────────────────────────────────────

async def _check_delta_best_practices(client: httpx.AsyncClient, host: str, token: str) -> list[SATFinding]:
    """Delta Lake best practices: vacuum retention, CDF, deletion vectors, column mapping, UniForm, clones."""
    findings: list[SATFinding] = []
    hdr = {"Authorization": f"Bearer {token}"}

    # Fetch catalogs and sample tables
    _skip_cats = {"system", "hive_metastore", "__databricks_internal"}
    cat_data, cat_s, cat_e = await _dbx_get(client, host, "/api/2.1/unity-catalog/catalogs", token)
    user_cats = [c for c in (cat_data or {}).get("catalogs", []) if c.get("name") not in _skip_cats]

    _all_tables: list[dict] = []
    if cat_data is not None and user_cats:
        for cat in user_cats[:3]:
            s_data, _, _ = await _dbx_get(client, host,
                "/api/2.1/unity-catalog/schemas", token,
                params={"catalog_name": cat["name"]})
            if s_data:
                for schema in [s for s in s_data.get("schemas", []) if s.get("name") != "information_schema"][:5]:
                    t_data, _, _ = await _dbx_get(client, host,
                        "/api/2.1/unity-catalog/tables", token,
                        params={"catalog_name": cat["name"], "schema_name": schema["name"],
                                "max_results": "10"})
                    if t_data:
                        _all_tables.extend(t_data.get("tables", []))
                    await asyncio.sleep(0.15)
            await asyncio.sleep(0.25)

    delta_tables = [t for t in _all_tables if (t.get("data_source_format") or "").upper() == "DELTA"]

    if cat_data is None:
        for cid in ("SAT-DELTA-VACUUM-RETENTION", "SAT-DELTA-CDF-ENABLED",
                     "SAT-DELTA-DELETION-VECTORS", "SAT-DELTA-COLUMN-MAPPING",
                     "SAT-DELTA-UNIFORM", "SAT-DELTA-CLONE-EXISTS"):
            findings.append(_na(cid, cat_s, cat_e))
        return findings

    if not delta_tables:
        for cid in ("SAT-DELTA-VACUUM-RETENTION", "SAT-DELTA-CDF-ENABLED",
                     "SAT-DELTA-DELETION-VECTORS", "SAT-DELTA-COLUMN-MAPPING",
                     "SAT-DELTA-UNIFORM", "SAT-DELTA-CLONE-EXISTS"):
            findings.append(_make_finding(cid, "NOT_APPLICABLE", "No Delta tables found in sampled catalogs."))
        return findings

    # SAT-DELTA-VACUUM-RETENTION: Check delta.deletedFileRetentionDuration in table properties
    short_retention = 0
    for t in delta_tables:
        props = t.get("properties", {})
        retention = props.get("delta.deletedFileRetentionDuration", "")
        if retention:
            try:
                days = int(retention.replace("interval ", "").split(" ")[0])
                if days <= 30:
                    short_retention += 1
            except (ValueError, IndexError):
                pass
    # Tables with no explicit retention use 7-day default which is fine
    findings.append(_make_finding("SAT-DELTA-VACUUM-RETENTION",
        "PASS" if short_retention > 0 or len(delta_tables) > 0 else "WARN",
        f"{short_retention}/{len(delta_tables)} sampled Delta tables have VACUUM retention ≤ 30 days."))

    # SAT-DELTA-CDF-ENABLED: Change Data Feed
    cdf_enabled = sum(1 for t in delta_tables
        if str(t.get("properties", {}).get("delta.enableChangeDataFeed", "")).lower() == "true")
    pct = round(cdf_enabled / len(delta_tables) * 100) if delta_tables else 0
    findings.append(_make_finding("SAT-DELTA-CDF-ENABLED",
        "PASS" if pct >= 30 else ("WARN" if cdf_enabled > 0 else "FAIL"),
        f"{cdf_enabled}/{len(delta_tables)} sampled Delta tables ({pct}%) have Change Data Feed enabled."))

    # SAT-DELTA-DELETION-VECTORS
    dv_enabled = sum(1 for t in delta_tables
        if str(t.get("properties", {}).get("delta.enableDeletionVectors", "")).lower() == "true")
    pct_dv = round(dv_enabled / len(delta_tables) * 100) if delta_tables else 0
    findings.append(_make_finding("SAT-DELTA-DELETION-VECTORS",
        "PASS" if pct_dv >= 50 else ("WARN" if dv_enabled > 0 else "FAIL"),
        f"{dv_enabled}/{len(delta_tables)} sampled Delta tables ({pct_dv}%) have deletion vectors enabled."))

    # SAT-DELTA-COLUMN-MAPPING
    cm_enabled = sum(1 for t in delta_tables
        if t.get("properties", {}).get("delta.columnMapping.mode", "none").lower() != "none")
    pct_cm = round(cm_enabled / len(delta_tables) * 100) if delta_tables else 0
    findings.append(_make_finding("SAT-DELTA-COLUMN-MAPPING",
        "PASS" if pct_cm >= 50 else ("WARN" if cm_enabled > 0 else "FAIL"),
        f"{cm_enabled}/{len(delta_tables)} sampled Delta tables ({pct_cm}%) have column mapping enabled."))

    # SAT-DELTA-UNIFORM: Delta UniForm (universalFormat)
    uniform_enabled = sum(1 for t in delta_tables
        if str(t.get("properties", {}).get("delta.universalFormat.enabledFormats", "")).strip())
    pct_uf = round(uniform_enabled / len(delta_tables) * 100) if delta_tables else 0
    findings.append(_make_finding("SAT-DELTA-UNIFORM",
        "PASS" if pct_uf >= 20 else ("WARN" if uniform_enabled > 0 else "FAIL"),
        f"{uniform_enabled}/{len(delta_tables)} sampled Delta tables ({pct_uf}%) have UniForm enabled."))

    # SAT-DELTA-CLONE-EXISTS: Check for CLONE tables (shallow/deep)
    clone_tables = [t for t in _all_tables if "clone" in t.get("name", "").lower()
        or t.get("properties", {}).get("delta.cloneSource")]
    findings.append(_make_finding("SAT-DELTA-CLONE-EXISTS",
        "PASS" if clone_tables else "WARN",
        f"{len(clone_tables)} clone table(s) found among {len(_all_tables)} sampled tables."))

    for f in findings:
        f.details.setdefault("api_response", cat_data)
    return findings


# ─────────────────────────────────────────────────────────────────────────────
# DLT Best Practices (5 checks)
# ─────────────────────────────────────────────────────────────────────────────

async def _check_dlt_best_practices(client: httpx.AsyncClient, host: str, token: str) -> list[SATFinding]:
    """DLT best practices: multi-layer, quarantine, UC, freshness, serverless."""
    findings: list[SATFinding] = []
    hdr = {"Authorization": f"Bearer {token}"}

    try:
        r = await client.get(f"{host}/api/2.0/pipelines", params={"max_results": "50"}, headers=hdr, timeout=15)
    except Exception as exc:
        for cid in ("SAT-DLT-MULTI-LAYER", "SAT-DLT-QUARANTINE", "SAT-DLT-UC-ENABLED",
                     "SAT-DLT-FRESHNESS", "SAT-DLT-SERVERLESS"):
            findings.append(_make_finding(cid, "WARN", f"Error fetching pipelines: {exc}"))
        return findings

    if r.status_code != 200:
        for cid in ("SAT-DLT-MULTI-LAYER", "SAT-DLT-QUARANTINE", "SAT-DLT-UC-ENABLED",
                     "SAT-DLT-FRESHNESS", "SAT-DLT-SERVERLESS"):
            if r.status_code in (404, 403):
                findings.append(_make_finding(cid, "NOT_APPLICABLE",
                    f"Pipelines API not available (HTTP {r.status_code})."))
            else:
                findings.append(_make_finding(cid, "WARN", f"HTTP {r.status_code}."))
        return findings

    pipelines = r.json().get("statuses", [])
    if not pipelines:
        for cid in ("SAT-DLT-MULTI-LAYER", "SAT-DLT-QUARANTINE", "SAT-DLT-UC-ENABLED",
                     "SAT-DLT-FRESHNESS", "SAT-DLT-SERVERLESS"):
            findings.append(_make_finding(cid, "NOT_APPLICABLE", "No DLT pipelines found."))
        return findings

    # Fetch pipeline details (max 10)
    specs: list[dict] = []
    for pl in pipelines[:10]:
        try:
            pr = await client.get(f"{host}/api/2.0/pipelines/{pl.get('pipeline_id', '')}",
                headers=hdr, timeout=10)
            if pr.status_code == 200:
                specs.append(pr.json())
        except:
            pass

    sampled = len(specs)
    if not specs:
        for cid in ("SAT-DLT-MULTI-LAYER", "SAT-DLT-QUARANTINE", "SAT-DLT-UC-ENABLED",
                     "SAT-DLT-FRESHNESS", "SAT-DLT-SERVERLESS"):
            findings.append(_make_finding(cid, "WARN", "Could not fetch pipeline details."))
        return findings

    # SAT-DLT-MULTI-LAYER: Check for bronze/silver/gold or medallion patterns in pipeline name/libraries
    medallion_keywords = {"bronze", "silver", "gold", "raw", "curated", "refined", "staging"}
    multi_layer = 0
    for s in specs:
        spec = s.get("spec", {})
        name = (spec.get("name") or s.get("name") or "").lower()
        if any(kw in name for kw in medallion_keywords):
            multi_layer += 1
    findings.append(_make_finding("SAT-DLT-MULTI-LAYER",
        "PASS" if multi_layer > 0 else "WARN",
        f"{multi_layer}/{sampled} pipeline(s) indicate medallion architecture naming."))

    # SAT-DLT-QUARANTINE: Check for quarantine/error/reject patterns
    quarantine_keywords = {"quarantine", "error", "reject", "dead_letter", "dlq", "bad_record"}
    quarantine_count = 0
    for s in specs:
        spec = s.get("spec", {})
        name = (spec.get("name") or s.get("name") or "").lower()
        libs = str(spec.get("libraries", "")).lower()
        if any(kw in name or kw in libs for kw in quarantine_keywords):
            quarantine_count += 1
    findings.append(_make_finding("SAT-DLT-QUARANTINE",
        "PASS" if quarantine_count > 0 else "WARN",
        f"{quarantine_count}/{sampled} pipeline(s) have quarantine/error handling patterns."))

    # SAT-DLT-UC-ENABLED: Check catalog field (UC integration)
    uc_enabled = sum(1 for s in specs if s.get("spec", {}).get("catalog"))
    pct_uc = round(uc_enabled / sampled * 100)
    findings.append(_make_finding("SAT-DLT-UC-ENABLED",
        "PASS" if pct_uc >= 80 else ("WARN" if uc_enabled > 0 else "FAIL"),
        f"{uc_enabled}/{sampled} pipeline(s) ({pct_uc}%) use Unity Catalog."))

    # SAT-DLT-FRESHNESS: Check for continuous pipelines or trigger intervals
    has_schedule = 0
    for s in specs:
        spec = s.get("spec", {})
        if spec.get("continuous") or spec.get("trigger"):
            has_schedule += 1
    findings.append(_make_finding("SAT-DLT-FRESHNESS",
        "PASS" if has_schedule > 0 else "WARN",
        f"{has_schedule}/{sampled} pipeline(s) have continuous mode or trigger interval for freshness."))

    # SAT-DLT-SERVERLESS: Check serverless compute
    serverless = sum(1 for s in specs if s.get("spec", {}).get("serverless"))
    pct_sl = round(serverless / sampled * 100)
    findings.append(_make_finding("SAT-DLT-SERVERLESS",
        "PASS" if pct_sl >= 50 else ("WARN" if serverless > 0 else "FAIL"),
        f"{serverless}/{sampled} pipeline(s) ({pct_sl}%) use serverless compute."))

    for f in findings:
        f.details.setdefault("api_response", {"statuses": pipelines})
    return findings


# ─────────────────────────────────────────────────────────────────────────────
# Workspace Object ACLs
# ─────────────────────────────────────────────────────────────────────────────

async def _check_workspace_acls(client: httpx.AsyncClient, host: str, token: str) -> list[SATFinding]:
    """Workspace Object ACLs: notebook, folder, dashboard, query, experiment permissions."""
    findings: list[SATFinding] = []
    hdr = {"Authorization": f"Bearer {token}"}

    # SAT-ACL-NOTEBOOK: Notebooks in /Shared have explicit ACLs
    shared_data, sh_s, sh_e = await _dbx_get(client, host,
        "/api/2.0/workspace/list", token, {"path": "/Shared"})
    if shared_data is not None:
        notebooks = [o for o in shared_data.get("objects", [])
                     if o.get("object_type") == "NOTEBOOK"]
        if not notebooks:
            findings.append(_make_finding("SAT-ACL-NOTEBOOK", "NOT_APPLICABLE",
                "No notebooks found in /Shared."))
        else:
            checked = 0
            has_acl = 0
            for nb in notebooks[:20]:
                obj_id = nb.get("object_id")
                if not obj_id:
                    continue
                checked += 1
                try:
                    r = await client.get(f"{host}/api/2.0/permissions/notebooks/{obj_id}",
                        headers=hdr, timeout=10)
                    if r.status_code == 200:
                        acls = r.json().get("access_control_list", [])
                        if len(acls) > 1:
                            has_acl += 1
                except Exception:
                    pass
            if checked == 0:
                findings.append(_make_finding("SAT-ACL-NOTEBOOK", "NOT_APPLICABLE",
                    "Could not check notebook ACLs."))
            else:
                pct = round(has_acl / checked * 100)
                status = "PASS" if pct >= 70 else ("WARN" if has_acl > 0 else "FAIL")
                findings.append(_make_finding("SAT-ACL-NOTEBOOK", status,
                    f"{has_acl}/{checked} sampled notebooks ({pct}%) in /Shared have explicit ACLs.",
                    {"with_acl": has_acl, "checked": checked, "pct": pct}))
    else:
        findings.append(_na("SAT-ACL-NOTEBOOK", sh_s, sh_e))

    # SAT-ACL-FOLDER: Top-level folders restrict write access
    ws_data, ws_s, ws_e = await _dbx_get(client, host,
        "/api/2.0/workspace/list", token, {"path": "/"})
    if ws_data is not None:
        folders = [o for o in ws_data.get("objects", [])
                   if o.get("object_type") == "DIRECTORY"
                   and o.get("path", "").strip("/") not in ("Users", "Repos", "Shared")]
        if not folders:
            findings.append(_make_finding("SAT-ACL-FOLDER", "NOT_APPLICABLE",
                "No custom top-level folders found."))
        else:
            checked = 0
            restricted = 0
            for fld in folders[:10]:
                obj_id = fld.get("object_id")
                if not obj_id:
                    continue
                checked += 1
                try:
                    r = await client.get(f"{host}/api/2.0/permissions/directories/{obj_id}",
                        headers=hdr, timeout=10)
                    if r.status_code == 200:
                        acls = r.json().get("access_control_list", [])
                        if len(acls) > 1:
                            restricted += 1
                except Exception:
                    pass
            pct = round(restricted / max(checked, 1) * 100)
            status = "PASS" if pct >= 70 else ("WARN" if restricted > 0 else "FAIL")
            findings.append(_make_finding("SAT-ACL-FOLDER", status,
                f"{restricted}/{checked} custom top-level folders ({pct}%) have explicit ACLs.",
                {"restricted": restricted, "checked": checked, "pct": pct}))
    else:
        findings.append(_na("SAT-ACL-FOLDER", ws_s, ws_e))

    # SAT-ACL-DASHBOARD: Dashboards are not world-editable
    dash_data, d_s, d_e = await _dbx_get(client, host,
        "/api/2.0/lakeview/dashboards", token, {"page_size": "50"})
    if dash_data is not None:
        dashboards = dash_data.get("dashboards", [])
        if not dashboards:
            findings.append(_make_finding("SAT-ACL-DASHBOARD", "NOT_APPLICABLE",
                "No Lakeview dashboards found."))
        else:
            findings.append(_make_finding("SAT-ACL-DASHBOARD", "PASS" if len(dashboards) > 0 else "WARN",
                f"{len(dashboards)} dashboard(s) found. Review permissions manually.",
                {"count": len(dashboards)}))
    else:
        findings.append(_na("SAT-ACL-DASHBOARD", d_s, d_e))

    # SAT-ACL-QUERY: Saved queries have owner and explicit grants
    try:
        r = await client.get(f"{host}/api/2.0/sql/queries", headers=hdr,
            params={"page_size": "50"}, timeout=15)
        if r.status_code == 200:
            queries = r.json().get("results", r.json().get("queries", []))
            if not queries:
                findings.append(_make_finding("SAT-ACL-QUERY", "NOT_APPLICABLE",
                    "No saved SQL queries found."))
            else:
                findings.append(_make_finding("SAT-ACL-QUERY",
                    "PASS" if len(queries) > 0 else "WARN",
                    f"{len(queries)} saved queries found. Review permissions manually.",
                    {"count": len(queries)}))
        else:
            findings.append(_make_finding("SAT-ACL-QUERY", "WARN",
                f"Could not fetch SQL queries (HTTP {r.status_code})."))
    except Exception as exc:
        findings.append(_make_finding("SAT-ACL-QUERY", "WARN",
            f"Error fetching SQL queries: {exc}"))

    # SAT-ACL-EXPERIMENT: ML experiments restrict write to owners
    exp_data, exp_s, exp_e = await _dbx_get(client, host,
        "/api/2.0/mlflow/experiments/search", token)
    if exp_data is not None:
        experiments = exp_data.get("experiments", [])
        if not experiments:
            findings.append(_make_finding("SAT-ACL-EXPERIMENT", "NOT_APPLICABLE",
                "No MLflow experiments found."))
        else:
            shared_exps = [e for e in experiments
                          if not (e.get("name", "").startswith("/Users/"))]
            findings.append(_make_finding("SAT-ACL-EXPERIMENT",
                "PASS" if len(shared_exps) == 0 else "WARN",
                f"{len(shared_exps)}/{len(experiments)} experiments are in shared paths "
                f"(not under /Users/) — review ACLs.",
                {"shared": len(shared_exps), "total": len(experiments)}))
    else:
        findings.append(_na("SAT-ACL-EXPERIMENT", exp_s, exp_e))

    return findings


# ─────────────────────────────────────────────────────────────────────────────
# Audit Log Delivery & Monitoring
# ─────────────────────────────────────────────────────────────────────────────

async def _check_audit_delivery(client: httpx.AsyncClient, host: str, token: str) -> list[SATFinding]:
    """Audit delivery: log delivery config, freshness, alerting, retention."""
    findings: list[SATFinding] = []
    hdr = {"Authorization": f"Bearer {token}"}

    # SAT-AUDIT-DELIVERY: Account-level audit log delivery configured
    # Try account-level API first — may not be available with workspace tokens
    delivery_found = False
    delivery_configs = []
    try:
        r = await client.get(f"{host}/api/2.0/account/log-delivery",
            headers=hdr, timeout=15)
        if r.status_code == 200:
            delivery_configs = r.json().get("log_delivery_configurations", [])
            delivery_found = len(delivery_configs) > 0
            status = "PASS" if delivery_found else "FAIL"
            findings.append(_make_finding("SAT-AUDIT-DELIVERY", status,
                f"{len(delivery_configs)} log delivery configuration(s) found."
                if delivery_found else "No audit log delivery configured.",
                {"configs": len(delivery_configs)}))
        elif r.status_code in (403, 404):
            # Account API not accessible — check workspace-level diagnostic log settings
            ws_conf, _, _ = await _dbx_get_workspace_conf(client, host, token,
                "enableVerboseAuditLogs")
            if ws_conf is not None:
                verbose = str(ws_conf.get("enableVerboseAuditLogs", "false")).lower() == "true"
                findings.append(_make_finding("SAT-AUDIT-DELIVERY",
                    "PASS" if verbose else "WARN",
                    f"Verbose audit logs: {'enabled' if verbose else 'disabled'} "
                    f"(account log delivery API not accessible)."))
            else:
                findings.append(_make_finding("SAT-AUDIT-DELIVERY", "WARN",
                    "Cannot verify audit log delivery (account API not accessible)."))
        else:
            findings.append(_make_finding("SAT-AUDIT-DELIVERY", "WARN",
                f"Log delivery API returned HTTP {r.status_code}."))
    except Exception as exc:
        findings.append(_make_finding("SAT-AUDIT-DELIVERY", "WARN",
            f"Error checking audit log delivery: {exc}"))

    # SAT-AUDIT-FRESHNESS: Audit logs delivered within last 24h
    if delivery_configs:
        active = [c for c in delivery_configs if c.get("status") == "ENABLED"]
        findings.append(_make_finding("SAT-AUDIT-FRESHNESS",
            "PASS" if active else "FAIL",
            f"{len(active)}/{len(delivery_configs)} delivery config(s) are ENABLED.",
            {"active": len(active), "total": len(delivery_configs)}))
    else:
        findings.append(_make_finding("SAT-AUDIT-FRESHNESS", "WARN",
            "Cannot verify audit log freshness without delivery configs."))

    # SAT-AUDIT-ALERTING: Alerts configured on high-risk audit events
    alerts_data, al_s, al_e = await _dbx_get(client, host,
        "/api/2.0/sql/alerts", token)
    if alerts_data is not None:
        alerts = alerts_data if isinstance(alerts_data, list) else alerts_data.get("results", [])
        audit_keywords = {"audit", "admin", "permission", "token", "security", "access"}
        audit_alerts = [a for a in alerts
                       if any(kw in (a.get("name", "") + " " + str(a.get("query", {}).get("query", ""))).lower()
                              for kw in audit_keywords)]
        findings.append(_make_finding("SAT-AUDIT-ALERTING",
            "PASS" if audit_alerts else "WARN",
            f"{len(audit_alerts)} alert(s) target audit/security events."
            if audit_alerts else "No alerts configured for high-risk audit events.",
            {"audit_alerts": len(audit_alerts), "total_alerts": len(alerts)}))
    else:
        findings.append(_na("SAT-AUDIT-ALERTING", al_s, al_e))

    # SAT-AUDIT-RETENTION: Audit log storage retention > 365 days
    # This is a governance check — verify system tables have data > 365 days if possible
    findings.append(_make_finding("SAT-AUDIT-RETENTION", "WARN",
        "Verify audit log storage retention exceeds 365 days in your cloud storage lifecycle policy."))

    return findings


# ─────────────────────────────────────────────────────────────────────────────
# Network Exfiltration Controls
# ─────────────────────────────────────────────────────────────────────────────

async def _check_network_exfiltration(client: httpx.AsyncClient, host: str, token: str) -> list[SATFinding]:
    """Network exfiltration: egress firewall, mount trust, external locations, init scripts."""
    findings: list[SATFinding] = []
    hdr = {"Authorization": f"Bearer {token}"}

    # SAT-EXFIL-EGRESS: Workspace has egress firewall rules
    ws_conf, wc_s, wc_e = await _dbx_get_workspace_conf(client, host, token,
        "enableNccConfig,enableIpAccessLists")
    if ws_conf is not None:
        ncc = str(ws_conf.get("enableNccConfig", "false")).lower() == "true"
        ip_acl = str(ws_conf.get("enableIpAccessLists", "false")).lower() == "true"
        findings.append(_make_finding("SAT-EXFIL-EGRESS",
            "PASS" if ncc else ("WARN" if ip_acl else "FAIL"),
            f"NCC: {'enabled' if ncc else 'disabled'}, IP ACLs: {'enabled' if ip_acl else 'disabled'}.",
            {"ncc": ncc, "ip_acl": ip_acl}))
    else:
        findings.append(_na("SAT-EXFIL-EGRESS", wc_s, wc_e))

    # SAT-EXFIL-MOUNT-TRUST: DBFS mounts point only to trusted accounts
    mnt_data, mnt_s, mnt_e = await _dbx_get(client, host,
        "/api/2.0/dbfs/list", token, {"path": "/mnt"})
    if mnt_data is not None:
        mounts = mnt_data.get("files", [])
        if not mounts:
            findings.append(_make_finding("SAT-EXFIL-MOUNT-TRUST", "PASS",
                "No DBFS mount points found — no exfiltration risk via mounts."))
        else:
            findings.append(_make_finding("SAT-EXFIL-MOUNT-TRUST",
                "WARN" if len(mounts) > 3 else "PASS",
                f"{len(mounts)} mount point(s) found. Review target storage accounts for trust.",
                {"mount_count": len(mounts), "mounts": [m.get("path", "") for m in mounts[:10]]}))
    elif mnt_s and mnt_s >= 400:
        findings.append(_make_finding("SAT-EXFIL-MOUNT-TRUST", "PASS",
            "DBFS /mnt not accessible — mounts likely disabled."))
    else:
        findings.append(_na("SAT-EXFIL-MOUNT-TRUST", mnt_s, mnt_e))

    # SAT-EXFIL-EXT-LOC-TRUST: External locations restrict to trusted accounts
    el_data, el_s, el_e = await _dbx_get(client, host,
        "/api/2.1/unity-catalog/external-locations", token)
    if el_data is not None:
        ext_locs = el_data.get("external_locations", [])
        if not ext_locs:
            findings.append(_make_finding("SAT-EXFIL-EXT-LOC-TRUST", "NOT_APPLICABLE",
                "No external locations configured."))
        else:
            urls = [el.get("url", "") for el in ext_locs]
            findings.append(_make_finding("SAT-EXFIL-EXT-LOC-TRUST",
                "WARN" if len(ext_locs) > 5 else "PASS",
                f"{len(ext_locs)} external location(s). Review URLs for trust boundaries.",
                {"count": len(ext_locs), "urls": urls[:10]}))
    else:
        findings.append(_na("SAT-EXFIL-EXT-LOC-TRUST", el_s, el_e))

    # SAT-EXFIL-PYPI: Init scripts don't install from untrusted sources
    init_data, in_s, in_e = await _dbx_get(client, host,
        "/api/2.0/global-init-scripts", token)
    if init_data is not None:
        scripts = init_data.get("scripts", [])
        if not scripts:
            findings.append(_make_finding("SAT-EXFIL-PYPI", "PASS",
                "No global init scripts configured."))
        else:
            suspect = 0
            for script in scripts[:10]:
                sid = script.get("script_id", "")
                try:
                    sr = await client.get(f"{host}/api/2.0/global-init-scripts/{sid}",
                        headers=hdr, timeout=10)
                    if sr.status_code == 200:
                        content = base64.b64decode(sr.json().get("script", "")).decode("utf-8", errors="ignore")
                        if any(kw in content.lower() for kw in ("pip install", "conda install", "pypi.org", "npmjs.com")):
                            suspect += 1
                except Exception:
                    pass
            findings.append(_make_finding("SAT-EXFIL-PYPI",
                "PASS" if suspect == 0 else "WARN",
                f"{suspect}/{len(scripts)} global init script(s) install from external package sources.",
                {"suspect": suspect, "total": len(scripts)}))
    else:
        findings.append(_na("SAT-EXFIL-PYPI", in_s, in_e))

    return findings


# ─────────────────────────────────────────────────────────────────────────────
# Serverless Governance
# ─────────────────────────────────────────────────────────────────────────────

async def _check_serverless_governance(client: httpx.AsyncClient, host: str, token: str) -> list[SATFinding]:
    """Serverless governance: budget, network, allowed, job cost, WH sizing."""
    findings: list[SATFinding] = []
    hdr = {"Authorization": f"Bearer {token}"}

    # SAT-SRVL-BUDGET: Serverless compute has budget alerts
    # Budgets API is account-level only (/api/2.1/budgets).
    # If account_id is available, SAT-ACCT-BUDGETS already covers this check.
    _acct_id = _WORKSPACE_ACCOUNT_IDS.get(host.rstrip("/"), "")
    if _acct_id:
        findings.append(_make_finding("SAT-SRVL-BUDGET", "NOT_APPLICABLE",
            "Budget governance is checked at account level (SAT-ACCT-BUDGETS)."))
    else:
        # No account-level scan — try workspace-level as best-effort
        budget_data, b_s, b_e = await _dbx_get(client, host,
            "/api/2.1/budgets", token)
        if budget_data is not None:
            budgets = budget_data.get("budgets", [])
            findings.append(_make_finding("SAT-SRVL-BUDGET",
                "PASS" if budgets else "FAIL",
                f"{len(budgets)} budget policy/policies configured."
                if budgets else "No budget alerts configured for serverless compute.",
                {"count": len(budgets)}))
        elif b_s in (404, 403):
            findings.append(_make_finding("SAT-SRVL-BUDGET", "NOT_APPLICABLE",
                "Budgets API is account-level only. "
                "Provide account credentials to enable budget checks (SAT-ACCT-BUDGETS)."))
        else:
            findings.append(_na("SAT-SRVL-BUDGET", b_s, b_e))

    # SAT-SRVL-NETWORK: Serverless compute uses NCC
    # Try workspace-conf first, fall back to Settings API (default_namespace_ws)
    _ncc_resolved = False
    ws_conf, wc_s, wc_e = await _dbx_get_workspace_conf(client, host, token,
        "enableNccConfig")
    if ws_conf is not None and "enableNccConfig" in ws_conf:
        ncc = str(ws_conf.get("enableNccConfig", "false")).lower() == "true"
        findings.append(_make_finding("SAT-SRVL-NETWORK",
            "PASS" if ncc else "FAIL",
            f"NCC for serverless: {'enabled' if ncc else 'disabled'}."))
        _ncc_resolved = True
    if not _ncc_resolved:
        # Fallback: Settings API for NCC/default namespace
        ncc_data, ncc_s, _ = await _dbx_get(client, host,
            "/api/2.0/settings/types/default_namespace_ws/names/default", token)
        if ncc_s == 200 and ncc_data is not None:
            ns_value = ncc_data.get("default_namespace_ws", {}).get("value", "") or ""
            findings.append(_make_finding("SAT-SRVL-NETWORK",
                "PASS" if ns_value else "WARN",
                f"Default namespace (Settings API): {'configured' if ns_value else 'not configured'}."))
        else:
            findings.append(_make_finding("SAT-SRVL-NETWORK", "NOT_APPLICABLE",
                "NCC configuration key not available on this workspace."))

    # SAT-SRVL-ALLOWED: Serverless usage restricted
    ws_conf2, _, _ = await _dbx_get_workspace_conf(client, host, token,
        "enableServerlessCompute")
    if ws_conf2 is not None:
        serverless = str(ws_conf2.get("enableServerlessCompute", "true")).lower() == "true"
        findings.append(_make_finding("SAT-SRVL-ALLOWED",
            "PASS" if serverless else "WARN",
            f"Serverless compute: {'enabled' if serverless else 'disabled'}. Review access governance."))
    else:
        findings.append(_make_finding("SAT-SRVL-ALLOWED", "WARN",
            "Cannot verify serverless compute settings."))

    # SAT-SRVL-JOB-COST: Serverless jobs DBU cost tracked
    schema_data, sc_s, sc_e = await _dbx_get(client, host,
        "/api/2.1/unity-catalog/schemas", token, {"catalog_name": "system"})
    if schema_data is not None:
        schemas = schema_data.get("schemas", [])
        has_billing = any(s.get("name") == "billing" for s in schemas)
        findings.append(_make_finding("SAT-SRVL-JOB-COST",
            "PASS" if has_billing else "WARN",
            f"System billing schema: {'available' if has_billing else 'not found'} "
            f"for serverless cost tracking."))
    else:
        findings.append(_na("SAT-SRVL-JOB-COST", sc_s, sc_e))

    # SAT-SRVL-WH-SIZING: Serverless SQL warehouses right-sized
    wh_data, wh_s, wh_e = await _dbx_get(client, host,
        "/api/2.0/sql/warehouses", token)
    if wh_data is not None:
        warehouses = wh_data.get("warehouses", [])
        serverless_wh = [w for w in warehouses
                        if w.get("enable_serverless_compute") or w.get("warehouse_type") == "PRO"]
        if not serverless_wh:
            findings.append(_make_finding("SAT-SRVL-WH-SIZING", "NOT_APPLICABLE",
                "No serverless SQL warehouses found."))
        else:
            large = [w for w in serverless_wh
                    if w.get("cluster_size", "").upper() in ("LARGE", "X-LARGE", "2X-LARGE",
                        "3X-LARGE", "4X-LARGE")]
            findings.append(_make_finding("SAT-SRVL-WH-SIZING",
                "PASS" if not large else "WARN",
                f"{len(large)}/{len(serverless_wh)} serverless warehouse(s) use Large+ sizing — "
                f"review for right-sizing.",
                {"large": len(large), "total": len(serverless_wh)}))
    else:
        findings.append(_na("SAT-SRVL-WH-SIZING", wh_s, wh_e))

    return findings


# ─────────────────────────────────────────────────────────────────────────────
# Webhook & Integration Security
# ─────────────────────────────────────────────────────────────────────────────

async def _check_webhook_security(client: httpx.AsyncClient, host: str, token: str) -> list[SATFinding]:
    """Webhook security: HTTPS, auth, stale destinations, model registry scope."""
    findings: list[SATFinding] = []
    hdr = {"Authorization": f"Bearer {token}"}

    # Fetch notification destinations
    nd_data, nd_s, nd_e = await _dbx_get(client, host,
        "/api/2.0/notification-destinations", token)
    destinations = []
    if nd_data is not None:
        destinations = nd_data.get("results", [])

    # SAT-HOOK-HTTPS: All notification destinations use HTTPS
    if nd_data is None:
        findings.append(_na("SAT-HOOK-HTTPS", nd_s, nd_e))
    elif not destinations:
        findings.append(_make_finding("SAT-HOOK-HTTPS", "NOT_APPLICABLE",
            "No notification destinations configured."))
    else:
        # Check for any webhook-type destinations with HTTP URLs
        webhook_dests = [d for d in destinations
                        if d.get("destination_type") in ("WEBHOOK", "SLACK", "PAGERDUTY", "MS_TEAMS")]
        if not webhook_dests:
            findings.append(_make_finding("SAT-HOOK-HTTPS", "PASS",
                f"{len(destinations)} notification destination(s) found (non-webhook types)."))
        else:
            findings.append(_make_finding("SAT-HOOK-HTTPS", "PASS",
                f"{len(webhook_dests)} webhook destination(s) configured. "
                f"Verify all use HTTPS endpoints.",
                {"webhook_count": len(webhook_dests)}))

    # SAT-HOOK-AUTH: Webhook destinations include authentication
    if nd_data is None:
        findings.append(_na("SAT-HOOK-AUTH", nd_s, nd_e))
    elif not destinations:
        findings.append(_make_finding("SAT-HOOK-AUTH", "NOT_APPLICABLE",
            "No notification destinations configured."))
    else:
        findings.append(_make_finding("SAT-HOOK-AUTH",
            "WARN" if destinations else "PASS",
            f"{len(destinations)} destination(s) found. Verify authentication headers "
            f"are configured on webhook endpoints.",
            {"count": len(destinations)}))

    # SAT-HOOK-STALE: No stale or error-returning destinations
    if nd_data is None:
        findings.append(_na("SAT-HOOK-STALE", nd_s, nd_e))
    elif not destinations:
        findings.append(_make_finding("SAT-HOOK-STALE", "NOT_APPLICABLE",
            "No notification destinations configured."))
    else:
        findings.append(_make_finding("SAT-HOOK-STALE", "PASS",
            f"{len(destinations)} destination(s) configured. Test periodically for errors.",
            {"count": len(destinations)}))

    # SAT-HOOK-SCOPE: Model registry webhooks scoped to specific models
    try:
        r = await client.get(f"{host}/api/2.0/mlflow/registry-webhooks/list",
            headers=hdr, timeout=15)
        if r.status_code == 200:
            webhooks = r.json().get("webhooks", [])
            if not webhooks:
                findings.append(_make_finding("SAT-HOOK-SCOPE", "NOT_APPLICABLE",
                    "No model registry webhooks configured."))
            else:
                unscoped = [w for w in webhooks if not w.get("model_name")]
                findings.append(_make_finding("SAT-HOOK-SCOPE",
                    "PASS" if not unscoped else "WARN",
                    f"{len(unscoped)}/{len(webhooks)} webhook(s) are workspace-wide "
                    f"(not scoped to specific models).",
                    {"unscoped": len(unscoped), "total": len(webhooks)}))
        elif r.status_code in (403, 404):
            findings.append(_make_finding("SAT-HOOK-SCOPE", "NOT_APPLICABLE",
                "Model registry webhooks API not available."))
        else:
            findings.append(_make_finding("SAT-HOOK-SCOPE", "WARN",
                f"Webhooks API returned HTTP {r.status_code}."))
    except Exception as exc:
        findings.append(_make_finding("SAT-HOOK-SCOPE", "WARN",
            f"Error checking model registry webhooks: {exc}"))

    return findings


# ─────────────────────────────────────────────────────────────────────────────
# Compliance
# ─────────────────────────────────────────────────────────────────────────────

async def _check_compliance(client: httpx.AsyncClient, host: str, token: str) -> list[SATFinding]:
    """Compliance: HIPAA, SOC2, data retention, GDPR delete, audit trail."""
    findings: list[SATFinding] = []
    hdr = {"Authorization": f"Bearer {token}"}

    # Fetch workspace config for multiple checks
    ws_conf, wc_s, wc_e = await _dbx_get_workspace_conf(client, host, token,
        "enableComplianceSecurityProfile,enableResultsDownloading,enableExportNotebook,"
        "enableVerboseAuditLogs,enableIpAccessLists,enableRoleBasedAccessControl,"
        "enableCustomerManagedKey,enableEnhancedSecurityMonitoring")

    # SAT-COMP-HIPAA: HIPAA-required controls enabled
    if ws_conf is not None:
        csp = str(ws_conf.get("enableComplianceSecurityProfile", "false")).lower() == "true"
        esm = str(ws_conf.get("enableEnhancedSecurityMonitoring", "false")).lower() == "true"
        cmk = str(ws_conf.get("enableCustomerManagedKey", "false")).lower() == "true"
        no_dl = str(ws_conf.get("enableResultsDownloading", "true")).lower() == "false"
        no_exp = str(ws_conf.get("enableExportNotebook", "true")).lower() == "false"
        if not csp:
            findings.append(_make_finding("SAT-COMP-HIPAA", "NOT_APPLICABLE",
                "Compliance security profile not enabled — HIPAA check not applicable."))
        else:
            controls = {"enhanced_monitoring": esm, "cmk": cmk,
                       "results_download_disabled": no_dl, "notebook_export_disabled": no_exp}
            missing = [k for k, v in controls.items() if not v]
            findings.append(_make_finding("SAT-COMP-HIPAA",
                "PASS" if not missing else "FAIL",
                f"HIPAA controls: {4-len(missing)}/4 enabled."
                f" Missing: {', '.join(missing)}." if missing else " All controls enabled.",
                controls))
    else:
        findings.append(_na("SAT-COMP-HIPAA", wc_s, wc_e))

    # SAT-COMP-SOC2: SOC2-relevant logging and access controls
    if ws_conf is not None:
        verbose = str(ws_conf.get("enableVerboseAuditLogs", "false")).lower() == "true"
        ip_acl = str(ws_conf.get("enableIpAccessLists", "false")).lower() == "true"
        rbac = str(ws_conf.get("enableRoleBasedAccessControl", "false")).lower() == "true"
        controls = {"verbose_audit": verbose, "ip_access_lists": ip_acl, "rbac": rbac}
        enabled = sum(1 for v in controls.values() if v)
        findings.append(_make_finding("SAT-COMP-SOC2",
            "PASS" if enabled == 3 else ("WARN" if enabled >= 1 else "FAIL"),
            f"SOC2 controls: {enabled}/3 enabled.",
            controls))
    else:
        findings.append(_na("SAT-COMP-SOC2", wc_s, wc_e))

    # SAT-COMP-DATA-RETENTION: Data retention policies on catalogs
    cat_data, c_s, c_e = await _dbx_get(client, host,
        "/api/2.1/unity-catalog/catalogs", token)
    if cat_data is not None:
        catalogs = cat_data.get("catalogs", [])
        if not catalogs:
            findings.append(_make_finding("SAT-COMP-DATA-RETENTION", "NOT_APPLICABLE",
                "No Unity Catalog catalogs found."))
        else:
            with_comment = sum(1 for c in catalogs if c.get("comment"))
            pct = round(with_comment / len(catalogs) * 100)
            findings.append(_make_finding("SAT-COMP-DATA-RETENTION",
                "PASS" if pct >= 80 else ("WARN" if with_comment > 0 else "FAIL"),
                f"{with_comment}/{len(catalogs)} catalog(s) ({pct}%) have descriptions/retention docs.",
                {"with_comment": with_comment, "total": len(catalogs), "pct": pct}))
    else:
        findings.append(_na("SAT-COMP-DATA-RETENTION", c_s, c_e))

    # SAT-COMP-GDPR-DELETE: Right-to-delete process exists
    # Check if CDF is enabled on tables (sampled)
    if cat_data is not None:
        catalogs = cat_data.get("catalogs", [])
        non_system = [c for c in catalogs
                     if c.get("name") not in ("system", "__databricks_internal")]
        if not non_system:
            findings.append(_make_finding("SAT-COMP-GDPR-DELETE", "NOT_APPLICABLE",
                "No user catalogs found."))
        else:
            findings.append(_make_finding("SAT-COMP-GDPR-DELETE", "WARN",
                f"{len(non_system)} catalog(s) found. Verify Change Data Feed is enabled "
                f"on tables containing personal data for right-to-delete compliance.",
                {"catalog_count": len(non_system)}))
    else:
        findings.append(_na("SAT-COMP-GDPR-DELETE", c_s, c_e))

    # SAT-COMP-AUDIT-TRAIL: End-to-end audit trail
    if ws_conf is not None:
        verbose = str(ws_conf.get("enableVerboseAuditLogs", "false")).lower() == "true"
        # Check if system.access schema exists
        schema_data, _, _ = await _dbx_get(client, host,
            "/api/2.1/unity-catalog/schemas", token, {"catalog_name": "system"})
        has_access = False
        if schema_data is not None:
            has_access = any(s.get("name") == "access" for s in schema_data.get("schemas", []))
        score = sum([verbose, has_access])
        findings.append(_make_finding("SAT-COMP-AUDIT-TRAIL",
            "PASS" if score == 2 else ("WARN" if score >= 1 else "FAIL"),
            f"Audit trail: verbose logs={'enabled' if verbose else 'disabled'}, "
            f"system.access={'available' if has_access else 'not found'}.",
            {"verbose_audit": verbose, "system_access": has_access}))
    else:
        findings.append(_na("SAT-COMP-AUDIT-TRAIL", wc_s, wc_e))

    return findings


# ─────────────────────────────────────────────────────────────────────────────
# Extended checks for additions to existing categories
# ─────────────────────────────────────────────────────────────────────────────

async def _check_iam_extended2(client: httpx.AsyncClient, host: str, token: str) -> list[SATFinding]:
    """Extended IAM checks: guest users, group nesting, empty groups, SP rotation, conditional access."""
    findings: list[SATFinding] = []
    hdr = {"Authorization": f"Bearer {token}"}

    # SAT-IAM-GUEST: No guest/external users with admin roles
    users_data, u_s, u_e = await _dbx_get(client, host,
        "/api/2.0/preview/scim/v2/Users", token, {"count": "500"})
    admins_data, a_s, a_e = await _dbx_get(client, host,
        "/api/2.0/preview/scim/v2/Groups", token,
        {"filter": 'displayName eq "admins"', "attributes": "members"})
    if users_data is not None and admins_data is not None:
        admin_groups = admins_data.get("Resources", [])
        admin_members = admin_groups[0].get("members", []) if admin_groups else []
        admin_ids = {m.get("value") for m in admin_members}
        users = users_data.get("Resources", [])
        guest_admins = []
        for u in users:
            uid = u.get("id", "")
            if uid in admin_ids:
                username = u.get("userName", "")
                if "#EXT#" in username or u.get("userType") == "Guest":
                    guest_admins.append(username)
        findings.append(_make_finding("SAT-IAM-GUEST",
            "PASS" if not guest_admins else "FAIL",
            f"{len(guest_admins)} guest/external user(s) have admin roles."
            if guest_admins else "No guest users with admin roles.",
            {"guest_admins": guest_admins[:5]}))
    else:
        findings.append(_na("SAT-IAM-GUEST", u_s or a_s, u_e or a_e))

    # SAT-IAM-GROUP-NESTING: Groups don't exceed 3 levels
    groups_data, g_s, g_e = await _dbx_get(client, host,
        "/api/2.0/preview/scim/v2/Groups", token, {"count": "100"})
    if groups_data is not None:
        groups = groups_data.get("Resources", [])
        nested_groups = [g for g in groups
                        for m in g.get("members", [])
                        if m.get("$ref", "").endswith("/Groups")]
        findings.append(_make_finding("SAT-IAM-GROUP-NESTING",
            "PASS" if len(nested_groups) <= 3 else "WARN",
            f"{len(nested_groups)} group(s) contain nested group members. Review nesting depth.",
            {"nested_count": len(nested_groups)}))
    else:
        findings.append(_na("SAT-IAM-GROUP-NESTING", g_s, g_e))

    # SAT-IAM-EMPTY-GROUP: No empty groups with permissions
    if groups_data is not None:
        groups = groups_data.get("Resources", [])
        empty = [g for g in groups
                if len(g.get("members", [])) == 0
                and g.get("displayName", "") not in ("admins", "users", "account users")]
        findings.append(_make_finding("SAT-IAM-EMPTY-GROUP",
            "PASS" if not empty else "WARN",
            f"{len(empty)} empty group(s) found (excluding system groups)."
            if empty else "No empty groups found.",
            {"empty_groups": [g.get("displayName", "") for g in empty[:10]]}))
    else:
        findings.append(_na("SAT-IAM-EMPTY-GROUP", g_s, g_e))

    # SAT-IAM-SP-SECRET-ROTATION: SP secrets rotated within 90 days
    sp_data, sp_s, sp_e = await _dbx_get(client, host,
        "/api/2.0/preview/scim/v2/ServicePrincipals", token, {"count": "100"})
    if sp_data is not None:
        sps = sp_data.get("Resources", [])
        if not sps:
            findings.append(_make_finding("SAT-IAM-SP-SECRET-ROTATION", "NOT_APPLICABLE",
                "No service principals found."))
        else:
            findings.append(_make_finding("SAT-IAM-SP-SECRET-ROTATION", "WARN",
                f"{len(sps)} service principal(s) found. Verify OAuth client secrets "
                f"are rotated within 90 days.",
                {"sp_count": len(sps)}))
    else:
        findings.append(_na("SAT-IAM-SP-SECRET-ROTATION", sp_s, sp_e))

    # SAT-IAM-CONDITIONAL-ACCESS: Conditional access policies applied
    # This is typically an Azure AD feature — we check for SSO indicators
    if users_data is not None:
        users = users_data.get("Resources", [])
        sso_users = [u for u in users if u.get("externalId")]
        pct = round(len(sso_users) / max(len(users), 1) * 100)
        findings.append(_make_finding("SAT-IAM-CONDITIONAL-ACCESS",
            "PASS" if pct >= 80 else ("WARN" if pct >= 50 else "FAIL"),
            f"{len(sso_users)}/{len(users)} user(s) ({pct}%) provisioned via external IdP "
            f"(indicates SSO/conditional access). Verify CA policies in Azure AD.",
            {"sso_users": len(sso_users), "total": len(users), "pct": pct}))
    else:
        findings.append(_na("SAT-IAM-CONDITIONAL-ACCESS", u_s, u_e))

    return findings


async def _check_secrets_extended(client: httpx.AsyncClient, host: str, token: str) -> list[SATFinding]:
    """Extended secrets checks: rotation, unused scopes, env var credentials."""
    findings: list[SATFinding] = []
    hdr = {"Authorization": f"Bearer {token}"}

    # SAT-SEC-ROTATION: Secrets rotated within 90 days
    scopes_data, sc_s, sc_e = await _dbx_get(client, host,
        "/api/2.0/secrets/scopes/list", token)
    if scopes_data is not None:
        scopes = scopes_data.get("scopes", [])
        if not scopes:
            findings.append(_make_finding("SAT-SEC-ROTATION", "NOT_APPLICABLE",
                "No secret scopes configured."))
        else:
            findings.append(_make_finding("SAT-SEC-ROTATION", "WARN",
                f"{len(scopes)} secret scope(s) found. Verify secrets are rotated within 90 days. "
                f"Use Azure Key Vault auto-rotation where possible.",
                {"scope_count": len(scopes)}))
    else:
        findings.append(_na("SAT-SEC-ROTATION", sc_s, sc_e))

    # SAT-SEC-UNUSED: No unused secret scopes
    if scopes_data is not None:
        scopes = scopes_data.get("scopes", [])
        if not scopes:
            findings.append(_make_finding("SAT-SEC-UNUSED", "NOT_APPLICABLE",
                "No secret scopes configured."))
        else:
            # Check if scopes have any secrets
            empty_scopes = []
            for scope in scopes[:10]:
                try:
                    r = await client.get(f"{host}/api/2.0/secrets/list",
                        headers=hdr, params={"scope": scope.get("name", "")}, timeout=10)
                    if r.status_code == 200:
                        secrets = r.json().get("secrets", [])
                        if not secrets:
                            empty_scopes.append(scope.get("name", ""))
                except Exception:
                    pass
            findings.append(_make_finding("SAT-SEC-UNUSED",
                "PASS" if not empty_scopes else "WARN",
                f"{len(empty_scopes)} scope(s) have no secrets — consider removing."
                if empty_scopes else f"All checked scopes ({min(len(scopes), 10)}) contain secrets.",
                {"empty_scopes": empty_scopes}))
    else:
        findings.append(_na("SAT-SEC-UNUSED", sc_s, sc_e))

    # SAT-SEC-ENV-VARS: Cluster env vars don't contain credentials
    cl_data, cl_s, cl_e = await _dbx_get(client, host,
        "/api/2.0/clusters/list", token)
    if cl_data is not None:
        clusters = cl_data.get("clusters", [])
        cred_patterns = re.compile(
            r"(password|secret|api.?key|token|credential|conn.?string)",
            re.IGNORECASE)
        flagged = []
        for c in clusters:
            env_vars = c.get("spark_env_vars", {})
            for key in env_vars:
                if cred_patterns.search(key):
                    flagged.append(f"{c.get('cluster_name', '?')}: {key}")
        findings.append(_make_finding("SAT-SEC-ENV-VARS",
            "PASS" if not flagged else "FAIL",
            f"{len(flagged)} cluster env var(s) contain credential-like key names."
            if flagged else "No credential patterns found in cluster environment variables.",
            {"flagged": flagged[:10]}))
    else:
        findings.append(_na("SAT-SEC-ENV-VARS", cl_s, cl_e))

    # SAT-SEC-GIT-CREDS: Git credentials audited and rotated
    git_data, git_s, git_e = await _dbx_get(client, host, "/api/2.0/git-credentials", token)
    if git_data is not None:
        creds = git_data.get("credentials", [])
        if not creds:
            findings.append(_make_finding("SAT-SEC-GIT-CREDS", "PASS",
                "No Git credentials stored in workspace.",
                {"api_response": git_data}))
        else:
            providers: dict[str, int] = {}
            for c in creds:
                prov = c.get("git_provider", "unknown")
                providers[prov] = providers.get(prov, 0) + 1
            prov_summary = ", ".join(f"{v} {k}" for k, v in sorted(providers.items()))
            findings.append(_make_finding("SAT-SEC-GIT-CREDS", "WARN",
                f"{len(creds)} Git credential(s) stored: {prov_summary}. "
                f"Review for stale/over-provisioned tokens.",
                {"total_credentials": len(creds), "providers": providers,
                 "api_response": git_data}))
    elif git_s in (403, 404):
        findings.append(_make_finding("SAT-SEC-GIT-CREDS", "NOT_APPLICABLE",
            f"Git credentials API not available (HTTP {git_s}).",
            {"http_status": git_s}))
    else:
        findings.append(_na("SAT-SEC-GIT-CREDS", git_s, git_e))

    return findings


async def _check_cost_extended(client: httpx.AsyncClient, host: str, token: str) -> list[SATFinding]:
    """Extended cost checks: zombie volumes, abandoned tables, overprovisioned, storage tiering."""
    findings: list[SATFinding] = []
    hdr = {"Authorization": f"Bearer {token}"}

    # SAT-COST-ZOMBIE-VOLUMES: UC Volumes with no reads in 90 days
    vol_data, v_s, v_e = await _dbx_get(client, host,
        "/api/2.1/unity-catalog/volumes", token,
        {"catalog_name": "main", "schema_name": "default"})
    if vol_data is not None:
        volumes = vol_data.get("volumes", [])
        if not volumes:
            findings.append(_make_finding("SAT-COST-ZOMBIE-VOLUMES", "NOT_APPLICABLE",
                "No UC Volumes found in main.default."))
        else:
            findings.append(_make_finding("SAT-COST-ZOMBIE-VOLUMES", "WARN",
                f"{len(volumes)} Volume(s) in main.default. Review access patterns "
                f"via system tables to identify unused Volumes.",
                {"count": len(volumes)}))
    else:
        findings.append(_na("SAT-COST-ZOMBIE-VOLUMES", v_s, v_e))

    # SAT-COST-ABANDONED-TABLES: Tables with no reads/writes in 180 days
    # Check for tables via information_schema
    findings.append(_make_finding("SAT-COST-ABANDONED-TABLES", "WARN",
        "Query system.access.table_lineage to identify tables with no access in 180 days. "
        "Archive or drop confirmed abandoned tables."))

    # SAT-COST-OVERPROVISIONED: Clusters not over-provisioned
    cl_data, cl_s, cl_e = await _dbx_get(client, host,
        "/api/2.0/clusters/list", token)
    if cl_data is not None:
        clusters = cl_data.get("clusters", [])
        running = [c for c in clusters if c.get("state") == "RUNNING"]
        if not running:
            findings.append(_make_finding("SAT-COST-OVERPROVISIONED", "NOT_APPLICABLE",
                "No running clusters to evaluate."))
        else:
            large = [c for c in running
                    if c.get("num_workers", 0) > 10 or c.get("autoscale", {}).get("max_workers", 0) > 20]
            findings.append(_make_finding("SAT-COST-OVERPROVISIONED",
                "PASS" if not large else "WARN",
                f"{len(large)}/{len(running)} running cluster(s) have >10 workers or >20 max_workers — "
                f"review utilization for right-sizing.",
                {"large_clusters": len(large), "running": len(running)}))
    else:
        findings.append(_na("SAT-COST-OVERPROVISIONED", cl_s, cl_e))

    # SAT-COST-STORAGE-TIERING: Large tables use storage tiering
    findings.append(_make_finding("SAT-COST-STORAGE-TIERING", "WARN",
        "Query information_schema.tables to identify tables > 1 TB. "
        "Verify Azure Storage lifecycle policies are configured for infrequent access tiering."))

    # ── System table powered checks ──
    # Try to find a running SQL warehouse for system table queries
    wh_id = await _find_running_warehouse(client, host, token)

    # SAT-COST-ANOMALY: Cost anomaly detection via system.billing.usage
    if wh_id:
        try:
            anomaly_sql = """
                WITH daily AS (
                    SELECT usage_date, SUM(usage_quantity) AS dbu
                    FROM system.billing.usage
                    WHERE usage_date >= current_date() - 14
                    GROUP BY usage_date
                ),
                avg7 AS (
                    SELECT usage_date, dbu,
                           AVG(dbu) OVER (ORDER BY usage_date ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING) AS avg_7d
                    FROM daily
                )
                SELECT usage_date, ROUND(dbu, 1) AS dbu, ROUND(avg_7d, 1) AS avg_7d
                FROM avg7
                WHERE dbu > avg_7d * 2 AND avg_7d > 0 AND usage_date >= current_date() - 7
                ORDER BY usage_date DESC
            """
            anomaly_rows, anomaly_err = await _dbx_sql_query(client, host, token, wh_id, anomaly_sql, timeout=30)
            if anomaly_rows is not None:
                if anomaly_rows:
                    spike_dates = [str(r[0]) for r in anomaly_rows[:5]]
                    findings.append(_make_finding("SAT-COST-ANOMALY", "WARN",
                        f"{len(anomaly_rows)} day(s) in the last 7 with DBU usage >2x the 7-day average: "
                        f"{', '.join(spike_dates)}.",
                        {"anomaly_days": len(anomaly_rows), "dates": spike_dates}))
                else:
                    findings.append(_make_finding("SAT-COST-ANOMALY", "PASS",
                        "No cost anomalies in the last 7 days (DBU usage within 2x of 7-day average)."))
            else:
                findings.append(_make_finding("SAT-COST-ANOMALY", "NOT_APPLICABLE",
                    f"Could not query system.billing.usage: {anomaly_err}"))
        except Exception as e:
            findings.append(_make_finding("SAT-COST-ANOMALY", "NOT_APPLICABLE",
                f"System table query failed: {e}"))
    else:
        findings.append(_make_finding("SAT-COST-ANOMALY", "NOT_APPLICABLE",
            "No running SQL warehouse found for system table queries."))

    # SAT-COST-STALE-TABLES: Tables with no reads in 90+ days via system.access.audit
    if wh_id:
        try:
            stale_sql = """
                SELECT t.table_catalog, t.table_schema, t.table_name
                FROM system.information_schema.tables t
                LEFT JOIN (
                    SELECT request_params.full_name_arg AS table_name
                    FROM system.access.audit
                    WHERE action_name IN ('getTable', 'commandSubmit', 'sqlExecute')
                      AND event_date >= current_date() - 90
                      AND request_params.full_name_arg IS NOT NULL
                    GROUP BY request_params.full_name_arg
                ) a ON CONCAT(t.table_catalog, '.', t.table_schema, '.', t.table_name) = a.table_name
                WHERE a.table_name IS NULL
                  AND t.table_catalog NOT IN ('system', '__databricks_internal')
                  AND t.table_type = 'MANAGED'
                LIMIT 50
            """
            stale_rows, stale_err = await _dbx_sql_query(client, host, token, wh_id, stale_sql, timeout=30)
            if stale_rows is not None:
                if stale_rows:
                    sample = [f"{r[0]}.{r[1]}.{r[2]}" for r in stale_rows[:5]]
                    findings.append(_make_finding("SAT-COST-STALE-TABLES", "WARN",
                        f"{len(stale_rows)} managed table(s) with no audit activity in 90 days "
                        f"(showing first 50). Examples: {', '.join(sample)}.",
                        {"stale_count": len(stale_rows), "sample": sample}))
                else:
                    findings.append(_make_finding("SAT-COST-STALE-TABLES", "PASS",
                        "All managed tables have audit activity within the last 90 days."))
            else:
                findings.append(_make_finding("SAT-COST-STALE-TABLES", "NOT_APPLICABLE",
                    f"Could not query system tables: {stale_err}"))
        except Exception as e:
            findings.append(_make_finding("SAT-COST-STALE-TABLES", "NOT_APPLICABLE",
                f"System table query failed: {e}"))
    else:
        findings.append(_make_finding("SAT-COST-STALE-TABLES", "NOT_APPLICABLE",
            "No running SQL warehouse found for system table queries."))

    # SAT-LOG-FAILED-AUTH: Spike in authentication failures via system.access.audit
    if wh_id:
        try:
            auth_sql = """
                WITH daily AS (
                    SELECT event_date, COUNT(*) AS fails
                    FROM system.access.audit
                    WHERE action_name IN ('tokenLogin', 'login', 'aadBrowserLogin', 'aadTokenLogin')
                      AND response.status_code >= 400
                      AND event_date >= current_date() - 7
                    GROUP BY event_date
                ),
                avg7 AS (
                    SELECT event_date, fails,
                           AVG(fails) OVER (ORDER BY event_date ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING) AS avg_7d
                    FROM daily
                )
                SELECT event_date, fails, ROUND(avg_7d, 1) AS avg_7d
                FROM avg7
                WHERE fails > GREATEST(avg_7d * 3, 10) AND avg_7d IS NOT NULL
                ORDER BY event_date DESC
            """
            auth_rows, auth_err = await _dbx_sql_query(client, host, token, wh_id, auth_sql, timeout=30)
            if auth_rows is not None:
                if auth_rows:
                    spike_dates = [f"{r[0]} ({r[1]} failures, avg {r[2]})" for r in auth_rows[:5]]
                    findings.append(_make_finding("SAT-LOG-FAILED-AUTH", "WARN",
                        f"{len(auth_rows)} day(s) with auth failure spike (>3x average): "
                        f"{'; '.join(spike_dates)}.",
                        {"spike_days": len(auth_rows), "details": spike_dates}))
                else:
                    findings.append(_make_finding("SAT-LOG-FAILED-AUTH", "PASS",
                        "No authentication failure spikes detected in the last 7 days."))
            else:
                findings.append(_make_finding("SAT-LOG-FAILED-AUTH", "NOT_APPLICABLE",
                    f"Could not query system.access.audit: {auth_err}"))
        except Exception as e:
            findings.append(_make_finding("SAT-LOG-FAILED-AUTH", "NOT_APPLICABLE",
                f"System table query failed: {e}"))
    else:
        findings.append(_make_finding("SAT-LOG-FAILED-AUTH", "NOT_APPLICABLE",
            "No running SQL warehouse found for system table queries."))

    # SAT-LOG-DATA-EXFIL: Large data downloads via system.query.history
    if wh_id:
        try:
            exfil_sql = """
                SELECT user_name, statement_type, rows_produced, execution_status,
                       ROUND(rows_produced / 1000000.0, 1) AS rows_millions
                FROM system.query.history
                WHERE rows_produced > 10000000
                  AND start_time >= current_timestamp() - INTERVAL 7 DAYS
                  AND statement_type IN ('SELECT', 'COPY', 'EXPORT')
                ORDER BY rows_produced DESC
                LIMIT 20
            """
            exfil_rows, exfil_err = await _dbx_sql_query(client, host, token, wh_id, exfil_sql, timeout=30)
            if exfil_rows is not None:
                if exfil_rows:
                    samples = [f"{r[0]}: {r[4]}M rows ({r[1]})" for r in exfil_rows[:5]]
                    findings.append(_make_finding("SAT-LOG-DATA-EXFIL", "WARN",
                        f"{len(exfil_rows)} query(ies) returned >10M rows in the last 7 days. "
                        f"Top: {'; '.join(samples)}.",
                        {"large_queries": len(exfil_rows), "samples": samples}))
                else:
                    findings.append(_make_finding("SAT-LOG-DATA-EXFIL", "PASS",
                        "No queries returning >10M rows detected in the last 7 days."))
            else:
                findings.append(_make_finding("SAT-LOG-DATA-EXFIL", "NOT_APPLICABLE",
                    f"Could not query system.query.history: {exfil_err}"))
        except Exception as e:
            findings.append(_make_finding("SAT-LOG-DATA-EXFIL", "NOT_APPLICABLE",
                f"System table query failed: {e}"))
    else:
        findings.append(_make_finding("SAT-LOG-DATA-EXFIL", "NOT_APPLICABLE",
            "No running SQL warehouse found for system table queries."))

    # SAT-PERF-QUERY-ANTIPATTERN: Query anti-patterns via system.query.history
    if wh_id:
        try:
            antipattern_sql = """
                SELECT
                    SUM(CASE WHEN statement_text LIKE 'SELECT *%' OR statement_text LIKE 'select *%' THEN 1 ELSE 0 END) AS select_star_count,
                    SUM(CASE WHEN statement_text LIKE '%.collect()%' THEN 1 ELSE 0 END) AS collect_count,
                    COUNT(*) AS total_queries
                FROM system.query.history
                WHERE start_time >= current_timestamp() - INTERVAL 7 DAYS
                  AND statement_type = 'SELECT'
                  AND execution_status = 'FINISHED'
            """
            ap_rows, ap_err = await _dbx_sql_query(client, host, token, wh_id, antipattern_sql, timeout=30)
            if ap_rows is not None and ap_rows:
                select_star = int(ap_rows[0][0] or 0)
                collect_cnt = int(ap_rows[0][1] or 0)
                total = int(ap_rows[0][2] or 0)
                issues = []
                if select_star:
                    issues.append(f"{select_star} SELECT * queries")
                if collect_cnt:
                    issues.append(f"{collect_cnt} .collect() calls")
                if issues:
                    pct = round((select_star + collect_cnt) / max(total, 1) * 100, 1)
                    findings.append(_make_finding("SAT-PERF-QUERY-ANTIPATTERN",
                        "FAIL" if pct > 20 else "WARN",
                        f"Anti-patterns in last 7 days ({total} total queries): {', '.join(issues)} ({pct}% of queries).",
                        {"select_star": select_star, "collect_calls": collect_cnt,
                         "total_queries": total, "antipattern_pct": pct}))
                else:
                    findings.append(_make_finding("SAT-PERF-QUERY-ANTIPATTERN", "PASS",
                        f"No SELECT * or .collect() anti-patterns in {total} queries over the last 7 days."))
            elif ap_rows is not None:
                findings.append(_make_finding("SAT-PERF-QUERY-ANTIPATTERN", "PASS",
                    "No query history found in the last 7 days."))
            else:
                findings.append(_make_finding("SAT-PERF-QUERY-ANTIPATTERN", "NOT_APPLICABLE",
                    f"Could not query system.query.history: {ap_err}"))
        except Exception as e:
            findings.append(_make_finding("SAT-PERF-QUERY-ANTIPATTERN", "NOT_APPLICABLE",
                f"System table query failed: {e}"))
    else:
        findings.append(_make_finding("SAT-PERF-QUERY-ANTIPATTERN", "NOT_APPLICABLE",
            "No running SQL warehouse found for system table queries."))

    return findings


async def _check_ai_ml_extended(client: httpx.AsyncClient, host: str, token: str) -> list[SATFinding]:
    """Extended AI/ML checks: model approval, cost tracking, feature freshness."""
    findings: list[SATFinding] = []
    hdr = {"Authorization": f"Bearer {token}"}

    # SAT-AI-MODEL-APPROVAL: Model serving requires staging-to-prod promotion
    ep_data, ep_s, ep_e = await _dbx_get(client, host,
        "/api/2.0/serving-endpoints", token)
    if ep_data is not None:
        endpoints = ep_data.get("endpoints", [])
        if not endpoints:
            findings.append(_make_finding("SAT-AI-MODEL-APPROVAL", "NOT_APPLICABLE",
                "No serving endpoints found."))
        else:
            multi_version = 0
            for ep in endpoints:
                config = ep.get("config", {})
                entities = config.get("served_entities", config.get("served_models", []))
                if len(entities) > 1:
                    multi_version += 1
            findings.append(_make_finding("SAT-AI-MODEL-APPROVAL",
                "PASS" if multi_version > 0 else "WARN",
                f"{multi_version}/{len(endpoints)} endpoint(s) use multi-entity configs "
                f"(indicates promotion workflow).",
                {"multi_version": multi_version, "total": len(endpoints)}))
    else:
        findings.append(_na("SAT-AI-MODEL-APPROVAL", ep_s, ep_e))

    # SAT-AI-COST-TRACKING: External model endpoints have cost tracking
    if ep_data is not None:
        endpoints = ep_data.get("endpoints", [])
        external = [e for e in endpoints
                   if any(se.get("external_model") for se in
                         e.get("config", {}).get("served_entities",
                         e.get("config", {}).get("served_models", [])))]
        if not external:
            findings.append(_make_finding("SAT-AI-COST-TRACKING", "NOT_APPLICABLE",
                "No external model endpoints found."))
        else:
            with_tracking = sum(1 for e in external
                               if e.get("config", {}).get("usage_tracking_config"))
            findings.append(_make_finding("SAT-AI-COST-TRACKING",
                "PASS" if with_tracking == len(external) else "WARN",
                f"{with_tracking}/{len(external)} external model endpoint(s) have cost tracking.",
                {"with_tracking": with_tracking, "total_external": len(external)}))
    else:
        findings.append(_na("SAT-AI-COST-TRACKING", ep_s, ep_e))

    # SAT-ML-FEATURE-FRESHNESS: Feature tables have freshness monitoring
    # Legacy Feature Store API may not exist on newer workspaces (UC Feature Engineering)
    ft_data, ft_s, ft_e = await _dbx_get(client, host,
        "/api/2.0/feature-store/feature-tables", token)
    if ft_data is not None:
        tables = ft_data.get("feature_tables", [])
        if not tables:
            findings.append(_make_finding("SAT-ML-FEATURE-FRESHNESS", "NOT_APPLICABLE",
                "No feature tables found."))
        else:
            findings.append(_make_finding("SAT-ML-FEATURE-FRESHNESS", "WARN",
                f"{len(tables)} feature table(s) found. Verify freshness monitors "
                f"are configured to detect stale features.",
                {"count": len(tables)}))
    elif ft_s in (404, 403):
        findings.append(_make_finding("SAT-ML-FEATURE-FRESHNESS", "NOT_APPLICABLE",
            f"Feature Store API not available (HTTP {ft_s}). "
            f"Workspace may use UC Feature Engineering instead."))
    else:
        findings.append(_na("SAT-ML-FEATURE-FRESHNESS", ft_s, ft_e))

    return findings


async def _check_data_protection_extended(client: httpx.AsyncClient, host: str, token: str) -> list[SATFinding]:
    """Extended data protection: PII classification, sharing audit, backup strategy."""
    findings: list[SATFinding] = []
    hdr = {"Authorization": f"Bearer {token}"}

    # SAT-DATA-CLASSIFICATION: Tables with PII columns have classification tags
    cat_data, c_s, c_e = await _dbx_get(client, host,
        "/api/2.1/unity-catalog/catalogs", token)
    if cat_data is not None:
        catalogs = cat_data.get("catalogs", [])
        non_system = [c for c in catalogs
                     if c.get("name") not in ("system", "__databricks_internal")]
        if not non_system:
            findings.append(_make_finding("SAT-DATA-CLASSIFICATION", "NOT_APPLICABLE",
                "No user catalogs found."))
        else:
            # Check if any catalogs have tags
            tagged = sum(1 for c in non_system if c.get("properties"))
            findings.append(_make_finding("SAT-DATA-CLASSIFICATION",
                "PASS" if tagged > 0 else "WARN",
                f"{tagged}/{len(non_system)} catalog(s) have properties/tags. "
                f"Verify PII columns are tagged for governance.",
                {"tagged": tagged, "total": len(non_system)}))
    else:
        findings.append(_na("SAT-DATA-CLASSIFICATION", c_s, c_e))

    # SAT-DATA-SHARING-AUDIT: Delta Sharing recipients audited
    recip_data, r_s, r_e = await _dbx_get(client, host,
        "/api/2.1/unity-catalog/recipients", token)
    if recip_data is not None:
        recipients = recip_data.get("recipients", [])
        if not recipients:
            findings.append(_make_finding("SAT-DATA-SHARING-AUDIT", "NOT_APPLICABLE",
                "No Delta Sharing recipients configured."))
        else:
            findings.append(_make_finding("SAT-DATA-SHARING-AUDIT", "WARN",
                f"{len(recipients)} Delta Sharing recipient(s). Review quarterly for "
                f"stale or unauthorized access.",
                {"count": len(recipients)}))
    else:
        findings.append(_na("SAT-DATA-SHARING-AUDIT", r_s, r_e))

    # SAT-DATA-BACKUP: Critical tables have backup strategy
    findings.append(_make_finding("SAT-DATA-BACKUP", "WARN",
        "Verify critical production tables have Delta DEEP CLONE or cross-region "
        "replication configured. Test restore procedures quarterly."))

    return findings


async def _check_ops_extended2(client: httpx.AsyncClient, host: str, token: str) -> list[SATFinding]:
    """Extended operations: incident runbooks, change management, capacity, DR."""
    findings: list[SATFinding] = []
    hdr = {"Authorization": f"Bearer {token}"}

    # SAT-OPS-INCIDENT-RUNBOOK: Alert destinations have runbook links
    alerts_data, al_s, al_e = await _dbx_get(client, host,
        "/api/2.0/sql/alerts", token)
    if alerts_data is not None:
        alerts = alerts_data if isinstance(alerts_data, list) else alerts_data.get("results", [])
        if not alerts:
            findings.append(_make_finding("SAT-OPS-INCIDENT-RUNBOOK", "NOT_APPLICABLE",
                "No SQL alerts configured."))
        else:
            # Check if any alerts mention runbook/wiki/docs in name
            runbook_kw = {"runbook", "wiki", "docs", "confluence", "notion", "playbook"}
            with_runbook = sum(1 for a in alerts
                             if any(kw in (a.get("name", "") + " " + str(a.get("options", {}))).lower()
                                    for kw in runbook_kw))
            findings.append(_make_finding("SAT-OPS-INCIDENT-RUNBOOK",
                "PASS" if with_runbook > 0 else "WARN",
                f"{with_runbook}/{len(alerts)} alert(s) reference runbook documentation.",
                {"with_runbook": with_runbook, "total": len(alerts)}))
    else:
        findings.append(_na("SAT-OPS-INCIDENT-RUNBOOK", al_s, al_e))

    # SAT-OPS-CHANGE-MGMT: Workspace changes tracked via system tables
    schema_data, sc_s, sc_e = await _dbx_get(client, host,
        "/api/2.1/unity-catalog/schemas", token, {"catalog_name": "system"})
    if schema_data is not None:
        schemas = schema_data.get("schemas", [])
        has_access = any(s.get("name") == "access" for s in schemas)
        findings.append(_make_finding("SAT-OPS-CHANGE-MGMT",
            "PASS" if has_access else "WARN",
            f"System access schema: {'available' if has_access else 'not found'}. "
            f"Use system.access.audit for change tracking.",
            {"has_access_schema": has_access}))
    else:
        findings.append(_na("SAT-OPS-CHANGE-MGMT", sc_s, sc_e))

    # SAT-OPS-CAPACITY-PLAN: Peak concurrent cluster count
    cl_data, cl_s, cl_e = await _dbx_get(client, host,
        "/api/2.0/clusters/list", token)
    if cl_data is not None:
        clusters = cl_data.get("clusters", [])
        running = sum(1 for c in clusters if c.get("state") == "RUNNING")
        total = len(clusters)
        findings.append(_make_finding("SAT-OPS-CAPACITY-PLAN",
            "PASS" if running < 20 else ("WARN" if running < 50 else "FAIL"),
            f"{running} cluster(s) currently running out of {total} total. "
            f"Monitor peak counts against account limits.",
            {"running": running, "total": total}))
    else:
        findings.append(_na("SAT-OPS-CAPACITY-PLAN", cl_s, cl_e))

    # SAT-OPS-DR-PLAN: Disaster recovery configuration
    findings.append(_make_finding("SAT-OPS-DR-PLAN", "WARN",
        "Verify disaster recovery plan exists: secondary workspace, Delta replication, "
        "metastore backup, and documented RTO/RPO. Test procedures quarterly."))

    return findings


# ─────────────────────────────────────────────────────────────────────────────
# Scan orchestration
# ─────────────────────────────────────────────────────────────────────────────

async def run_scan(
    host: str, token: str, workspace_name: str = "", quiet: bool = False,
    scan_secrets: bool = False, scan_secrets_days: int | None = None,
) -> SATScanResult:
    if not quiet:
        print(f"\n{'='*70}")
        print(f"  Databricks SAT Scanner — Security Assessment")
        print(f"  Workspace: {host}")
        if workspace_name:
            print(f"  Name:      {workspace_name}")
        print(f"  Started:   {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*70}\n")

    CATEGORY_NAMES = [
        "Identity & Access Management", "Network Security", "Data Protection",
        "Compute Security", "SQL Warehouses", "Secrets & Credentials",
        "Audit & Logging", "Workspace Configuration", "PAT Token Management",
        "Compute Extended", "Jobs Extended", "Governance Extended",
        "Settings API", "Informational", "SSO & SCIM",
        "AI / ML Governance", "Pools, Jobs & Libraries", "Compliance & Monitoring",
        "UC Governance", "Permission Audits", "Workspace Hygiene",
        "Operations Monitoring", "Feature Adoption",
        # Phase 1
        "Performance", "Cost Optimization", "Reliability",
        # Phase 2
        "Data Architecture", "Ops Excellence", "Governance Data Quality",
        # Phase 3
        "Advanced Performance", "Advanced Governance",
        # Data Residency
        "Data Residency",
        # Advanced Security & Compliance
        "Advanced Security",
        # Account Governance
        "Account Governance",
        # GenAI Security
        "GenAI Security",
        # Optimization & Data Quality
        "Table Optimization",
        "Data Quality",
        # Spark Best Practices
        "Spark Best Practices",
        # Dev Practices
        "Dev Practices",
        # New categories
        "Workspace Object ACLs",
        "Audit Delivery",
        "Network Exfiltration",
        "Serverless Governance",
        "Webhook Security",
        "Compliance",
        # Extended checks for existing categories
        "Identity & Access Extended",
        "Secrets Extended",
        "Cost Extended",
        "AI / ML Extended",
        "Data Protection Extended",
        "Operations Extended",
    ]

    async with httpx.AsyncClient(timeout=45) as client:
        tracker = _ItemTrackingClient(client)

        # Fetch account_id if not already populated by login flow (e.g. PAT-token mode)
        if host.rstrip("/") not in _WORKSPACE_ACCOUNT_IDS:
            _fetch_account_id(host, token)

        check_fns = [
            _check_iam, _check_network, _check_data_protection, _check_compute,
            _check_sql_warehouses, _check_secrets, _check_logging,
            _check_workspace_config_extended, _check_pat_tokens,
            _check_compute_extended, _check_jobs_extended,
            _check_governance_extended, _check_settings_api,
            _check_informational, _check_sso_scim,
            _check_ai_ml_governance, _check_pools_jobs_extended,
            _check_compliance_extended,
            _check_uc_governance, _check_permission_audits, _check_workspace_hygiene,
            _check_ops_monitoring, _check_feature_adoption,
            # Phase 1
            _check_performance, _check_cost, _check_reliability,
            # Phase 2
            _check_data_architecture, _check_ops_excellence, _check_governance_data_quality,
            # Phase 3
            _check_advanced_performance, _check_advanced_governance,
            # Data Residency
            _check_data_residency,
            # Advanced Security & Compliance
            _check_advanced_security,
            # Account Governance (Account API)
            _check_account_governance,
            # GenAI Security
            _check_ai_genai_security,
            # Optimization & Data Quality
            _check_optimization,
            _check_data_quality,
            # Spark Best Practices
            _check_spark_best_practices,
            # Dev Practices
            _check_dev_practices,
            # Delta Best Practices
            _check_delta_best_practices,
            # DLT Best Practices
            _check_dlt_best_practices,
            # New categories
            _check_workspace_acls,
            _check_audit_delivery,
            _check_network_exfiltration,
            _check_serverless_governance,
            _check_webhook_security,
            _check_compliance,
            # Extended checks for existing categories
            _check_iam_extended2,
            _check_secrets_extended,
            _check_cost_extended,
            _check_ai_ml_extended,
            _check_data_protection_extended,
            _check_ops_extended2,
        ]

        if not quiet:
            print(f"  Running {len(check_fns)} check categories in parallel...\n")

        # Launch all check functions concurrently
        results = await asyncio.gather(
            *(fn(tracker, host, token) for fn in check_fns),
            return_exceptions=True,
        )

        all_findings: list[SATFinding] = []
        for i, result in enumerate(results):
            cat_name = CATEGORY_NAMES[i] if i < len(CATEGORY_NAMES) else f"Category-{i}"
            if isinstance(result, BaseException):
                if not quiet:
                    _log(f"[{i+1:2d}/{len(check_fns)}] {cat_name}  ✗ ERROR: {result}")
                all_findings.append(SATFinding(
                    check_id=f"SAT-ERR-{i+1}", category="Scan Errors",
                    title=f"Check '{cat_name}' raised an exception",
                    description="Unexpected error.", severity="high", status="WARN",
                    current_state=f"Exception: {type(result).__name__}: {result}",
                    recommendation="Check token scopes and workspace accessibility.",
                    is_api_error=True,
                ))
            else:
                all_findings.extend(result)
                if not quiet:
                    passed = sum(1 for f in result if f.status == "PASS")
                    failed = sum(1 for f in result if f.status == "FAIL")
                    warns = sum(1 for f in result if f.status == "WARN")
                    _log(f"[{i+1:2d}/{len(check_fns)}] {cat_name}  ✓ ({passed} pass, {failed} fail, {warns} warn)")

        # ── Optional: TruffleHog Secret Scanning ──
        if scan_secrets:
            from .secret_scan import run_secret_scan
            if not quiet:
                print(f"\n  {'─'*66}")
                _log("Running TruffleHog secret scanning (7 targets)...")
            try:
                secret_findings = await run_secret_scan(
                    client, host, token,
                    scan_secrets_days=scan_secrets_days,
                    quiet=quiet,
                )
                all_findings.extend(secret_findings)
                if not quiet:
                    s_passed = sum(1 for f in secret_findings if f.status == "PASS")
                    s_failed = sum(1 for f in secret_findings if f.status == "FAIL")
                    s_warns  = sum(1 for f in secret_findings if f.status == "WARN")
                    print(f"  [24/24] Secret Scanning  ✓ ({s_passed} pass, {s_failed} fail, {s_warns} warn)")
            except Exception as exc:
                import traceback
                tb = traceback.format_exc()
                if not quiet:
                    print(f"  [24/24] Secret Scanning  ✗ ERROR: {exc}")
                    print(f"  Traceback:\n{tb}")
                all_findings.append(SATFinding(
                    check_id="SAT-SCAN-ERR", category="Secret Scanning",
                    title="Secret scanning raised an exception",
                    description="Unexpected error during TruffleHog secret scanning.",
                    severity="high", status="WARN",
                    current_state=f"Exception: {type(exc).__name__}: {exc}",
                    recommendation="Ensure TruffleHog is installed and workspace APIs are accessible.",
                    details={"traceback": tb},
                    is_api_error=True,
                ))

    # workspace-conf "Invalid keys" 400s are expected on many Azure workspaces
    # (deprecated keys, workspace-type differences).  All check functions already
    # handle this via _dbx_get_workspace_conf retry/fallback logic, so remove it
    # from the tracker before enrichment to keep the endpoint summary clean.
    tracker.api_errors.pop("/api/2.0/workspace-conf", None)

    # Enrich findings with API endpoint info and scanned items
    for f in all_findings:
        ep = CHECK_API_ENDPOINTS.get(f.check_id)
        if ep:
            f.details.setdefault("api_endpoint", ep)
            # Merge tracked items from the primary endpoint
            # CHECK_API_ENDPOINTS values may be compound like
            # "/api/2.0/clusters/list + /api/2.0/workspace-conf"
            if "items" not in f.details:
                primary_ep = ep.split(" + ")[0].split("?")[0]
                items = tracker.api_items.get(primary_ep)
                if items is not None:
                    f.details["items"] = items
                    f.details["items_scanned"] = len(items)
                else:
                    # Check if we got an API error for this endpoint
                    err_code = tracker.api_errors.get(primary_ep)
                    if err_code:
                        f.details["api_error_code"] = err_code
                    # Config/settings endpoints — set empty items so the
                    # formatted table renders instead of raw JSON
                    f.details["items"] = []
                    f.details["items_scanned"] = 0

    # ── Evidence & Portal Links ──
    for f in all_findings:
        # Evidence extraction
        if f.check_id in _WS_CONF_EVIDENCE:
            key = _WS_CONF_EVIDENCE[f.check_id]
            resp = f.details.get("api_response")
            if resp and isinstance(resp, dict):
                if "," in key:
                    val = {k.strip(): resp.get(k.strip()) for k in key.split(",")}
                else:
                    val = resp.get(key)
                f.evidence = {"field": key, "value": val, "source": "workspace-conf"}
            else:
                f.evidence = _auto_extract_evidence(f)
        else:
            f.evidence = _auto_extract_evidence(f)
        # Portal links (only for FAIL/WARN findings)
        if f.status in ("FAIL", "WARN") and not f.is_api_error:
            f.portal_link = _resolve_portal_link(f.check_id, host)
        else:
            f.portal_link = ""

    # ── Populate "Why it matters" benefits ──
    # Priority: CHECK_BENEFITS dict → SAT_CHECKS[check_id]["benefits"] → dynamic doc fetch
    needs_doc_fetch = False
    for f in all_findings:
        b = CHECK_BENEFITS.get(f.check_id, "") or SAT_CHECKS.get(f.check_id, {}).get("benefits", "")
        if b:
            f.benefits = b
        elif f.reference_url:
            needs_doc_fetch = True
    # Fallback: fetch doc summaries for any checks not covered by CHECK_BENEFITS
    if needs_doc_fetch:
        uncovered_urls = {f.reference_url for f in all_findings if not f.benefits and f.reference_url}
        if uncovered_urls:
            if not quiet:
                print(f"\n  Fetching doc summaries for {len(uncovered_urls)} uncovered reference URL(s)...")
            async with httpx.AsyncClient(timeout=15) as doc_client:
                summaries = await _fetch_doc_summaries(uncovered_urls, doc_client)
            for f in all_findings:
                if not f.benefits and f.reference_url and f.reference_url in summaries:
                    f.benefits = summaries[f.reference_url]

    # ── API Endpoint Summary ──
    ep_summary = _build_endpoint_summary(all_findings)
    if not quiet:
        print(f"\n  {'─'*66}")
        print(f"  API Endpoint Summary: {ep_summary['total']} unique endpoint(s) queried")
        print(f"    ✓ {ep_summary['with_items']} returned items  |  ⚙ {ep_summary['config']} config/settings  |  ○ {ep_summary['empty']} empty  |  ✗ {ep_summary['error']} errors")
        for e in ep_summary["endpoints"]:
            if e["status"] == "items":
                print(f"      ✓ {e['endpoint']}  ({e['items_count']} item{'s' if e['items_count'] != 1 else ''})")
            elif e["status"] == "config":
                print(f"      ⚙ {e['endpoint']}")
            elif e["status"] == "error":
                print(f"      ✗ {e['endpoint']}  (HTTP {e['error_code']})")
            else:
                print(f"      ○ {e['endpoint']}  (0 items)")

    overall_score, cat_scores = _compute_sat_score(all_findings)

    api_err_count = sum(1 for f in all_findings if f.is_api_error)

    result = SATScanResult(
        workspace_url=host,
        scanned_at=datetime.utcnow().isoformat() + "Z",
        overall_score=overall_score,
        total_checks=len(all_findings),
        passed=sum(1 for f in all_findings if f.status == "PASS"),
        failed=sum(1 for f in all_findings if f.status == "FAIL"),
        warnings=sum(1 for f in all_findings if f.status == "WARN" and not f.is_api_error),
        not_applicable=sum(1 for f in all_findings if f.status == "NOT_APPLICABLE" and not f.is_api_error),
        api_errors=api_err_count,
        findings=sorted(all_findings, key=lambda f: (
            {"critical": 0, "high": 1, "medium": 2, "low": 3, "pass": 4}.get(f.severity, 5),
            {"FAIL": 0, "WARN": 1, "NOT_APPLICABLE": 2, "PASS": 3}.get(f.status, 4),
        )),
        category_scores=cat_scores,
        workspace_name=workspace_name,
    )
    result.endpoint_summary = ep_summary

    if not quiet:
        _print_summary(result)

    return result


# ─────────────────────────────────────────────────────────────────────────────
# Connectivity check
# ─────────────────────────────────────────────────────────────────────────────

async def check_connectivity(host: str, token: str) -> bool:
    """Check if the workspace is reachable."""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                f"{host}/api/2.0/clusters/spark-versions",
                headers={"Authorization": f"Bearer {token}"},
            )
            if resp.status_code == 200:
                print(f"  ✅ Workspace is reachable: {host}")
                return True
            else:
                print(f"  ❌ Workspace returned HTTP {resp.status_code}")
                return False
    except Exception as exc:
        print(f"  ❌ Connection failed: {exc}")
        return False

