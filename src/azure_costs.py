"""Azure Cost Management API integration for FinOps.

Queries Azure Cost Management API for:
- Daily cost trend (total Azure spend)
- Service breakdown (by MeterCategory)
- Resource group breakdown
- Databricks-specific costs (by MeterSubCategory)
- Top resources by cost

Ported from azure-assesment-tool/assesment/azure_scanner/costs.py
Simplified for Clone-Xs: synchronous httpx calls, config-driven auth.
"""

import logging
import subprocess
import time
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field, asdict
from typing import Any

import httpx

logger = logging.getLogger(__name__)


# ── Result model ──────────────────────────────────────────────────────

@dataclass
class AzureCostResult:
    daily_trend: list[dict] = field(default_factory=list)
    service_breakdown: list[dict] = field(default_factory=list)
    rg_breakdown: list[dict] = field(default_factory=list)
    databricks_costs: dict[str, Any] = field(default_factory=dict)
    top_resources: list[dict] = field(default_factory=list)
    total_cost: float = 0.0
    projected_monthly: float = 0.0
    avg_daily_cost: float = 0.0
    currency: str = "USD"
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)


# ── Auth ──────────────────────────────────────────────────────────────

_arm_token_cache: dict[str, tuple[str, float]] = {}  # key -> (token, expiry)


def get_arm_token(tenant_id: str = "", session_auth_method: str = "", session_client=None) -> str:
    """Get ARM token for Azure Cost Management API.

    Resolution order:
    1. Check in-memory cache (tokens cached for ~50 min)
    2. If user logged in via Azure CLI or service principal → reuse those credentials
    3. Fall back to `az account get-access-token` subprocess

    Args:
        tenant_id: Azure tenant ID (optional)
        session_auth_method: Current session's auth method (azure-cli, service-principal, pat, etc.)
        session_client: Current WorkspaceClient from the session (for SP credential reuse)
    """
    cache_key = tenant_id or "default"
    if cache_key in _arm_token_cache:
        token, expiry = _arm_token_cache[cache_key]
        if time.time() < expiry - 60:
            return token

    # Strategy 1: Reuse service principal credentials for ARM token
    if session_auth_method == "service-principal" and session_client:
        try:
            config = session_client.config
            client_id = getattr(config, "azure_client_id", None) or getattr(config, "client_id", None)
            client_secret = getattr(config, "azure_client_secret", None) or getattr(config, "client_secret", None)
            sp_tenant = tenant_id or getattr(config, "azure_tenant_id", None) or ""

            if client_id and client_secret and sp_tenant:
                logger.info("Getting ARM token via service principal credentials")
                resp = httpx.post(
                    f"https://login.microsoftonline.com/{sp_tenant}/oauth2/v2.0/token",
                    data={
                        "grant_type": "client_credentials",
                        "client_id": client_id,
                        "client_secret": client_secret,
                        "scope": "https://management.azure.com/.default",
                    },
                    timeout=15,
                )
                if resp.status_code == 200:
                    token = resp.json()["access_token"]
                    _arm_token_cache[cache_key] = (token, time.time() + 3000)
                    return token
                logger.warning("SP token request failed: %s", resp.text[:200])
        except Exception as e:
            logger.warning("Failed to get ARM token via service principal: %s", e)

    # Strategy 2: Azure CLI (works if user ran az login — either in-app or externally)
    cmd = ["az", "account", "get-access-token", "--resource", "https://management.azure.com/", "-o", "json"]
    if tenant_id:
        cmd.extend(["--tenant", tenant_id])
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            import json
            data = json.loads(result.stdout)
            token = data["accessToken"]
            _arm_token_cache[cache_key] = (token, time.time() + 3000)
            return token
        # az CLI failed — log but don't raise yet
        logger.debug("az CLI token failed: %s", result.stderr[:200])
    except FileNotFoundError:
        logger.debug("Azure CLI (az) not installed")
    except subprocess.TimeoutExpired:
        logger.debug("Azure CLI timed out")

    raise RuntimeError(
        "Could not get Azure ARM token. Options:\n"
        "1. Log in via Azure CLI in Settings → Authentication → Azure Login\n"
        "2. Use a service principal with Azure tenant credentials\n"
        "3. Install Azure CLI and run 'az login' externally"
    )


# ── Query payloads ────────────────────────────────────────────────────

def _daily_cost_body(days: int) -> dict:
    start = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%dT00:00:00+00:00")
    end = datetime.now(timezone.utc).strftime("%Y-%m-%dT23:59:59+00:00")
    return {
        "type": "ActualCost",
        "timeframe": "Custom",
        "timePeriod": {"from": start, "to": end},
        "dataset": {
            "granularity": "Daily",
            "aggregation": {"totalCost": {"name": "Cost", "function": "Sum"}},
        },
    }


def _service_breakdown_body(days: int) -> dict:
    start = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%dT00:00:00+00:00")
    end = datetime.now(timezone.utc).strftime("%Y-%m-%dT23:59:59+00:00")
    return {
        "type": "ActualCost",
        "timeframe": "Custom",
        "timePeriod": {"from": start, "to": end},
        "dataset": {
            "granularity": "None",
            "grouping": [{"type": "Dimension", "name": "MeterCategory"}],
            "aggregation": {"totalCost": {"name": "Cost", "function": "Sum"}},
        },
    }


def _rg_breakdown_body(days: int) -> dict:
    start = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%dT00:00:00+00:00")
    end = datetime.now(timezone.utc).strftime("%Y-%m-%dT23:59:59+00:00")
    return {
        "type": "ActualCost",
        "timeframe": "Custom",
        "timePeriod": {"from": start, "to": end},
        "dataset": {
            "granularity": "None",
            "grouping": [{"type": "Dimension", "name": "ResourceGroupName"}],
            "aggregation": {"totalCost": {"name": "Cost", "function": "Sum"}},
        },
    }


def _databricks_cost_body(days: int) -> dict:
    start = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%dT00:00:00+00:00")
    end = datetime.now(timezone.utc).strftime("%Y-%m-%dT23:59:59+00:00")
    return {
        "type": "ActualCost",
        "timeframe": "Custom",
        "timePeriod": {"from": start, "to": end},
        "dataset": {
            "granularity": "Daily",
            "filter": {
                "dimensions": {
                    "name": "MeterCategory",
                    "operator": "In",
                    "values": ["Azure Databricks"],
                },
            },
            "grouping": [{"type": "Dimension", "name": "MeterSubCategory"}],
            "aggregation": {"totalCost": {"name": "Cost", "function": "Sum"}},
        },
    }


def _top_resources_body(days: int) -> dict:
    start = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%dT00:00:00+00:00")
    end = datetime.now(timezone.utc).strftime("%Y-%m-%dT23:59:59+00:00")
    return {
        "type": "ActualCost",
        "timeframe": "Custom",
        "timePeriod": {"from": start, "to": end},
        "dataset": {
            "granularity": "None",
            "grouping": [
                {"type": "Dimension", "name": "ResourceId"},
                {"type": "Dimension", "name": "MeterCategory"},
            ],
            "aggregation": {"totalCost": {"name": "Cost", "function": "Sum"}},
        },
    }


# ── API call ──────────────────────────────────────────────────────────

def _query_cost_management(
    token: str, subscription_id: str, resource_group: str, body: dict,
    timeout: int = 30,
) -> dict:
    """Query Azure Cost Management API with retry on 429."""
    scope = f"/subscriptions/{subscription_id}"
    if resource_group:
        scope += f"/resourceGroups/{resource_group}"

    url = (
        f"https://management.azure.com{scope}"
        "/providers/Microsoft.CostManagement/query"
        "?api-version=2023-11-01"
    )
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    for attempt in range(5):
        resp = httpx.post(url, headers=headers, json=body, timeout=timeout)
        if resp.status_code == 429:
            raw_retry = resp.headers.get("Retry-After", "")
            wait = min(int(raw_retry) if raw_retry.isdigit() else 5 * (attempt + 1), 30)
            logger.info("Cost API 429, waiting %ds (%d/5)", wait, attempt + 1)
            time.sleep(wait)
            continue
        if resp.status_code in (401, 403):
            raise PermissionError(
                "No Cost Management access — requires Cost Management Reader role on the subscription"
            )
        break

    if resp.status_code == 404:
        return {"properties": {"columns": [], "rows": []}}
    if resp.status_code != 200:
        raise RuntimeError(f"Cost API returned {resp.status_code}: {resp.text[:300]}")

    return resp.json()


def _parse_response(data: dict) -> tuple[list[str], list[list]]:
    props = data.get("properties", {})
    columns = [c["name"] for c in props.get("columns", [])]
    rows = props.get("rows", [])
    return columns, rows


def _parse_date(raw_date) -> str:
    if isinstance(raw_date, (int, float)):
        s = str(int(raw_date))
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return str(raw_date)[:10]


# ── Public API ────────────────────────────────────────────────────────

def query_azure_costs(
    subscription_id: str,
    resource_group: str = "",
    tenant_id: str = "",
    days: int = 30,
    timeout: int = 30,
    session_auth_method: str = "",
    session_client=None,
) -> AzureCostResult:
    """Query Azure Cost Management for comprehensive cost data.

    Returns AzureCostResult with daily_trend, service_breakdown,
    rg_breakdown, databricks_costs, top_resources.

    If session_auth_method/session_client are provided, reuses the
    existing Azure login credentials (no separate az login needed).
    """
    token = get_arm_token(tenant_id, session_auth_method, session_client)
    errors: list[str] = []
    daily_trend: list[dict] = []
    service_breakdown: list[dict] = []
    rg_breakdown: list[dict] = []
    databricks_costs: dict[str, Any] = {}
    top_resources: list[dict] = []
    total_cost = 0.0
    currency = "USD"

    # 1. Daily trend
    try:
        raw = _query_cost_management(token, subscription_id, resource_group, _daily_cost_body(days), timeout)
        columns, rows = _parse_response(raw)
        cost_idx = columns.index("Cost") if "Cost" in columns else 0
        date_idx = columns.index("UsageDate") if "UsageDate" in columns else -1
        currency_idx = columns.index("Currency") if "Currency" in columns else -1
        for row in rows:
            cost = float(row[cost_idx]) if row[cost_idx] else 0.0
            date_str = _parse_date(row[date_idx]) if date_idx >= 0 and row[date_idx] else ""
            if currency_idx >= 0 and row[currency_idx]:
                currency = str(row[currency_idx])
            total_cost += cost
            daily_trend.append({"date": date_str, "cost": round(cost, 2)})
        daily_trend.sort(key=lambda x: x["date"])
    except PermissionError as e:
        errors.append(str(e))
        return AzureCostResult(errors=errors)
    except Exception as e:
        errors.append(f"daily_trend: {e}")

    # 2. Service breakdown
    try:
        raw = _query_cost_management(token, subscription_id, resource_group, _service_breakdown_body(days), timeout)
        columns, rows = _parse_response(raw)
        cost_idx = columns.index("Cost") if "Cost" in columns else 0
        svc_idx = columns.index("MeterCategory") if "MeterCategory" in columns else -1
        for row in rows:
            cost = float(row[cost_idx]) if row[cost_idx] else 0.0
            svc = str(row[svc_idx]) if svc_idx >= 0 else "Unknown"
            if cost > 0.01:
                service_breakdown.append({"service": svc, "cost": round(cost, 2)})
        service_breakdown.sort(key=lambda x: -x["cost"])
        svc_total = sum(s["cost"] for s in service_breakdown) or 1
        for s in service_breakdown:
            s["pct"] = round(s["cost"] * 100 / svc_total, 1)
    except Exception as e:
        errors.append(f"service_breakdown: {e}")

    time.sleep(2)  # Rate limit between queries

    # 3. Resource group breakdown
    try:
        if not resource_group:
            raw = _query_cost_management(token, subscription_id, "", _rg_breakdown_body(days), timeout)
            columns, rows = _parse_response(raw)
            cost_idx = columns.index("Cost") if "Cost" in columns else 0
            rg_idx = columns.index("ResourceGroupName") if "ResourceGroupName" in columns else -1
            for row in rows:
                cost = float(row[cost_idx]) if row[cost_idx] else 0.0
                rg = str(row[rg_idx]) if rg_idx >= 0 else "Unknown"
                if cost > 0.01:
                    rg_breakdown.append({"resource_group": rg, "cost": round(cost, 2)})
            rg_breakdown.sort(key=lambda x: -x["cost"])
    except Exception as e:
        errors.append(f"rg_breakdown: {e}")

    time.sleep(2)

    # 4. Databricks-specific costs
    try:
        raw = _query_cost_management(token, subscription_id, resource_group, _databricks_cost_body(days), timeout)
        columns, rows = _parse_response(raw)
        cost_idx = columns.index("Cost") if "Cost" in columns else 0
        date_idx = columns.index("UsageDate") if "UsageDate" in columns else -1
        sub_idx = columns.index("MeterSubCategory") if "MeterSubCategory" in columns else -1
        dbr_subcats: dict[str, float] = {}
        dbr_daily: dict[str, float] = {}
        dbr_total = 0.0
        for row in rows:
            cost = float(row[cost_idx]) if row[cost_idx] else 0.0
            dbr_total += cost
            subcat = str(row[sub_idx]) if sub_idx >= 0 else "Other"
            dbr_subcats[subcat] = dbr_subcats.get(subcat, 0) + cost
            date_str = _parse_date(row[date_idx]) if date_idx >= 0 and row[date_idx] else ""
            dbr_daily[date_str] = dbr_daily.get(date_str, 0) + cost
        dbr_trend = sorted([{"date": d, "cost": round(c, 2)} for d, c in dbr_daily.items()], key=lambda x: x["date"])
        dbr_subcat_list = sorted([{"sub_category": k, "cost": round(v, 2)} for k, v in dbr_subcats.items()], key=lambda x: -x["cost"])
        for s in dbr_subcat_list:
            s["pct"] = round(s["cost"] * 100 / (dbr_total or 1), 1)
        databricks_costs = {
            "total": round(dbr_total, 2),
            "daily_trend": dbr_trend,
            "sub_categories": dbr_subcat_list,
            "pct_of_total": round(dbr_total * 100 / (total_cost or 1), 1),
        }
    except Exception as e:
        errors.append(f"databricks_costs: {e}")
        databricks_costs = {"total": 0, "daily_trend": [], "sub_categories": [], "pct_of_total": 0}

    time.sleep(2)

    # 5. Top resources
    try:
        raw = _query_cost_management(token, subscription_id, resource_group, _top_resources_body(days), timeout)
        columns, rows = _parse_response(raw)
        cost_idx = columns.index("Cost") if "Cost" in columns else 0
        res_idx = columns.index("ResourceId") if "ResourceId" in columns else -1
        cat_idx = columns.index("MeterCategory") if "MeterCategory" in columns else -1
        for row in rows:
            cost = float(row[cost_idx]) if row[cost_idx] else 0.0
            resource_id = str(row[res_idx]) if res_idx >= 0 else ""
            category = str(row[cat_idx]) if cat_idx >= 0 else ""
            if cost > 0.01:
                parts = resource_id.split("/")
                name = parts[-1] if parts else resource_id
                top_resources.append({
                    "resource_id": resource_id, "resource_name": name,
                    "service": category, "cost": round(cost, 2),
                })
        top_resources.sort(key=lambda x: -x["cost"])
        top_resources = top_resources[:30]
    except Exception as e:
        errors.append(f"top_resources: {e}")

    avg_daily = round(total_cost / max(len(daily_trend), 1), 2)
    projected = round(avg_daily * 30, 2)

    return AzureCostResult(
        daily_trend=daily_trend,
        service_breakdown=service_breakdown,
        rg_breakdown=rg_breakdown,
        databricks_costs=databricks_costs,
        top_resources=top_resources,
        total_cost=round(total_cost, 2),
        projected_monthly=projected,
        avg_daily_cost=avg_daily,
        currency=currency,
        errors=errors,
    )


def is_azure_configured(config: dict) -> dict:
    """Check if Azure Cost Management is configured."""
    azure = config.get("azure", {})
    sub_id = azure.get("subscription_id", "")
    return {
        "configured": bool(sub_id),
        "subscription_id": sub_id[:8] + "..." if sub_id else "",
        "resource_group": azure.get("resource_group", ""),
        "tenant_id": azure.get("tenant_id", "")[:8] + "..." if azure.get("tenant_id") else "",
    }
