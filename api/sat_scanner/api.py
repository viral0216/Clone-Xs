"""SAT Scanner — HTTP client functions and API helpers."""

from __future__ import annotations

import asyncio
import json
import random
import re
from typing import Any

import httpx

from .models import SATFinding
from .checks import (
    SAT_CHECKS, _get_effort, _WORKSPACE_ACCOUNT_IDS, _ITEM_EXTRACTORS,
)


def _make_finding(check_id: str, status: str, current_state: str, details: dict | None = None) -> SATFinding:
    meta = SAT_CHECKS[check_id]
    sev = meta["severity"]
    if status == "PASS":
        sev = "pass"
    elif status == "NOT_APPLICABLE":
        sev = "low"
    return SATFinding(
        check_id=check_id,
        category=meta["category"],
        title=meta["title"],
        description=meta["description"],
        severity=sev,
        status=status,
        current_state=current_state,
        recommendation=meta["recommendation"],
        details=details or {},
        reference_url=meta.get("reference_url", ""),
        effort=_get_effort(check_id),
    )


def _na(check_id: str, http_status: int, error: Any) -> SATFinding:
    meta = SAT_CHECKS[check_id]
    if isinstance(error, dict):
        err_msg = error.get("message") or error.get("error_code") or json.dumps(error)
    else:
        err_msg = str(error) if error else "unknown error"

    # Build a human-readable justification explaining WHY the API call failed
    if http_status in (401, 403):
        status = "WARN"
        state = f"Permission denied (HTTP {http_status}) — {err_msg}"
        justification = ("The PAT token or Azure AD credential lacks admin privileges "
            "for this endpoint. Use a Workspace Admin token to resolve.")
    elif http_status == 0:
        status = "WARN"
        state = f"API unreachable — {err_msg}"
        justification = ("The Databricks API could not be reached (timeout or "
            "connection refused). Check network connectivity, VPN, or firewall rules.")
    elif http_status == 404:
        status = "NOT_APPLICABLE"
        state = f"Feature not available (HTTP 404). {err_msg}"
        justification = ("This API endpoint does not exist on the workspace. The feature "
            "may require a higher pricing tier (Premium) or is not enabled on this workspace.")
    elif http_status == 400 and "Invalid keys" in err_msg:
        status = "NOT_APPLICABLE"
        state = f"API returned HTTP {http_status}: {err_msg}"
        justification = ("The workspace does not recognise this configuration key. "
            "It may be deprecated, renamed, or managed via Unity Catalog / Account Console instead.")
    elif http_status == 429:
        status = "WARN"
        state = f"Rate limited (HTTP 429) — {err_msg}"
        justification = ("The Databricks API rate limit was exceeded. This is transient; "
            "re-running the scan should succeed.")
    elif http_status >= 500:
        status = "WARN"
        state = f"Server error (HTTP {http_status}) — {err_msg}"
        justification = ("The Databricks API returned a server error. This is likely transient; "
            "re-running the scan should succeed.")
    else:
        status = "WARN"
        state = f"API returned HTTP {http_status}: {err_msg}"
        justification = f"Unexpected HTTP {http_status} response from the Databricks API."

    api_error_detail = error if isinstance(error, dict) else err_msg
    # Downgrade severity for N/A findings — the original severity only applies
    # when the check actually runs and finds a problem (FAIL/WARN).
    sev = meta["severity"]
    if status == "NOT_APPLICABLE":
        sev = "low"
    return SATFinding(
        check_id=check_id,
        category=meta["category"],
        title=meta["title"],
        description=meta["description"],
        severity=sev,
        status=status,
        current_state=state,
        recommendation=meta["recommendation"],
        details={"http_status": http_status, "api_error": api_error_detail, "justification": justification},
        reference_url=meta.get("reference_url", ""),
        is_api_error=True,
        effort=_get_effort(check_id),
    )


_MAX_RETRIES = 5          # retry on HTTP 429 (rate-limited) / 503
_RETRY_BACKOFF = [1, 2, 4, 8, 16]  # seconds to wait between retries (exponential)


def _retry_wait(resp: Any, attempt: int) -> float:
    """Seconds to wait before retrying — honour the Retry-After header if present.

    Databricks returns Retry-After (seconds) on 429/503; respecting it is the
    server-directed way to back off and avoids compounding the rate limit.
    """
    jitter = random.uniform(0.0, 0.5)  # de-sync concurrent retries (thundering herd)
    ra = resp.headers.get("Retry-After", "") if resp is not None else ""
    if ra:
        try:
            return min(float(ra), 60.0) + jitter
        except (TypeError, ValueError):
            pass
    return float(_RETRY_BACKOFF[min(attempt, len(_RETRY_BACKOFF) - 1)]) + jitter


async def _dbx_get(
    client: httpx.AsyncClient, host: str, path: str, token: str,
    params: dict | None = None,
) -> tuple[Any, int, Any]:
    url = f"{host.rstrip('/')}{path}"
    for attempt in range(_MAX_RETRIES + 1):
        try:
            resp = await client.get(url, headers={"Authorization": f"Bearer {token}"}, params=params or {})
            if resp.status_code == 200:
                return resp.json(), 200, None
            if resp.status_code in (429, 503) and attempt < _MAX_RETRIES:
                await asyncio.sleep(_retry_wait(resp, attempt))
                continue
            try:
                err_body = resp.json()
            except Exception:
                err_body = resp.text[:300].strip() if resp.text else f"HTTP {resp.status_code}"
            return None, resp.status_code, err_body
        except httpx.TimeoutException:
            if attempt < _MAX_RETRIES:
                await asyncio.sleep(_RETRY_BACKOFF[min(attempt, len(_RETRY_BACKOFF) - 1)])
                continue
            return None, 0, "Request timed out"
        except httpx.ConnectError as exc:
            return None, 0, f"Connection error: {exc}"
        except Exception as exc:
            return None, 0, str(exc)
    return None, 0, "Max retries exceeded"


async def _dbx_post(
    client: httpx.AsyncClient, host: str, path: str, token: str,
    json_body: dict | None = None,
) -> tuple[Any, int, Any]:
    url = f"{host.rstrip('/')}{path}"
    for attempt in range(_MAX_RETRIES + 1):
        try:
            resp = await client.post(url, headers={"Authorization": f"Bearer {token}"}, json=json_body or {})
            if resp.status_code == 200:
                return resp.json(), 200, None
            if resp.status_code in (429, 503) and attempt < _MAX_RETRIES:
                await asyncio.sleep(_retry_wait(resp, attempt))
                continue
            try:
                err_body = resp.json()
            except Exception:
                err_body = resp.text[:300].strip() if resp.text else f"HTTP {resp.status_code}"
            return None, resp.status_code, err_body
        except httpx.TimeoutException:
            if attempt < _MAX_RETRIES:
                await asyncio.sleep(_RETRY_BACKOFF[min(attempt, len(_RETRY_BACKOFF) - 1)])
                continue
            return None, 0, "Request timed out"
        except httpx.ConnectError as exc:
            return None, 0, f"Connection error: {exc}"
        except Exception as exc:
            return None, 0, str(exc)
    return None, 0, "Max retries exceeded"


_MAX_JOBS_LIMIT: int = 0  # 0 = fetch all jobs (no ceiling)


async def _dbx_get_all_jobs(
    client: httpx.AsyncClient, host: str, token: str,
    extra_params: dict | None = None,
) -> tuple[Any, int, Any]:
    """Paginate through /api/2.1/jobs/list and return all jobs up to _MAX_JOBS_LIMIT."""
    all_jobs: list[dict] = []
    params: dict[str, str] = {"limit": "100"}
    if extra_params:
        params.update(extra_params)
    page_token: str | None = None
    while True:
        if page_token:
            params["page_token"] = page_token
        data, status, err = await _dbx_get(client, host, "/api/2.1/jobs/list", token, params)
        if data is None:
            return None, status, err
        jobs = data.get("jobs", [])
        all_jobs.extend(jobs)
        if _MAX_JOBS_LIMIT > 0 and len(all_jobs) >= _MAX_JOBS_LIMIT:
            all_jobs = all_jobs[:_MAX_JOBS_LIMIT]
            break
        if not data.get("has_more", False):
            break
        page_token = data.get("next_page_token")
        if not page_token:
            break
    return {"jobs": all_jobs}, 200, None


_ACCT_API_BASE = "https://accounts.azuredatabricks.net/api/2.0/accounts"


async def _acct_get(
    client: httpx.AsyncClient, account_id: str, path: str, token: str,
    params: dict | None = None,
) -> tuple[Any, int, Any]:
    """GET from the Databricks Account API (accounts.azuredatabricks.net)."""
    url = f"{_ACCT_API_BASE}/{account_id}/{path.lstrip('/')}"
    for attempt in range(_MAX_RETRIES + 1):
        try:
            resp = await client.get(url, headers={"Authorization": f"Bearer {token}"}, params=params or {}, timeout=20)
            if resp.status_code == 200:
                return resp.json(), 200, None
            if resp.status_code == 429 and attempt < _MAX_RETRIES:
                wait = _RETRY_BACKOFF[attempt] if attempt < len(_RETRY_BACKOFF) else 4
                await asyncio.sleep(wait)
                continue
            try:
                err_body = resp.json()
            except Exception:
                err_body = resp.text[:300].strip() if resp.text else f"HTTP {resp.status_code}"
            return None, resp.status_code, err_body
        except httpx.TimeoutException:
            if attempt < _MAX_RETRIES:
                await asyncio.sleep(_RETRY_BACKOFF[min(attempt, len(_RETRY_BACKOFF) - 1)])
                continue
            return None, 0, "Request timed out"
        except httpx.ConnectError as exc:
            return None, 0, f"Connection error: {exc}"
        except Exception as exc:
            return None, 0, str(exc)
    return None, 0, "Max retries exceeded"


async def _dbx_get_workspace_conf(
    client: httpx.AsyncClient, host: str, token: str, keys: str,
) -> tuple[dict | None, int, Any]:
    """Fetch workspace-conf keys with automatic fallback for unsupported keys.

    The /api/2.0/workspace-conf endpoint returns HTTP 400 "Invalid keys: [...]"
    when ANY requested key is unsupported by the workspace. This wrapper detects
    that error and retries each key individually, returning partial results
    for the keys that do work (instead of failing the entire batch).
    """
    data, status, err = await _dbx_get(client, host, "/api/2.0/workspace-conf", token, {"keys": keys})
    if status == 200 and data is not None:
        return data, 200, None

    # Parse "Invalid keys: [...]" from the error and retry without those keys
    if status == 400 and err:
        err_msg = err.get("message", str(err)) if isinstance(err, dict) else str(err)
        if "Invalid keys" in err_msg:
            # Extract the bad keys from the error, e.g. 'Invalid keys: ["foo","bar"]'
            m = re.search(r'Invalid keys:\s*\[([^\]]*)\]', err_msg)
            bad_keys: set[str] = set()
            if m:
                bad_keys = {k.strip().strip('"').strip("'") for k in m.group(1).split(",")}
            all_keys = [k.strip() for k in keys.split(",")]
            good_keys = [k for k in all_keys if k not in bad_keys]
            if good_keys:
                # Retry with only the valid keys
                data2, s2, e2 = await _dbx_get(client, host, "/api/2.0/workspace-conf", token,
                    {"keys": ",".join(good_keys)})
                if s2 == 200 and data2 is not None:
                    return data2, 200, None
                # If that also failed, fall back to per-key requests
            # Last resort: try each key individually and collect what works
            merged: dict[str, Any] = {}
            for k in all_keys:
                kd, ks, _ = await _dbx_get(client, host, "/api/2.0/workspace-conf", token, {"keys": k})
                if ks == 200 and kd is not None:
                    merged.update(kd)
            if merged:
                # Clear any residual tracker error — we got partial results
                if hasattr(client, "api_errors"):
                    client.api_errors.pop("/api/2.0/workspace-conf", None)
                return merged, 200, None
    # workspace-conf "Invalid keys" 400s are expected — always clear from tracker
    # so they don't pollute the endpoint summary.  Check functions have their own
    # fallback logic (Settings API, per-key retry, etc.).
    if hasattr(client, "api_errors"):
        client.api_errors.pop("/api/2.0/workspace-conf", None)
    return data, status, err


def _fetch_account_id(workspace_url: str, token: str) -> None:
    """Fetch the Databricks account_id UUID via the UC metastores API (sync).

    Populates _WORKSPACE_ACCOUNT_IDS[workspace_url] on success.
    """
    import httpx as _httpx
    url = f"{workspace_url.rstrip('/')}/api/2.1/unity-catalog/metastores"
    try:
        resp = _httpx.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            for ms in data.get("metastores", []):
                acct = ms.get("metastore_account_id", "")
                if acct:
                    _WORKSPACE_ACCOUNT_IDS[workspace_url.rstrip("/")] = acct
                    return
    except Exception:
        pass


def _extract_api_items(data: Any, list_key: str, name_key: str) -> list[str]:
    """Extract display names from a parsed API JSON response."""
    if not isinstance(data, dict):
        return []
    items = data.get(list_key, [])
    if not isinstance(items, list):
        return []
    names: list[str] = []
    for item in items[:100]:
        if not isinstance(item, dict):
            continue
        val: Any = item
        for part in name_key.split("."):
            val = val.get(part, "") if isinstance(val, dict) else ""
        name = str(val) if val else ""
        if name:
            names.append(name)
    return names


class _ItemTrackingClient:
    """Transparent httpx.AsyncClient wrapper that captures API list items.

    Intercepts GET responses and extracts item names using _ITEM_EXTRACTORS.
    The captured items are stored in ``api_items`` keyed by endpoint path,
    and later merged into finding details during the enrichment step.
    """

    def __init__(self, client: httpx.AsyncClient):
        self._client = client
        self.api_items: dict[str, list[str]] = {}
        self.api_errors: dict[str, int] = {}  # endpoint path -> HTTP status code

    async def get(self, url: str, **kw: Any) -> Any:
        from urllib.parse import urlparse
        path = urlparse(url).path
        for attempt in range(_MAX_RETRIES + 1):
            resp = await self._client.get(url, **kw)
            if resp.status_code == 429 and attempt < _MAX_RETRIES:
                await asyncio.sleep(_RETRY_BACKOFF[min(attempt, len(_RETRY_BACKOFF) - 1)])
                continue
            break
        if resp.status_code == 200:
            self._extract(url, resp)
            self.api_errors.pop(path, None)
        elif resp.status_code >= 400:
            self.api_errors.setdefault(path, resp.status_code)
        return resp

    async def post(self, url: str, **kw: Any) -> Any:
        for attempt in range(_MAX_RETRIES + 1):
            resp = await self._client.post(url, **kw)
            if resp.status_code == 429 and attempt < _MAX_RETRIES:
                await asyncio.sleep(_RETRY_BACKOFF[min(attempt, len(_RETRY_BACKOFF) - 1)])
                continue
            break
        return resp

    def _extract(self, url: str, resp: Any) -> None:
        from urllib.parse import urlparse
        path = urlparse(url).path
        cfg = _ITEM_EXTRACTORS.get(path)
        if not cfg:
            return
        try:
            data = resp.json()
            names = _extract_api_items(data, cfg[0], cfg[1])
            # Accumulate items across multiple calls to the same endpoint
            # (e.g. /api/2.1/unity-catalog/tables called per catalog/schema)
            existing = self.api_items.get(path)
            if existing is None:
                self.api_items[path] = names
            elif names:
                seen = set(existing)
                self.api_items[path].extend(n for n in names if n not in seen)
        except Exception:
            pass

    def __getattr__(self, name: str) -> Any:
        return getattr(self._client, name)


# Cache: reference_url → extracted summary text
_DOC_SUMMARIES: dict[str, str] = {}


async def _fetch_doc_summaries(reference_urls: set[str], client: httpx.AsyncClient) -> dict[str, str]:
    """Fetch Microsoft Learn pages and extract the meta description as a benefits summary.

    Returns {url: summary_text} for each URL that was successfully fetched.
    Results are cached in _DOC_SUMMARIES to avoid re-fetching.
    """
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


# ── Async SQL Statement Execution ──────────────────────────────────────────

async def _dbx_sql_query(
    client: httpx.AsyncClient, host: str, token: str,
    warehouse_id: str, sql: str, timeout: int = 60,
) -> tuple[list[list] | None, str]:
    """Execute SQL via the Statement Execution API (async) and return rows.

    Returns (rows, error_msg).  rows is None on failure.
    Polls for PENDING/RUNNING states up to 30 iterations.
    """
    url = f"{host}/api/2.0/sql/statements"
    payload = {
        "warehouse_id": warehouse_id,
        "statement": sql,
        "wait_timeout": "30s",
        "on_wait_timeout": "CONTINUE",
    }
    hdr = {"Authorization": f"Bearer {token}"}
    try:
        resp = await client.post(url, headers=hdr, json=payload, timeout=timeout)
        if resp.status_code != 200:
            return None, f"HTTP {resp.status_code}"
        data = resp.json()
    except Exception as exc:
        return None, str(exc)

    # Poll if warehouse is starting
    stmt_id = data.get("statement_id", "")
    state = data.get("status", {}).get("state", "")
    polls = 0
    while state in ("PENDING", "RUNNING") and polls < 30:
        await asyncio.sleep(2)
        polls += 1
        try:
            pr = await client.get(f"{url}/{stmt_id}", headers=hdr, timeout=30)
            if pr.status_code != 200:
                return None, f"Poll HTTP {pr.status_code}"
            data = pr.json()
            state = data.get("status", {}).get("state", "")
        except Exception as exc:
            return None, str(exc)

    if state == "FAILED":
        err = data.get("status", {}).get("error", {}).get("message", "Unknown")
        return None, err
    if state != "SUCCEEDED":
        return None, f"Unexpected state: {state}"

    result = data.get("result", {})
    rows = result.get("data_array", [])
    # Follow pagination
    next_link = result.get("next_chunk_internal_link")
    while next_link:
        try:
            cr = await client.get(f"{host}{next_link}", headers=hdr, timeout=30)
            if cr.status_code != 200:
                break
            cd = cr.json()
            rows.extend(cd.get("data_array", []))
            next_link = cd.get("next_chunk_internal_link")
        except Exception:
            break
    return rows, ""


async def _find_running_warehouse(
    client: httpx.AsyncClient, host: str, token: str,
) -> str | None:
    """Return the ID of any RUNNING SQL warehouse, or None."""
    hdr = {"Authorization": f"Bearer {token}"}
    try:
        r = await client.get(f"{host}/api/2.0/sql/warehouses", headers=hdr, timeout=15)
        if r.status_code != 200:
            return None
        for wh in r.json().get("warehouses", []):
            if wh.get("state") == "RUNNING":
                return wh["id"]
    except Exception:
        pass
    return None
