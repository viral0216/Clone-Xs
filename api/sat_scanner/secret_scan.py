"""
TruffleHog-based secret scanning for Databricks workspaces.
=============================================================

Scans notebooks, cluster configs, job definitions, init scripts,
DLT pipelines, and SQL warehouses for hardcoded secrets using
TruffleHog (800+ built-in detectors + custom Databricks token patterns).

Install via pip:
    pip install sat-scanner[secrets]    # installs trufflehog3 automatically

Or use the Go binary:
    brew install trufflehog             # macOS
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx


# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

# Resolved at runtime: "trufflehog3" (pip) or "trufflehog" (Go binary)
_resolved_cmd: str | None = None
_resolved_variant: str | None = None  # "trufflehog3" or "trufflehog"

# Databricks-specific token patterns for TruffleHog custom detectors.
# See: https://github.com/databricks-industry-solutions/security-analysis-tool
_CUSTOM_DETECTORS_YAML = """\
detectors:
  - name: DatabricksPAT
    keywords:
      - dapi
    regex:
      value: "dapi[a-h0-9]{32}"
  - name: DatabricksKeyEncryption
    keywords:
      - dkea
    regex:
      value: "dkea[a-h0-9]{32}"
  - name: DatabricksScopedAPI
    keywords:
      - dsapi
    regex:
      value: "dsapi[a-h0-9]{32}"
  - name: DatabricksOSE
    keywords:
      - dose
    regex:
      value: "dose[a-h0-9]{32}"
"""

_NOTEBOOK_EXPORT_CONCURRENCY = 5   # max concurrent notebook exports
_NOTEBOOK_EXPORT_DELAY = 0.2       # seconds between exports (rate limit)
_TRUFFLEHOG_TIMEOUT = 300          # 5 minutes per scan target
_MAX_FINDINGS_PER_CHECK = 50       # cap findings detail list per check

_SCAN_CHECK_IDS = (
    "SAT-SCAN-NOTEBOOKS", "SAT-SCAN-CLUSTER-CONF", "SAT-SCAN-JOB-CONF",
    "SAT-SCAN-INIT-GLOBAL", "SAT-SCAN-INIT-CLUSTER", "SAT-SCAN-DLT",
    "SAT-SCAN-SQL-WH",
)

_LANG_EXT = {
    "PYTHON": ".py", "SQL": ".sql", "SCALA": ".scala",
    "R": ".r", "SHELL": ".sh",
}


# ─────────────────────────────────────────────────────────────────────────────
# Helpers (imported lazily from cli to avoid circular imports at module level)
# ─────────────────────────────────────────────────────────────────────────────

def _get_cli_helpers():
    """Lazy-import helpers from cli module to avoid circular imports."""
    from .cli import SATFinding, SAT_CHECKS, _make_finding, _dbx_get
    return SATFinding, SAT_CHECKS, _make_finding, _dbx_get


def _get_list(data: Any, key: str) -> list:
    """Safely extract a list from an API response (handles both dict and list)."""
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return data.get(key, [])
    return []


# ─────────────────────────────────────────────────────────────────────────────
# TruffleHog detection — supports both pip package and Go binary
# ─────────────────────────────────────────────────────────────────────────────

def _check_trufflehog_available() -> tuple[bool, str]:
    """Detect TruffleHog: prefer trufflehog3 (pip) then trufflehog (Go binary)."""
    global _resolved_cmd, _resolved_variant

    # 1. Try direct Python import (same environment — most reliable)
    try:
        import trufflehog3 as _th3
        version = getattr(_th3, "__VERSION__", "unknown")
        _resolved_cmd = "trufflehog3_module"
        _resolved_variant = "trufflehog3"
        return True, f"trufflehog3 {version}"
    except ImportError:
        pass

    # 2. Try trufflehog3 / trufflehog on PATH
    for cmd_name, variant in [("trufflehog3", "trufflehog3"), ("trufflehog", "trufflehog")]:
        path = shutil.which(cmd_name)
        if not path:
            continue
        try:
            proc = subprocess.run(
                [cmd_name, "--version"],
                capture_output=True, text=True, timeout=10,
            )
            version = (proc.stdout.strip() or proc.stderr.strip() or "unknown version")
            _resolved_cmd = cmd_name
            _resolved_variant = variant
            return True, f"{variant} {version}"
        except Exception:
            continue

    return False, (
        "TruffleHog not found. "
        "Install: pip install sat-scanner[secrets]  (recommended) or "
        "brew install trufflehog  (macOS)"
    )


# ─────────────────────────────────────────────────────────────────────────────
# TruffleHog invocation & result parsing
# ─────────────────────────────────────────────────────────────────────────────

def _run_trufflehog(scan_dir: Path, config_path: Path | None = None) -> list[dict]:
    """Run TruffleHog on a directory, return parsed JSON results."""
    if not _resolved_cmd:
        return [{"_error": "TruffleHog not available"}]

    if _resolved_variant == "trufflehog3":
        # trufflehog3 (pip): scan local files, no git history, JSON output
        if _resolved_cmd == "trufflehog3_module":
            cmd = [sys.executable, "-m", "trufflehog3", "--no-history", "--format", "json", str(scan_dir)]
        else:
            cmd = [_resolved_cmd, "--no-history", "--format", "json", str(scan_dir)]
        if config_path:
            cmd.extend(["--config", str(config_path)])
    else:
        # trufflehog (Go binary): filesystem scan mode
        cmd = [_resolved_cmd, "filesystem", str(scan_dir), "--json", "--no-update"]
        if config_path:
            cmd.extend(["--config", str(config_path)])

    try:
        proc = subprocess.run(
            cmd, capture_output=True, text=True, timeout=_TRUFFLEHOG_TIMEOUT,
        )
    except subprocess.TimeoutExpired:
        return [{"_error": f"TruffleHog timed out after {_TRUFFLEHOG_TIMEOUT}s"}]
    except Exception as exc:
        return [{"_error": f"TruffleHog failed: {exc}"}]

    output = proc.stdout.strip()
    if not output:
        return []

    # trufflehog3 outputs a JSON array; Go binary outputs one JSON object per line
    results: list[dict] = []
    try:
        parsed = json.loads(output)
        if isinstance(parsed, list):
            results = [item for item in parsed if isinstance(item, dict)]
        elif isinstance(parsed, dict):
            results = [parsed]
    except json.JSONDecodeError:
        # Fall back to line-by-line parsing (Go binary format)
        for line in output.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if isinstance(obj, dict):
                    results.append(obj)
            except json.JSONDecodeError:
                continue
    return results


def _hash_secret(raw: str) -> str:
    """SHA-256 hash a detected secret (never store plaintext)."""
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _parse_trufflehog_results(results: list[dict]) -> list[dict]:
    """Normalise TruffleHog JSON output into a flat list of findings.

    Handles both trufflehog3 (pip) and trufflehog (Go binary) output formats.
    """
    parsed: list[dict] = []
    for r in results:
        if not isinstance(r, dict):
            continue
        if "_error" in r:
            parsed.append(r)
            continue

        # trufflehog3 (pip) format: {"rule": {...}, "path": "...", "secret": "...", ...}
        if "rule" in r:
            raw = r.get("secret", "")
            parsed.append({
                "detector_name": r.get("rule", {}).get("id", "unknown"),
                "secret_hash": _hash_secret(raw) if raw else "N/A",
                "source_file": r.get("path", "unknown"),
                "verified": False,
                "line_number": r.get("line"),
                "detector_type": r.get("rule", {}).get("severity", ""),
            })
            continue

        # trufflehog (Go binary) format: {"Raw": "...", "SourceMetadata": {...}, ...}
        raw = r.get("Raw", r.get("raw", ""))
        source = r.get("SourceMetadata", r.get("source_metadata", {}))
        if isinstance(source, dict):
            data = source.get("Data", source.get("data", {}))
            file_info = data.get("Filesystem", data.get("filesystem", {})) if isinstance(data, dict) else {}
        else:
            file_info = {}
        parsed.append({
            "detector_name": r.get("DetectorName", r.get("detector_name", "unknown")),
            "secret_hash": _hash_secret(raw) if raw else "N/A",
            "source_file": file_info.get("file", "unknown") if isinstance(file_info, dict) else "unknown",
            "verified": r.get("Verified", r.get("verified", False)),
            "line_number": file_info.get("line") if isinstance(file_info, dict) else None,
            "detector_type": r.get("DetectorType", r.get("detector_type", "")),
        })
    return parsed


def _dedup_findings(parsed: list[dict]) -> list[dict]:
    """Deduplicate by (secret_hash, source_file)."""
    seen: set[tuple[str, str]] = set()
    unique: list[dict] = []
    for p in parsed:
        if "_error" in p:
            unique.append(p)
            continue
        key = (p.get("secret_hash", ""), p.get("source_file", ""))
        if key not in seen:
            seen.add(key)
            unique.append(p)
    return unique


# ─────────────────────────────────────────────────────────────────────────────
# Content fetchers — all async, write to temp dir, return count
# ─────────────────────────────────────────────────────────────────────────────

_DIR_LIST_CONCURRENCY = 10  # max concurrent directory listing requests


async def _list_workspace_recursive(
    client: httpx.AsyncClient, host: str, token: str,
    root: str = "/", max_depth: int = 20, quiet: bool = False,
) -> list[dict]:
    """Recursively list NOTEBOOK objects via /api/2.0/workspace/list.

    Uses concurrent directory listing (up to _DIR_LIST_CONCURRENCY parallel
    requests) for much faster traversal of large workspaces.
    """
    hdr = {"Authorization": f"Bearer {token}"}
    notebooks: list[dict] = []
    dirs_scanned = 0
    sem = asyncio.Semaphore(_DIR_LIST_CONCURRENCY)

    async def _walk(path: str, depth: int):
        nonlocal dirs_scanned
        if depth > max_depth:
            return
        async with sem:
            try:
                resp = await client.get(
                    f"{host}/api/2.0/workspace/list",
                    headers=hdr, params={"path": path}, timeout=15,
                )
                if resp.status_code != 200:
                    return
            except Exception:
                return

        dirs_scanned += 1
        if not quiet and dirs_scanned % 50 == 0:
            print(f"  [Secret Scan]   ...scanned {dirs_scanned} directories, found {len(notebooks)} notebooks so far")

        child_dirs: list[str] = []
        for obj in resp.json().get("objects", []):
            otype = obj.get("object_type")
            if otype == "NOTEBOOK":
                notebooks.append(obj)
            elif otype == "DIRECTORY":
                child_dirs.append(obj["path"])

        # Fan out child directories concurrently
        if child_dirs:
            await asyncio.gather(*(_walk(d, depth + 1) for d in child_dirs))

    await _walk(root, 0)
    return notebooks


async def _export_notebooks_to_dir(
    client: httpx.AsyncClient, host: str, token: str,
    notebooks: list[dict], target_dir: Path, quiet: bool = False,
) -> int:
    """Export notebook source code to local files. Returns count exported."""
    hdr = {"Authorization": f"Bearer {token}"}
    sem = asyncio.Semaphore(_NOTEBOOK_EXPORT_CONCURRENCY)
    count = 0

    total = len(notebooks)

    async def _export_one(nb: dict) -> bool:
        nonlocal count
        async with sem:
            nb_path = nb.get("path", "")
            lang = nb.get("language", "PYTHON")
            try:
                resp = await client.get(
                    f"{host}/api/2.0/workspace/export",
                    headers=hdr,
                    params={"path": nb_path, "format": "SOURCE"},
                    timeout=30,
                )
                if resp.status_code != 200:
                    return False
                content = resp.json().get("content", "")
                if not content:
                    return False
                source = base64.b64decode(content).decode("utf-8", errors="replace")
                safe_name = nb_path.strip("/").replace("/", "__")
                ext = _LANG_EXT.get(lang, ".txt")
                (target_dir / f"{safe_name}{ext}").write_text(source, encoding="utf-8")
                count += 1
                if not quiet and count % 50 == 0:
                    print(f"  [Secret Scan]   ...exported {count}/{total} notebooks")
                await asyncio.sleep(_NOTEBOOK_EXPORT_DELAY)
                return True
            except Exception:
                return False

    await asyncio.gather(*(_export_one(nb) for nb in notebooks))
    return count


async def _fetch_cluster_configs(
    client: httpx.AsyncClient, host: str, token: str, target_dir: Path,
) -> tuple[int, list[str]]:
    """Write cluster spark_conf + spark_env_vars to JSON files."""
    _, SAT_CHECKS, _, _dbx_get = _get_cli_helpers()
    data, status, _ = await _dbx_get(client, host, "/api/2.0/clusters/list", token)
    if not data:
        return 0, []
    count = 0
    names: list[str] = []
    for cl in _get_list(data, "clusters"):
        cid = cl.get("cluster_id", "unknown")
        cname = cl.get("cluster_name", cid)
        conf = cl.get("spark_conf", {})
        env = cl.get("spark_env_vars", {})
        if not conf and not env:
            continue
        content = json.dumps({
            "cluster_name": cname,
            "spark_conf": conf,
            "spark_env_vars": env,
        }, indent=2)
        (target_dir / f"cluster_{cid}.json").write_text(content, encoding="utf-8")
        count += 1
        names.append(cname)
    return count, names


async def _fetch_job_configs(
    client: httpx.AsyncClient, host: str, token: str, target_dir: Path,
) -> tuple[int, list[str]]:
    """Write job params, env vars, and task parameters to JSON files."""
    _, SAT_CHECKS, _, _dbx_get = _get_cli_helpers()
    data, status, _ = await _dbx_get(
        client, host, "/api/2.1/jobs/list", token, {"limit": "100"},
    )
    if not data:
        return 0, []
    count = 0
    names: list[str] = []
    for job in _get_list(data, "jobs"):
        jid = job.get("job_id", "unknown")
        settings = job.get("settings", {})
        jname = settings.get("name", str(jid))
        extracted: dict[str, Any] = {
            "job_name": jname,
            "parameters": settings.get("parameters", []),
            "job_clusters": [],
            "tasks": [],
        }
        for jc in settings.get("job_clusters", []):
            nc = jc.get("new_cluster", {})
            extracted["job_clusters"].append({
                "spark_conf": nc.get("spark_conf", {}),
                "spark_env_vars": nc.get("spark_env_vars", {}),
            })
        for task in settings.get("tasks", []):
            task_data: dict[str, Any] = {
                "task_key": task.get("task_key", ""),
                "notebook_params": task.get("notebook_task", {}).get("base_parameters", {}),
                "python_wheel_params": task.get("python_wheel_task", {}).get("parameters", []),
                "spark_jar_params": task.get("spark_jar_task", {}).get("parameters", []),
                "spark_python_params": task.get("spark_python_task", {}).get("parameters", []),
                "spark_submit_params": task.get("spark_submit_task", {}).get("parameters", []),
                "environment_vars": task.get("environment_vars", {}),
            }
            nc = task.get("new_cluster", {})
            if nc:
                task_data["new_cluster_spark_conf"] = nc.get("spark_conf", {})
                task_data["new_cluster_env_vars"] = nc.get("spark_env_vars", {})
            extracted["tasks"].append(task_data)
        content = json.dumps(extracted, indent=2)
        (target_dir / f"job_{jid}.json").write_text(content, encoding="utf-8")
        count += 1
        names.append(jname)
    return count, names


async def _fetch_global_init_scripts(
    client: httpx.AsyncClient, host: str, token: str, target_dir: Path,
) -> tuple[int, list[str]]:
    """Fetch global init script contents (base64-decoded)."""
    hdr = {"Authorization": f"Bearer {token}"}
    _, SAT_CHECKS, _, _dbx_get = _get_cli_helpers()
    data, status, _ = await _dbx_get(client, host, "/api/2.0/global-init-scripts", token)
    if not data:
        return 0, []
    count = 0
    names: list[str] = []
    for script in _get_list(data, "scripts"):
        sid = script.get("script_id", "unknown")
        sname = script.get("name", sid)
        try:
            resp = await client.get(
                f"{host}/api/2.0/global-init-scripts/{sid}",
                headers=hdr, timeout=15,
            )
            if resp.status_code != 200:
                continue
            content_b64 = resp.json().get("script", "")
            if not content_b64:
                continue
            decoded = base64.b64decode(content_b64).decode("utf-8", errors="replace")
            (target_dir / f"global_{sid}.sh").write_text(decoded, encoding="utf-8")
            count += 1
            names.append(sname)
        except Exception:
            pass
    return count, names


async def _fetch_cluster_init_scripts(
    client: httpx.AsyncClient, host: str, token: str, target_dir: Path,
) -> tuple[int, list[str]]:
    """Fetch cluster-level init scripts from DBFS and workspace paths."""
    hdr = {"Authorization": f"Bearer {token}"}
    _, SAT_CHECKS, _, _dbx_get = _get_cli_helpers()
    data, status, _ = await _dbx_get(client, host, "/api/2.0/clusters/list", token)
    if not data:
        return 0, []
    count = 0
    names: list[str] = []
    seen: set[str] = set()

    for cl in _get_list(data, "clusters"):
        for script in cl.get("init_scripts", []):
            # DBFS-hosted init scripts
            dbfs = script.get("dbfs", {})
            if dbfs and dbfs.get("destination"):
                path = dbfs["destination"]
                if path in seen:
                    continue
                seen.add(path)
                try:
                    resp = await client.get(
                        f"{host}/api/2.0/dbfs/read",
                        headers=hdr, params={"path": path}, timeout=15,
                    )
                    if resp.status_code == 200:
                        content_b64 = resp.json().get("data", "")
                        if content_b64:
                            decoded = base64.b64decode(content_b64).decode("utf-8", errors="replace")
                            safe = path.strip("/").replace("/", "__")
                            (target_dir / f"{safe}.sh").write_text(decoded, encoding="utf-8")
                            count += 1
                            names.append(path)
                except Exception:
                    pass

            # Workspace-hosted init scripts
            ws = script.get("workspace", {})
            if ws and ws.get("destination"):
                path = ws["destination"]
                if path in seen:
                    continue
                seen.add(path)
                try:
                    resp = await client.get(
                        f"{host}/api/2.0/workspace/export",
                        headers=hdr,
                        params={"path": path, "format": "SOURCE"},
                        timeout=15,
                    )
                    if resp.status_code == 200:
                        content_b64 = resp.json().get("content", "")
                        if content_b64:
                            decoded = base64.b64decode(content_b64).decode("utf-8", errors="replace")
                            safe = path.strip("/").replace("/", "__")
                            (target_dir / f"{safe}.sh").write_text(decoded, encoding="utf-8")
                            count += 1
                            names.append(path)
                except Exception:
                    pass

            # Volumes-hosted init scripts
            volumes = script.get("volumes", {})
            if volumes and volumes.get("destination"):
                # Volumes paths cannot be downloaded via REST API — skip
                pass

    return count, names


async def _fetch_dlt_configs(
    client: httpx.AsyncClient, host: str, token: str, target_dir: Path,
) -> tuple[int, list[str]]:
    """Fetch DLT pipeline configs (clusters, libraries, configuration map)."""
    hdr = {"Authorization": f"Bearer {token}"}
    _, SAT_CHECKS, _, _dbx_get = _get_cli_helpers()
    data, status, _ = await _dbx_get(
        client, host, "/api/2.0/pipelines", token, {"max_results": "100"},
    )
    if not data:
        return 0, []
    count = 0
    names: list[str] = []
    for pl in _get_list(data, "statuses"):
        pid = pl.get("pipeline_id", "unknown")
        try:
            resp = await client.get(
                f"{host}/api/2.0/pipelines/{pid}", headers=hdr, timeout=15,
            )
            if resp.status_code != 200:
                continue
            spec = resp.json().get("spec", {})
            pname = spec.get("name", pid)
            extracted = {
                "pipeline_name": pname,
                "clusters": spec.get("clusters", []),
                "libraries": spec.get("libraries", []),
                "configuration": spec.get("configuration", {}),
            }
            content = json.dumps(extracted, indent=2)
            (target_dir / f"pipeline_{pid}.json").write_text(content, encoding="utf-8")
            count += 1
            names.append(pname)
        except Exception:
            pass
    return count, names


async def _fetch_sql_warehouse_configs(
    client: httpx.AsyncClient, host: str, token: str, target_dir: Path,
) -> tuple[int, list[str]]:
    """Fetch SQL warehouse configurations."""
    _, SAT_CHECKS, _, _dbx_get = _get_cli_helpers()
    data, status, _ = await _dbx_get(client, host, "/api/2.0/sql/warehouses", token)
    if not data:
        return 0, []
    count = 0
    names: list[str] = []
    for wh in _get_list(data, "warehouses"):
        wid = wh.get("id", "unknown")
        wname = wh.get("name", wid)
        extracted = {
            "name": wname,
            "channel": wh.get("channel", {}),
            "tags": wh.get("tags", {}),
            "spot_instance_policy": wh.get("spot_instance_policy", ""),
            "warehouse_type": wh.get("warehouse_type", ""),
        }
        content = json.dumps(extracted, indent=2)
        (target_dir / f"warehouse_{wid}.json").write_text(content, encoding="utf-8")
        count += 1
        names.append(wname)
    return count, names


# ─────────────────────────────────────────────────────────────────────────────
# Scan a single target directory with TruffleHog
# ─────────────────────────────────────────────────────────────────────────────

def _scan_target(
    check_id: str, label: str, scan_dir: Path, item_count: int,
    config_path: Path, item_names: list[str] | None = None,
) -> Any:  # returns SATFinding
    """Run TruffleHog on one scan target and return a finding."""
    SATFinding, SAT_CHECKS, _make_finding, _ = _get_cli_helpers()

    # Look up the API endpoint for this check (for display in reports)
    from .cli import CHECK_API_ENDPOINTS
    api_ep = CHECK_API_ENDPOINTS.get(check_id, "")
    base_details: dict[str, Any] = {
        "api_endpoint": api_ep,
        "items_scanned": item_count,
        "items": item_names[:100] if item_names else [],  # cap at 100 for report size
    }

    if item_count == 0:
        return _make_finding(
            check_id, "NOT_APPLICABLE",
            f"No {label} content to scan.",
            details=base_details,
        )

    # Run built-in detectors
    results = _run_trufflehog(scan_dir)
    # Run custom Databricks token detectors
    custom_results = _run_trufflehog(scan_dir, config_path)

    all_parsed = _parse_trufflehog_results(results + custom_results)
    unique = _dedup_findings(all_parsed)

    # Separate errors from real findings
    errors = [u for u in unique if "_error" in u]
    secrets = [u for u in unique if "_error" not in u]

    if secrets:
        verified_count = sum(1 for s in secrets if s.get("verified"))
        detectors = sorted({s.get("detector_name", "unknown") for s in secrets})
        return _make_finding(
            check_id, "FAIL",
            f"{len(secrets)} secret(s) detected ({verified_count} verified). "
            f"Detector(s): {', '.join(detectors)}",
            details={
                **base_details,
                "secrets_found": len(secrets),
                "verified_count": verified_count,
                "detectors": detectors,
                "findings": secrets[:_MAX_FINDINGS_PER_CHECK],
            },
        )

    if errors:
        return _make_finding(
            check_id, "WARN",
            f"TruffleHog reported errors: {errors[0].get('_error', 'unknown')}",
            details={**base_details, "errors": [e.get("_error") for e in errors]},
        )

    return _make_finding(
        check_id, "PASS",
        f"Scanned {item_count} {label} item(s) — no secrets found.",
        details=base_details,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Main orchestrator
# ─────────────────────────────────────────────────────────────────────────────

async def run_secret_scan(
    client: httpx.AsyncClient,
    host: str,
    token: str,
    scan_secrets_days: int | None = None,
    quiet: bool = False,
) -> list:  # list[SATFinding]
    """Run TruffleHog-based secret scanning on all workspace content.

    Args:
        client: Shared httpx.AsyncClient.
        host: Databricks workspace URL.
        token: PAT or OAuth token.
        scan_secrets_days: Only scan notebooks modified in the last N days.
        quiet: Suppress progress output.

    Returns:
        list[SATFinding] with one finding per scan target.
    """
    SATFinding, SAT_CHECKS, _make_finding, _ = _get_cli_helpers()
    findings: list = []

    # ── 1. Check TruffleHog availability ─────────────────────────────────
    available, version = _check_trufflehog_available()
    if not available:
        if not quiet:
            print(f"\n  [Secret Scan] {version}")
            print(f"  [Secret Scan] Skipping secret scanning.\n")
        for cid in _SCAN_CHECK_IDS:
            findings.append(_make_finding(cid, "NOT_APPLICABLE", version))
        return findings

    if not quiet:
        print(f"\n  [Secret Scan] TruffleHog: {version}")
        print(f"  [Secret Scan] Extracting workspace content...")

    # ── 2. Create temp directory structure ────────────────────────────────
    with tempfile.TemporaryDirectory(prefix="sat_secrets_") as tmpdir:
        tmp = Path(tmpdir)

        config_path = tmp / "custom_detectors.yaml"
        config_path.write_text(_CUSTOM_DETECTORS_YAML, encoding="utf-8")

        dirs = {
            "notebooks":      tmp / "notebooks",
            "cluster_configs": tmp / "cluster_configs",
            "job_configs":     tmp / "job_configs",
            "init_global":     tmp / "init_scripts" / "global",
            "init_cluster":    tmp / "init_scripts" / "cluster",
            "dlt_configs":     tmp / "dlt_configs",
            "sql_wh_configs":  tmp / "sql_warehouse_configs",
        }
        for d in dirs.values():
            d.mkdir(parents=True, exist_ok=True)

        # ── 3. Fetch ALL content in parallel ─────────────────────────────
        # Notebooks: list → filter → export runs as one combined task,
        # concurrently with the other 6 fetchers.
        if not quiet:
            print(f"  [Secret Scan] Fetching content from 7 targets in parallel...")

        async def _notebooks_pipeline() -> tuple[int, list[str]]:
            """List, filter, and export notebooks — runs as a single task."""
            if not quiet:
                print(f"  [Secret Scan]   Listing workspace notebooks...")
            notebooks = await _list_workspace_recursive(
                client, host, token, quiet=quiet,
            )
            if scan_secrets_days is not None and scan_secrets_days > 0:
                cutoff_ms = int(
                    (datetime.now(timezone.utc) - timedelta(days=scan_secrets_days))
                    .timestamp() * 1000
                )
                notebooks = [
                    nb for nb in notebooks if nb.get("modified_at", 0) >= cutoff_ms
                ]
                if not quiet:
                    print(f"  [Secret Scan]   Filtered to {len(notebooks)} notebook(s) "
                          f"modified in last {scan_secrets_days} day(s)")
            if not quiet:
                print(f"  [Secret Scan]   Found {len(notebooks)} notebook(s), exporting...")
            nb_names = [nb.get("path", "unknown") for nb in notebooks]
            exported = await _export_notebooks_to_dir(
                client, host, token, notebooks, dirs["notebooks"], quiet,
            )
            return exported, nb_names

        fetch_results = await asyncio.gather(
            _notebooks_pipeline(),
            _fetch_cluster_configs(client, host, token, dirs["cluster_configs"]),
            _fetch_job_configs(client, host, token, dirs["job_configs"]),
            _fetch_global_init_scripts(client, host, token, dirs["init_global"]),
            _fetch_cluster_init_scripts(client, host, token, dirs["init_cluster"]),
            _fetch_dlt_configs(client, host, token, dirs["dlt_configs"]),
            _fetch_sql_warehouse_configs(client, host, token, dirs["sql_wh_configs"]),
            return_exceptions=True,
        )

        labels = [
            "notebooks", "cluster_configs", "job_configs",
            "init_global", "init_cluster", "dlt_configs", "sql_wh_configs",
        ]
        counts: dict[str, int] = {}
        item_names: dict[str, list[str]] = {}
        for i, label in enumerate(labels):
            val = fetch_results[i]
            if isinstance(val, tuple) and len(val) == 2:
                counts[label] = val[0] if isinstance(val[0], int) else 0
                item_names[label] = val[1] if isinstance(val[1], list) else []
            elif isinstance(val, int):
                counts[label] = val
                item_names[label] = []
            else:
                counts[label] = 0
                item_names[label] = []

        if not quiet:
            print(f"  [Secret Scan] Content fetched:")
            for label, count in counts.items():
                print(f"  [Secret Scan]   {label}: {count} item(s)")

        # ── 5. Run TruffleHog per target ─────────────────────────────────
        if not quiet:
            print(f"  [Secret Scan] Running TruffleHog scans...")

        check_map = {
            "notebooks":      "SAT-SCAN-NOTEBOOKS",
            "cluster_configs": "SAT-SCAN-CLUSTER-CONF",
            "job_configs":     "SAT-SCAN-JOB-CONF",
            "init_global":     "SAT-SCAN-INIT-GLOBAL",
            "init_cluster":    "SAT-SCAN-INIT-CLUSTER",
            "dlt_configs":     "SAT-SCAN-DLT",
            "sql_wh_configs":  "SAT-SCAN-SQL-WH",
        }

        for label, check_id in check_map.items():
            if not quiet:
                print(f"  [Secret Scan]   Scanning {label.replace('_', ' ')}...")
            finding = _scan_target(
                check_id, label.replace("_", " "),
                dirs[label], counts.get(label, 0),
                config_path, item_names.get(label, []),
            )
            # Inject workspace host so HTML renderer can build clickable links
            if not finding.details:
                finding.details = {}
            finding.details["workspace_host"] = host
            findings.append(finding)
            if not quiet:
                icon = {"PASS": "PASS", "FAIL": "FAIL", "WARN": "WARN"}.get(finding.status, "N/A")
                print(f"  [Secret Scan]   {check_id}: {icon}"
                      + (f" - {finding.current_state[:80]}" if finding.status == "FAIL" else ""))

    if not quiet:
        passed = sum(1 for f in findings if f.status == "PASS")
        failed = sum(1 for f in findings if f.status == "FAIL")
        print(f"\n  [Secret Scan] Done: {passed} pass, {failed} fail, "
              f"{len(findings) - passed - failed} other\n")

    return findings
