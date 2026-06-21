"""Scan lifecycle endpoints: trigger scan, poll status, and scheduler."""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, BackgroundTasks, Header, HTTPException, Query

from ._storage import (
    _STORE, _JOBS, _scan_dir, _latest_result, _grade, _SCHEDULE_PATH,
)
from .workspace import _collect_workspace_resources

router = APIRouter()


# ---------------------------------------------------------------------------
# Webhook helper
# ---------------------------------------------------------------------------


async def _fire_scan_webhooks(scan_id: str, meta: dict) -> None:
    """POST scan summary to all configured webhooks (non-blocking, never raises)."""
    try:
        wh_file = Path("config/webhooks.json")
        if not wh_file.exists():
            return
        webhooks = json.loads(wh_file.read_text())
        if not webhooks:
            return
        import httpx
        payload = {
            "event":      "scan_complete",
            "scan_id":    scan_id,
            "workspace":  meta.get("workspace_name") or meta.get("workspace_url", ""),
            "score":      meta.get("overall_score"),
            "grade":      meta.get("grade"),
            "passed":     meta.get("passed"),
            "failed":     meta.get("failed"),
            "scan_type":  meta.get("scan_type"),
            "scanned_at": meta.get("scanned_at"),
        }
        async with httpx.AsyncClient(timeout=10) as c:
            for wh in webhooks:
                try:
                    await c.post(
                        wh.get("url", ""), json=payload,
                        headers={"Content-Type": "application/json"},
                    )
                except Exception:
                    pass
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Background runners
# ---------------------------------------------------------------------------


async def _run_inventory_only(
    job_id: str,
    scan_id: str,
    host: str,
    token: str,
    workspace_name: str,
) -> None:
    """Run UC inventory scan only — no security checks."""
    try:
        _JOBS[job_id]["status"]   = "running"
        _JOBS[job_id]["progress"] = "Importing sat_scanner…"

        try:
            from sat_scanner.inventory import run_inventory
            from sat_scanner.exporters import export_inventory_hierarchy_html
        except ImportError as e:
            _JOBS[job_id]["status"] = "error"
            _JOBS[job_id]["error"]  = f"sat_scanner package not found: {e}."
            return

        _JOBS[job_id]["progress"] = "Scanning Unity Catalog objects…"
        inv = await run_inventory(
            host=host.rstrip("/"),
            token=token,
            workspace_name=workspace_name,
            quiet=True,
            grants="coarse",
        )

        out_dir  = _scan_dir(scan_id)
        inv_dict = inv.to_dict()

        _JOBS[job_id]["progress"] = "Generating visualisations…"
        (out_dir / "inventory.json").write_text(json.dumps(inv_dict, default=str, indent=2))
        export_inventory_hierarchy_html(inv, out_dir)

        _JOBS[job_id]["progress"] = "Collecting workspace resources…"
        try:
            ws_resources = await _collect_workspace_resources(host, token)
            (out_dir / "workspace_resources.json").write_text(
                json.dumps(ws_resources, default=str, indent=2)
            )
        except Exception:
            pass

        now  = datetime.now(timezone.utc).isoformat()
        meta = {
            "scan_id":        scan_id,
            "workspace_url":  host,
            "workspace_name": workspace_name,
            "scanned_at":     inv_dict.get("scanned_at", now),
            "overall_score":  None,
            "grade":          None,
            "total_checks":   0,
            "passed":         0,
            "failed":         0,
            "warnings":       0,
            "not_applicable": 0,
            "scan_type":      "inventory",
            "with_inventory": True,
            "catalog_count":  inv_dict.get("catalog_count", 0),
            "schema_count":   inv_dict.get("schema_count", 0),
            "table_count":    inv_dict.get("table_count", 0),
        }
        (out_dir / "meta.json").write_text(json.dumps(meta, indent=2))

        await _fire_scan_webhooks(scan_id, meta)
        _JOBS[job_id]["status"]    = "completed"
        _JOBS[job_id]["progress"]  = "Done"
        _JOBS[job_id]["result_id"] = scan_id

    except Exception as exc:
        _JOBS[job_id]["status"] = "error"
        _JOBS[job_id]["error"]  = str(exc)


async def _run_scan_task(
    job_id: str,
    scan_id: str,
    host: str,
    token: str,
    workspace_name: str,
    scan_type: str,
) -> None:
    """Run sat_scanner scan (and optionally UC inventory) then persist results."""
    if scan_type == "inventory":
        await _run_inventory_only(job_id, scan_id, host, token, workspace_name)
        return

    with_inventory = scan_type == "full"

    try:
        _JOBS[job_id]["status"]   = "running"
        _JOBS[job_id]["progress"] = "Importing sat_scanner…"

        try:
            from sat_scanner.scanner  import run_scan
            from sat_scanner.exporters import export_json, export_html, export_inventory_hierarchy_html
            from sat_scanner.models   import SATScanResult
        except ImportError as e:
            _JOBS[job_id]["status"] = "error"
            _JOBS[job_id]["error"]  = f"sat_scanner package not found: {e}."
            return

        _JOBS[job_id]["progress"] = "Running security scan (345 checks)…"
        result: SATScanResult = await run_scan(
            host=host.rstrip("/"),
            token=token,
            workspace_name=workspace_name,
            quiet=True,
        )

        out_dir = _scan_dir(scan_id)

        _JOBS[job_id]["progress"] = "Saving results…"
        result_dict = result.to_dict()
        (out_dir / "result.json").write_text(json.dumps(result_dict, default=str, indent=2))

        try:
            export_html(result, out_dir)
        except Exception:
            pass

        _JOBS[job_id]["progress"] = "Collecting workspace resources…"
        ws_task = asyncio.create_task(_collect_workspace_resources(host, token))

        if with_inventory:
            _JOBS[job_id]["progress"] = "Running UC inventory scan…"
            try:
                from sat_scanner.inventory import run_inventory
                inv = await run_inventory(
                    host=host.rstrip("/"),
                    token=token,
                    workspace_name=workspace_name,
                    quiet=True,
                    grants="coarse",
                )
                (out_dir / "inventory.json").write_text(
                    json.dumps(inv.to_dict(), default=str, indent=2)
                )
                export_inventory_hierarchy_html(inv, out_dir)
            except Exception as inv_err:
                _JOBS[job_id]["inventory_error"] = str(inv_err)

        try:
            ws_resources = await ws_task
            (out_dir / "workspace_resources.json").write_text(
                json.dumps(ws_resources, default=str, indent=2)
            )
        except Exception:
            pass

        overall = result_dict.get("overall_score", 0)
        meta = {
            "scan_id":        scan_id,
            "workspace_url":  host,
            "workspace_name": workspace_name,
            "scanned_at":     result_dict.get("scanned_at", datetime.now(timezone.utc).isoformat()),
            "overall_score":  overall,
            "grade":          _grade(overall),
            "total_checks":   result_dict.get("total_checks", 0),
            "passed":         result_dict.get("passed", 0),
            "failed":         result_dict.get("failed", 0),
            "warnings":       result_dict.get("warnings", 0),
            "not_applicable": result_dict.get("not_applicable", 0),
            "scan_type":      scan_type,
            "with_inventory": with_inventory,
        }
        (out_dir / "meta.json").write_text(json.dumps(meta, indent=2))

        await _fire_scan_webhooks(scan_id, meta)
        _JOBS[job_id]["status"]    = "completed"
        _JOBS[job_id]["progress"]  = "Done"
        _JOBS[job_id]["result_id"] = scan_id

    except Exception as exc:
        _JOBS[job_id]["status"] = "error"
        _JOBS[job_id]["error"]  = str(exc)


# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------


async def start_scan_scheduler() -> None:
    """Check every 60 s whether a scheduled scan is due and trigger it."""
    from datetime import timedelta
    while True:
        await asyncio.sleep(60)
        try:
            if not _SCHEDULE_PATH.exists():
                continue
            cfg = json.loads(_SCHEDULE_PATH.read_text())
            if not cfg.get("enabled"):
                continue
            host  = cfg.get("host", "")
            token = cfg.get("token", "")
            if not host or not token:
                continue
            next_run_str = cfg.get("next_run")
            if next_run_str:
                next_run = datetime.fromisoformat(next_run_str)
                if next_run.tzinfo is None:
                    next_run = next_run.replace(tzinfo=timezone.utc)
                if next_run > datetime.now(timezone.utc):
                    continue
            job_id  = str(uuid.uuid4())
            scan_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S") + "_" + job_id[:8]
            _JOBS[job_id] = {
                "job_id":        job_id,
                "scan_id":       scan_id,
                "scan_type":     cfg.get("scan_type", "full"),
                "status":        "queued",
                "progress":      "Scheduled scan queued…",
                "submitted_at":  datetime.now(timezone.utc).isoformat(),
                "result_id":     None,
                "error":         None,
            }
            asyncio.create_task(
                _run_scan_task(
                    job_id, scan_id,
                    host, token,
                    cfg.get("workspace_name", ""),
                    cfg.get("scan_type", "full"),
                )
            )
            freq  = cfg.get("frequency", "daily")
            delta = timedelta(days=1 if freq == "daily" else 7)
            cfg["next_run"]        = (datetime.now(timezone.utc) + delta).isoformat()
            cfg["last_triggered"]  = datetime.now(timezone.utc).isoformat()
            _SCHEDULE_PATH.write_text(json.dumps(cfg, indent=2))
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.post("/run")
async def run_assessment(
    background_tasks: BackgroundTasks,
    x_databricks_host: str | None = Header(None),
    x_databricks_token: str | None = Header(None),
    workspace_name: str = Query(""),
    scan_type: str = Query("full"),
):
    """Trigger an async assessment scan. Returns job_id to poll.

    scan_type:
      - full       — 345 security checks + UC inventory (default)
      - security   — 345 security checks only
      - inventory  — UC inventory only (no security checks)
    """
    host  = x_databricks_host  or ""
    token = x_databricks_token or ""
    if not host or not token:
        raise HTTPException(status_code=401, detail="Databricks host and token required")
    if scan_type not in ("full", "security", "inventory"):
        raise HTTPException(status_code=400, detail="scan_type must be full, security, or inventory")

    job_id  = str(uuid.uuid4())
    scan_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S") + "_" + job_id[:8]
    _JOBS[job_id] = {
        "job_id":       job_id,
        "scan_id":      scan_id,
        "scan_type":    scan_type,
        "status":       "queued",
        "progress":     "Queued…",
        "submitted_at": datetime.now(timezone.utc).isoformat(),
        "result_id":    None,
        "error":        None,
    }
    background_tasks.add_task(
        _run_scan_task, job_id, scan_id, host, token, workspace_name, scan_type
    )
    return {"job_id": job_id, "scan_id": scan_id, "scan_type": scan_type, "status": "queued"}


@router.get("/status/{job_id}")
async def get_job_status(job_id: str):
    """Poll scan job status."""
    job = _JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


@router.get("/schedule")
async def get_schedule():
    """Return current scan schedule config."""
    if _SCHEDULE_PATH.exists():
        try:
            return json.loads(_SCHEDULE_PATH.read_text())
        except Exception:
            pass
    return {"enabled": False}


@router.put("/schedule")
async def update_schedule(body: dict):
    """Save scan schedule. body: {enabled, frequency, hour, host, token, scan_type, workspace_name}"""
    _SCHEDULE_PATH.write_text(json.dumps(body, indent=2))
    return body


@router.delete("/schedule")
async def delete_schedule():
    """Delete scan schedule."""
    if _SCHEDULE_PATH.exists():
        _SCHEDULE_PATH.unlink()
    return {"enabled": False}
