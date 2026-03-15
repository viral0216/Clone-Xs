"""Job manager — runs clone operations in background threads with log capture."""

import asyncio
import io
import logging
import re
import sys
import threading
import uuid
from datetime import datetime

from api.websocket.manager import ConnectionManager

logger = logging.getLogger(__name__)


class JobLogHandler(logging.Handler):
    """Captures log messages into a list for a specific job."""

    def __init__(self, job_logs: list, job_dict: dict | None = None, workspace_host: str = "", max_lines: int = 500):
        super().__init__()
        self.job_logs = job_logs
        self.job_dict = job_dict
        self.workspace_host = workspace_host.rstrip("/")
        self.max_lines = max_lines
        self._run_id_re = re.compile(r"run_id[=:]?\s*(\d+)")

    def emit(self, record):
        try:
            msg = self.format(record)
            self.job_logs.append(msg)
            # Keep only the last N lines
            if len(self.job_logs) > self.max_lines:
                del self.job_logs[: len(self.job_logs) - self.max_lines]
            # Detect Databricks run_id in log messages and set run_url early
            if self.job_dict and not self.job_dict.get("run_url") and self.workspace_host:
                m = self._run_id_re.search(msg)
                if m:
                    self.job_dict["run_url"] = f"{self.workspace_host}/#job/{m.group(1)}"
        except Exception:
            pass


class StderrCapture:
    """Captures stderr writes (progress bars) into a job's log list."""

    def __init__(self, original, job_logs: list, max_lines: int = 500):
        self.original = original
        self.job_logs = job_logs
        self.max_lines = max_lines
        self._ansi_re = re.compile(r"\x1b\[[0-9;]*[a-zA-Z]")

    def write(self, text):
        self.original.write(text)
        # Clean up progress bar output
        cleaned = self._ansi_re.sub("", text).strip()
        if not cleaned or cleaned.startswith("\r"):
            return
        # Deduplicate progress bar lines — find and replace the last one with same prefix
        progress_prefixes = ("Scanning", "Cloning", "Validating", "Schemas")
        for prefix in progress_prefixes:
            if cleaned.startswith(prefix):
                # Replace the last line with same prefix (search backwards)
                for i in range(len(self.job_logs) - 1, max(len(self.job_logs) - 10, -1), -1):
                    if i >= 0 and self.job_logs[i].startswith(prefix):
                        self.job_logs[i] = cleaned
                        return
                break
        self.job_logs.append(cleaned)
        if len(self.job_logs) > self.max_lines:
            del self.job_logs[: len(self.job_logs) - self.max_lines]

    def flush(self):
        self.original.flush()

    def fileno(self):
        return self.original.fileno()

    def isatty(self):
        return False


class JobManager:
    """Manages background clone/analysis jobs with log capture."""

    def __init__(self, max_concurrent: int = 2):
        self.max_concurrent = max_concurrent
        self.jobs: dict[str, dict] = {}
        self.connection_manager = ConnectionManager()
        self._executor_semaphore = threading.Semaphore(max_concurrent)

    async def submit_job(self, job_type: str, config: dict, client) -> str:
        """Submit a job for background execution. Returns job_id."""
        job_id = str(uuid.uuid4())[:8]
        now = datetime.now().isoformat()

        self.jobs[job_id] = {
            "job_id": job_id,
            "status": "queued",
            "job_type": job_type,
            "source_catalog": config.get("source_catalog"),
            "destination_catalog": config.get("destination_catalog"),
            "clone_type": config.get("clone_type"),
            "progress": None,
            "result": None,
            "error": None,
            "run_url": None,
            "logs": [],
            "created_at": now,
            "started_at": None,
            "completed_at": None,
        }

        # Run in background thread
        loop = asyncio.get_event_loop()
        threading.Thread(
            target=self._run_job,
            args=(job_id, job_type, config, client, loop),
            daemon=True,
        ).start()

        return job_id

    def _run_job(self, job_id: str, job_type: str, config: dict, client, loop):
        """Execute the job in a background thread with log capture."""
        self._executor_semaphore.acquire()

        job_logs = self.jobs[job_id]["logs"]

        # Capture workspace host for building Databricks run URLs
        workspace_host = getattr(getattr(client, "config", None), "host", None) or ""

        # Set up log capture for src.* loggers
        log_handler = JobLogHandler(job_logs, job_dict=self.jobs[job_id], workspace_host=workspace_host)
        log_handler.setFormatter(logging.Formatter("%(levelname)s %(name)s: %(message)s"))
        src_logger = logging.getLogger("src")
        src_logger.addHandler(log_handler)

        # Capture stderr (progress bars)
        original_stderr = sys.stderr
        sys.stderr = StderrCapture(original_stderr, job_logs)

        try:
            self.jobs[job_id]["status"] = "running"
            self.jobs[job_id]["started_at"] = datetime.now().isoformat()
            job_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] Job {job_id} started — {job_type}")
            self._broadcast_sync(loop, job_id, {"type": "status", "status": "running"})

            # Mark as API-managed so clone_catalog doesn't double-save run logs
            config["_api_managed_logs"] = True

            # Log operation start to audit trail (clone_operations table)
            audit_start_time = datetime.utcnow()
            try:
                from src.audit_trail import log_operation_start
                log_operation_start(client, config.get("sql_warehouse_id", ""),
                                    config, job_id, operation_type=job_type)
            except Exception:
                pass

            if job_type == "clone":
                if config.get("serverless") and config.get("volume"):
                    from src.serverless import submit_clone_job
                    result = submit_clone_job(
                        client,
                        config,
                        volume_path=config["volume"],
                    )
                else:
                    from src.clone_catalog import clone_catalog
                    result = clone_catalog(client, config)
            elif job_type == "validate":
                from src.validation import validate_catalog
                result = validate_catalog(
                    client, config["sql_warehouse_id"],
                    config["source_catalog"], config["destination_catalog"],
                    config.get("exclude_schemas", []), config.get("max_workers", 4),
                    _api_managed_logs=True,
                )
            elif job_type == "sync":
                from src.sync_catalog import sync_catalogs
                result = sync_catalogs(
                    client, config["sql_warehouse_id"],
                    config["source_catalog"], config["destination_catalog"],
                    config.get("exclude_schemas", ["information_schema", "default"]),
                    dry_run=config.get("dry_run", True),
                    drop_extra=config.get("drop_extra", False),
                    _api_managed_logs=True,
                )
            elif job_type == "incremental_sync":
                from src.incremental_sync import get_tables_needing_sync, sync_changed_table
                schema = config["schema_name"]
                tables = get_tables_needing_sync(
                    client, config["sql_warehouse_id"],
                    config["source_catalog"], config["destination_catalog"], schema,
                )
                synced, failed = 0, 0
                for t in tables:
                    ok = sync_changed_table(
                        client, config["sql_warehouse_id"],
                        config["source_catalog"], config["destination_catalog"],
                        schema, t["table_name"],
                        clone_type=config.get("clone_type", "DEEP"),
                        dry_run=config.get("dry_run", False),
                    )
                    if ok:
                        synced += 1
                    else:
                        failed += 1
                result = {
                    "schema": schema, "tables_checked": len(tables),
                    "synced": synced, "failed": failed,
                }
            elif job_type == "pii-scan":
                from src.pii_detection import scan_catalog_for_pii
                result = scan_catalog_for_pii(
                    client, config["sql_warehouse_id"],
                    config["source_catalog"],
                    config.get("exclude_schemas", ["information_schema", "default"]),
                    max_workers=config.get("max_workers", 4),
                )
            elif job_type == "preflight":
                from src.preflight import run_preflight
                result = run_preflight(
                    client, config["sql_warehouse_id"],
                    config["source_catalog"], config["destination_catalog"],
                    check_write=config.get("check_write", True),
                )
            elif job_type == "terraform":
                from src.terraform import generate_terraform, generate_pulumi
                fmt = config.get("format", "terraform")
                catalog = config["source_catalog"]
                default_path = f"{catalog}_pulumi.py" if fmt == "pulumi" else f"{catalog}.tf.json"
                out_path = config.get("output_path") or default_path
                if fmt == "pulumi":
                    output = generate_pulumi(
                        client, config["sql_warehouse_id"],
                        catalog,
                        config.get("exclude_schemas", ["information_schema", "default"]),
                        output_path=out_path,
                    )
                else:
                    output = generate_terraform(
                        client, config["sql_warehouse_id"],
                        catalog,
                        config.get("exclude_schemas", ["information_schema", "default"]),
                        output_path=out_path,
                    )
                # Read generated file content
                content = ""
                try:
                    with open(output) as f:
                        content = f.read()
                except Exception:
                    pass
                result = {"output_path": output, "content": content, "format": fmt}
            else:
                result = {"error": f"Unknown job type: {job_type}"}

            # Build Databricks job run URL if a run_id is available
            run_id = result.get("run_id") if isinstance(result, dict) else None
            if run_id and workspace_host:
                host = workspace_host.rstrip("/")
                self.jobs[job_id]["run_url"] = f"{host}/#job/{run_id}"

            self.jobs[job_id]["status"] = "completed"
            self.jobs[job_id]["result"] = result
            self.jobs[job_id]["completed_at"] = datetime.now().isoformat()
            job_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] Job {job_id} completed successfully")
            self._broadcast_sync(loop, job_id, {"type": "completed", "result": result})

        except Exception as e:
            logger.error(f"Job {job_id} failed: {e}")
            self.jobs[job_id]["status"] = "failed"
            self.jobs[job_id]["error"] = str(e)
            self.jobs[job_id]["completed_at"] = datetime.now().isoformat()
            job_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] ERROR: {e}")
            self._broadcast_sync(loop, job_id, {"type": "error", "error": str(e)})
        finally:
            # Restore stderr and remove log handler
            sys.stderr = original_stderr
            src_logger.removeHandler(log_handler)
            self._executor_semaphore.release()

            # Persist run logs to Unity Catalog Delta table
            try:
                from src.run_logs import save_run_log
                save_run_log(client, config.get("sql_warehouse_id", ""), self.jobs[job_id], config)
            except Exception as log_err:
                logger.debug(f"Could not persist run log to Delta: {log_err}")

            # Log operation completion to audit trail (clone_operations table)
            try:
                from src.audit_trail import log_operation_complete
                job_data = self.jobs[job_id]
                summary = job_data.get("result") or {}
                log_operation_complete(
                    client, config.get("sql_warehouse_id", ""), config,
                    job_id, summary, audit_start_time,
                    error_message=job_data.get("error"),
                )
            except Exception as audit_err:
                logger.debug(f"Could not persist audit trail to Delta: {audit_err}")

            # Save operation metrics to clone_metrics table
            try:
                from src.metrics import save_operation_metrics
                save_operation_metrics(client, config.get("sql_warehouse_id", ""),
                                       self.jobs[job_id], config)
            except Exception as metrics_err:
                logger.debug(f"Could not persist metrics to Delta: {metrics_err}")

    def _broadcast_sync(self, loop, job_id: str, data: dict):
        """Thread-safe broadcast to WebSocket clients."""
        try:
            asyncio.run_coroutine_threadsafe(
                self.connection_manager.broadcast(job_id, data), loop
            )
        except Exception:
            pass

    def list_jobs(self) -> list[dict]:
        """List all jobs, most recent first."""
        return sorted(self.jobs.values(), key=lambda j: j.get("created_at", ""), reverse=True)

    def get_job(self, job_id: str) -> dict | None:
        return self.jobs.get(job_id)

    def cancel_job(self, job_id: str):
        job = self.jobs.get(job_id)
        if job and job["status"] == "queued":
            job["status"] = "cancelled"

    async def shutdown(self):
        """Cleanup on app shutdown."""
        logger.info("JobManager shutting down")
