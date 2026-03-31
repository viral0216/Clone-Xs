"""Job manager — runs clone operations in background threads with log capture."""

import asyncio
import logging
import re
import sys
import threading
import uuid
from datetime import datetime, timezone

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

    _config_noise_re = re.compile(r"\. Config: .*$")

    def emit(self, record):
        try:
            msg = self.format(record)
            # Strip verbose SDK config/env details from error messages
            msg = self._config_noise_re.sub("", msg)
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
        # Ensure INFO level is captured (default root may be WARNING)
        previous_log_level = src_logger.level
        if src_logger.level > logging.INFO or src_logger.level == logging.NOTSET:
            src_logger.setLevel(logging.INFO)

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
            audit_start_time = datetime.now(timezone.utc)
            try:
                from src.audit_trail import ensure_audit_table, log_operation_start
                ensure_audit_table(client, config.get("sql_warehouse_id", ""), config)
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
                if config.get("serverless") and config.get("volume"):
                    # Serverless: build a clone config and submit via serverless
                    from src.serverless import submit_clone_job
                    clone_config = {
                        "source_catalog": config["source_catalog"],
                        "destination_catalog": config["destination_catalog"],
                        "clone_type": config.get("clone_type", "DEEP"),
                        "include_schemas": [config["schema_name"]],
                        "exclude_schemas": [],
                        "sql_warehouse_id": config.get("sql_warehouse_id", ""),
                        "max_workers": config.get("max_workers", 4),
                        "copy_permissions": False,
                        "copy_ownership": False,
                        "copy_tags": False,
                        "enable_rollback": False,
                    }
                    result = submit_clone_job(client, clone_config, volume_path=config["volume"])
                elif config.get("serverless"):
                    # Spark Connect: run locally via databricks-connect serverless
                    from src.client import spark_connect_executor
                    from src.incremental_sync import get_tables_needing_sync, sync_changed_table
                    schema = config["schema_name"]
                    with spark_connect_executor():
                        tables = get_tables_needing_sync(
                            client, "SPARK_CONNECT",
                            config["source_catalog"], config["destination_catalog"], schema,
                        )
                        synced, failed = 0, 0
                        for t in tables:
                            ok = sync_changed_table(
                                client, "SPARK_CONNECT",
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
                        "mode": "spark_connect",
                    }
                else:
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
            elif job_type == "demo-data":
                from src.demo_generator import generate_demo_catalog
                # Use the job dict's "progress" key for live progress updates
                self.jobs[job_id]["progress"] = {}
                result = generate_demo_catalog(
                    client, config["sql_warehouse_id"],
                    config["catalog_name"],
                    industries=config.get("industries"),
                    owner=config.get("owner"),
                    scale_factor=config.get("scale_factor", 1.0),
                    batch_size=config.get("batch_size", 5_000_000),
                    max_workers=config.get("max_workers", 4),
                    storage_location=config.get("storage_location"),
                    drop_existing=config.get("drop_existing", False),
                    medallion=config.get("medallion", True),
                    uc_best_practices=config.get("uc_best_practices", True),
                    create_functions=config.get("create_functions", True),
                    create_volumes=config.get("create_volumes", True),
                    start_date=config.get("start_date", "2020-01-01"),
                    end_date=config.get("end_date", "2025-01-01"),
                    progress_dict=self.jobs[job_id]["progress"],
                )
            elif job_type == "reconciliation-batch":
                tables_list = config.get("tables", [])
                use_spark = config.get("use_spark", False)
                use_checksum = config.get("use_checksum", False)
                wid = config.get("sql_warehouse_id", "")
                total = len(tables_list)
                all_details = []

                self.jobs[job_id]["progress"] = {
                    "total": total, "completed": 0,
                    "current_table": "", "results": [],
                }

                for idx, tbl in enumerate(tables_list):
                    schema_name = tbl.get("schema_name", "")
                    table_name = tbl.get("table_name", "")
                    fqn = f"{schema_name}.{table_name}"

                    self.jobs[job_id]["progress"]["current_table"] = fqn
                    self.jobs[job_id]["progress"]["completed"] = idx
                    job_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] [{idx+1}/{total}] Validating {fqn}...")

                    self._broadcast_sync(loop, job_id, {
                        "type": "progress",
                        "completed": idx,
                        "total": total,
                        "current_table": fqn,
                    })

                    try:
                        if use_spark:
                            from src.reconciliation_spark import validate_table_spark
                            detail = validate_table_spark(
                                config["source_catalog"], config["destination_catalog"],
                                schema_name, table_name, use_checksum,
                            )
                        else:
                            from src.validation import validate_table
                            detail = validate_table(
                                client, wid,
                                config["source_catalog"], config["destination_catalog"],
                                schema_name, table_name, use_checksum,
                            )
                        status = "OK" if detail.get("match") else "MISMATCH"
                        job_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}]   {status} {fqn}: src={detail.get('source_count', '?')} dst={detail.get('dest_count', '?')}")
                        all_details.append(detail)
                    except Exception as e:
                        job_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}]   ERROR {fqn}: {e}")
                        all_details.append({
                            "schema": schema_name, "table": table_name,
                            "source_count": None, "dest_count": None,
                            "match": False, "error": str(e),
                        })

                    self._broadcast_sync(loop, job_id, {
                        "type": "table_result",
                        "index": idx + 1,
                        "total": total,
                        "table_fqn": fqn,
                        "detail": all_details[-1],
                    })

                matched = sum(1 for d in all_details if d.get("match"))
                errors = sum(1 for d in all_details if d.get("error"))
                result = {
                    "total_tables": total,
                    "matched": matched,
                    "mismatched": total - matched - errors,
                    "errors": errors,
                    "details": all_details,
                }

                # Store in Delta
                try:
                    from src.reconciliation_store import store_reconciliation_result
                    started = self.jobs[job_id].get("started_at", "")
                    duration = (datetime.now() - datetime.fromisoformat(started)).total_seconds() if started else 0
                    run_id = store_reconciliation_result(
                        client, wid, config, result,
                        run_type="row-level-batch",
                        source_catalog=config.get("source_catalog", ""),
                        destination_catalog=config.get("destination_catalog", ""),
                        execution_mode="spark" if use_spark else "sql",
                        use_checksum=use_checksum,
                        max_workers=config.get("max_workers", 4),
                        duration_seconds=duration,
                    )
                    result["run_id"] = run_id
                except Exception as store_err:
                    logger.debug(f"Could not store reconciliation result: {store_err}")

                # Evaluate alert rules
                try:
                    from src.reconciliation_alerts import evaluate_alerts
                    evaluate_alerts(client, wid, config,
                        run_id=result.get("run_id", ""), result=result,
                        source_catalog=config.get("source_catalog", ""),
                        destination_catalog=config.get("destination_catalog", ""),
                    )
                except Exception:
                    pass

                # Auto-evaluate SLAs after reconciliation batch
                try:
                    from src.sla_monitor import check_sla
                    sla_results = check_sla(client, wid, config)
                    sla_failed = sum(1 for s in sla_results if not s.get("passed"))
                    if sla_failed:
                        job_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] SLA check: {sla_failed} SLA(s) failing")
                    else:
                        job_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] SLA check: all SLAs passing")
                except Exception as sla_err:
                    logger.debug(f"Post-reconciliation SLA check failed: {sla_err}")

                self.jobs[job_id]["progress"]["completed"] = total
                self.jobs[job_id]["progress"]["current_table"] = ""
                job_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] Batch complete: {matched}/{total} matched, {errors} errors")

            elif job_type == "reconciliation-batch-compare":
                tables_list = config.get("tables", [])
                use_spark = config.get("use_spark", False)
                use_checksum = config.get("use_checksum", False)
                max_workers = config.get("max_workers", 4)
                wid = config.get("sql_warehouse_id", "")
                total = len(tables_list)
                all_details = []

                self.jobs[job_id]["progress"] = {
                    "total": total, "completed": 0,
                    "current_table": "", "results": [],
                }

                for idx, tbl in enumerate(tables_list):
                    schema_name = tbl.get("schema_name", "")
                    table_name = tbl.get("table_name", "")
                    fqn = f"{schema_name}.{table_name}"

                    self.jobs[job_id]["progress"]["current_table"] = fqn
                    self.jobs[job_id]["progress"]["completed"] = idx
                    job_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] [{idx+1}/{total}] Comparing {fqn}...")

                    self._broadcast_sync(loop, job_id, {
                        "type": "progress",
                        "completed": idx,
                        "total": total,
                        "current_table": fqn,
                    })

                    try:
                        if use_spark:
                            from src.reconciliation_spark import compare_table_spark
                            detail = compare_table_spark(
                                config["source_catalog"], config["destination_catalog"],
                                schema_name, table_name, use_checksum,
                            )
                        else:
                            from src.compare import compare_table_deep
                            detail = compare_table_deep(
                                client, wid,
                                config["source_catalog"], config["destination_catalog"],
                                schema_name, table_name, use_checksum,
                            )
                        has_issues = bool(detail.get("issues"))
                        status = "ISSUES" if has_issues else "OK"
                        job_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}]   {status} {fqn}")
                        all_details.append(detail)
                    except Exception as e:
                        job_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}]   ERROR {fqn}: {e}")
                        all_details.append({
                            "schema": schema_name, "table": table_name,
                            "issues": [str(e)],
                        })

                    self._broadcast_sync(loop, job_id, {
                        "type": "table_result",
                        "index": idx + 1,
                        "total": total,
                        "table_fqn": fqn,
                        "detail": all_details[-1],
                    })

                tables_ok = sum(1 for d in all_details if not d.get("issues"))
                result = {
                    "total_tables": total,
                    "tables_ok": tables_ok,
                    "tables_with_issues": total - tables_ok,
                    "details": all_details,
                }

                try:
                    from src.reconciliation_store import store_reconciliation_result
                    started = self.jobs[job_id].get("started_at", "")
                    duration = (datetime.now() - datetime.fromisoformat(started)).total_seconds() if started else 0
                    adapted = {
                        "total_tables": total,
                        "matched": tables_ok,
                        "mismatched": total - tables_ok,
                        "errors": 0,
                        "details": [
                            {
                                "schema": d.get("schema", ""),
                                "table": d.get("table", ""),
                                "source_count": d.get("source_rows"),
                                "dest_count": d.get("dest_rows"),
                                "match": not d.get("issues"),
                                "checksum_match": d.get("checksum_match"),
                                "error": "; ".join(d.get("issues", [])) if d.get("issues") else None,
                            }
                            for d in all_details
                        ],
                    }
                    run_id = store_reconciliation_result(
                        client, wid, config, adapted,
                        run_type="column-level-batch",
                        source_catalog=config.get("source_catalog", ""),
                        destination_catalog=config.get("destination_catalog", ""),
                        execution_mode="spark" if use_spark else "sql",
                        use_checksum=use_checksum,
                        max_workers=max_workers,
                        duration_seconds=duration,
                    )
                    result["run_id"] = run_id
                except Exception as store_err:
                    logger.debug(f"Could not store batch compare result: {store_err}")

                self.jobs[job_id]["progress"]["completed"] = total
                self.jobs[job_id]["progress"]["current_table"] = ""
                job_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] Batch compare complete: {tables_ok}/{total} OK")

            elif job_type == "reconciliation-batch-deep":
                tables_list = config.get("tables", [])
                use_checksum = config.get("use_checksum", False)
                max_workers = config.get("max_workers", 4)
                wid = config.get("sql_warehouse_id", "")
                comparison_options = config.get("comparison_options", {})
                key_columns = config.get("key_columns", []) or None
                include_columns = config.get("include_columns", []) or None
                ignore_columns = config.get("ignore_columns", []) or None
                sample_diffs = config.get("sample_diffs", 10)
                total = len(tables_list)
                all_details = []

                self.jobs[job_id]["progress"] = {
                    "total": total, "completed": 0,
                    "current_table": "", "results": [],
                }

                for idx, tbl in enumerate(tables_list):
                    schema_name = tbl.get("schema_name", "")
                    table_name = tbl.get("table_name", "")
                    fqn = f"{schema_name}.{table_name}"

                    self.jobs[job_id]["progress"]["current_table"] = fqn
                    self.jobs[job_id]["progress"]["completed"] = idx
                    job_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] [{idx+1}/{total}] Deep reconciling {fqn}...")

                    self._broadcast_sync(loop, job_id, {
                        "type": "progress",
                        "completed": idx,
                        "total": total,
                        "current_table": fqn,
                    })

                    try:
                        from src.reconciliation_deep import deep_reconcile_table
                        detail = deep_reconcile_table(
                            config["source_catalog"], config["destination_catalog"],
                            schema_name, table_name, key_columns,
                            include_columns, ignore_columns, sample_diffs,
                            use_checksum=use_checksum,
                            comparison_options=comparison_options,
                        )
                        matched = detail.get("matched_rows", 0)
                        missing = detail.get("missing_in_dest", 0)
                        extra = detail.get("extra_in_dest", 0)
                        modified = detail.get("modified_rows", 0)
                        job_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}]   {fqn}: matched={matched} missing={missing} extra={extra} modified={modified}")
                        all_details.append(detail)
                    except Exception as e:
                        job_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}]   ERROR {fqn}: {e}")
                        all_details.append({
                            "schema": schema_name, "table": table_name,
                            "source_count": 0, "dest_count": 0,
                            "matched_rows": 0, "missing_in_dest": 0,
                            "extra_in_dest": 0, "modified_rows": 0,
                            "error": str(e),
                        })

                    self._broadcast_sync(loop, job_id, {
                        "type": "table_result",
                        "index": idx + 1,
                        "total": total,
                        "table_fqn": fqn,
                        "detail": all_details[-1],
                    })

                total_matched = sum(d.get("matched_rows", 0) for d in all_details)
                total_missing = sum(d.get("missing_in_dest", 0) for d in all_details)
                total_extra = sum(d.get("extra_in_dest", 0) for d in all_details)
                total_modified = sum(d.get("modified_rows", 0) for d in all_details)
                total_source = sum(d.get("source_count", 0) for d in all_details)
                errors = sum(1 for d in all_details if d.get("error"))

                result = {
                    "total_tables": total,
                    "source_rows": total_source,
                    "matched_rows": total_matched,
                    "missing_in_dest": total_missing,
                    "extra_in_dest": total_extra,
                    "modified_rows": total_modified,
                    "errors": errors,
                    "match_rate_pct": round((total_matched / max(total_source, 1)) * 100, 2),
                    "details": all_details,
                }

                try:
                    from src.reconciliation_store import store_reconciliation_result
                    started = self.jobs[job_id].get("started_at", "")
                    duration = (datetime.now() - datetime.fromisoformat(started)).total_seconds() if started else 0
                    adapted = {
                        "total_tables": total,
                        "matched": total_matched,
                        "mismatched": total_missing + total_extra + total_modified,
                        "errors": errors,
                        "details": [
                            {
                                "schema": d.get("schema", ""),
                                "table": d.get("table", ""),
                                "source_count": d.get("source_count"),
                                "dest_count": d.get("dest_count"),
                                "match": d.get("missing_in_dest", 0) == 0 and d.get("extra_in_dest", 0) == 0 and d.get("modified_rows", 0) == 0,
                                "checksum_match": d.get("checksum_match"),
                                "error": d.get("error"),
                            }
                            for d in all_details
                        ],
                    }
                    run_id = store_reconciliation_result(
                        client, wid, config, adapted,
                        run_type="deep-batch",
                        source_catalog=config.get("source_catalog", ""),
                        destination_catalog=config.get("destination_catalog", ""),
                        execution_mode="spark-deep",
                        use_checksum=use_checksum,
                        max_workers=max_workers,
                        duration_seconds=duration,
                    )
                    result["run_id"] = run_id
                except Exception as store_err:
                    logger.debug(f"Could not store batch deep result: {store_err}")

                self.jobs[job_id]["progress"]["completed"] = total
                self.jobs[job_id]["progress"]["current_table"] = ""
                job_logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] Batch deep complete: {total_matched} matched across {total} tables")

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
            # Restore stderr, log level, and remove log handler
            sys.stderr = original_stderr
            src_logger.setLevel(previous_log_level)
            src_logger.removeHandler(log_handler)
            self._executor_semaphore.release()

            # Invalidate metadata cache for catalogs modified by this job
            if job_type in ("clone", "sync", "incremental_sync"):
                try:
                    from src.client import invalidate_catalog_cache
                    for cat_key in ("source_catalog", "destination_catalog"):
                        cat = config.get(cat_key, "")
                        if cat:
                            invalidate_catalog_cache(cat)
                except Exception:
                    pass

            # Persist run logs to Unity Catalog Delta table
            try:
                from src.run_logs import save_run_log, ensure_run_logs_table
                ensure_run_logs_table(client, config.get("sql_warehouse_id", ""), config)
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
