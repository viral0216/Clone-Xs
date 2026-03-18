import argparse
import logging
import sys
import time

from src.auth import (
    _load_session,
    add_auth_args, ensure_authenticated, ensure_logged_in,
    get_client, interactive_login, list_profiles, select_warehouse,
)
from src.clone_catalog import clone_catalog
from src.config import load_config

logger = logging.getLogger(__name__)


def setup_logging(verbose: bool = False, log_file: str | None = None) -> None:
    """Configure logging for the CLI with optional file output."""
    level = logging.DEBUG if verbose else logging.INFO
    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"

    handlers: list[logging.Handler] = [logging.StreamHandler()]

    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(logging.Formatter(fmt, datefmt=datefmt))
        handlers.append(file_handler)

    logging.basicConfig(level=level, format=fmt, datefmt=datefmt, handlers=handlers)


def add_common_args(parser: argparse.ArgumentParser) -> None:
    """Add common arguments shared across subcommands."""
    parser.add_argument(
        "-c", "--config",
        default="config/clone_config.yaml",
        help="Path to the clone config YAML file (default: config/clone_config.yaml)",
    )
    parser.add_argument("--warehouse-id", help="Override SQL warehouse ID from config")
    parser.add_argument("--serverless", action="store_true", help="Use serverless compute instead of SQL warehouse")
    parser.add_argument("--volume", help="UC Volume path for serverless (e.g. /Volumes/catalog/schema/volume)")
    parser.add_argument("--max-parallel-queries", type=int, default=None,
                        help="Max parallel SQL queries (default: 10)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose/debug logging")
    parser.add_argument("--profile", help="Config profile to use")
    parser.add_argument("--log-file", help="Write logs to a file in addition to console")
    add_auth_args(parser)


def _save_cli_run_log(client, config, job_type, result, start_time, error=None):
    """Save run log + audit trail for CLI commands that don't go through JobManager."""
    if not config.get("save_run_logs", True):
        return

    import uuid
    from datetime import datetime

    job_id = str(uuid.uuid4())[:8]
    wid = config.get("sql_warehouse_id", "")
    started_dt = datetime.fromtimestamp(start_time)

    # 1. Save to run_logs table (detailed execution trace)
    try:
        from src.run_logs import save_run_log
        job_record = {
            "job_id": job_id,
            "job_type": job_type,
            "source_catalog": config.get("source_catalog", ""),
            "destination_catalog": config.get("destination_catalog", ""),
            "clone_type": config.get("clone_type", ""),
            "status": "failed" if error else "completed",
            "started_at": started_dt.isoformat(),
            "completed_at": datetime.now().isoformat(),
            "result": result,
            "error": str(error) if error else None,
            "logs": [],
        }
        save_run_log(client, wid, job_record, config)
    except Exception as e:
        logging.getLogger(__name__).debug(f"Could not save run log to Delta: {e}")

    # 2. Save to clone_operations table (audit trail)
    try:
        from src.audit_trail import log_operation_start, log_operation_complete
        log_operation_start(client, wid, config, job_id, operation_type=job_type)
        log_operation_complete(client, wid, config, job_id, result or {}, started_dt,
                              error_message=str(error) if error else None)
    except Exception as e:
        logging.getLogger(__name__).debug(f"Could not save audit trail to Delta: {e}")


def _get_auth_client(args):
    """Get authenticated WorkspaceClient from CLI args.

    Uses --host, --token, --auth-profile flags if provided, otherwise
    falls back to env vars, CLI profile, or notebook native auth.
    Runs --login for browser OAuth or --verify-auth check if requested.
    """
    host = getattr(args, "host", None)
    token = getattr(args, "token", None)
    auth_profile = getattr(args, "auth_profile", None)
    verify = getattr(args, "verify_auth", False)
    login = getattr(args, "login", False)

    # Browser-based login if --login flag is set
    if login:
        if host:
            ensure_logged_in(host=host, force=True)
        else:
            # Full interactive flow: browser -> tenant -> subscription -> workspace
            result = interactive_login()
            host = result.get("host")

    if verify:
        ensure_authenticated(host, token, auth_profile)

    return get_client(host, token, auth_profile)


def _resolve_warehouse_id(args, config: dict, client=None) -> str:
    """Resolve SQL warehouse ID from CLI args, config, or interactive selection.

    Priority: --serverless flag > --warehouse-id flag > config file > saved session > interactive picker.
    Returns "SERVERLESS" if serverless compute is selected.
    """
    # Check --serverless flag first
    if getattr(args, "serverless", False):
        config["sql_warehouse_id"] = "SERVERLESS"
        config["use_serverless"] = True
        return "SERVERLESS"

    wid = getattr(args, "warehouse_id", None) or config.get("sql_warehouse_id", "")
    # Skip placeholder values
    if wid and wid not in ("your-warehouse-id", "your_warehouse_id", ""):
        config["sql_warehouse_id"] = wid
        return wid

    # Check saved session from previous interactive login
    session = _load_session()
    if session.get("warehouse_id"):
        wid = session["warehouse_id"]
        logger.info("Using saved warehouse: %s", wid)
        config["sql_warehouse_id"] = wid
        return wid

    # No warehouse ID — interactive selection with serverless option
    if client is None:
        client = _get_auth_client(args)
    print("\n  No SQL warehouse ID provided. Discovering warehouses...")
    from src.auth import list_warehouses
    warehouses = list_warehouses(client)

    # Build options: serverless + warehouses
    print("\n  Compute options:")
    print("    1. Serverless compute (no SQL warehouse needed)")
    for i, wh in enumerate(warehouses, 2):
        state_icon = "*" if wh["state"] == "RUNNING" else " "
        print(f"    {i}. {wh['name']:<30} {wh['size']:<12} {wh['state']:<10} {wh['type']}{state_icon}")

    total = len(warehouses) + 1
    pick = input(f"\n  Select compute [1-{total}] (default: 1): ").strip()
    idx = int(pick) if pick.isdigit() and 1 <= int(pick) <= total else 1

    if idx == 1:
        print("  Using serverless compute.")
        config["sql_warehouse_id"] = "SERVERLESS"
        config["use_serverless"] = True
        from src.auth import _save_session, _load_session as _ls
        sess = _ls()
        _save_session(sess.get("host", ""), "SERVERLESS")
        return "SERVERLESS"
    else:
        selected = warehouses[idx - 2]
        print(f"  Warehouse: {selected['name']} ({selected['id']})")
        config["sql_warehouse_id"] = selected["id"]
        from src.auth import _save_session, _load_session as _ls
        sess = _ls()
        _save_session(sess.get("host", ""), selected["id"])
        return selected["id"]


def cmd_clone(args):
    """Execute the clone command."""
    logger = logging.getLogger(__name__)

    try:
        config = load_config(args.config, profile=args.profile)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Config error: {e}")
        sys.exit(1)

    # Apply CLI overrides
    if args.source:
        config["source_catalog"] = args.source
    if args.dest:
        config["destination_catalog"] = args.dest
    if args.clone_type:
        config["clone_type"] = args.clone_type
    if args.load_type:
        config["load_type"] = args.load_type
    if args.max_workers:
        config["max_workers"] = args.max_workers
    if args.no_permissions:
        config["copy_permissions"] = False
    if args.no_ownership:
        config["copy_ownership"] = False
    if args.no_tags:
        config["copy_tags"] = False
    if args.no_properties:
        config["copy_properties"] = False
    if args.no_security:
        config["copy_security"] = False
    if args.dry_run:
        config["dry_run"] = True
    if args.include_schemas:
        config["include_schemas"] = args.include_schemas
    if args.report:
        config["generate_report"] = True
    if args.enable_rollback:
        config["enable_rollback"] = True
    if args.validate:
        config["validate_after_clone"] = True
    if args.checksum:
        config["validate_checksum"] = True
    if args.no_constraints:
        config["copy_constraints"] = False
    if args.no_comments:
        config["copy_comments"] = False
    if args.parallel_tables:
        config["parallel_tables"] = args.parallel_tables
    if args.include_tables_regex:
        config["include_tables_regex"] = args.include_tables_regex
    if args.exclude_tables_regex:
        config["exclude_tables_regex"] = args.exclude_tables_regex
    if args.resume:
        config["resume"] = args.resume
    if args.no_progress:
        config["show_progress"] = False
    if args.progress:
        config["show_progress"] = True
    if args.order_by_size:
        config["order_by_size"] = args.order_by_size
    if args.max_rps:
        config["max_rps"] = args.max_rps
    if args.as_of_timestamp:
        config["as_of_timestamp"] = args.as_of_timestamp
    if args.as_of_version is not None:
        config["as_of_version"] = args.as_of_version
    if getattr(args, "location", None):
        config["catalog_location"] = args.location

    # New feature CLI overrides
    if getattr(args, "template", None):
        from src.clone_templates import apply_template
        config = apply_template(config, args.template)
    if getattr(args, "where", None):
        config.setdefault("where_clauses", {})
        if config["where_clauses"] is None:
            config["where_clauses"] = {}
        config["where_clauses"]["*"] = args.where
    if getattr(args, "table_filter", None):
        config.setdefault("where_clauses", {})
        if config["where_clauses"] is None:
            config["where_clauses"] = {}
        for tf in args.table_filter:
            if ":" in tf:
                tbl, cond = tf.split(":", 1)
                config["where_clauses"][tbl] = cond
    if getattr(args, "auto_rollback", False):
        config["auto_rollback_on_failure"] = True
        config["enable_rollback"] = True
        config["validate_after_clone"] = True
    if getattr(args, "rollback_threshold", None) is not None:
        config["rollback_threshold"] = args.rollback_threshold
    if getattr(args, "throttle", None):
        config["throttle"] = args.throttle
    if getattr(args, "checkpoint", False):
        config["checkpoint_enabled"] = True
    if getattr(args, "resume_from_checkpoint", None):
        from src.checkpoint import CheckpointManager
        config["resume"] = args.resume_from_checkpoint
        completed = CheckpointManager.get_completed_from_checkpoint(args.resume_from_checkpoint)
        config["_checkpoint_completed"] = completed
    if getattr(args, "require_approval", False):
        config["approval_required"] = True
    if getattr(args, "impact_check", False):
        config["impact_check_before_clone"] = True
    if getattr(args, "ttl", None):
        config["ttl"] = args.ttl
    if getattr(args, "skip_unused", False):
        config["skip_unused"] = True
    if getattr(args, "schema_only", False):
        config["schema_only"] = True

    # Cross-workspace destination
    if args.dest_host and args.dest_token:
        config["dest_workspace"] = {
            "host": args.dest_host,
            "token": args.dest_token,
            "sql_warehouse_id": args.dest_warehouse_id or config["sql_warehouse_id"],
        }

    if not config.get("source_catalog"):
        logger.error("Source catalog is required. Use --source <catalog_name> or set source_catalog in config.")
        sys.exit(1)
    if not config.get("destination_catalog"):
        logger.error("Destination catalog is required. Use --dest <catalog_name> or set destination_catalog in config.")
        sys.exit(1)

    logger.info(
        f"Cloning catalog: {config['source_catalog']} -> {config['destination_catalog']}"
    )

    dest_ws = config.get("dest_workspace")
    if dest_ws:
        logger.info(f"Cross-workspace mode: destination host = {dest_ws['host']}")
        client = get_client(host=dest_ws["host"], token=dest_ws["token"])
        if dest_ws.get("sql_warehouse_id"):
            config["sql_warehouse_id"] = dest_ws["sql_warehouse_id"]
    else:
        client = _get_auth_client(args)

    # Serverless mode: submit entire clone as a single Databricks job
    if getattr(args, "serverless", False) or config.get("use_serverless"):
        from src.serverless import submit_clone_job
        logger.info("Serverless mode: submitting clone as a single Databricks job")
        volume_path = getattr(args, "volume", None) or config.get("volume_path")
        summary = submit_clone_job(client, config, volume_path=volume_path)
    else:
        _resolve_warehouse_id(args, config, client)
        summary = clone_catalog(client, config)

    total_failed = sum(summary[t]["failed"] for t in ("tables", "views", "functions", "volumes"))
    if total_failed > 0 or summary.get("errors"):
        sys.exit(1)


def cmd_diff(args):
    """Execute the diff command."""
    from src.diff import compare_catalogs, print_diff

    logger = logging.getLogger(__name__)
    try:
        config = load_config(args.config, profile=args.profile)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Config error: {e}")
        sys.exit(1)

    if args.source:
        config["source_catalog"] = args.source
    if args.dest:
        config["destination_catalog"] = args.dest
    _resolve_warehouse_id(args, config)

    client = _get_auth_client(args)
    start_time = time.time()
    diff = compare_catalogs(
        client, config["sql_warehouse_id"],
        config["source_catalog"], config["destination_catalog"],
        config["exclude_schemas"],
    )
    print_diff(diff, config["source_catalog"], config["destination_catalog"])
    _save_cli_run_log(client, config, "diff", diff, start_time)


def cmd_rollback(args):
    """Execute the rollback command."""
    from src.rollback import list_rollback_logs, rollback

    logger = logging.getLogger(__name__)

    if args.list:
        logs = list_rollback_logs()
        if not logs:
            logger.info("No rollback logs found.")
            return
        logger.info("Available rollback logs:")
        for log in logs:
            logger.info(
                f"  {log['file']} | {log['timestamp']} | "
                f"{log['destination_catalog']} | {log['total_objects']} objects"
            )
        return

    if not args.rollback_log_file:
        logger.error("Please provide --rollback-log or use --list to see available logs.")
        sys.exit(1)

    try:
        config = load_config(args.config, profile=args.profile)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Config error: {e}")
        sys.exit(1)

    _resolve_warehouse_id(args, config)

    client = _get_auth_client(args)
    start_time = time.time()
    results = rollback(
        client, config["sql_warehouse_id"], args.rollback_log_file,
        drop_catalog=args.drop_catalog,
    )
    _save_cli_run_log(client, config, "rollback", results, start_time,
                      error=f"{results['failed']} failed" if results["failed"] else None)

    if results["failed"] > 0:
        sys.exit(1)


def cmd_validate(args):
    """Execute the validate command."""
    from src.validation import validate_catalog

    logger = logging.getLogger(__name__)
    try:
        config = load_config(args.config, profile=args.profile)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Config error: {e}")
        sys.exit(1)

    if args.source:
        config["source_catalog"] = args.source
    if args.dest:
        config["destination_catalog"] = args.dest
    _resolve_warehouse_id(args, config)

    client = _get_auth_client(args)
    summary = validate_catalog(
        client, config["sql_warehouse_id"],
        config["source_catalog"], config["destination_catalog"],
        config["exclude_schemas"], config["max_workers"],
        use_checksum=args.checksum,
    )
    if summary["mismatched"] > 0 or summary["errors"] > 0:
        sys.exit(1)


def cmd_estimate(args):
    """Execute the cost estimation command."""
    from src.cost_estimation import estimate_clone_cost

    logger = logging.getLogger(__name__)
    try:
        config = load_config(args.config, profile=args.profile)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Config error: {e}")
        sys.exit(1)

    if args.source:
        config["source_catalog"] = args.source
    _resolve_warehouse_id(args, config)

    client = _get_auth_client(args)
    estimate_clone_cost(
        client, config["sql_warehouse_id"], config["source_catalog"],
        config["exclude_schemas"],
        include_schemas=config.get("include_schemas") or None,
        price_per_gb=args.price_per_gb,
    )


def cmd_generate_workflow(args):
    """Execute the generate-workflow command."""
    from src.workflow import generate_workflow, generate_workflow_yaml

    logger = logging.getLogger(__name__)
    try:
        config = load_config(args.config, profile=args.profile)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Config error: {e}")
        sys.exit(1)

    if args.format == "json":
        output = generate_workflow(
            config, output_path=args.output or "databricks_workflow.json",
            job_name=args.job_name, cluster_id=args.cluster_id,
            schedule_cron=args.schedule, notification_email=args.notification_email,
        )
    else:
        output = generate_workflow_yaml(
            config, output_path=args.output or "databricks_workflow.yaml",
            job_name=args.job_name, schedule_cron=args.schedule,
        )
    logger.info(f"Workflow file generated: {output}")


def cmd_create_job(args):
    """Create a persistent Databricks Job for scheduled catalog cloning."""
    from src.create_job import create_persistent_job

    logger = logging.getLogger(__name__)
    try:
        config = load_config(args.config, profile=args.profile)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Config error: {e}")
        sys.exit(1)

    if args.source:
        config["source_catalog"] = args.source
    if args.dest:
        config["destination_catalog"] = args.dest

    if not config.get("source_catalog") or not config.get("destination_catalog"):
        logger.error("Both --source and --dest are required.")
        sys.exit(1)

    client = _get_auth_client(args)

    result = create_persistent_job(
        client,
        config,
        job_name=args.job_name,
        volume_path=getattr(args, "volume", None),
        schedule_cron=args.schedule,
        schedule_timezone=args.timezone,
        notification_emails=args.notification_email.split(",") if args.notification_email else None,
        max_retries=args.max_retries,
        timeout_seconds=args.timeout,
        tags=dict(t.split("=", 1) for t in args.tag) if args.tag else None,
        update_job_id=args.update_job_id,
    )

    print(f"\n  Job {'updated' if args.update_job_id else 'created'} successfully!")
    print(f"  Job ID:   {result['job_id']}")
    print(f"  Job URL:  {result['job_url']}")
    print(f"  Notebook: {result['notebook_path']}")
    print(f"  Wheel:    {result['volume_wheel_path']}")
    if result.get("schedule"):
        print(f"  Schedule: {result['schedule']}")
    if getattr(args, "run_now", False) and result.get("job_id"):
        print("\n  Running job now...")
        try:
            run = client.jobs.run_now(result["job_id"])
            print(f"  Run started! Run ID: {run.run_id}")
        except Exception as e:
            print(f"  Failed to start run: {e}")
    else:
        print("\n  Run or schedule this job from the Databricks Jobs UI.")


def cmd_compare(args):
    """Execute the deep compare command."""
    from src.compare import compare_catalogs_deep

    logger = logging.getLogger(__name__)
    try:
        config = load_config(args.config, profile=args.profile)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Config error: {e}")
        sys.exit(1)

    if args.source:
        config["source_catalog"] = args.source
    if args.dest:
        config["destination_catalog"] = args.dest
    _resolve_warehouse_id(args, config)

    client = _get_auth_client(args)
    start_time = time.time()
    summary = compare_catalogs_deep(
        client, config["sql_warehouse_id"],
        config["source_catalog"], config["destination_catalog"],
        config["exclude_schemas"], config["max_workers"],
    )
    _save_cli_run_log(client, config, "compare", summary, start_time)

    if summary["tables_with_issues"] > 0:
        sys.exit(1)


def cmd_sync(args):
    """Execute the sync command."""
    from src.sync_catalog import sync_catalogs

    logger = logging.getLogger(__name__)
    try:
        config = load_config(args.config, profile=args.profile)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Config error: {e}")
        sys.exit(1)

    if args.source:
        config["source_catalog"] = args.source
    if args.dest:
        config["destination_catalog"] = args.dest
    _resolve_warehouse_id(args, config)

    client = _get_auth_client(args)
    results = sync_catalogs(
        client, config["sql_warehouse_id"],
        config["source_catalog"], config["destination_catalog"],
        config["exclude_schemas"], config["clone_type"],
        dry_run=args.dry_run, drop_extra=args.drop_extra,
    )
    if results["errors"]:
        sys.exit(1)


def cmd_snapshot(args):
    """Execute the snapshot command."""
    from src.snapshot import create_snapshot

    logger = logging.getLogger(__name__)
    try:
        config = load_config(args.config, profile=args.profile)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Config error: {e}")
        sys.exit(1)

    if args.source:
        config["source_catalog"] = args.source
    _resolve_warehouse_id(args, config)

    client = _get_auth_client(args)
    start_time = time.time()
    output = create_snapshot(
        client, config["sql_warehouse_id"], config["source_catalog"],
        config["exclude_schemas"], output_path=args.output,
    )
    logger.info(f"Snapshot saved: {output}")
    _save_cli_run_log(client, config, "snapshot", {"output_path": output}, start_time)


def cmd_schema_drift(args):
    """Execute the schema-drift command."""
    from src.schema_drift import detect_schema_drift

    logger = logging.getLogger(__name__)
    try:
        config = load_config(args.config, profile=args.profile)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Config error: {e}")
        sys.exit(1)

    if args.source:
        config["source_catalog"] = args.source
    if args.dest:
        config["destination_catalog"] = args.dest
    _resolve_warehouse_id(args, config)

    client = _get_auth_client(args)
    start_time = time.time()
    summary = detect_schema_drift(
        client, config["sql_warehouse_id"],
        config["source_catalog"], config["destination_catalog"],
        config["exclude_schemas"],
        include_schemas=config.get("include_schemas"),
    )
    _save_cli_run_log(client, config, "schema-drift", summary, start_time)

    if summary["tables_with_drift"] > 0:
        sys.exit(1)


def cmd_init(args):
    """Execute the init (config wizard) command."""
    from src.wizard import run_wizard
    run_wizard(output_path=args.output)


def cmd_terraform(args):
    """Execute the terraform/pulumi export command."""
    from src.terraform import generate_pulumi, generate_terraform

    logger = logging.getLogger(__name__)
    try:
        config = load_config(args.config, profile=args.profile)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Config error: {e}")
        sys.exit(1)

    if args.source:
        config["source_catalog"] = args.source
    _resolve_warehouse_id(args, config)

    client = _get_auth_client(args)

    if args.format == "pulumi":
        output = generate_pulumi(
            client, config["sql_warehouse_id"], config["source_catalog"],
            config["exclude_schemas"], output_path=args.output or "pulumi_catalog.py",
        )
    else:
        output = generate_terraform(
            client, config["sql_warehouse_id"], config["source_catalog"],
            config["exclude_schemas"], output_path=args.output or "terraform_catalog.tf.json",
        )
    logger.info(f"IaC config generated: {output}")


def cmd_preflight(args):
    """Execute the preflight checks command."""
    from src.preflight import run_preflight

    logger = logging.getLogger(__name__)
    try:
        config = load_config(args.config, profile=args.profile)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Config error: {e}")
        sys.exit(1)

    if args.source:
        config["source_catalog"] = args.source
    if args.dest:
        config["destination_catalog"] = args.dest
    _resolve_warehouse_id(args, config)

    client = _get_auth_client(args)
    start_time = time.time()
    result = run_preflight(
        client, config["sql_warehouse_id"],
        config["source_catalog"], config["destination_catalog"],
        check_write=not args.no_write_check,
    )
    _save_cli_run_log(client, config, "preflight", result, start_time,
                      error="Preflight checks failed" if not result["ready"] else None)

    if not result["ready"]:
        sys.exit(1)


def cmd_search(args):
    """Execute the search command."""
    from src.search import search_tables

    logger = logging.getLogger(__name__)
    try:
        config = load_config(args.config, profile=args.profile)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Config error: {e}")
        sys.exit(1)

    if args.source:
        config["source_catalog"] = args.source
    _resolve_warehouse_id(args, config)

    client = _get_auth_client(args)
    search_tables(
        client, config["sql_warehouse_id"], config["source_catalog"],
        args.pattern, config["exclude_schemas"],
        search_columns=args.columns,
    )


def cmd_stats(args):
    """Execute the stats command."""
    from src.stats import catalog_stats

    logger = logging.getLogger(__name__)
    try:
        config = load_config(args.config, profile=args.profile)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Config error: {e}")
        sys.exit(1)

    if args.source:
        config["source_catalog"] = args.source
    _resolve_warehouse_id(args, config)

    client = _get_auth_client(args)
    catalog_stats(
        client, config["sql_warehouse_id"], config["source_catalog"],
        config["exclude_schemas"],
    )


def cmd_storage_metrics(args):
    """Execute the storage-metrics command."""
    from src.storage_metrics import catalog_storage_metrics

    logger = logging.getLogger(__name__)
    try:
        config = load_config(args.config, profile=args.profile)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Config error: {e}")
        sys.exit(1)

    if args.source:
        config["source_catalog"] = args.source
    _resolve_warehouse_id(args, config)

    client = _get_auth_client(args)
    catalog_storage_metrics(
        client, config["sql_warehouse_id"], config["source_catalog"],
        config["exclude_schemas"],
        schema_filter=args.schema,
        table_filter=args.table,
        include_schemas=config.get("include_schemas"),
    )


def cmd_optimize(args):
    """Execute the optimize command."""
    from src.table_maintenance import run_optimize, _enumerate_tables, check_predictive_optimization

    logger = logging.getLogger(__name__)
    try:
        config = load_config(args.config, profile=args.profile)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Config error: {e}")
        sys.exit(1)

    if args.source:
        config["source_catalog"] = args.source
    _resolve_warehouse_id(args, config)

    client = _get_auth_client(args)
    catalog = config["source_catalog"]
    wid = config["sql_warehouse_id"]

    # Check predictive optimization
    po = check_predictive_optimization(client, wid, catalog, config["exclude_schemas"])
    if po["enabled"]:
        logger.warning(
            "Predictive Optimization is enabled — OPTIMIZE may run automatically. "
            "Manual execution may be unnecessary."
        )

    tables = _enumerate_tables(
        client, wid, catalog,
        schema_filter=args.schema, table_filter=args.table,
        exclude_schemas=config["exclude_schemas"],
    )
    run_optimize(client, wid, tables, dry_run=args.dry_run)


def cmd_vacuum(args):
    """Execute the vacuum command."""
    from src.table_maintenance import run_vacuum, _enumerate_tables, check_predictive_optimization

    logger = logging.getLogger(__name__)
    try:
        config = load_config(args.config, profile=args.profile)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Config error: {e}")
        sys.exit(1)

    if args.source:
        config["source_catalog"] = args.source
    _resolve_warehouse_id(args, config)

    client = _get_auth_client(args)
    catalog = config["source_catalog"]
    wid = config["sql_warehouse_id"]

    # Check predictive optimization
    po = check_predictive_optimization(client, wid, catalog, config["exclude_schemas"])
    if po["enabled"]:
        logger.warning(
            "Predictive Optimization is enabled — VACUUM may run automatically. "
            "Manual execution may be unnecessary."
        )

    tables = _enumerate_tables(
        client, wid, catalog,
        schema_filter=args.schema, table_filter=args.table,
        exclude_schemas=config["exclude_schemas"],
    )
    run_vacuum(client, wid, tables, retention_hours=args.retention_hours, dry_run=args.dry_run)


def cmd_profile(args):
    """Execute the profile command."""
    from src.profiling import profile_catalog

    logger = logging.getLogger(__name__)
    try:
        config = load_config(args.config, profile=args.profile)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Config error: {e}")
        sys.exit(1)

    if args.source:
        config["source_catalog"] = args.source
    _resolve_warehouse_id(args, config)

    client = _get_auth_client(args)
    start_time = time.time()
    result = profile_catalog(
        client, config["sql_warehouse_id"], config["source_catalog"],
        config["exclude_schemas"], config["max_workers"],
        output_path=args.output,
        include_schemas=config.get("include_schemas"),
    )
    _save_cli_run_log(client, config, "profile", result or {}, start_time)


def cmd_monitor(args):
    """Execute the monitor command."""
    from src.monitor import monitor_loop, monitor_once

    logger = logging.getLogger(__name__)
    try:
        config = load_config(args.config, profile=args.profile)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Config error: {e}")
        sys.exit(1)

    if args.source:
        config["source_catalog"] = args.source
    if args.dest:
        config["destination_catalog"] = args.dest
    _resolve_warehouse_id(args, config)

    client = _get_auth_client(args)

    if args.once:
        result = monitor_once(
            client, config["sql_warehouse_id"],
            config["source_catalog"], config["destination_catalog"],
            config["exclude_schemas"],
            check_drift=args.check_drift, check_counts=args.check_counts,
        )
        if not result["in_sync"]:
            sys.exit(1)
    else:
        monitor_loop(
            client, config["sql_warehouse_id"],
            config["source_catalog"], config["destination_catalog"],
            config["exclude_schemas"],
            interval_minutes=args.interval,
            max_iterations=args.max_checks,
            check_drift=args.check_drift, check_counts=args.check_counts,
        )


def cmd_export(args):
    """Execute the export command."""
    from src.export import export_catalog_metadata

    logger = logging.getLogger(__name__)
    try:
        config = load_config(args.config, profile=args.profile)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Config error: {e}")
        sys.exit(1)

    if args.source:
        config["source_catalog"] = args.source
    _resolve_warehouse_id(args, config)

    client = _get_auth_client(args)
    start_time = time.time()
    output = export_catalog_metadata(
        client, config["sql_warehouse_id"], config["source_catalog"],
        config["exclude_schemas"], output_format=args.format,
        output_path=args.output,
    )
    logger.info(f"Export saved: {output}")
    _save_cli_run_log(client, config, "export", {"output_path": output}, start_time)


def cmd_config_diff(args):
    """Execute the config-diff command."""
    from src.config_diff import print_config_diff
    print_config_diff(args.file_a, args.file_b)


def cmd_completion(args):
    """Execute the completion command."""
    from src.completions import install_completions
    install_completions(args.shell)


def cmd_dashboard(args):
    """Launch the Streamlit web dashboard."""
    from src.web_dashboard import launch_dashboard
    launch_dashboard(config_path=args.config, port=args.port)


def cmd_generate_dab(args):
    """Generate a Databricks Asset Bundle."""
    from src.dab_integration import generate_dab_bundle

    logger = logging.getLogger(__name__)
    try:
        config = load_config(args.config, profile=args.profile)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Config error: {e}")
        sys.exit(1)

    if args.source:
        config["source_catalog"] = args.source
    if args.dest:
        config["destination_catalog"] = args.dest

    generate_dab_bundle(
        config, output_dir=args.output,
        job_name=args.job_name, schedule_cron=args.schedule,
        notification_email=args.notification_email,
    )


def cmd_multi_clone(args):
    """Clone to multiple destination workspaces."""
    import yaml
    from src.multi_workspace_clone import clone_to_multiple_workspaces

    logger = logging.getLogger(__name__)
    try:
        config = load_config(args.config, profile=args.profile)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Config error: {e}")
        sys.exit(1)

    if args.source:
        config["source_catalog"] = args.source

    # Load destinations from a YAML file
    with open(args.destinations) as f:
        dest_config = yaml.safe_load(f)

    destinations = dest_config.get("destinations", [])
    start_time = time.time()
    summary = clone_to_multiple_workspaces(config, destinations, max_parallel=args.max_parallel)
    _save_cli_run_log(None, config, "multi-clone", summary, start_time,
                      error=f"{summary['failed']} workspaces failed" if summary["failed"] else None)

    if summary["failed"] > 0:
        sys.exit(1)


def cmd_cost_estimate(args):
    """Estimate clone cost."""
    from src.clone_cost_estimator import estimate_clone_cost

    logger = logging.getLogger(__name__)
    try:
        config = load_config(args.config, profile=args.profile)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Config error: {e}")
        sys.exit(1)

    if args.source:
        config["source_catalog"] = args.source
    _resolve_warehouse_id(args, config)

    client = _get_auth_client(args)
    estimate_clone_cost(
        client, config["sql_warehouse_id"], config["source_catalog"],
        config.get("exclude_schemas", []),
        clone_type=args.clone_type or config.get("clone_type", "DEEP"),
        warehouse_type=args.warehouse_type,
    )


def cmd_audit(args):
    """Query the audit trail."""
    from src.audit_trail import ensure_audit_table, query_audit_history

    logger = logging.getLogger(__name__)
    try:
        config = load_config(args.config, profile=args.profile)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Config error: {e}")
        sys.exit(1)

    _resolve_warehouse_id(args, config)

    client = _get_auth_client(args)

    if args.init:
        ensure_audit_table(client, config["sql_warehouse_id"], config)
    else:
        query_audit_history(
            client, config["sql_warehouse_id"], config,
            limit=args.limit, source_catalog=args.source, status=args.status,
        )


def cmd_lineage(args):
    """Query lineage tracking."""
    from src.lineage_tracker import ensure_lineage_table, query_lineage

    logger = logging.getLogger(__name__)
    try:
        config = load_config(args.config, profile=args.profile)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Config error: {e}")
        sys.exit(1)

    _resolve_warehouse_id(args, config)

    client = _get_auth_client(args)

    if args.init:
        ensure_lineage_table(client, config["sql_warehouse_id"])
    else:
        query_lineage(
            client, config["sql_warehouse_id"],
            table_fqn=args.table, operation_id=args.operation_id,
            limit=args.limit,
        )


def cmd_pii_scan(args):
    """Scan catalog for PII columns."""
    from src.pii_detection import scan_catalog_for_pii

    logger = logging.getLogger(__name__)
    try:
        config = load_config(args.config, profile=args.profile)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Config error: {e}")
        sys.exit(1)

    if args.source:
        config["source_catalog"] = args.source
    _resolve_warehouse_id(args, config)

    # Schema filtering: if --schema-filter given, exclude everything else
    exclude_schemas = config.get("exclude_schemas", [])
    client = _get_auth_client(args)
    start_time = time.time()
    result = scan_catalog_for_pii(
        client, config["sql_warehouse_id"], config["source_catalog"],
        exclude_schemas,
        sample_data=args.sample_data, max_workers=config.get("max_workers", 4),
        pii_config=config.get("pii_detection"),
        read_uc_tags=args.read_uc_tags,
        save_history=args.save_history,
        schema_filter=args.schema_filter,
        table_filter=args.table_filter,
    )
    _save_cli_run_log(client, config, "pii-scan", result, start_time)

    # Apply UC tags if requested
    if args.apply_tags:
        from src.pii_tagging import apply_pii_tags
        tag_result = apply_pii_tags(
            client, config["sql_warehouse_id"], config["source_catalog"],
            result.get("columns", []),
            tag_prefix=args.tag_prefix,
            min_confidence=0.7,
        )
        logger.info(
            f"PII tagging: {tag_result['tagged']} tagged, "
            f"{tag_result['skipped']} skipped, {tag_result['errors']} errors"
        )

    if result["summary"]["pii_columns_found"] > 0 and not args.no_exit_code:
        sys.exit(1)


def cmd_policy_check(args):
    """Check clone policies."""
    from src.clone_policies import enforce_policies

    logger = logging.getLogger(__name__)
    try:
        config = load_config(args.config, profile=args.profile)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Config error: {e}")
        sys.exit(1)

    _resolve_warehouse_id(args, config)

    client = _get_auth_client(args)
    passed = enforce_policies(
        client, config["sql_warehouse_id"], config,
        policy_path=args.policy_file,
    )
    if not passed:
        sys.exit(1)


def cmd_schema_evolve(args):
    """Detect and apply schema evolution."""
    from src.schema_evolution import evolve_catalog_schema

    logger = logging.getLogger(__name__)
    try:
        config = load_config(args.config, profile=args.profile)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Config error: {e}")
        sys.exit(1)

    if args.source:
        config["source_catalog"] = args.source
    if args.dest:
        config["destination_catalog"] = args.dest
    _resolve_warehouse_id(args, config)

    client = _get_auth_client(args)
    start_time = time.time()
    result = evolve_catalog_schema(
        client, config["sql_warehouse_id"],
        config["source_catalog"], config["destination_catalog"],
        config.get("exclude_schemas", []),
        dry_run=args.dry_run, drop_removed=args.drop_removed,
    )
    if not args.dry_run:
        _save_cli_run_log(client, config, "schema-evolve", result, start_time,
                          error=f"{result['tables_with_errors']} errors" if result["tables_with_errors"] else None)

    if result["tables_with_errors"] > 0:
        sys.exit(1)


def cmd_incremental_sync(args):
    """Execute the incremental sync command."""
    from src.incremental_sync import get_tables_needing_sync, sync_changed_table

    logger = logging.getLogger(__name__)
    try:
        config = load_config(args.config, profile=args.profile)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Config error: {e}")
        sys.exit(1)

    if args.source:
        config["source_catalog"] = args.source
    if args.dest:
        config["destination_catalog"] = args.dest
    _resolve_warehouse_id(args, config)

    client = _get_auth_client(args)
    source = config["source_catalog"]
    dest = config["destination_catalog"]
    wid = config["sql_warehouse_id"]
    clone_type = getattr(args, "clone_type", None) or config.get("clone_type", "DEEP")
    start_time = time.time()

    schemas = config.get("include_schemas") or []
    if args.schema:
        schemas = [args.schema]

    if not schemas:
        # Discover schemas
        from src.client import execute_sql
        rows = execute_sql(client, wid,
            f"SELECT schema_name FROM {source}.information_schema.schemata "
            f"WHERE schema_name NOT IN ('information_schema', 'default')")
        schemas = [r["schema_name"] for r in rows]

    total_synced, total_failed = 0, 0
    for schema in schemas:
        tables = get_tables_needing_sync(client, wid, source, dest, schema)
        if not tables:
            logger.info(f"Schema {schema}: all tables up to date")
            continue

        logger.info(f"Schema {schema}: {len(tables)} tables need sync")
        for t in tables:
            ok = sync_changed_table(
                client, wid, source, dest, schema, t["table_name"],
                clone_type=clone_type, dry_run=args.dry_run,
            )
            if ok:
                total_synced += 1
            else:
                total_failed += 1

    result = {"synced": total_synced, "failed": total_failed, "schemas": len(schemas)}
    logger.info(f"Incremental sync complete: {total_synced} synced, {total_failed} failed")

    if not args.dry_run:
        _save_cli_run_log(client, config, "incremental_sync", result, start_time,
                          error=f"{total_failed} tables failed" if total_failed else None)

    if total_failed > 0:
        sys.exit(1)


def cmd_sample(args):
    """Execute the sample command."""
    from src.sampling import compare_samples, preview_table

    logger = logging.getLogger(__name__)
    try:
        config = load_config(args.config, profile=args.profile)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Config error: {e}")
        sys.exit(1)

    if args.source:
        config["source_catalog"] = args.source
    _resolve_warehouse_id(args, config)

    client = _get_auth_client(args)
    wid = config["sql_warehouse_id"]

    if args.dest:
        # Compare mode
        result = compare_samples(
            client, wid, config["source_catalog"], args.dest,
            args.schema, args.table, limit=args.limit,
        )
        import json
        print(json.dumps(result, indent=2, default=str))
    else:
        # Preview mode
        preview_table(client, wid, config["source_catalog"], args.schema, args.table, args.limit)


def cmd_view_deps(args):
    """Execute the view/function dependencies command."""
    from src.dependencies import get_view_dependencies, get_function_dependencies, get_ordered_views

    logger = logging.getLogger(__name__)
    try:
        config = load_config(args.config, profile=args.profile)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Config error: {e}")
        sys.exit(1)

    if args.source:
        config["source_catalog"] = args.source
    _resolve_warehouse_id(args, config)

    client = _get_auth_client(args)
    wid = config["sql_warehouse_id"]
    catalog = config["source_catalog"]

    view_deps = get_view_dependencies(client, wid, catalog, args.schema)
    func_deps = get_function_dependencies(client, wid, catalog, args.schema)
    order = get_ordered_views(client, wid, catalog, args.schema)

    if view_deps:
        logger.info(f"\nView dependencies in {catalog}.{args.schema}:")
        for view, deps in view_deps.items():
            dep_str = ", ".join(deps) if deps else "(none)"
            logger.info(f"  {view} -> {dep_str}")

    if func_deps:
        logger.info(f"\nFunction dependencies in {catalog}.{args.schema}:")
        for func, deps in func_deps.items():
            dep_str = ", ".join(deps) if deps else "(none)"
            logger.info(f"  {func} -> {dep_str}")

    if order:
        logger.info(f"\nRecommended creation order: {' -> '.join(order)}")

    if args.output:
        import json
        result = {"views": view_deps, "functions": func_deps, "creation_order": order}
        with open(args.output, "w") as f:
            json.dump(result, f, indent=2)
        logger.info(f"Exported to {args.output}")


def cmd_slack_bot(args):
    """Start the Slack bot."""
    from src.slack_bot import start_slack_bot
    start_slack_bot(config_path=args.config)


def cmd_dep_graph(args):
    """Build and display dependency graph."""
    from src.dependency_graph import build_dependency_graph, export_dependency_graph, print_dependency_graph

    logger = logging.getLogger(__name__)
    try:
        config = load_config(args.config, profile=args.profile)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Config error: {e}")
        sys.exit(1)

    if args.source:
        config["source_catalog"] = args.source
    _resolve_warehouse_id(args, config)

    client = _get_auth_client(args)
    graph = build_dependency_graph(
        client, config["sql_warehouse_id"], config["source_catalog"],
        config.get("exclude_schemas", []),
    )
    print_dependency_graph(graph)

    if args.output:
        export_dependency_graph(graph, args.output)


def cmd_tui(args):
    """Launch the interactive TUI."""
    from src.tui import run_tui
    run_tui(config_path=args.config)


def cmd_templates(args):
    """List or export clone templates."""
    from src.clone_templates import export_template, list_templates

    if args.export:
        export_template(args.export, output_path=args.output)
    else:
        list_templates()


def cmd_distributed_clone(args):
    """Generate or submit a distributed clone notebook."""
    from src.distributed_clone import generate_spark_clone_notebook

    logger = logging.getLogger(__name__)
    try:
        config = load_config(args.config, profile=args.profile)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Config error: {e}")
        sys.exit(1)

    if args.source:
        config["source_catalog"] = args.source
    if args.dest:
        config["destination_catalog"] = args.dest

    generate_spark_clone_notebook(config, output_path=args.output)


def cmd_run_sql(args):
    """Execute an arbitrary SQL statement."""
    from src.client import execute_sql as _execute_sql
    logger = logging.getLogger(__name__)
    client = _get_auth_client(args)
    config = {"sql_warehouse_id": args.warehouse_id or ""}
    _resolve_warehouse_id(args, config, client)
    warehouse_id = config["sql_warehouse_id"]
    try:
        rows = _execute_sql(client, warehouse_id, args.sql)
        if rows:
            for row in rows:
                print(row)
        else:
            logger.info("Statement executed successfully (no rows returned)")
    except Exception as e:
        logger.error(f"SQL execution failed: {e}")
        sys.exit(1)


def cmd_warehouse(args):
    """Manage SQL warehouse."""
    from src.warehouse_autoscale import ensure_warehouse_running, get_warehouse_status, scale_warehouse

    logger = logging.getLogger(__name__)
    try:
        config = load_config(args.config, profile=args.profile)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Config error: {e}")
        sys.exit(1)

    client = _get_auth_client(args)
    wid = args.warehouse_id or config.get("sql_warehouse_id")
    if not wid:
        wid = select_warehouse(client)

    if args.action == "status":
        status = get_warehouse_status(client, wid)
        for k, v in status.items():
            logger.info(f"  {k}: {v}")
    elif args.action == "start":
        ensure_warehouse_running(client, wid)
    elif args.action == "scale":
        scale_warehouse(client, wid, new_size=args.size)


def cmd_state(args):
    """Manage clone state store."""
    from src.state_store import StateStore

    logger = logging.getLogger(__name__)
    try:
        config = load_config(args.config, profile=args.profile)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Config error: {e}")
        sys.exit(1)

    if args.source:
        config["source_catalog"] = args.source
    if args.dest:
        config["destination_catalog"] = args.dest
    _resolve_warehouse_id(args, config)

    client = _get_auth_client(args)
    store = StateStore(client, config["sql_warehouse_id"])

    if args.action == "init":
        store.init_tables()
    elif args.action == "summary":
        store.get_summary(config["source_catalog"], config["destination_catalog"])
    elif args.action == "stale":
        tables = store.get_stale_tables(config["source_catalog"], config["destination_catalog"])
        logger.info(f"Stale tables: {len(tables)}")
        for t in tables:
            logger.info(f"  {t['source_fqn']}")
    elif args.action == "failed":
        tables = store.get_failed_tables(config["source_catalog"], config["destination_catalog"])
        logger.info(f"Failed tables: {len(tables)}")
        for t in tables:
            logger.info(f"  {t['source_fqn']}: {t.get('error_message', '')}")
    elif args.action == "mark-stale":
        store.mark_all_stale(config["source_catalog"], config["destination_catalog"])
    elif args.action == "operations":
        ops = store.get_operations(limit=args.limit)
        for op in ops:
            logger.info(
                f"  {op.get('operation_id', 'N/A')} | {op.get('started_at', '')} | "
                f"{op.get('status', '')} | {op.get('tables_cloned', 0)}/{op.get('tables_failed', 0)}"
            )


def cmd_auth(args):
    """Check authentication status, login via browser, or list profiles."""
    logger = logging.getLogger(__name__)
    host = getattr(args, "host", None)
    token = getattr(args, "token", None)
    auth_profile = getattr(args, "auth_profile", None)

    # --list-profiles: show available CLI profiles
    if getattr(args, "list_profiles", False):
        profiles = list_profiles()
        if not profiles:
            print("  No profiles found in ~/.databrickscfg")
            print("  Run: clxs auth --login --host <workspace-url>")
            return
        print("  Available Databricks CLI profiles:")
        print(f"  {'Name':<20} {'Host':<50} {'Auth Type'}")
        print(f"  {'-'*20} {'-'*50} {'-'*15}")
        for p in profiles:
            print(f"  {p['name']:<20} {p['host']:<50} {p['auth_type']}")
        return

    # --login: browser-based OAuth login (like `az login`)
    if getattr(args, "login", False):
        # If --host not provided, fall through to interactive flow
        if host:
            try:
                username = ensure_logged_in(host=host, force=True)
                print(f"  Logged in as: {username}")
            except RuntimeError as e:
                logger.error(str(e))
                sys.exit(1)
            return
        # No --host: run full interactive flow
        try:
            interactive_login()
        except SystemExit:
            pass
        return

    # Default: verify and show current auth status
    try:
        info = ensure_authenticated(host, token, auth_profile)
        print("  Status:      Authenticated")
        print(f"  User:        {info['user']}")
        print(f"  Host:        {info['host']}")
        print(f"  Auth method: {info['auth_method']}")
    except RuntimeError as e:
        logger.error(str(e))
        sys.exit(1)


def cmd_multi_cloud(args):
    """List configured workspaces."""
    from src.multi_cloud import list_workspaces

    logger = logging.getLogger(__name__)
    try:
        config = load_config(args.config, profile=args.profile)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Config error: {e}")
        sys.exit(1)

    list_workspaces(config)


def cmd_plan(args):
    """Execute the plan (enhanced dry-run) command."""
    from src.dry_run import build_execution_plan, output_plan

    logger = logging.getLogger(__name__)
    try:
        config = load_config(args.config, profile=args.profile)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Config error: {e}")
        sys.exit(1)

    if args.source:
        config["source_catalog"] = args.source
    if args.dest:
        config["destination_catalog"] = args.dest

    client = _get_auth_client(args)
    _resolve_warehouse_id(args, config, client)

    plan = build_execution_plan(client, config)
    output_plan(plan, fmt=args.format, output_path=getattr(args, "output", None))

    # --capture-sql: write all SQL statements to a .sql file
    capture_sql = getattr(args, "capture_sql", None)
    if capture_sql:
        statements = plan.get("sql_statements", [])
        with open(capture_sql, "w") as f:
            f.write("-- Clone-Xs Execution Plan\n")
            f.write(f"-- Source: {config.get('source_catalog')} -> Destination: {config.get('destination_catalog')}\n")
            f.write(f"-- Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"-- Total statements: {len(statements)}\n\n")
            for i, stmt in enumerate(statements, 1):
                sql = stmt if isinstance(stmt, str) else stmt.get("sql", str(stmt))
                cat = stmt.get("category", "SQL") if isinstance(stmt, dict) else "SQL"
                f.write(f"-- [{cat}] Statement {i}\n{sql};\n\n")
        logger.info(f"SQL statements saved to {capture_sql} ({len(statements)} statements)")


def cmd_lint(args):
    """Execute the config lint command."""
    from src.config_lint import lint_config, format_lint_results, lint_has_errors

    logger = logging.getLogger(__name__)
    try:
        config = load_config(args.config, profile=getattr(args, "profile", None))
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Config error: {e}")
        sys.exit(1)

    results = lint_config(config)
    print(format_lint_results(results))

    if getattr(args, "strict", False) and lint_has_errors(results):
        sys.exit(1)


def cmd_usage_analysis(args):
    """Execute the usage analysis command."""
    from src.usage_analysis import (
        query_table_access_patterns, analyze_usage,
        format_usage_report, export_usage_json, recommend_skip_tables,
    )

    logger = logging.getLogger(__name__)
    try:
        config = load_config(args.config, profile=args.profile)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Config error: {e}")
        sys.exit(1)

    if args.source:
        config["source_catalog"] = args.source

    client = _get_auth_client(args)
    _resolve_warehouse_id(args, config, client)

    days = args.days
    unused_days = args.unused_days
    source = config["source_catalog"]
    warehouse_id = config["sql_warehouse_id"]

    if args.recommend:
        skip = recommend_skip_tables(client, warehouse_id, source,
                                     config["exclude_schemas"], days, unused_days)
        print(f"\nRecommended tables to skip ({len(skip)}):")
        for t in skip:
            print(f"  - {t}")
    else:
        access_data = query_table_access_patterns(client, warehouse_id, source, days)
        analysis = analyze_usage(access_data, unused_days)
        print(format_usage_report(analysis))

        if args.output:
            export_usage_json(analysis, args.output)


def cmd_preview(args):
    """Execute the preview command."""
    from src.preview import preview_comparison, preview_catalog, format_side_by_side

    logger = logging.getLogger(__name__)
    try:
        config = load_config(args.config, profile=args.profile)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Config error: {e}")
        sys.exit(1)

    if args.source:
        config["source_catalog"] = args.source
    if args.dest:
        config["destination_catalog"] = args.dest

    client = _get_auth_client(args)
    _resolve_warehouse_id(args, config, client)

    source = config["source_catalog"]
    dest = config["destination_catalog"]
    warehouse_id = config["sql_warehouse_id"]
    limit = args.limit
    order_by = args.order_by

    table = args.table
    if table:
        parts = table.split(".")
        if len(parts) == 2:
            comparison = preview_comparison(client, warehouse_id, source, dest,
                                            parts[0], parts[1], limit=limit, order_by=order_by)
            print(format_side_by_side(comparison))
    elif getattr(args, "all", False):
        max_tables = args.max_tables
        results = preview_catalog(client, warehouse_id, source, dest,
                                   config["exclude_schemas"], limit=limit, max_tables=max_tables)
        for r in results:
            print(format_side_by_side(r))
    else:
        logger.error("Specify --table schema.table or --all")
        sys.exit(1)


def cmd_metrics(args):
    """Execute the metrics command."""
    from src.metrics import query_metrics_history, format_metrics_report

    logger = logging.getLogger(__name__)
    try:
        config = load_config(args.config, profile=args.profile)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Config error: {e}")
        sys.exit(1)

    client = _get_auth_client(args)
    _resolve_warehouse_id(args, config, client)

    warehouse_id = config["sql_warehouse_id"]
    table_fqn = config.get("metrics_table", "clone_audit.metrics.clone_metrics")
    limit = args.limit
    source_filter = args.source

    if args.init:
        from src.metrics import save_metrics_delta
        save_metrics_delta(client, warehouse_id, {}, table_fqn)
        print(f"Metrics table initialized: {table_fqn}")
    else:
        history = query_metrics_history(client, warehouse_id, table_fqn, source_filter, limit)
        if args.format == "json":
            import json
            print(json.dumps(history, indent=2, default=str))
        else:
            print(format_metrics_report(history))


def cmd_history(args):
    """Execute the history command."""
    from src.clone_history import CloneHistory

    logger = logging.getLogger(__name__)
    try:
        config = load_config(args.config, profile=args.profile)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Config error: {e}")
        sys.exit(1)

    client = _get_auth_client(args)
    _resolve_warehouse_id(args, config, client)

    history = CloneHistory(client, config["sql_warehouse_id"], config)

    action = args.action
    if action == "list":
        ops = history.list_operations(limit=args.limit,
                                       source_catalog=args.source)
        print(history.format_log(ops))
    elif action == "show":
        if not args.ids:
            logger.error("Provide an operation ID: history show <id>")
            sys.exit(1)
        op = history.show_operation(args.ids[0])
        if op:
            import json
            print(json.dumps(op, indent=2, default=str))
        else:
            print("Operation not found.")
    elif action == "diff":
        if not args.ids or len(args.ids) < 2:
            logger.error("Provide two operation IDs: history diff <id1> <id2>")
            sys.exit(1)
        diff = history.diff_operations(args.ids[0], args.ids[1])
        print(history.format_diff(diff))


def cmd_ttl(args):
    """Execute the TTL management command."""
    from src.ttl_manager import TTLManager, format_ttl_report

    logger = logging.getLogger(__name__)
    try:
        config = load_config(args.config, profile=args.profile)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Config error: {e}")
        sys.exit(1)

    client = _get_auth_client(args)
    _resolve_warehouse_id(args, config, client)

    mgr = TTLManager(client, config["sql_warehouse_id"])
    mgr.init_ttl_table()

    action = args.action
    if action == "set":
        if not args.dest or not args.days:
            logger.error("Provide --dest and --days for ttl set")
            sys.exit(1)
        mgr.set_ttl(args.dest, args.days)
    elif action == "check":
        policies = mgr.list_all()
        print(format_ttl_report(policies))
    elif action == "cleanup":
        result = mgr.cleanup_expired(confirm=args.confirm, dry_run=args.dry_run_ttl)
        print(f"Cleanup result: {result}")
    elif action == "extend":
        if not args.dest or not args.days:
            logger.error("Provide --dest and --days for ttl extend")
            sys.exit(1)
        mgr.extend_ttl(args.dest, args.days)
    elif action == "remove":
        if not args.dest:
            logger.error("Provide --dest for ttl remove")
            sys.exit(1)
        mgr.remove_ttl(args.dest)


def cmd_rbac(args):
    """Execute the RBAC command."""
    from src.rbac import load_rbac_policy, print_policy, print_user_permissions, get_current_user

    logger = logging.getLogger(__name__)
    try:
        config = load_config(args.config, profile=args.profile)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Config error: {e}")
        sys.exit(1)

    policy_path = config.get("rbac_policy_path", "~/.clone-xs/rbac_policy.yaml")
    policy = load_rbac_policy(policy_path)

    action = args.action
    if action == "show":
        print_policy(policy)
    elif action == "check":
        client = _get_auth_client(args)
        user = args.user or get_current_user(client)
        print_user_permissions(policy, user)


def cmd_approval(args):
    """Execute the approval workflow command."""
    from src.approval import (
        approve_request, deny_request, list_pending_requests,
        check_approval_status,
    )

    logger = logging.getLogger(__name__)

    action = args.action
    if action == "list":
        pending = list_pending_requests()
        if not pending:
            print("No pending approval requests.")
        else:
            for r in pending:
                print(f"  {r.request_id}: {r.source_catalog} -> {r.dest_catalog} "
                      f"(by {r.requested_by}, {r.requested_at})")
    elif action == "approve":
        if not args.request_id:
            logger.error("Provide a request ID")
            sys.exit(1)
        user = getattr(args, "user", "cli")
        approve_request(args.request_id, user)
    elif action == "deny":
        if not args.request_id:
            logger.error("Provide a request ID")
            sys.exit(1)
        reason = getattr(args, "reason", "")
        deny_request(args.request_id, "cli", reason)
    elif action == "status":
        if not args.request_id:
            logger.error("Provide a request ID")
            sys.exit(1)
        req = check_approval_status(args.request_id)
        if req:
            import json
            from dataclasses import asdict
            print(json.dumps(asdict(req), indent=2, default=str))
        else:
            print("Request not found.")


def cmd_impact(args):
    """Execute the impact analysis command."""
    from src.impact_analysis import analyze_impact, print_impact_report

    logger = logging.getLogger(__name__)
    try:
        config = load_config(args.config, profile=args.profile)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Config error: {e}")
        sys.exit(1)

    if args.source:
        config["source_catalog"] = args.source
    catalog = getattr(args, "dest", None) or config.get("destination_catalog", "")
    if not catalog:
        catalog = config.get("source_catalog", "")

    client = _get_auth_client(args)
    _resolve_warehouse_id(args, config, client)

    config["impact_high_threshold"] = args.threshold
    impact = analyze_impact(client, config["sql_warehouse_id"], catalog, config)
    print_impact_report(impact)


def cmd_compliance_report(args):
    """Execute the compliance report command."""
    from src.compliance import generate_compliance_report

    logger = logging.getLogger(__name__)
    try:
        config = load_config(args.config, profile=args.profile)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Config error: {e}")
        sys.exit(1)

    client = _get_auth_client(args)
    _resolve_warehouse_id(args, config, client)

    result = generate_compliance_report(
        client, config["sql_warehouse_id"], config,
        from_date=getattr(args, "from_date", None),
        to_date=getattr(args, "to_date", None),
        output_dir=getattr(args, "output_dir", "reports/compliance"),
        output_format=getattr(args, "format", "all"),
    )
    for fmt, path in result.get("paths", {}).items():
        print(f"  {fmt}: {path}")


def cmd_plugin(args):
    """Execute the plugin management command."""
    from src.plugin_registry import PluginRegistry, format_plugin_list, list_plugins, toggle_plugin

    logger = logging.getLogger(__name__)

    try:
        config = load_config(args.config, profile=getattr(args, "profile", None))
    except Exception:
        config = {}

    registry = PluginRegistry(
        plugin_dir=config.get("plugin_dir", "~/.clone-xs/plugins"),
        registry_url=config.get("plugin_registry_url"),
    )

    action = args.action
    if action == "list":
        if getattr(args, "available", False):
            plugins = registry.list_available()
            print(format_plugin_list(plugins, "Available Plugins"))
        elif getattr(args, "installed", False):
            plugins = registry.list_installed()
            print(format_plugin_list(plugins, "Installed Plugins"))
        else:
            plugins = list_plugins()
            for p in plugins:
                status = "enabled" if p["enabled"] else "disabled"
                print(f"  {p['id']:30s} {p['version']:10s} [{p['type']:10s}] {status:10s} {p['description']}")
    elif action == "enable":
        if not args.name:
            logger.error("Provide a plugin name to enable")
            sys.exit(1)
        result = toggle_plugin(args.name, enabled=True)
        print(f"Plugin '{args.name}' enabled.")
    elif action == "disable":
        if not args.name:
            logger.error("Provide a plugin name to disable")
            sys.exit(1)
        result = toggle_plugin(args.name, enabled=False)
        print(f"Plugin '{args.name}' disabled.")
    elif action == "install":
        if not args.name:
            logger.error("Provide a plugin name to install")
            sys.exit(1)
        path = registry.install(args.name)
        print(f"Plugin installed: {path}")
    elif action == "remove":
        if not args.name:
            logger.error("Provide a plugin name to remove")
            sys.exit(1)
        registry.remove(args.name)
    elif action == "info":
        if not args.name:
            logger.error("Provide a plugin name")
            sys.exit(1)
        import json
        info = registry.info(args.name)
        print(json.dumps(info, indent=2, default=str))
    elif action == "update":
        print("Plugin update not yet implemented for remote registry.")


def cmd_schedule(args):
    """Execute the schedule command."""
    from src.scheduler import parse_interval, parse_cron, schedule_loop

    logger = logging.getLogger(__name__)
    try:
        config = load_config(args.config, profile=args.profile)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Config error: {e}")
        sys.exit(1)

    if args.source:
        config["source_catalog"] = args.source
    if args.dest:
        config["destination_catalog"] = args.dest
    if getattr(args, "no_drift_check", False):
        config["drift_check_before_clone"] = False

    client = _get_auth_client(args)
    _resolve_warehouse_id(args, config, client)

    interval = getattr(args, "interval", None)
    cron = getattr(args, "cron", None)
    max_runs = getattr(args, "max_runs", 0)

    if interval:
        interval_seconds = parse_interval(interval)
    elif cron:
        interval_seconds = parse_cron(cron)
    else:
        logger.error("Provide --interval or --cron")
        sys.exit(1)

    schedule_loop(client, config, interval_seconds, max_runs=max_runs)


def cmd_serve(args):
    """Execute the API server command."""
    from src.api_server import start_server

    logger = logging.getLogger(__name__)
    try:
        config = load_config(args.config, profile=args.profile)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Config error: {e}")
        sys.exit(1)

    client = _get_auth_client(args)
    _resolve_warehouse_id(args, config, client)

    host = getattr(args, "host_addr", None) or config.get("api_host", "0.0.0.0")
    port = getattr(args, "port", None) or config.get("api_port", 8080)
    api_key = getattr(args, "api_key", None) or config.get("api_key")

    start_server(host=host, port=port, config=config, client=client, api_key=api_key)


def cmd_demo_data(args):
    """Generate a demo catalog with synthetic data across multiple industries."""
    try:
        config = load_config(args.config, profile=args.profile)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Config error: {e}")
        sys.exit(1)

    client = _get_auth_client(args)
    _resolve_warehouse_id(args, config, client)

    from src.demo_generator import generate_demo_catalog, cleanup_demo_catalog, ALL_INDUSTRIES

    catalog_name = args.catalog

    # Handle cleanup mode
    if getattr(args, "cleanup", False):
        result = cleanup_demo_catalog(client, config["sql_warehouse_id"], catalog_name)
        print(f"\nCatalog '{catalog_name}' cleaned up: {result['schemas_dropped']} schemas, {result['tables_dropped']} objects dropped")
        if result["errors"]:
            for err in result["errors"]:
                print(f"  Error: {err}")
        return
    industries = args.industry if args.industry else None
    scale_factor = args.scale if hasattr(args, "scale") else 1.0
    batch_size_val = args.batch_size if hasattr(args, "batch_size") else 5_000_000
    max_workers_val = args.max_workers if hasattr(args, "max_workers") and args.max_workers else config.get("max_workers", 4)
    storage_loc = getattr(args, "storage_location", None)
    drop_existing = getattr(args, "drop_existing", False)
    medallion = not getattr(args, "no_medallion", False)
    owner = getattr(args, "owner", None)

    result = generate_demo_catalog(
        client, config["sql_warehouse_id"], catalog_name,
        industries=industries,
        owner=owner,
        scale_factor=scale_factor,
        batch_size=batch_size_val,
        max_workers=max_workers_val,
        storage_location=storage_loc,
        drop_existing=drop_existing,
        medallion=medallion,
        start_date=getattr(args, "start_date", "2020-01-01"),
        end_date=getattr(args, "end_date", "2025-01-01"),
    )

    # Multi-catalog: clone to destination if specified
    dest_catalog = getattr(args, "dest_catalog", None)
    if dest_catalog and result.get("catalog"):
        logger.info(f"Cloning {catalog_name} → {dest_catalog}...")
        from src.clone_catalog import clone_catalog
        clone_config = dict(config)
        clone_config["source_catalog"] = catalog_name
        clone_config["destination_catalog"] = dest_catalog
        clone_config["clone_type"] = "DEEP"
        clone_config["load_type"] = "FULL"
        try:
            clone_result = clone_catalog(client, clone_config)
            print(f"\nCloned to {dest_catalog}: {clone_result.get('tables', {}).get('success', 0)} tables")
        except Exception as e:
            print(f"\nClone to {dest_catalog} failed: {e}")

    print(f"\n{'='*60}")
    print(f"Demo catalog '{result['catalog']}' generated successfully!")
    print(f"  Industries: {', '.join(result['industries'])}")
    print(f"  Schemas:    {result['schemas_created']}")
    print(f"  Tables:     {result['tables_created']}")
    print(f"  Views:      {result['views_created']}")
    print(f"  UDFs:       {result['udfs_created']}")
    print(f"  Total rows: {result['total_rows']:,}")
    print(f"  Duration:   {result['elapsed_seconds']}s")
    if result["errors"]:
        print(f"  Errors:     {len(result['errors'])}")
        for err in result["errors"]:
            print(f"    - {err}")
    print(f"{'='*60}")


def build_parser() -> argparse.ArgumentParser:
    """Build the argument parser for the CLI. Separated for testability."""
    parser = argparse.ArgumentParser(
        description="Unity Catalog clone utility for Databricks."
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # --- auth command ---
    auth_parser = subparsers.add_parser("auth", help="Check authentication status")
    auth_parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose/debug logging")
    auth_parser.add_argument("--list-profiles", action="store_true", help="List available CLI profiles from ~/.databrickscfg")
    add_auth_args(auth_parser)
    auth_parser.set_defaults(func=cmd_auth)

    # --- clone command ---
    clone_parser = subparsers.add_parser("clone", help="Clone a catalog from source to destination")
    add_common_args(clone_parser)
    clone_parser.add_argument("--source", help="Override source catalog name")
    clone_parser.add_argument("--dest", help="Override destination catalog name")
    clone_parser.add_argument("--clone-type", choices=["DEEP", "SHALLOW"], help="Clone type")
    clone_parser.add_argument("--load-type", choices=["FULL", "INCREMENTAL"], help="Load type")
    clone_parser.add_argument("--max-workers", type=int, help="Max parallel workers for schemas")
    clone_parser.add_argument("--no-permissions", action="store_true", help="Skip copying permissions")
    clone_parser.add_argument("--no-ownership", action="store_true", help="Skip copying ownership")
    clone_parser.add_argument("--no-tags", action="store_true", help="Skip copying tags")
    clone_parser.add_argument("--no-properties", action="store_true", help="Skip copying table properties")
    clone_parser.add_argument("--no-security", action="store_true", help="Skip copying row/column security")
    clone_parser.add_argument("--no-constraints", action="store_true", help="Skip copying CHECK constraints")
    clone_parser.add_argument("--no-comments", action="store_true", help="Skip copying table/column comments")
    clone_parser.add_argument("--dry-run", action="store_true", help="Preview without executing writes")
    clone_parser.add_argument("--include-schemas", nargs="+", help="Only clone these schemas")
    clone_parser.add_argument("--report", action="store_true", help="Generate JSON/HTML report")
    clone_parser.add_argument("--enable-rollback", action="store_true", help="Enable rollback logging")
    clone_parser.add_argument("--validate", action="store_true", help="Run validation after clone")
    clone_parser.add_argument("--checksum", action="store_true", help="Use checksum validation (slower)")
    clone_parser.add_argument("--parallel-tables", type=int, help="Parallel workers for tables within a schema")
    clone_parser.add_argument("--include-tables-regex", help="Only clone tables matching this regex")
    clone_parser.add_argument("--exclude-tables-regex", help="Exclude tables matching this regex")
    clone_parser.add_argument("--resume", help="Resume from a previous rollback log file")
    clone_parser.add_argument("--progress", action="store_true", help="Show progress bar")
    clone_parser.add_argument("--no-progress", action="store_true", help="Disable progress bar")
    clone_parser.add_argument("--order-by-size", choices=["asc", "desc"], help="Clone tables by size order")
    clone_parser.add_argument("--max-rps", type=float, help="Max SQL requests per second (rate limit)")
    clone_parser.add_argument("--dest-host", help="Destination workspace host (cross-workspace)")
    clone_parser.add_argument("--dest-token", help="Destination workspace token (cross-workspace)")
    clone_parser.add_argument("--dest-warehouse-id", help="Destination SQL warehouse ID (cross-workspace)")
    clone_parser.add_argument("--as-of-timestamp", help="Clone tables as of this timestamp (Delta time travel)")
    clone_parser.add_argument("--as-of-version", type=int, help="Clone tables as of this version (Delta time travel)")
    clone_parser.add_argument("--location", help="Managed storage location for the destination catalog (required if workspace has no metastore root)")
    clone_parser.add_argument("--template", help="Apply a clone template (e.g., dev-refresh, dr-replica)")
    clone_parser.add_argument("--where", help="Global WHERE filter for all tables (deep clone only)")
    clone_parser.add_argument("--table-filter", action="append", help="Per-table WHERE filter: 'schema.table:condition' (repeatable)")
    clone_parser.add_argument("--auto-rollback", action="store_true", help="Auto-rollback if post-clone validation fails")
    clone_parser.add_argument("--rollback-threshold", type=float, default=None, help="Max allowed mismatch %% before auto-rollback (default: 5)")
    clone_parser.add_argument("--throttle", choices=["low", "medium", "high", "max"], help="Throttle profile for resource control")
    clone_parser.add_argument("--checkpoint", action="store_true", help="Enable periodic checkpointing")
    clone_parser.add_argument("--resume-from-checkpoint", help="Resume from a checkpoint file")
    clone_parser.add_argument("--require-approval", action="store_true", help="Require approval before cloning")
    clone_parser.add_argument("--impact-check", action="store_true", help="Run impact analysis before cloning")
    clone_parser.add_argument("--ttl", help="Set TTL on destination (e.g., 7d, 30d, 2w)")
    clone_parser.add_argument("--skip-unused", action="store_true", help="Skip tables with no recent queries")
    clone_parser.add_argument("--schema-only", action="store_true", help="Create empty tables (structure only, no data) with all other artifacts (views, functions, volumes, permissions)")
    clone_parser.set_defaults(func=cmd_clone)

    # --- diff command ---
    diff_parser = subparsers.add_parser("diff", help="Compare source and destination catalogs")
    add_common_args(diff_parser)
    diff_parser.add_argument("--source", help="Override source catalog name")
    diff_parser.add_argument("--dest", help="Override destination catalog name")
    diff_parser.set_defaults(func=cmd_diff)

    # --- compare command ---
    cmp_parser = subparsers.add_parser("compare", help="Deep column-level comparison of catalogs")
    add_common_args(cmp_parser)
    cmp_parser.add_argument("--source", help="Override source catalog name")
    cmp_parser.add_argument("--dest", help="Override destination catalog name")
    cmp_parser.set_defaults(func=cmd_compare)

    # --- rollback command ---
    rb_parser = subparsers.add_parser("rollback", help="Rollback a previous clone operation")
    add_common_args(rb_parser)
    rb_parser.add_argument("--rollback-log", dest="rollback_log_file", help="Path to rollback log JSON file")
    rb_parser.add_argument("--list", action="store_true", help="List available rollback logs")
    rb_parser.add_argument("--drop-catalog", action="store_true", help="Also drop the destination catalog")
    rb_parser.set_defaults(func=cmd_rollback)

    # --- validate command ---
    val_parser = subparsers.add_parser("validate", help="Validate clone by comparing row counts")
    add_common_args(val_parser)
    val_parser.add_argument("--source", help="Override source catalog name")
    val_parser.add_argument("--dest", help="Override destination catalog name")
    val_parser.add_argument("--checksum", action="store_true", help="Include hash-based checksum validation")
    val_parser.set_defaults(func=cmd_validate)

    # --- estimate command ---
    est_parser = subparsers.add_parser("estimate", help="Estimate storage cost for a deep clone")
    add_common_args(est_parser)
    est_parser.add_argument("--source", "--catalog", dest="source", help="Override source catalog name")
    est_parser.add_argument("--price-per-gb", type=float, default=0.023, help="Storage price $/GB/month")
    est_parser.set_defaults(func=cmd_estimate)

    # --- generate-workflow command ---
    wf_parser = subparsers.add_parser("generate-workflow", help="Generate Databricks Workflows job definition")
    add_common_args(wf_parser)
    wf_parser.add_argument("--format", choices=["json", "yaml"], default="json", help="Output format")
    wf_parser.add_argument("--output", help="Output file path")
    wf_parser.add_argument("--job-name", help="Job name")
    wf_parser.add_argument("--cluster-id", help="Existing cluster ID")
    wf_parser.add_argument("--schedule", help="Quartz cron expression for scheduling")
    wf_parser.add_argument("--notification-email", help="Email for job notifications")
    wf_parser.set_defaults(func=cmd_generate_workflow)

    # --- create-job command ---
    cj_parser = subparsers.add_parser("create-job", help="Create a persistent Databricks Job for scheduled cloning")
    add_common_args(cj_parser)
    cj_parser.add_argument("--source", help="Source catalog name")
    cj_parser.add_argument("--dest", help="Destination catalog name")
    cj_parser.add_argument("--job-name", help="Job name (default: Clone-Xs: <source> -> <dest>)")
    cj_parser.add_argument("--schedule", help="Quartz cron expression (e.g. '0 0 6 * * ?')")
    cj_parser.add_argument("--timezone", default="UTC", help="Schedule timezone (default: UTC)")
    cj_parser.add_argument("--notification-email", help="Comma-separated emails for job notifications")
    cj_parser.add_argument("--max-retries", type=int, default=0, help="Number of retries on failure (default: 0)")
    cj_parser.add_argument("--timeout", type=int, default=7200, help="Job timeout in seconds (default: 7200)")
    cj_parser.add_argument("--tag", action="append", default=[], help="Job tag as key=value (repeatable)")
    cj_parser.add_argument("--update-job-id", type=int, help="Update an existing job instead of creating a new one")
    cj_parser.add_argument("--run-now", action="store_true", help="Run the job immediately after creation")
    cj_parser.set_defaults(func=cmd_create_job)

    # --- sync command ---
    sync_parser = subparsers.add_parser("sync", help="Two-way sync: add missing objects, optionally drop extras")
    add_common_args(sync_parser)
    sync_parser.add_argument("--source", help="Override source catalog name")
    sync_parser.add_argument("--dest", help="Override destination catalog name")
    sync_parser.add_argument("--dry-run", action="store_true", help="Preview without executing")
    sync_parser.add_argument("--drop-extra", action="store_true", help="Drop objects in dest that don't exist in source")
    sync_parser.set_defaults(func=cmd_sync)

    # --- snapshot command ---
    snap_parser = subparsers.add_parser("snapshot", help="Export catalog metadata to a JSON manifest")
    add_common_args(snap_parser)
    snap_parser.add_argument("--source", "--catalog", dest="source", help="Override source catalog name")
    snap_parser.add_argument("--output", help="Output file path")
    snap_parser.set_defaults(func=cmd_snapshot)

    # --- schema-drift command ---
    drift_parser = subparsers.add_parser("schema-drift", help="Detect schema drift between catalogs")
    add_common_args(drift_parser)
    drift_parser.add_argument("--source", help="Override source catalog name")
    drift_parser.add_argument("--dest", help="Override destination catalog name")
    drift_parser.set_defaults(func=cmd_schema_drift)

    # --- init command ---
    init_parser = subparsers.add_parser("init", help="Interactive config wizard")
    init_parser.add_argument("--output", default="config/clone_config.yaml", help="Output config path")
    init_parser.set_defaults(func=cmd_init)

    # --- export-iac command ---
    tf_parser = subparsers.add_parser("export-iac", help="Generate Terraform or Pulumi config from catalog")
    add_common_args(tf_parser)
    tf_parser.add_argument("--source", help="Override source catalog name")
    tf_parser.add_argument("--format", choices=["terraform", "pulumi"], default="terraform", help="IaC format")
    tf_parser.add_argument("--output", help="Output file path")
    tf_parser.set_defaults(func=cmd_terraform)

    # --- preflight command ---
    pf_parser = subparsers.add_parser("preflight", help="Run pre-flight checks before cloning")
    add_common_args(pf_parser)
    pf_parser.add_argument("--source", help="Override source catalog name")
    pf_parser.add_argument("--dest", help="Override destination catalog name")
    pf_parser.add_argument("--no-write-check", action="store_true", help="Skip write permission check")
    pf_parser.set_defaults(func=cmd_preflight)

    # --- search command ---
    search_parser = subparsers.add_parser("search", help="Search for tables and columns by pattern")
    add_common_args(search_parser)
    search_parser.add_argument("--source", "--catalog", dest="source", help="Override source catalog name")
    search_parser.add_argument("--pattern", required=True, help="Regex pattern to search for")
    search_parser.add_argument("--columns", action="store_true", help="Also search column names")
    search_parser.set_defaults(func=cmd_search)

    # --- stats command ---
    stats_parser = subparsers.add_parser("stats", help="Show catalog statistics (sizes, row counts)")
    add_common_args(stats_parser)
    stats_parser.add_argument("--source", "--catalog", dest="source", help="Override source catalog name")
    stats_parser.set_defaults(func=cmd_stats)

    # --- storage-metrics command ---
    sm_parser = subparsers.add_parser(
        "storage-metrics",
        help="Analyze storage metrics (active, vacuumable, time-travel) for all tables",
    )
    add_common_args(sm_parser)
    sm_parser.add_argument("--source", "--catalog", dest="source", help="Override source catalog name")
    sm_parser.add_argument("--schema", help="Filter to a specific schema")
    sm_parser.add_argument("--table", help="Filter to a specific table (requires --schema)")
    sm_parser.set_defaults(func=cmd_storage_metrics)

    # --- optimize command ---
    opt_parser = subparsers.add_parser("optimize", help="Run OPTIMIZE on tables to compact small files")
    add_common_args(opt_parser)
    opt_parser.add_argument("--source", "--catalog", dest="source", help="Override source catalog name")
    opt_parser.add_argument("--schema", help="Filter to a specific schema")
    opt_parser.add_argument("--table", help="Filter to a specific table (requires --schema)")
    opt_parser.add_argument("--dry-run", action="store_true", help="Preview without executing")
    opt_parser.set_defaults(func=cmd_optimize)

    # --- vacuum command ---
    vac_parser = subparsers.add_parser("vacuum", help="Run VACUUM on tables to reclaim storage from old files")
    add_common_args(vac_parser)
    vac_parser.add_argument("--source", "--catalog", dest="source", help="Override source catalog name")
    vac_parser.add_argument("--schema", help="Filter to a specific schema")
    vac_parser.add_argument("--table", help="Filter to a specific table (requires --schema)")
    vac_parser.add_argument("--retention-hours", type=int, default=168, help="Data retention in hours (default: 168 = 7 days)")
    vac_parser.add_argument("--dry-run", action="store_true", help="Preview without executing")
    vac_parser.set_defaults(func=cmd_vacuum)

    # --- profile command ---
    prof_parser = subparsers.add_parser("profile", help="Profile table data quality (nulls, distinct, min/max)")
    add_common_args(prof_parser)
    prof_parser.add_argument("--source", "--catalog", dest="source", help="Override source catalog name")
    prof_parser.add_argument("--output", help="Save profile results to JSON file")
    prof_parser.set_defaults(func=cmd_profile)

    # --- monitor command ---
    mon_parser = subparsers.add_parser("monitor", help="Continuous monitoring of catalog sync status")
    add_common_args(mon_parser)
    mon_parser.add_argument("--source", help="Override source catalog name")
    mon_parser.add_argument("--dest", help="Override destination catalog name")
    mon_parser.add_argument("--interval", type=int, default=30, help="Minutes between checks (default: 30)")
    mon_parser.add_argument("--max-checks", type=int, default=0, help="Max number of checks (0 = infinite)")
    mon_parser.add_argument("--check-drift", action="store_true", default=True, help="Check for schema drift")
    mon_parser.add_argument("--check-counts", action="store_true", help="Check row counts (slower)")
    mon_parser.add_argument("--once", action="store_true", help="Run a single check and exit")
    mon_parser.set_defaults(func=cmd_monitor)

    # --- export command ---
    exp_parser = subparsers.add_parser("export", help="Export catalog metadata to CSV or JSON")
    add_common_args(exp_parser)
    exp_parser.add_argument("--source", "--catalog", dest="source", help="Override source catalog name")
    exp_parser.add_argument("--format", choices=["csv", "json"], default="csv", help="Export format")
    exp_parser.add_argument("--output", help="Output file path")
    exp_parser.set_defaults(func=cmd_export)

    # --- config-diff command ---
    cd_parser = subparsers.add_parser("config-diff", help="Compare two config YAML files")
    cd_parser.add_argument("file_a", help="First config file")
    cd_parser.add_argument("file_b", help="Second config file")
    cd_parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose/debug logging")
    cd_parser.add_argument("--log-file", help="Write logs to a file in addition to console")
    cd_parser.set_defaults(func=cmd_config_diff)

    # --- completion command ---
    comp_parser = subparsers.add_parser("completion", help="Generate shell completion script")
    comp_parser.add_argument("shell", choices=["bash", "zsh", "fish"], help="Shell type")
    comp_parser.set_defaults(func=cmd_completion)

    # --- dashboard command ---
    dash_parser = subparsers.add_parser("dashboard", help="Launch Streamlit web dashboard")
    dash_parser.add_argument("-c", "--config", default="config/clone_config.yaml", help="Config file path")
    dash_parser.add_argument("--port", type=int, default=8501, help="Dashboard port (default: 8501)")
    dash_parser.set_defaults(func=cmd_dashboard)

    # --- generate-dab command ---
    dab_parser = subparsers.add_parser("generate-dab", help="Generate Databricks Asset Bundle for clone jobs")
    add_common_args(dab_parser)
    dab_parser.add_argument("--source", help="Override source catalog name")
    dab_parser.add_argument("--dest", help="Override destination catalog name")
    dab_parser.add_argument("--output", default="dab_bundle", help="Output directory (default: dab_bundle)")
    dab_parser.add_argument("--job-name", help="Job name")
    dab_parser.add_argument("--schedule", help="Quartz cron expression for scheduling")
    dab_parser.add_argument("--notification-email", help="Email for job notifications")
    dab_parser.set_defaults(func=cmd_generate_dab)

    # --- multi-clone command ---
    mc_parser = subparsers.add_parser("multi-clone", help="Clone to multiple destination workspaces in parallel")
    add_common_args(mc_parser)
    mc_parser.add_argument("--source", help="Override source catalog name")
    mc_parser.add_argument("--destinations", required=True, help="YAML file with destination workspace configs")
    mc_parser.add_argument("--max-parallel", type=int, default=2, help="Max parallel workspace clones")
    mc_parser.set_defaults(func=cmd_multi_clone)

    # --- cost-estimate command ---
    ce_parser = subparsers.add_parser("cost-estimate", help="Estimate clone cost (storage + compute)")
    add_common_args(ce_parser)
    ce_parser.add_argument("--source", "--catalog", dest="source", help="Override source catalog name")
    ce_parser.add_argument("--clone-type", choices=["DEEP", "SHALLOW"], help="Clone type for estimate")
    ce_parser.add_argument("--warehouse-type", choices=["serverless", "classic"], default="serverless", help="Warehouse type")
    ce_parser.set_defaults(func=cmd_cost_estimate)

    # --- audit command ---
    audit_parser = subparsers.add_parser("audit", help="Query clone audit trail")
    add_common_args(audit_parser)
    audit_parser.add_argument("--init", action="store_true", help="Initialize audit table")
    audit_parser.add_argument("--source", help="Filter by source catalog")
    audit_parser.add_argument("--status", help="Filter by status")
    audit_parser.add_argument("--limit", type=int, default=20, help="Max results")
    audit_parser.set_defaults(func=cmd_audit)

    # --- lineage command ---
    lin_parser = subparsers.add_parser("lineage", help="Query clone lineage tracking")
    add_common_args(lin_parser)
    lin_parser.add_argument("--init", action="store_true", help="Initialize lineage table")
    lin_parser.add_argument("--table", help="Filter by table FQN")
    lin_parser.add_argument("--operation-id", help="Filter by operation ID")
    lin_parser.add_argument("--limit", type=int, default=50, help="Max results")
    lin_parser.set_defaults(func=cmd_lineage)

    # --- pii-scan command ---
    pii_parser = subparsers.add_parser("pii-scan", help="Scan catalog for PII columns")
    add_common_args(pii_parser)
    pii_parser.add_argument("--source", "--catalog", dest="source", help="Override source catalog name")
    pii_parser.add_argument("--schema-filter", nargs="+", help="Only scan these schemas (e.g. --schema-filter bronze silver)")
    pii_parser.add_argument("--table-filter", help="Regex to filter table names (e.g. 'customer|user')")
    pii_parser.add_argument("--sample-data", action="store_true", help="Sample actual data values (slower)")
    pii_parser.add_argument("--no-exit-code", action="store_true", help="Don't exit with error if PII found")
    pii_parser.add_argument("--read-uc-tags", action="store_true", help="Read UC column tags to enhance detection")
    pii_parser.add_argument("--save-history", action="store_true", help="Save scan results to Delta tables")
    pii_parser.add_argument("--apply-tags", action="store_true", help="Apply PII tags to Unity Catalog after scan")
    pii_parser.add_argument("--tag-prefix", default="pii", help="Prefix for UC tags (default: pii)")
    pii_parser.set_defaults(func=cmd_pii_scan)

    # --- policy-check command ---
    pol_parser = subparsers.add_parser("policy-check", help="Check clone policies (guardrails)")
    add_common_args(pol_parser)
    pol_parser.add_argument("--policy-file", help="Path to policies YAML file")
    pol_parser.set_defaults(func=cmd_policy_check)

    # --- schema-evolve command ---
    se_parser = subparsers.add_parser("schema-evolve", help="Detect and apply schema evolution")
    add_common_args(se_parser)
    se_parser.add_argument("--source", help="Override source catalog name")
    se_parser.add_argument("--dest", help="Override destination catalog name")
    se_parser.add_argument("--dry-run", action="store_true", help="Preview changes without applying")
    se_parser.add_argument("--drop-removed", action="store_true", help="Drop columns removed from source")
    se_parser.set_defaults(func=cmd_schema_evolve)

    # --- dep-graph command ---
    dg_parser = subparsers.add_parser("dep-graph", help="Build and display table/view dependency graph")
    add_common_args(dg_parser)
    dg_parser.add_argument("--source", "--catalog", dest="source", help="Override source catalog name")
    dg_parser.add_argument("--output", help="Export graph to JSON file")
    dg_parser.set_defaults(func=cmd_dep_graph)

    # --- tui command ---
    tui_parser = subparsers.add_parser("tui", help="Launch interactive terminal UI")
    tui_parser.add_argument("-c", "--config", default="config/clone_config.yaml", help="Config file path")
    tui_parser.set_defaults(func=cmd_tui)

    # --- templates command ---
    tmpl_parser = subparsers.add_parser("templates", help="List or export clone templates")
    tmpl_parser.add_argument("--export", help="Export a template by name (e.g., dev-copy, dr-backup)")
    tmpl_parser.add_argument("--output", help="Output file path for export")
    tmpl_parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    tmpl_parser.add_argument("--log-file", help="Log file")
    tmpl_parser.set_defaults(func=cmd_templates)

    # --- distributed-clone command ---
    dc_parser = subparsers.add_parser("distributed-clone", help="Generate a Spark-based distributed clone notebook")
    add_common_args(dc_parser)
    dc_parser.add_argument("--source", help="Override source catalog name")
    dc_parser.add_argument("--dest", help="Override destination catalog name")
    dc_parser.add_argument("--output", default="notebooks/distributed_clone.py", help="Output notebook path")
    dc_parser.set_defaults(func=cmd_distributed_clone)

    # --- warehouse command ---
    wh_parser = subparsers.add_parser("warehouse", help="Manage SQL warehouse (status, start, scale)")
    add_common_args(wh_parser)
    wh_parser.add_argument("action", choices=["status", "start", "scale"], help="Warehouse action")
    wh_parser.add_argument("--size", help="New warehouse size (for scale action)")
    wh_parser.set_defaults(func=cmd_warehouse)

    # --- state command ---
    st_parser = subparsers.add_parser("state", help="Manage clone state store (Delta table)")
    add_common_args(st_parser)
    st_parser.add_argument("action", choices=["init", "summary", "stale", "failed", "mark-stale", "operations"],
                           help="State store action")
    st_parser.add_argument("--source", "--catalog", dest="source", help="Override source catalog")
    st_parser.add_argument("--dest", help="Override destination catalog")
    st_parser.add_argument("--limit", type=int, default=20, help="Max results for operations")
    st_parser.set_defaults(func=cmd_state)

    # --- workspaces command ---
    ws_parser = subparsers.add_parser("workspaces", help="List configured multi-cloud workspaces")
    add_common_args(ws_parser)
    ws_parser.set_defaults(func=cmd_multi_cloud)

    # --- run-sql command ---
    sql_parser = subparsers.add_parser("run-sql", help="Execute a SQL statement against a warehouse")
    add_common_args(sql_parser)
    sql_parser.add_argument("--sql", required=True, help="SQL statement to execute")
    sql_parser.set_defaults(func=cmd_run_sql)

    # ===== NEW FEATURE COMMANDS =====

    # --- plan command (enhanced dry-run) ---
    plan_parser = subparsers.add_parser("plan", help="Generate execution plan (enhanced dry-run)")
    add_common_args(plan_parser)
    plan_parser.add_argument("--source", help="Override source catalog name")
    plan_parser.add_argument("--dest", help="Override destination catalog name")
    plan_parser.add_argument("--format", choices=["console", "json", "html", "sql"], default="console", help="Output format")
    plan_parser.add_argument("--output", help="Output file path")
    plan_parser.add_argument("--capture-sql", dest="capture_sql", help="Save all planned SQL statements to a .sql file")
    plan_parser.set_defaults(func=cmd_plan)

    # --- lint command ---
    lint_parser = subparsers.add_parser("lint", help="Validate and lint the config YAML")
    lint_parser.add_argument("-c", "--config", default="config/clone_config.yaml", help="Config file path")
    lint_parser.add_argument("--profile", help="Config profile to lint")
    lint_parser.add_argument("--strict", action="store_true", help="Treat warnings as errors")
    lint_parser.add_argument("-v", "--verbose", action="store_true")
    lint_parser.add_argument("--log-file", help="Log file")
    lint_parser.set_defaults(func=cmd_lint)

    # --- usage-analysis command ---
    ua_parser = subparsers.add_parser("usage-analysis", help="Analyze table access patterns")
    add_common_args(ua_parser)
    ua_parser.add_argument("--source", "--catalog", dest="source", help="Override source catalog name")
    ua_parser.add_argument("--days", type=int, default=90, help="Lookback period in days")
    ua_parser.add_argument("--unused-days", type=int, default=30, help="Threshold for unused tables")
    ua_parser.add_argument("--recommend", action="store_true", help="Show skip recommendations")
    ua_parser.add_argument("--output", help="Export to JSON file")
    ua_parser.set_defaults(func=cmd_usage_analysis)

    # --- preview command ---
    prev_parser = subparsers.add_parser("preview", help="Side-by-side data preview of source vs destination")
    add_common_args(prev_parser)
    prev_parser.add_argument("--source", help="Override source catalog name")
    prev_parser.add_argument("--dest", help="Override destination catalog name")
    prev_parser.add_argument("--table", help="Preview specific table (schema.table)")
    prev_parser.add_argument("--limit", type=int, default=10, help="Rows per table")
    prev_parser.add_argument("--order-by", help="Column to order by")
    prev_parser.add_argument("--all", action="store_true", help="Preview all tables")
    prev_parser.add_argument("--max-tables", type=int, default=20, help="Max tables for --all")
    prev_parser.set_defaults(func=cmd_preview)

    # --- metrics command ---
    met_parser = subparsers.add_parser("metrics", help="View clone operation metrics")
    add_common_args(met_parser)
    met_parser.add_argument("--init", action="store_true", help="Initialize metrics Delta table")
    met_parser.add_argument("--source", help="Filter by source catalog")
    met_parser.add_argument("--limit", type=int, default=50, help="Max results")
    met_parser.add_argument("--format", choices=["console", "json"], default="console")
    met_parser.set_defaults(func=cmd_metrics)

    # --- history command ---
    hist_parser = subparsers.add_parser("history", help="Git-style clone operation history")
    add_common_args(hist_parser)
    hist_parser.add_argument("action", choices=["list", "show", "diff"], help="History action")
    hist_parser.add_argument("ids", nargs="*", help="Operation ID(s) for show/diff")
    hist_parser.add_argument("--source", help="Filter by source catalog")
    hist_parser.add_argument("--limit", type=int, default=20, help="Max results")
    hist_parser.set_defaults(func=cmd_history)

    # --- ttl command ---
    ttl_parser = subparsers.add_parser("ttl", help="Manage data retention / TTL policies")
    add_common_args(ttl_parser)
    ttl_parser.add_argument("action", choices=["set", "check", "cleanup", "extend", "remove"], help="TTL action")
    ttl_parser.add_argument("--dest", help="Destination catalog")
    ttl_parser.add_argument("--days", type=int, help="TTL in days")
    ttl_parser.add_argument("--confirm", action="store_true", help="Confirm cleanup")
    ttl_parser.add_argument("--dry-run-ttl", action="store_true", help="Preview cleanup only")
    ttl_parser.set_defaults(func=cmd_ttl)

    # --- rbac command ---
    rbac_parser = subparsers.add_parser("rbac", help="RBAC policy management")
    add_common_args(rbac_parser)
    rbac_parser.add_argument("action", choices=["check", "show"], help="RBAC action")
    rbac_parser.add_argument("--user", help="Check permissions for specific user")
    rbac_parser.set_defaults(func=cmd_rbac)

    # --- approval command ---
    appr_parser = subparsers.add_parser("approval", help="Clone approval workflows")
    appr_parser.add_argument("action", choices=["list", "approve", "deny", "status"], help="Approval action")
    appr_parser.add_argument("request_id", nargs="?", help="Approval request ID")
    appr_parser.add_argument("--user", help="User performing approval")
    appr_parser.add_argument("--reason", help="Denial reason")
    appr_parser.add_argument("-c", "--config", default="config/clone_config.yaml")
    appr_parser.add_argument("-v", "--verbose", action="store_true")
    appr_parser.add_argument("--log-file", help="Log file")
    appr_parser.set_defaults(func=cmd_approval)

    # --- impact command ---
    imp_parser = subparsers.add_parser("impact", help="Analyze downstream impact before cloning")
    add_common_args(imp_parser)
    imp_parser.add_argument("--source", help="Source catalog to analyze")
    imp_parser.add_argument("--dest", help="Destination catalog to check dependents")
    imp_parser.add_argument("--threshold", type=int, default=10, help="High-impact threshold")
    imp_parser.set_defaults(func=cmd_impact)

    # --- compliance-report command ---
    comp_parser = subparsers.add_parser("compliance-report", help="Generate compliance report")
    add_common_args(comp_parser)
    comp_parser.add_argument("--from", dest="from_date", help="Start date (YYYY-MM-DD)")
    comp_parser.add_argument("--to", dest="to_date", help="End date (YYYY-MM-DD)")
    comp_parser.add_argument("--format", choices=["json", "html", "all"], default="all")
    comp_parser.add_argument("--output-dir", default="reports/compliance", help="Output directory")
    comp_parser.set_defaults(func=cmd_compliance_report)

    # --- plugin command ---
    plug_parser = subparsers.add_parser("plugin", help="Plugin marketplace (install, list, remove, enable, disable)")
    plug_parser.add_argument("action", choices=["list", "install", "remove", "info", "update", "enable", "disable"])
    plug_parser.add_argument("name", nargs="?", help="Plugin name")
    plug_parser.add_argument("--available", action="store_true", help="Show available plugins")
    plug_parser.add_argument("--installed", action="store_true", help="Show installed plugins")
    plug_parser.add_argument("-c", "--config", default="config/clone_config.yaml")
    plug_parser.add_argument("-v", "--verbose", action="store_true")
    plug_parser.add_argument("--log-file", help="Log file")
    plug_parser.add_argument("--profile", help="Config profile")
    plug_parser.set_defaults(func=cmd_plugin)

    # --- schedule command ---
    sched_parser = subparsers.add_parser("schedule", help="Run clones on a schedule with drift detection")
    add_common_args(sched_parser)
    sched_parser.add_argument("--source", help="Override source catalog name")
    sched_parser.add_argument("--dest", help="Override destination catalog name")
    sched_parser.add_argument("--interval", help="Run interval (e.g., 30m, 1h, 6h)")
    sched_parser.add_argument("--cron", help="Cron expression (e.g., '0 */6 * * *')")
    sched_parser.add_argument("--no-drift-check", action="store_true", help="Skip drift detection")
    sched_parser.add_argument("--max-runs", type=int, default=0, help="Stop after N runs (0=unlimited)")
    sched_parser.set_defaults(func=cmd_schedule)

    # --- serve command (API server) ---
    serve_parser = subparsers.add_parser("serve", help="Start REST API server for clone operations")
    add_common_args(serve_parser)
    serve_parser.add_argument("--port", type=int, default=8080, help="Server port")
    serve_parser.add_argument("--host-addr", default="0.0.0.0", help="Server host address")
    serve_parser.add_argument("--api-key", help="API key for authentication")
    serve_parser.set_defaults(func=cmd_serve)

    # --- incremental-sync command ---
    isync_parser = subparsers.add_parser("incremental-sync", help="Sync only changed tables using Delta history")
    add_common_args(isync_parser)
    isync_parser.add_argument("--source", help="Override source catalog name")
    isync_parser.add_argument("--dest", help="Override destination catalog name")
    isync_parser.add_argument("--schema", help="Specific schema to sync")
    isync_parser.add_argument("--clone-type", choices=["DEEP", "SHALLOW"], help="Clone type")
    isync_parser.add_argument("--dry-run", action="store_true", help="Preview without executing")
    isync_parser.set_defaults(func=cmd_incremental_sync)

    # --- sample command ---
    samp_parser = subparsers.add_parser("sample", help="Preview or compare table data samples")
    add_common_args(samp_parser)
    samp_parser.add_argument("--source", "--catalog", dest="source", help="Source catalog")
    samp_parser.add_argument("--dest", help="Destination catalog (enables compare mode)")
    samp_parser.add_argument("--schema", required=True, help="Schema name")
    samp_parser.add_argument("--table", required=True, help="Table name")
    samp_parser.add_argument("--limit", type=int, default=10, help="Number of rows")
    samp_parser.set_defaults(func=cmd_sample)

    # --- view-deps command ---
    vd_parser = subparsers.add_parser("view-deps", help="Analyze view/function dependencies and creation order")
    add_common_args(vd_parser)
    vd_parser.add_argument("--source", "--catalog", dest="source", help="Override source catalog name")
    vd_parser.add_argument("--schema", required=True, help="Schema to analyze")
    vd_parser.add_argument("--output", help="Export dependency graph to JSON file")
    vd_parser.set_defaults(func=cmd_view_deps)

    # --- slack-bot command ---
    sb_parser = subparsers.add_parser("slack-bot", help="Start Slack bot for clone operations (requires SLACK_BOT_TOKEN, SLACK_APP_TOKEN)")
    sb_parser.add_argument("-c", "--config", default="config/clone_config.yaml", help="Config file path")
    sb_parser.set_defaults(func=cmd_slack_bot)

    # --- demo-data command ---
    demo_parser = subparsers.add_parser("demo-data", help="Generate a demo catalog with synthetic data across industries")
    add_common_args(demo_parser)
    demo_parser.add_argument("--catalog", required=True, help="Name of the catalog to create")
    demo_parser.add_argument("--industry", nargs="+", choices=["healthcare", "financial", "retail", "telecom", "manufacturing", "energy", "education", "real_estate", "logistics", "insurance"], help="Industries to generate (default: all)")
    demo_parser.add_argument("--owner", help="Set catalog owner")
    demo_parser.add_argument("--scale", type=float, default=1.0, help="Scale factor: 1.0 = ~1B rows, 0.1 = ~100M rows, 0.01 = ~10M rows")
    demo_parser.add_argument("--batch-size", type=int, default=5_000_000, help="Rows per INSERT batch (default: 5000000)")
    demo_parser.add_argument("--max-workers", type=int, help="Parallel SQL workers")
    demo_parser.add_argument("--storage-location", help="Managed location for catalog (e.g., abfss://...)")
    demo_parser.add_argument("--drop-existing", action="store_true", help="Drop and recreate if catalog exists")
    demo_parser.add_argument("--no-medallion", action="store_true", help="Skip generating bronze/silver/gold schemas")
    demo_parser.add_argument("--cleanup", action="store_true", help="Remove the demo catalog instead of generating")
    demo_parser.add_argument("--start-date", default="2020-01-01", help="Data start date (default: 2020-01-01)")
    demo_parser.add_argument("--end-date", default="2025-01-01", help="Data end date (default: 2025-01-01)")
    demo_parser.add_argument("--dest-catalog", help="Clone generated catalog to this destination")
    demo_parser.set_defaults(func=cmd_demo_data)

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(0)

    # Commands that don't need standard logging setup
    if args.command in ("init", "completion", "dashboard", "tui", "templates", "approval", "slack-bot"):
        args.func(args)
        return

    setup_logging(
        getattr(args, "verbose", False),
        log_file=getattr(args, "log_file", None),
    )

    # Configure max parallel queries
    max_pq = getattr(args, "max_parallel_queries", None)
    if max_pq:
        from src.client import set_max_parallel_queries
        set_max_parallel_queries(max_pq)

    args.func(args)


if __name__ == "__main__":
    main()
