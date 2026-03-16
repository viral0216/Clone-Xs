import logging
import sys
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import SecurableType

from src.client import execute_sql, set_rate_limit
from src.clone_functions import clone_functions_in_schema
from src.clone_tables import clone_tables_in_schema
from src.clone_tags import copy_catalog_tags, copy_schema_tags
from src.clone_views import clone_views_in_schema
from src.clone_volumes import clone_volumes_in_schema
from src.hooks import run_post_clone_hooks, run_post_schema_hooks, run_pre_clone_hooks
from src.notifications import (
    send_email_notification,
    send_slack_notification,
    send_teams_notification,
    send_webhook_notification,
)
from src.permissions import (
    copy_catalog_permissions,
    copy_schema_permissions,
    update_ownership,
)
from src.progress import SchemaProgressTracker
from src.report import generate_report
from src.resume import get_completed_objects, get_resumed_tables_for_schema
from src.rollback import create_rollback_log, record_object

logger = logging.getLogger(__name__)


def get_schemas(
    client: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
    exclude: list[str],
    include: list[str] | None = None,
) -> list[str]:
    """List schemas in a catalog.

    If include is set, only those schemas are returned (minus excludes).
    Otherwise all schemas are returned minus excludes.
    Always excludes 'information_schema' and 'default' regardless of input.
    """
    # Always exclude system schemas
    always_exclude = {"information_schema", "default"}
    exclude_set = always_exclude | set(exclude)

    if include:
        return [s for s in include if s not in exclude_set]

    sql = f"""
        SELECT schema_name
        FROM {catalog}.information_schema.schemata
        WHERE schema_name NOT IN ({','.join(f"'{s}'" for s in exclude_set)})
    """
    try:
        rows = execute_sql(client, warehouse_id, sql)
    except RuntimeError as e:
        if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
            raise RuntimeError(
                f"Catalog '{catalog}' not found. Verify the catalog exists and you have access.\n"
                f"List available catalogs: clxs run-sql --sql \"SHOW CATALOGS\""
            ) from e
        raise
    return [row["schema_name"] for row in rows]


def _filter_schemas_by_tags(
    client: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
    schemas: list[str],
    required_tags: dict[str, str],
) -> list[str]:
    """Filter schemas to only those that have all required tags."""
    if not required_tags:
        return schemas

    filtered = []
    for schema in schemas:
        sql = f"""
            SELECT tag_name, tag_value
            FROM {catalog}.information_schema.schema_tags
            WHERE schema_name = '{schema}'
        """
        try:
            rows = execute_sql(client, warehouse_id, sql)
            tags = {r["tag_name"]: r["tag_value"] for r in rows}
            if all(tags.get(k) == v for k, v in required_tags.items()):
                filtered.append(schema)
            else:
                logger.info(f"Skipping schema {schema} (missing required tags)")
        except Exception:
            filtered.append(schema)  # Include if we can't check tags

    return filtered


def create_catalog_if_not_exists(
    client: WorkspaceClient, warehouse_id: str, catalog_name: str,
    dry_run: bool = False, location: str = "",
) -> None:
    """Create the destination catalog if it doesn't exist."""
    if dry_run:
        logger.info(f"[DRY RUN] Would create catalog: {catalog_name}")
        return

    # Check if catalog already exists
    try:
        client.catalogs.get(catalog_name)
        logger.info(f"Catalog already exists: {catalog_name}")
        return
    except Exception:
        pass  # Catalog doesn't exist, create it

    # Create catalog via SQL
    sql = f"CREATE CATALOG IF NOT EXISTS `{catalog_name}`"
    if location:
        sql += f" MANAGED LOCATION '{location}'"
    execute_sql(client, warehouse_id, sql)
    logger.info(f"Created catalog: {catalog_name}")

    # Set current user as owner and grant full access.
    # On serverless compute, spark.sql() may create catalogs as "System user".
    # We explicitly set ownership to the actual user.
    try:
        current_user = client.current_user.me().user_name
        # Set owner via SQL (works even when SDK update fails)
        try:
            execute_sql(
                client, warehouse_id,
                f"ALTER CATALOG `{catalog_name}` SET OWNER TO `{current_user}`",
            )
            logger.info(f"Set catalog owner: {catalog_name} -> {current_user}")
        except Exception:
            # Fallback: try SDK
            try:
                client.catalogs.update(catalog_name, owner=current_user)
                logger.info(f"Set catalog owner via SDK: {catalog_name} -> {current_user}")
            except Exception as oe:
                logger.warning(f"Could not set catalog owner: {oe}")

        # Grant full access
        execute_sql(
            client, warehouse_id,
            f"GRANT ALL PRIVILEGES ON CATALOG `{catalog_name}` TO `{current_user}`",
        )
        logger.info(f"Granted ALL PRIVILEGES on {catalog_name} to {current_user}")
    except Exception as e:
        logger.warning(f"Could not configure catalog ownership/grants: {e}")


def create_schema_if_not_exists(
    client: WorkspaceClient, warehouse_id: str, catalog_name: str, schema_name: str,
    dry_run: bool = False,
) -> None:
    """Create a schema in the destination catalog if it doesn't exist."""
    sql = f"CREATE SCHEMA IF NOT EXISTS `{catalog_name}`.`{schema_name}`"
    execute_sql(client, warehouse_id, sql, dry_run=dry_run)
    logger.info(f"{'[DRY RUN] ' if dry_run else ''}Ensured schema exists: {catalog_name}.{schema_name}")



def process_schema(
    client: WorkspaceClient,
    config: dict,
    schema: str,
    rollback_log: str | None = None,
    completed_objects: dict | None = None,
) -> dict:
    """Process a single schema: clone tables, views, functions, volumes."""
    source = config["source_catalog"]
    dest = config["destination_catalog"]
    warehouse_id = config["sql_warehouse_id"]
    clone_type = config["clone_type"]
    load_type = config["load_type"]
    exclude_tables = config["exclude_tables"]
    dry_run = config["dry_run"]
    copy_permissions = config["copy_permissions"]
    copy_ownership = config["copy_ownership"]
    copy_tags = config.get("copy_tags", False)
    copy_properties = config.get("copy_properties", False)
    copy_security = config.get("copy_security", False)
    copy_constraints = config.get("copy_constraints", False)
    copy_comments = config.get("copy_comments", False)
    parallel_tables = config.get("parallel_tables", 1)
    include_tables_regex = config.get("include_tables_regex")
    exclude_tables_regex = config.get("exclude_tables_regex")
    order_by_size = config.get("order_by_size")
    as_of_timestamp = config.get("as_of_timestamp")
    as_of_version = config.get("as_of_version")
    force_reclone = config.get("force_reclone", False)
    where_clause = config.get("where_clause")

    schema_start = time.time()

    schema_results = {
        "schema": schema,
        "tables": {"success": 0, "failed": 0, "skipped": 0},
        "views": {"success": 0, "failed": 0, "skipped": 0},
        "functions": {"success": 0, "failed": 0, "skipped": 0},
        "volumes": {"success": 0, "failed": 0, "skipped": 0},
    }

    # Get resumed tables for this schema
    resumed_tables = None
    if completed_objects:
        resumed_tables = get_resumed_tables_for_schema(completed_objects, schema)

    try:
        # Create schema in destination
        create_schema_if_not_exists(client, warehouse_id, dest, schema, dry_run=dry_run)

        # Record for rollback
        if rollback_log and not dry_run:
            record_object(rollback_log, "schemas", f"`{dest}`.`{schema}`")

        # Copy schema permissions
        if copy_permissions and not dry_run:
            copy_schema_permissions(client, source, dest, schema)

        # Copy schema ownership
        if copy_ownership and not dry_run:
            update_ownership(
                client, SecurableType.SCHEMA,
                f"{source}.{schema}", f"{dest}.{schema}",
            )

        # Copy schema tags
        if copy_tags and not dry_run:
            copy_schema_tags(client, warehouse_id, source, dest, schema, dry_run=dry_run)

        # Clone tables
        logger.info(f"Cloning tables in schema: {schema}")
        schema_results["tables"] = clone_tables_in_schema(
            client, warehouse_id, source, dest, schema, clone_type, exclude_tables, load_type,
            dry_run=dry_run, copy_permissions=copy_permissions, copy_ownership=copy_ownership,
            copy_tags=copy_tags, copy_properties=copy_properties, copy_security=copy_security,
            copy_constraints=copy_constraints, copy_comments=copy_comments,
            rollback_log=rollback_log, parallel_tables=parallel_tables,
            include_tables_regex=include_tables_regex, exclude_tables_regex=exclude_tables_regex,
            resumed_tables=resumed_tables, order_by_size=order_by_size,
            as_of_timestamp=as_of_timestamp, as_of_version=as_of_version,
            force_reclone=force_reclone, where_clauses=where_clause,
        )

        # Apply data masking after table cloning
        masking_rules = config.get("masking_rules")
        if masking_rules and not dry_run:
            from src.masking import apply_masking_rules
            # Get all tables that were just cloned
            table_sql = f"""
                SELECT table_name FROM {dest}.information_schema.tables
                WHERE table_schema = '{schema}' AND table_type IN ('MANAGED', 'EXTERNAL')
            """
            tables = execute_sql(client, warehouse_id, table_sql)
            for row in tables:
                apply_masking_rules(
                    client, warehouse_id, dest, schema, row["table_name"],
                    masking_rules, dry_run=dry_run,
                )

        # Record lineage for tables
        lineage_config = config.get("lineage")
        if lineage_config and not dry_run:
            from src.lineage import record_lineage_to_uc
            table_sql = f"""
                SELECT table_name FROM {dest}.information_schema.tables
                WHERE table_schema = '{schema}' AND table_type IN ('MANAGED', 'EXTERNAL')
            """
            tables = execute_sql(client, warehouse_id, table_sql)
            for row in tables:
                record_lineage_to_uc(
                    client, warehouse_id,
                    lineage_config["catalog"], lineage_config["schema"],
                    source, dest, schema, row["table_name"],
                    "TABLE", clone_type, dry_run=dry_run,
                )

        # Clone views (after tables, since views may depend on tables)
        logger.info(f"Cloning views in schema: {schema}")
        schema_results["views"] = clone_views_in_schema(
            client, warehouse_id, source, dest, schema, load_type,
            dry_run=dry_run, copy_permissions=copy_permissions, copy_ownership=copy_ownership,
            rollback_log=rollback_log,
            include_regex=include_tables_regex, exclude_regex=exclude_tables_regex,
        )

        # Clone functions
        logger.info(f"Cloning functions in schema: {schema}")
        schema_results["functions"] = clone_functions_in_schema(
            client, warehouse_id, source, dest, schema, load_type,
            dry_run=dry_run, copy_permissions=copy_permissions,
            rollback_log=rollback_log,
            include_regex=include_tables_regex, exclude_regex=exclude_tables_regex,
        )

        # Clone volumes
        logger.info(f"Cloning volumes in schema: {schema}")
        schema_results["volumes"] = clone_volumes_in_schema(
            client, warehouse_id, source, dest, schema, load_type,
            dry_run=dry_run, copy_permissions=copy_permissions, copy_ownership=copy_ownership,
            rollback_log=rollback_log,
        )

        # Run post-schema hooks
        run_post_schema_hooks(client, warehouse_id, config, schema, dry_run=dry_run)

    except Exception as e:
        logger.error(f"Error processing schema {schema}: {e}")

    schema_results["duration_seconds"] = round(time.time() - schema_start, 1)
    return schema_results


def clone_catalog(client: WorkspaceClient, config: dict) -> dict:
    """Main orchestrator: clone an entire catalog from source to destination."""
    clone_start = time.time()

    source = config["source_catalog"]
    dest = config["destination_catalog"]
    warehouse_id = config["sql_warehouse_id"]
    max_workers = config["max_workers"]
    exclude_schemas = config["exclude_schemas"]
    include_schemas = config.get("include_schemas", [])
    dry_run = config["dry_run"]
    show_progress = config.get("show_progress", False)

    # Configure rate limiting
    max_rps = config.get("max_rps", 0)
    if max_rps > 0:
        set_rate_limit(max_rps)

    # Configure max parallel queries
    from src.client import set_max_parallel_queries
    max_pq = config.get("max_parallel_queries", 10)
    set_max_parallel_queries(max_pq)

    # --- Pre-clone checks (new features) ---

    # RBAC check (#16)
    if config.get("rbac_enabled") and not dry_run:
        from src.rbac import enforce_rbac
        enforce_rbac(client, config)

    # Approval check (#17)
    if not dry_run:
        from src.approval import needs_approval, submit_approval_request, wait_for_approval
        if needs_approval(config):
            request_id = submit_approval_request(client, config)
            timeout = config.get("approval_timeout_hours", 24)
            if not wait_for_approval(request_id, timeout_hours=timeout):
                raise RuntimeError(f"Clone approval denied or timed out (request: {request_id})")

    # Impact analysis (#15)
    if config.get("impact_check_before_clone") and not dry_run:
        from src.impact_analysis import analyze_impact
        impact = analyze_impact(client, warehouse_id, dest, config)
        if impact.get("risk_level") == "high":
            logger.warning(
                f"High impact detected: {impact['total_dependent_objects']} dependent objects. "
                f"Proceeding with caution."
            )

    # Config lint (#12)
    if config.get("auto_lint"):
        from src.config_lint import lint_config, lint_has_errors, format_lint_results
        lint_results = lint_config(config)
        if lint_has_errors(lint_results):
            logger.error(f"Config validation failed:\n{format_lint_results(lint_results)}")
            raise ValueError("Config validation failed — fix errors before cloning")

    # Throttle controls (#14)
    throttle_setting = config.get("throttle")
    if throttle_setting:
        from src.throttle import resolve_throttle, apply_throttle_profile
        profile = resolve_throttle(config)
        if profile:
            apply_throttle_profile(profile, config)
            max_workers = config["max_workers"]

    # Metrics init (#6)
    metrics_collector = None
    if config.get("metrics_enabled"):
        from src.metrics import init_metrics
        metrics_collector = init_metrics(config)

    # Checkpoint init (#13)
    checkpoint_manager = None
    if config.get("checkpoint_enabled") and not dry_run:
        from src.checkpoint import CheckpointManager
        checkpoint_manager = CheckpointManager(
            config,
            interval_tables=config.get("checkpoint_interval_tables", 50),
            interval_minutes=config.get("checkpoint_interval_minutes", 5),
        )

    # Skip unused tables (#7)
    if config.get("skip_unused") and not dry_run:
        try:
            from src.usage_analysis import recommend_skip_tables
            unused = recommend_skip_tables(
                client, warehouse_id, source, exclude_schemas,
                days=config.get("usage_analysis_days", 90),
                days_threshold=config.get("usage_unused_threshold_days", 30),
            )
            if unused:
                existing_excludes = config.get("exclude_tables", [])
                # Extract just the table names (schema.table from catalog.schema.table)
                for fqn in unused:
                    parts = fqn.split(".")
                    if len(parts) == 3:
                        existing_excludes.append(f"{parts[1]}.{parts[2]}")
                config["exclude_tables"] = existing_excludes
                logger.info(f"Skipping {len(unused)} unused tables")
        except Exception as e:
            logger.warning(f"Could not analyze usage: {e}")

    # --- End pre-clone checks ---

    mode = "[DRY RUN] " if dry_run else ""
    logger.info(f"{mode}Starting catalog clone: {source} -> {dest}")
    logger.info(f"Clone type: {config['clone_type']}, Load type: {config['load_type']}")
    if dry_run:
        logger.info("DRY RUN MODE — no write operations will be executed")

    # Initialize rollback log
    rollback_log = None
    if config.get("enable_rollback") and not dry_run:
        rollback_log = create_rollback_log(config)

    # Load resume state if resuming
    completed_objects = None
    if config.get("resume"):
        completed_objects = get_completed_objects(config["resume"])
    # Also load checkpoint state if available
    if config.get("_checkpoint_completed"):
        completed_objects = config["_checkpoint_completed"]

    # Run pre-clone hooks
    run_pre_clone_hooks(client, warehouse_id, config, dry_run=dry_run)

    # Step 1: Create destination catalog
    create_catalog_if_not_exists(client, warehouse_id, dest, dry_run=dry_run,
                                 location=config.get("catalog_location", ""))
    if rollback_log:
        record_object(rollback_log, "catalog", f"`{dest}`")

    # Step 2: Copy catalog-level permissions, ownership, and tags
    if config["copy_permissions"] and not dry_run:
        copy_catalog_permissions(client, source, dest)

    if config["copy_ownership"] and not dry_run:
        update_ownership(client, SecurableType.CATALOG, source, dest)

    if config.get("copy_tags") and not dry_run:
        copy_catalog_tags(client, warehouse_id, source, dest, dry_run=dry_run)

    # Step 3: Get all schemas from source
    schemas = get_schemas(
        client, warehouse_id, source, exclude_schemas,
        include=include_schemas if include_schemas else None,
    )

    # Filter by required tags if configured
    filter_tags = config.get("filter_by_tags")
    if filter_tags:
        schemas = _filter_schemas_by_tags(client, warehouse_id, source, schemas, filter_tags)

    logger.info(f"Found {len(schemas)} schemas to clone: {schemas}")

    # Step 4: Process schemas in parallel with progress tracking
    progress = SchemaProgressTracker(schemas, show_progress=show_progress)
    progress.start()

    # Optional TUI dashboard for terminal sessions
    dashboard = None
    if show_progress and sys.stderr.isatty() and len(schemas) > 1:
        try:
            from src.dashboard import Dashboard
            dashboard = Dashboard(schemas)
            dashboard.start()
        except Exception:
            pass  # Fall back to standard progress tracker

    all_results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                process_schema, client, config, schema, rollback_log, completed_objects,
            ): schema
            for schema in schemas
        }

        for future in as_completed(futures):
            schema_name = futures[future]
            try:
                result = future.result()
                all_results.append(result)
                progress.schema_done(result)
                if dashboard:
                    dashboard.schema_completed(schema_name, result)
            except Exception as e:
                logger.error(f"Schema {schema_name} failed: {e}")
                error_result = {"schema": schema_name, "error": str(e)}
                all_results.append(error_result)
                progress.schema_done(error_result)
                if dashboard:
                    dashboard.schema_completed(schema_name, error_result)

    progress.stop()
    if dashboard:
        dashboard.stop()

    # Save final checkpoint (#13)
    if checkpoint_manager:
        try:
            checkpoint_manager.save_final()
        except Exception as e:
            logger.warning(f"Failed to save final checkpoint: {e}")

    # Step 5: Build and print summary
    summary = _build_summary(all_results)
    summary["duration_seconds"] = round(time.time() - clone_start, 1)
    _print_summary(summary, source, dest, dry_run=dry_run)

    # Save metrics (#6)
    if metrics_collector:
        try:
            metrics_collector.end_operation(summary)
            metrics_summary = metrics_collector.get_summary()
            dest_type = config.get("metrics_destination", "delta")
            if dest_type == "json":
                from src.metrics import save_metrics_json
                path = config.get("metrics_output_path", f"reports/metrics_{dest}.json")
                save_metrics_json(metrics_summary, path)
            elif dest_type == "prometheus":
                from src.metrics import save_metrics_prometheus
                path = config.get("metrics_output_path", f"reports/metrics_{dest}.txt")
                save_metrics_prometheus(metrics_summary, path)
            elif dest_type == "webhook":
                webhook_url = config.get("metrics_webhook_url")
                if webhook_url:
                    from src.metrics import save_metrics_webhook
                    save_metrics_webhook(metrics_summary, webhook_url)
            elif dest_type == "delta" and not dry_run:
                from src.metrics import save_metrics_delta
                table_fqn = config.get("metrics_table", "clone_audit.metrics.clone_metrics")
                save_metrics_delta(client, warehouse_id, metrics_summary, table_fqn)
        except Exception as e:
            logger.warning(f"Failed to save metrics: {e}")

    # Step 6: Post-clone validation
    if config.get("validate_after_clone") and not dry_run:
        from src.validation import validate_catalog
        logger.info("Running post-clone validation...")
        use_checksum = config.get("validate_checksum", False)
        validation = validate_catalog(
            client, warehouse_id, source, dest, exclude_schemas, max_workers,
            use_checksum=use_checksum,
        )
        summary["validation"] = validation

        # Auto-rollback on validation failure
        if config.get("auto_rollback_on_failure") and rollback_log:
            from src.validation import evaluate_threshold
            threshold = config.get("rollback_threshold", 5.0)
            eval_result = evaluate_threshold(validation, threshold)
            summary["validation_evaluation"] = eval_result

            if not eval_result["passed"]:
                logger.warning(
                    f"Validation failed threshold ({eval_result['mismatch_pct']:.1f}% > {threshold}%). "
                    f"Triggering auto-rollback..."
                )
                from src.rollback import rollback as do_rollback
                rollback_result = do_rollback(client, warehouse_id, rollback_log)
                summary["auto_rollback"] = {
                    "triggered": True,
                    "reason": eval_result["failed_checks"],
                    "mismatch_pct": eval_result["mismatch_pct"],
                    "threshold_pct": threshold,
                    "rollback_result": rollback_result,
                }
                logger.warning("Auto-rollback completed.")

                # Send auto-rollback notification
                if config.get("slack_webhook_url"):
                    send_slack_notification(config.get("slack_webhook_url"), summary, config)
                if config.get("teams_webhook_url"):
                    send_teams_notification(config.get("teams_webhook_url"), summary, config)
            else:
                logger.info(
                    f"Validation passed threshold ({eval_result['mismatch_pct']:.1f}% <= {threshold}%)"
                )

    # Step 7: Write audit log
    audit_config = config.get("audit")
    if audit_config and not dry_run:
        from src.audit import ensure_audit_table, write_audit_log
        audit_table = ensure_audit_table(
            client, warehouse_id,
            audit_config["catalog"], audit_config["schema"],
            audit_config.get("table", "clone_audit_log"),
        )
        write_audit_log(client, warehouse_id, audit_table, summary, config)

    # Step 8: Run post-clone hooks
    run_post_clone_hooks(client, warehouse_id, config, dry_run=dry_run)

    # Step 9: Generate report
    if config.get("generate_report"):
        generate_report(summary, config, output_dir=config.get("report_dir", "reports"))

    # Step 10: Send notifications
    slack_url = config.get("slack_webhook_url")
    if slack_url:
        send_slack_notification(slack_url, summary, config)

    teams_url = config.get("teams_webhook_url")
    if teams_url:
        send_teams_notification(teams_url, summary, config)

    webhook_config = config.get("webhook")
    if webhook_config:
        send_webhook_notification(
            webhook_config["url"], summary, config,
            headers=webhook_config.get("headers"),
        )

    email_config = config.get("email")
    if email_config:
        send_email_notification(
            smtp_host=email_config["smtp_host"],
            smtp_port=email_config.get("smtp_port", 587),
            sender=email_config["sender"],
            recipients=email_config["recipients"],
            summary=summary,
            config=config,
            smtp_user=email_config.get("smtp_user"),
            smtp_password=email_config.get("smtp_password"),
            use_tls=email_config.get("use_tls", True),
        )

    if rollback_log:
        logger.info(f"Rollback log saved: {rollback_log}")

    # Set TTL on destination (#8)
    ttl_str = config.get("ttl")
    if ttl_str and not dry_run:
        try:
            from src.ttl_manager import TTLManager, parse_ttl_string
            ttl_days = parse_ttl_string(ttl_str)
            ttl_mgr = TTLManager(client, warehouse_id)
            ttl_mgr.init_ttl_table()
            ttl_mgr.set_ttl(dest, ttl_days)
        except Exception as e:
            logger.warning(f"Failed to set TTL: {e}")

    # Step 11: Save run log to Delta table (enabled by default)
    # Skip if called from API job_manager (it saves with full logs separately)
    if config.get("save_run_logs", True) and not dry_run and not config.get("_api_managed_logs"):
        try:
            from src.run_logs import save_run_log
            import uuid
            job_record = {
                "job_id": str(uuid.uuid4())[:8],
                "job_type": "clone",
                "source_catalog": source,
                "destination_catalog": dest,
                "clone_type": config.get("clone_type", "DEEP"),
                "status": "failed" if summary.get("errors") else "completed",
                "started_at": datetime.fromtimestamp(clone_start).isoformat(),
                "completed_at": datetime.now().isoformat(),
                "result": summary,
                "error": "; ".join(summary.get("errors", [])) if summary.get("errors") else None,
                "logs": [],  # No in-memory log capture in direct calls
            }
            save_run_log(client, warehouse_id, job_record, config)
        except Exception as e:
            logger.debug(f"Could not save run log to Delta: {e}")

        # Also log to audit trail (clone_operations table)
        try:
            from src.audit_trail import log_operation_start, log_operation_complete
            log_operation_start(client, warehouse_id, config, job_record["job_id"], operation_type="clone")
            log_operation_complete(client, warehouse_id, config, job_record["job_id"],
                                   summary, datetime.fromtimestamp(clone_start),
                                   error_message=job_record.get("error"))
        except Exception as e:
            logger.debug(f"Could not save audit trail to Delta: {e}")

    return summary


def _build_summary(results: list[dict]) -> dict:
    """Build an aggregate summary from schema results."""
    summary = {
        "schemas_processed": len(results),
        "tables": {"success": 0, "failed": 0, "skipped": 0},
        "views": {"success": 0, "failed": 0, "skipped": 0},
        "functions": {"success": 0, "failed": 0, "skipped": 0},
        "volumes": {"success": 0, "failed": 0, "skipped": 0},
        "errors": [],
        "schema_durations": {},
    }

    for result in results:
        if "error" in result:
            summary["errors"].append(f"{result['schema']}: {result['error']}")
            continue

        for obj_type in ("tables", "views", "functions", "volumes"):
            if obj_type in result:
                for key in ("success", "failed", "skipped"):
                    summary[obj_type][key] += result[obj_type].get(key, 0)

        if "duration_seconds" in result:
            summary["schema_durations"][result["schema"]] = result["duration_seconds"]

    return summary


def _print_summary(summary: dict, source: str, dest: str, dry_run: bool = False) -> None:
    """Print a formatted summary of the clone operation."""
    mode = "[DRY RUN] " if dry_run else ""
    logger.info("=" * 60)
    logger.info(f"{mode}CLONE SUMMARY: {source} -> {dest}")
    logger.info("=" * 60)
    logger.info(f"Schemas processed: {summary['schemas_processed']}")

    duration = summary.get("duration_seconds")
    if duration:
        m, s = divmod(int(duration), 60)
        logger.info(f"Total duration:    {m}m{s}s")

    for obj_type in ("tables", "views", "functions", "volumes"):
        stats = summary[obj_type]
        logger.info(
            f"  {obj_type.capitalize():12s}: "
            f"{stats['success']} success, "
            f"{stats['failed']} failed, "
            f"{stats['skipped']} skipped"
        )

    if summary["errors"]:
        logger.warning(f"  Errors: {len(summary['errors'])}")
        for err in summary["errors"]:
            logger.warning(f"    - {err}")

    logger.info("=" * 60)
