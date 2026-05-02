import logging
import sys
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import SecurableType

from src.client import execute_sql, list_schemas_sdk, list_tables_sdk, set_rate_limit
from src.log_formatter import (
    header,
    divider,
    stat_line,
    kv,
    bold,
    bold_green,
    bold_red,
    bold_yellow,
    cyan,
    OK,
    FAIL,
    WARN,
    ARROW,
    SCHEMA,
    CATALOG,
    CLOCK,
)
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


def _log_schema_rollup(schema_name: str, result: dict) -> None:
    """Emit a one-line per-schema summary after a schema finishes.

    Example: ``[INFO] Schema bronze complete: 42/45 tables cloned (2 failed, 1 skipped) in 18s``
    """
    t = result.get("tables") or {}
    success = int(t.get("success", 0) or 0)
    failed = int(t.get("failed", 0) or 0)
    skipped = int(t.get("skipped", 0) or 0)
    total = success + failed + skipped
    dur = result.get("duration_seconds")
    dur_s = f" in {dur:.0f}s" if isinstance(dur, (int, float)) else ""
    # Only emit if the schema actually had tables — keeps logs quiet on metadata-only schemas
    if total == 0:
        return
    detail_parts = []
    if failed:
        detail_parts.append(f"{failed} failed")
    if skipped:
        detail_parts.append(f"{skipped} skipped")
    detail = f" ({', '.join(detail_parts)})" if detail_parts else ""
    logger.info(
        f"{SCHEMA} Schema {bold(schema_name)} complete: "
        f"{success}/{total} tables cloned{detail}{dur_s}"
    )


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

    schemas = list_schemas_sdk(client, catalog, exclude=list(exclude_set))
    if not schemas:
        # Fallback: the SDK call may return [] if the catalog doesn't exist
        raise RuntimeError(
            f"Catalog '{catalog}' not found or has no schemas. Verify the catalog exists and you have access.\n"
            f'List available catalogs: clxs run-sql --sql "SHOW CATALOGS"'
        )
    return schemas


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
    client: WorkspaceClient,
    warehouse_id: str,
    catalog_name: str,
    dry_run: bool = False,
    location: str = "",
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
    logger.info(f"{OK} Created catalog: {bold(catalog_name)}")

    # Set current user as owner and grant full access.
    # On serverless compute, spark.sql() may create catalogs as "System user".
    # We explicitly set ownership to the actual user.
    try:
        current_user = client.current_user.me().user_name
        # Set owner via SQL (works even when SDK update fails)
        try:
            execute_sql(
                client,
                warehouse_id,
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
            client,
            warehouse_id,
            f"GRANT ALL PRIVILEGES ON CATALOG `{catalog_name}` TO `{current_user}`",
        )
        logger.info(f"Granted ALL PRIVILEGES on {catalog_name} to {current_user}")
    except Exception as e:
        logger.warning(f"Could not configure catalog ownership/grants: {e}")


def create_schema_if_not_exists(
    client: WorkspaceClient,
    warehouse_id: str,
    catalog_name: str,
    schema_name: str,
    dry_run: bool = False,
) -> None:
    """Create a schema in the destination catalog if it doesn't exist."""
    sql = f"CREATE SCHEMA IF NOT EXISTS `{catalog_name}`.`{schema_name}`"
    execute_sql(client, warehouse_id, sql, dry_run=dry_run)
    logger.info(
        f"{'[DRY RUN] ' if dry_run else ''}Ensured schema exists: {catalog_name}.{schema_name}"
    )


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
    exclude_schemas = config.get("exclude_schemas", [])
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
                client,
                SecurableType.SCHEMA,
                f"{source}.{schema}",
                f"{dest}.{schema}",
            )

        # Copy schema tags
        if copy_tags and not dry_run:
            copy_schema_tags(client, warehouse_id, source, dest, schema, dry_run=dry_run)

        # Clone tables
        logger.info(f"  {SCHEMA} Cloning tables in schema: {bold(schema)}")
        schema_results["tables"] = clone_tables_in_schema(
            client,
            warehouse_id,
            source,
            dest,
            schema,
            clone_type,
            exclude_tables,
            load_type,
            dry_run=dry_run,
            copy_permissions=copy_permissions,
            copy_ownership=copy_ownership,
            copy_tags=copy_tags,
            copy_properties=copy_properties,
            copy_security=copy_security,
            copy_constraints=copy_constraints,
            copy_comments=copy_comments,
            rollback_log=rollback_log,
            parallel_tables=parallel_tables,
            include_tables_regex=include_tables_regex,
            exclude_tables_regex=exclude_tables_regex,
            resumed_tables=resumed_tables,
            order_by_size=order_by_size,
            as_of_timestamp=as_of_timestamp,
            as_of_version=as_of_version,
            force_reclone=force_reclone,
            where_clauses=where_clause,
            schema_only=config.get("schema_only", False),
            tables_progress=config.get("_tables_progress"),
            tbl_properties=config.get("clone_tbl_properties"),
            target_format=config.get("target_format", "DELTA"),
            iceberg_physical=config.get("iceberg_physical", False),
        )

        # Apply data masking after table cloning. Two sources of rules:
        #   1) User-supplied `masking_rules` (existing; column-pattern based,
        #      applied to every table)
        #   2) Auto-built rules from UC PII tags when `auto_mask_pii=True`
        #      (per-(schema,table,column), built once per catalog and filtered
        #      to the current schema)
        manual_rules = list(config.get("masking_rules") or [])
        auto_pii_rules: list[dict] = config.get("_auto_pii_rules") or []
        # Cache the auto-built list on `config` so we only query column_tags
        # once per clone job, not once per schema. The first schema processed
        # populates the cache; subsequent schemas read it.
        if config.get("auto_mask_pii") and "_auto_pii_rules" not in config:
            from src.masking import build_pii_masking_rules

            auto_pii_rules = build_pii_masking_rules(
                client,
                warehouse_id,
                source,
                exclude_schemas=exclude_schemas,
            )
            config["_auto_pii_rules"] = auto_pii_rules
            if auto_pii_rules:
                logger.info(
                    f"  {SCHEMA} Auto-detected {len(auto_pii_rules)} PII columns "
                    f"from UC tags in {source} — will mask post-clone"
                )

        if (manual_rules or auto_pii_rules) and not dry_run:
            from src.masking import apply_masking_rules

            # Get all tables that were just cloned
            tables = list_tables_sdk(client, dest, schema)
            tables = [t for t in tables if t["table_type"] in ("MANAGED", "EXTERNAL")]
            for row in tables:
                # Filter auto rules to this specific (schema, table); manual
                # rules apply broadly so they're concatenated as-is.
                table_rules = list(manual_rules) + [
                    r
                    for r in auto_pii_rules
                    if r.get("schema") == schema and r.get("table") == row["table_name"]
                ]
                if table_rules:
                    apply_masking_rules(
                        client,
                        warehouse_id,
                        dest,
                        schema,
                        row["table_name"],
                        table_rules,
                        dry_run=dry_run,
                    )

        # Column-level DQ comparison (row count + per-column NULL counts).
        # Complements the post-clone validation step (which only checks
        # row counts catalog-wide) with a finer signal that catches mid-
        # clone drift. Result is stashed in `_dq_comparisons` on the
        # config so the catalog-level rollback evaluator can read it.
        if config.get("compare_dq_after_clone") and not dry_run:
            from src.clone_dq_compare import compare_schema_dq

            tables_for_dq = list_tables_sdk(client, dest, schema)
            tables_for_dq = [t for t in tables_for_dq if t["table_type"] in ("MANAGED", "EXTERNAL")]
            if tables_for_dq:
                schema_dq = compare_schema_dq(
                    client,
                    warehouse_id,
                    source_catalog=source,
                    dest_catalog=dest,
                    schema=schema,
                    table_names=[t["table_name"] for t in tables_for_dq],
                    max_workers=int(config.get("max_workers", 4)),
                )
                schema_results["dq_comparison"] = schema_dq
                config.setdefault("_dq_comparisons", []).extend(schema_dq.get("comparisons", []))
                if schema_dq.get("max_drift_pct", 0) > 0:
                    logger.info(
                        f"  {SCHEMA} DQ comparison for {bold(schema)}: "
                        f"{schema_dq['tables_compared']} tables, "
                        f"max drift {schema_dq['max_drift_pct']}%"
                    )

        # Record lineage for tables
        lineage_config = config.get("lineage")
        if lineage_config and not dry_run:
            from src.lineage import record_lineage_batch

            tables = list_tables_sdk(client, dest, schema)
            tables = [t for t in tables if t["table_type"] in ("MANAGED", "EXTERNAL")]
            entries = [
                {
                    "source": source,
                    "dest": dest,
                    "schema": schema,
                    "object_name": row["table_name"],
                    "object_type": "TABLE",
                    "clone_type": clone_type,
                }
                for row in tables
            ]
            if entries:
                record_lineage_batch(
                    client,
                    warehouse_id,
                    lineage_config["catalog"],
                    lineage_config["schema"],
                    entries,
                    dry_run=dry_run,
                )

        # Clone views (after tables, since views may depend on tables)
        logger.info(f"  {SCHEMA} Cloning views in schema: {bold(schema)}")
        schema_results["views"] = clone_views_in_schema(
            client,
            warehouse_id,
            source,
            dest,
            schema,
            load_type,
            dry_run=dry_run,
            copy_permissions=copy_permissions,
            copy_ownership=copy_ownership,
            rollback_log=rollback_log,
            include_regex=include_tables_regex,
            exclude_regex=exclude_tables_regex,
        )

        # Clone functions
        logger.info(f"  {SCHEMA} Cloning functions in schema: {bold(schema)}")
        schema_results["functions"] = clone_functions_in_schema(
            client,
            warehouse_id,
            source,
            dest,
            schema,
            load_type,
            dry_run=dry_run,
            copy_permissions=copy_permissions,
            rollback_log=rollback_log,
            include_regex=include_tables_regex,
            exclude_regex=exclude_tables_regex,
        )

        # Clone volumes
        logger.info(f"  {SCHEMA} Cloning volumes in schema: {bold(schema)}")
        schema_results["volumes"] = clone_volumes_in_schema(
            client,
            warehouse_id,
            source,
            dest,
            schema,
            load_type,
            dry_run=dry_run,
            copy_permissions=copy_permissions,
            copy_ownership=copy_ownership,
            rollback_log=rollback_log,
        )

        # Run post-schema hooks
        run_post_schema_hooks(client, warehouse_id, config, schema, dry_run=dry_run)

    except Exception as e:
        logger.error(f"{FAIL} Error processing schema {bold_red(schema)}: {e}")

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

    # DQ Gate — block clone if data quality checks fail
    if config.get("dq_gate", {}).get("enabled") and not dry_run:
        from src.dq_gate import check_clone_dq_gate

        gate_result = check_clone_dq_gate(client, warehouse_id, config)
        if not gate_result.get("passed"):
            raise RuntimeError(f"Clone blocked by DQ gate: {gate_result.get('reason')}")
        elif gate_result.get("warning"):
            logger.warning(f"DQ gate warning: {gate_result.get('reason')}")

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
                client,
                warehouse_id,
                source,
                exclude_schemas,
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

    # --- Plugin system (opt-in) ---
    pm = None
    if config.get("plugins"):
        from src.plugin_system import PluginManager

        pm = PluginManager()
        pm.load_plugins_from_config(config)
        config = pm.run_on_clone_start(config, client, warehouse_id)

    mode = f"{bold_yellow('[DRY RUN]')} " if dry_run else ""
    logger.info(f"{mode}{CATALOG} Starting catalog clone: {bold(source)} {ARROW} {bold(dest)}")
    logger.info(
        kv("Clone type", config["clone_type"]) + "  " + kv("Load type", config["load_type"])
    )
    if dry_run:
        logger.info(
            f"  {WARN} {bold_yellow('DRY RUN MODE')} — no write operations will be executed"
        )

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
    create_catalog_if_not_exists(
        client, warehouse_id, dest, dry_run=dry_run, location=config.get("catalog_location", "")
    )
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
        client,
        warehouse_id,
        source,
        exclude_schemas,
        include=include_schemas if include_schemas else None,
    )

    # Filter by required tags if configured
    filter_tags = config.get("filter_by_tags")
    if filter_tags:
        schemas = _filter_schemas_by_tags(client, warehouse_id, source, schemas, filter_tags)

    # If the request names a snapshot, resolve its captured timestamp and
    # use it as the default `as_of_timestamp` so every table clones from the
    # snapshot's point-in-time state. Per-request `as_of_timestamp` /
    # `as_of_version` still win if explicitly set.
    snapshot_id = config.get("source_snapshot_id")
    if snapshot_id and not config.get("as_of_timestamp") and not config.get("as_of_version"):
        try:
            from src.clone_snapshots import resolve_snapshot_timestamp

            snap_ts = resolve_snapshot_timestamp(client, warehouse_id, config, snapshot_id)
            if snap_ts:
                config["as_of_timestamp"] = snap_ts
                logger.info(
                    f"{CATALOG} Cloning from snapshot {snapshot_id} (captured_at={snap_ts})"
                )
            else:
                logger.warning(f"Snapshot {snapshot_id} not found — ignoring source_snapshot_id")
        except Exception as e:
            logger.warning(f"Could not resolve snapshot {snapshot_id}: {e}")

    logger.info(
        f"{SCHEMA} Found {bold(str(len(schemas)))} schemas to clone: {', '.join(cyan(s) for s in schemas)}"
    )

    # Pre-count tables per schema so the progress bar has a catalog-level denominator
    # and we can emit a meaningful startup summary. Best-effort — on failure we
    # just skip the denominator and the Tables suffix disappears from the bar.
    tables_total = 0
    try:
        from src.client import list_tables_sdk

        for _s in schemas:
            try:
                tables_total += len(list_tables_sdk(client, source, _s) or [])
            except Exception:
                pass
    except Exception:
        tables_total = 0

    if tables_total:
        logger.info(
            f"{SCHEMA} Starting clone: {bold(str(tables_total))} tables across "
            f"{bold(str(len(schemas)))} schemas {ARROW} {cyan(dest)}"
        )

    # Step 4: Process schemas in parallel with progress tracking
    progress = SchemaProgressTracker(
        schemas, show_progress=show_progress, tables_total=tables_total
    )
    progress.start()
    # Stash on config so process_schema → clone_tables_in_schema can bump live
    config["_tables_progress"] = progress

    # Optional TUI dashboard for terminal sessions
    dashboard = None
    if show_progress and sys.stderr.isatty() and len(schemas) > 1:
        try:
            from src.dashboard import Dashboard

            dashboard = Dashboard(schemas)
            dashboard.start()
        except Exception:
            pass  # Fall back to standard progress tracker

    # Runtime guardrails — aborting the schema loop on breach so the error
    # surfaces in the job's `error` field and shows up in the UI summary.
    max_duration_min = config.get("max_duration_min")
    max_tables_budget = config.get("max_tables")
    budget_aborted = False

    # Pre-clone source quiesce — snapshot + revoke write privileges on source
    # schemas so concurrent writes can't land mid-clone and produce a target
    # with missing rows / out-of-order commits. The corresponding restore in
    # the finally block below runs even on clone failure (no orphaned
    # revocations). No-op when quiesce_source is unset/false.
    quiesce_snapshots = []
    if config.get("quiesce_source") and not dry_run:
        from src.quiesce import quiesce_source_schemas

        quiesce_snapshots = quiesce_source_schemas(client, source, schemas)

    all_results = []
    try:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    process_schema,
                    client,
                    config,
                    schema,
                    rollback_log,
                    completed_objects,
                ): schema
                for schema in schemas
            }

            for future in as_completed(futures):
                schema_name = futures[future]
                try:
                    if pm:
                        pm.run_on_table_start(schema_name, config, client, warehouse_id)
                    result = future.result()
                    all_results.append(result)
                    progress.schema_done(result)
                    _log_schema_rollup(schema_name, result)
                    if pm:
                        pm.run_on_table_complete(schema_name, "success", client, warehouse_id)
                    if dashboard:
                        dashboard.schema_completed(schema_name, result)
                except Exception as e:
                    logger.error(f"{FAIL} Schema {bold_red(schema_name)} failed: {e}")
                    error_result = {"schema": schema_name, "error": str(e)}
                    all_results.append(error_result)
                    progress.schema_done(error_result)
                    if pm:
                        pm.run_on_table_complete(schema_name, "failed", client, warehouse_id)
                    if dashboard:
                        dashboard.schema_completed(schema_name, error_result)

                # Budget check after every schema finishes — aborts remaining futures.
                if max_duration_min is not None:
                    elapsed_min = (time.time() - clone_start) / 60.0
                    if elapsed_min >= float(max_duration_min):
                        logger.error(
                            f"{FAIL} BUDGET: max_duration_min={max_duration_min} reached "
                            f"after {elapsed_min:.1f} min — aborting remaining schemas"
                        )
                        budget_aborted = "max_duration_min"
                        for f in futures:
                            if not f.done():
                                f.cancel()
                        break
                if max_tables_budget is not None:
                    tables_done = sum(
                        (r.get("tables", {}).get("success", 0) or 0)
                        + (r.get("tables", {}).get("failed", 0) or 0)
                        + (r.get("tables", {}).get("skipped", 0) or 0)
                        for r in all_results
                    )
                    if tables_done >= int(max_tables_budget):
                        logger.error(
                            f"{FAIL} BUDGET: max_tables={max_tables_budget} reached "
                            f"({tables_done} tables touched) — aborting remaining schemas"
                        )
                        budget_aborted = "max_tables"
                        for f in futures:
                            if not f.done():
                                f.cancel()
                        break
    finally:
        # Always restore source grants — runs whether the schema loop
        # succeeded, partially failed, or was aborted by a budget breach. Empty
        # snapshots list (quiesce_source disabled or dry-run) is a no-op.
        if quiesce_snapshots:
            from src.quiesce import restore_source_grants

            restore_source_grants(client, quiesce_snapshots)

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
    if budget_aborted:
        summary["aborted"] = True
        summary["abort_reason"] = budget_aborted
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

                table_fqn = config.get(
                    "metrics_table",
                    f"{config.get('audit_trail', {}).get('catalog', 'clone_audit')}.metrics.clone_metrics",
                )
                save_metrics_delta(client, warehouse_id, metrics_summary, table_fqn)
        except Exception as e:
            logger.warning(f"Failed to save metrics: {e}")

    # Step 6: Post-clone validation
    if config.get("validate_after_clone") and not dry_run:
        from src.validation import validate_catalog

        logger.info("Running post-clone validation...")
        use_checksum = config.get("validate_checksum", False)
        validation = validate_catalog(
            client,
            warehouse_id,
            source,
            dest,
            exclude_schemas,
            max_workers,
            use_checksum=use_checksum,
        )
        summary["validation"] = validation

        # Auto-rollback on validation failure or DQ drift
        if config.get("auto_rollback_on_failure") and rollback_log:
            from src.validation import evaluate_threshold

            threshold = config.get("rollback_threshold", 5.0)
            eval_result = evaluate_threshold(validation, threshold)
            summary["validation_evaluation"] = eval_result

            # DQ drift evaluation reuses the validation/rollback shape so
            # operator-facing semantics stay consistent — same threshold
            # mental model, same rollback path.
            dq_comparisons = config.get("_dq_comparisons") or []
            if config.get("compare_dq_after_clone") and dq_comparisons:
                from src.clone_dq_compare import evaluate_dq_drift

                dq_threshold = float(config.get("dq_drift_rollback_pct", threshold))
                dq_eval = evaluate_dq_drift(dq_comparisons, dq_threshold)
                summary["dq_drift_evaluation"] = dq_eval
                # OR-combine: either signal failing trips rollback. We rebuild
                # eval_result so the existing rollback branch below sees a
                # combined verdict and can log a meaningful reason.
                if not dq_eval["passed"]:
                    eval_result = {
                        "passed": False,
                        "mismatch_pct": max(
                            float(eval_result.get("mismatch_pct", 0)),
                            float(dq_eval.get("max_drift_pct", 0)),
                        ),
                        "failed_checks": (
                            eval_result.get("failed_checks", [])
                            + [f"dq_drift>{dq_threshold}%: {len(dq_eval['failed_tables'])} tables"]
                        ),
                    }

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
    if not dry_run:
        try:
            from src.audit_trail import ensure_audit_table, log_operation_complete

            ensure_audit_table(client, warehouse_id, config)
        except Exception as e:
            logger.warning(f"Failed to write audit log: {e}")

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
            webhook_config["url"],
            summary,
            config,
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

    # Plugin: on_clone_complete / on_clone_error
    if pm:
        try:
            if summary.get("errors"):
                pm.run_on_clone_error(
                    config, RuntimeError("; ".join(summary["errors"])), client, warehouse_id
                )
            else:
                pm.run_on_clone_complete(config, summary, client, warehouse_id)
        except Exception as e:
            logger.warning(f"Plugin post-clone hook failed: {e}")

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

            log_operation_start(
                client, warehouse_id, config, job_record["job_id"], operation_type="clone"
            )
            log_operation_complete(
                client,
                warehouse_id,
                config,
                job_record["job_id"],
                summary,
                datetime.fromtimestamp(clone_start),
                error_message=job_record.get("error"),
            )
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
        # Catalog-wide CLONE metric totals (Databricks per-CLONE response rows
        # summed across every table). Useful for cloud-egress finance reporting
        # and surfacing "GB transferred" on the run summary.
        "bytes_copied": 0,
        "files_copied": 0,
        "source_table_size": 0,
        "source_num_of_files": 0,
        # Per-source-format success counters (DELTA / PARQUET / ICEBERG / etc.)
        # — same CLONE syntax works across all three when the source is
        # registered in UC. Surfaced in the run summary so users can see the
        # mix of formats they migrated.
        "formats": {},
    }

    for result in results:
        if "error" in result:
            summary["errors"].append(f"{result['schema']}: {result['error']}")
            continue

        for obj_type in ("tables", "views", "functions", "volumes"):
            if obj_type in result:
                for key in ("success", "failed", "skipped"):
                    summary[obj_type][key] += result[obj_type].get(key, 0)

        # Roll up per-schema clone metrics into the catalog-wide totals.
        tbl = result.get("tables") or {}
        for metric in ("bytes_copied", "files_copied", "source_table_size", "source_num_of_files"):
            summary[metric] += tbl.get(metric, 0)

        # Roll up per-source-format counters (Delta / Parquet / Iceberg).
        for fmt, count in (tbl.get("formats") or {}).items():
            summary["formats"][fmt] = summary["formats"].get(fmt, 0) + count

        if "duration_seconds" in result:
            summary["schema_durations"][result["schema"]] = result["duration_seconds"]

    return summary


def _print_summary(summary: dict, source: str, dest: str, dry_run: bool = False) -> None:
    """Print a formatted summary of the clone operation."""
    mode = f"{bold_yellow('[DRY RUN]')} " if dry_run else ""
    title = f"{mode}CLONE SUMMARY: {source} {ARROW} {dest}"
    logger.info(header(title))

    logger.info(kv("Schemas processed", bold(str(summary["schemas_processed"]))))

    duration = summary.get("duration_seconds")
    if duration:
        m, s = divmod(int(duration), 60)
        logger.info(kv("Total duration", f"{CLOCK} {bold(f'{m}m{s}s')}"))

    logger.info(divider())
    for obj_type in ("tables", "views", "functions", "volumes"):
        stats = summary[obj_type]
        logger.info(
            stat_line(
                obj_type.capitalize(),
                stats["success"],
                stats["failed"],
                stats["skipped"],
            )
        )

    if summary["errors"]:
        logger.info(divider())
        error_count = len(summary["errors"])
        logger.warning(f"  {WARN} {bold_red(f'{error_count} error(s)')}")
        for err in summary["errors"]:
            logger.warning(f"    {FAIL} {err}")

    total_failed = sum(summary[t]["failed"] for t in ("tables", "views", "functions", "volumes"))
    logger.info(divider())
    if total_failed == 0:
        logger.info(f"  {OK} {bold_green('Clone completed successfully')}")
    else:
        logger.info(f"  {WARN} {bold_yellow(f'Clone completed with {total_failed} failure(s)')}")
