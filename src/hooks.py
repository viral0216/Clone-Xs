import logging

from databricks.sdk import WorkspaceClient

from src.client import execute_sql

logger = logging.getLogger(__name__)


def run_hooks(
    client: WorkspaceClient,
    warehouse_id: str,
    hooks: list[dict],
    phase: str,
    context: dict | None = None,
    dry_run: bool = False,
) -> list[dict]:
    """Execute a list of SQL hook statements.

    Args:
        hooks: List of hook dicts with keys: sql, description (optional), on_error (optional).
        phase: "pre" or "post" — for logging.
        context: Optional dict with {source_catalog, dest_catalog, schema} for variable substitution.
        dry_run: If True, log but don't execute.

    Returns list of results with status for each hook.
    """
    results = []

    for i, hook in enumerate(hooks):
        sql = hook.get("sql", "")
        description = hook.get("description", f"Hook {i + 1}")
        on_error = hook.get("on_error", "warn")  # "warn", "fail", "ignore"

        if not sql:
            continue

        # Variable substitution
        if context:
            sql = sql.replace("${source_catalog}", context.get("source_catalog", ""))
            sql = sql.replace("${dest_catalog}", context.get("dest_catalog", ""))
            sql = sql.replace("${schema}", context.get("schema", ""))

        logger.info(f"{'[DRY RUN] ' if dry_run else ''}[{phase.upper()}] Running: {description}")

        try:
            execute_sql(client, warehouse_id, sql, dry_run=dry_run)
            results.append({"hook": description, "status": "success"})
            logger.info(f"[{phase.upper()}] Completed: {description}")
        except Exception as e:
            result = {"hook": description, "status": "failed", "error": str(e)}
            results.append(result)

            if on_error == "fail":
                logger.error(f"[{phase.upper()}] Hook failed (aborting): {description}: {e}")
                raise
            elif on_error == "warn":
                logger.warning(f"[{phase.upper()}] Hook failed (continuing): {description}: {e}")
            else:
                logger.debug(f"[{phase.upper()}] Hook failed (ignored): {description}: {e}")

    return results


def run_pre_clone_hooks(
    client: WorkspaceClient, warehouse_id: str, config: dict, dry_run: bool = False
) -> list[dict]:
    """Run pre-clone hooks from config."""
    hooks = config.get("pre_clone_hooks", [])
    if not hooks:
        return []

    context = {
        "source_catalog": config.get("source_catalog", ""),
        "dest_catalog": config.get("destination_catalog", ""),
    }
    return run_hooks(client, warehouse_id, hooks, "pre", context=context, dry_run=dry_run)


def run_post_clone_hooks(
    client: WorkspaceClient, warehouse_id: str, config: dict, dry_run: bool = False
) -> list[dict]:
    """Run post-clone hooks from config."""
    hooks = config.get("post_clone_hooks", [])
    if not hooks:
        return []

    context = {
        "source_catalog": config.get("source_catalog", ""),
        "dest_catalog": config.get("destination_catalog", ""),
    }
    return run_hooks(client, warehouse_id, hooks, "post", context=context, dry_run=dry_run)


def run_post_schema_hooks(
    client: WorkspaceClient, warehouse_id: str, config: dict,
    schema: str, dry_run: bool = False,
) -> list[dict]:
    """Run post-schema hooks (e.g., OPTIMIZE, ANALYZE)."""
    hooks = config.get("post_schema_hooks", [])
    if not hooks:
        return []

    context = {
        "source_catalog": config.get("source_catalog", ""),
        "dest_catalog": config.get("destination_catalog", ""),
        "schema": schema,
    }
    return run_hooks(client, warehouse_id, hooks, "post-schema", context=context, dry_run=dry_run)
