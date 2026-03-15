"""High-level API for cloning Databricks Unity Catalog catalogs.

Designed for use in Databricks notebooks — either via wheel package or direct repo import.
Each function is self-contained with sensible defaults, so notebook users don't need to
manage config dicts or WorkspaceClient setup.

Usage (wheel):
    %pip install /Volumes/<catalog>/<schema>/<volume>/clone_xs-*.whl
    from src.catalog_clone_api import clone_full_catalog, run_preflight_checks

Usage (repo):
    import sys; sys.path.insert(0, "/Workspace/Repos/<user>/clone-xs")
    from src.catalog_clone_api import clone_full_catalog, run_preflight_checks
"""

import logging

from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)


def _get_client(host: str | None = None, token: str | None = None) -> WorkspaceClient:
    """Create a WorkspaceClient.

    Uses src.auth for multi-method authentication with caching.
    In a Databricks notebook, calling with no args uses automatic notebook auth.
    For external use, pass host/token, set env vars, or configure a CLI profile.
    """
    from src.auth import get_client
    return get_client(host, token)


def _build_config(
    source_catalog: str,
    dest_catalog: str,
    warehouse_id: str | None = None,
    clone_type: str = "DEEP",
    load_type: str = "FULL",
    max_workers: int = 4,
    parallel_tables: int = 2,
    dry_run: bool = False,
    exclude_schemas: list[str] | None = None,
    include_schemas: list[str] | None = None,
    exclude_tables: list[str] | None = None,
    copy_permissions: bool = True,
    copy_ownership: bool = True,
    copy_tags: bool = True,
    copy_properties: bool = True,
    copy_security: bool = True,
    copy_constraints: bool = True,
    copy_comments: bool = True,
    enable_rollback: bool = True,
    validate_after_clone: bool = False,
    show_progress: bool = True,
    **kwargs,
) -> dict:
    """Build a config dict from keyword arguments."""
    config = {
        "source_catalog": source_catalog,
        "destination_catalog": dest_catalog,
        "sql_warehouse_id": warehouse_id or "SPARK_SQL",
        "clone_type": clone_type.upper(),
        "load_type": load_type.upper(),
        "max_workers": max_workers,
        "parallel_tables": parallel_tables,
        "dry_run": dry_run,
        "exclude_schemas": exclude_schemas or ["information_schema", "default"],
        "include_schemas": include_schemas or [],
        "exclude_tables": exclude_tables or [],
        "copy_permissions": copy_permissions,
        "copy_ownership": copy_ownership,
        "copy_tags": copy_tags,
        "copy_properties": copy_properties,
        "copy_security": copy_security,
        "copy_constraints": copy_constraints,
        "copy_comments": copy_comments,
        "enable_rollback": enable_rollback,
        "validate_after_clone": validate_after_clone,
        "show_progress": show_progress,
    }
    config.update(kwargs)
    return config


def clone_full_catalog(
    source_catalog: str,
    dest_catalog: str,
    warehouse_id: str | None = None,
    clone_type: str = "DEEP",
    dry_run: bool = False,
    max_workers: int = 4,
    parallel_tables: int = 2,
    exclude_schemas: list[str] | None = None,
    include_schemas: list[str] | None = None,
    validate_after_clone: bool = True,
    enable_rollback: bool = True,
    host: str | None = None,
    token: str | None = None,
    **kwargs,
) -> dict:
    """Clone an entire Unity Catalog catalog from source to destination.

    Args:
        source_catalog: Source catalog name (e.g. "prod").
        dest_catalog: Destination catalog name (e.g. "dev").
        warehouse_id: SQL warehouse ID.
        clone_type: "DEEP" (full data copy) or "SHALLOW" (metadata-only).
        dry_run: If True, preview without executing writes.
        max_workers: Number of parallel schema workers.
        parallel_tables: Number of parallel tables per schema.
        exclude_schemas: Schemas to skip (defaults to information_schema, default).
        include_schemas: If set, only clone these schemas.
        validate_after_clone: Run row count validation after clone.
        enable_rollback: Create rollback log for undo capability.
        host: Databricks workspace URL (optional, auto-detected in notebooks).
        token: Databricks PAT (optional, auto-detected in notebooks).
        **kwargs: Additional config options passed to clone_catalog().

    Returns:
        dict with clone summary (schemas_processed, tables, views, duration, etc.)

    Example:
        >>> result = clone_full_catalog("prod", "dev", "abc123def456")
        >>> print(f"Cloned {result['tables']['success']} tables")
    """
    from src.clone_catalog import clone_catalog

    client = _get_client(host, token)
    config = _build_config(
        source_catalog, dest_catalog, warehouse_id,
        clone_type=clone_type, dry_run=dry_run,
        max_workers=max_workers, parallel_tables=parallel_tables,
        exclude_schemas=exclude_schemas, include_schemas=include_schemas,
        validate_after_clone=validate_after_clone,
        enable_rollback=enable_rollback, **kwargs,
    )

    logger.info(f"Cloning catalog: {source_catalog} -> {dest_catalog} ({clone_type})")
    return clone_catalog(client, config)


def clone_schema(
    source_catalog: str,
    schema_name: str,
    dest_catalog: str,
    warehouse_id: str,
    clone_type: str = "DEEP",
    dry_run: bool = False,
    host: str | None = None,
    token: str | None = None,
    **kwargs,
) -> dict:
    """Clone a single schema from source to destination catalog.

    Args:
        source_catalog: Source catalog name.
        schema_name: Schema to clone.
        dest_catalog: Destination catalog name.
        warehouse_id: SQL warehouse ID.
        clone_type: "DEEP" or "SHALLOW".
        dry_run: Preview mode.
        host: Workspace URL (optional).
        token: PAT (optional).

    Returns:
        dict with schema clone results (tables, views, functions, volumes).

    Example:
        >>> result = clone_schema("prod", "sales", "dev", "abc123")
        >>> print(f"Tables: {result['tables']['success']} success")
    """
    from src.clone_catalog import process_schema, create_catalog_if_not_exists, create_schema_if_not_exists

    client = _get_client(host, token)
    config = _build_config(
        source_catalog, dest_catalog, warehouse_id,
        clone_type=clone_type, dry_run=dry_run,
        include_schemas=[schema_name], **kwargs,
    )

    create_catalog_if_not_exists(client, warehouse_id, dest_catalog, dry_run=dry_run)
    return process_schema(client, config, schema_name)


def clone_single_table(
    source_fqn: str,
    dest_fqn: str,
    warehouse_id: str,
    clone_type: str = "DEEP",
    dry_run: bool = False,
    host: str | None = None,
    token: str | None = None,
) -> dict:
    """Clone a single table using its fully-qualified name.

    Args:
        source_fqn: Source table (e.g. "prod.sales.orders").
        dest_fqn: Destination table (e.g. "dev.sales.orders").
        warehouse_id: SQL warehouse ID.
        clone_type: "DEEP" or "SHALLOW".
        dry_run: Preview mode.
        host: Workspace URL (optional).
        token: PAT (optional).

    Returns:
        dict with clone result (status, row_count, duration).

    Example:
        >>> result = clone_single_table("prod.sales.orders", "dev.sales.orders", "abc123")
        >>> print(result["status"])
    """
    from src.client import execute_sql

    client = _get_client(host, token)
    clone_type = clone_type.upper()

    # Parse FQNs
    src_parts = source_fqn.split(".")
    dst_parts = dest_fqn.split(".")
    if len(src_parts) != 3 or len(dst_parts) != 3:
        raise ValueError("Table names must be fully qualified: catalog.schema.table")

    # Ensure destination schema exists
    dest_catalog, dest_schema, dest_table = dst_parts
    create_schema_sql = f"CREATE SCHEMA IF NOT EXISTS `{dest_catalog}`.`{dest_schema}`"
    create_catalog_sql = f"CREATE CATALOG IF NOT EXISTS `{dest_catalog}`"

    if dry_run:
        logger.info(f"[DRY RUN] Would clone: {source_fqn} -> {dest_fqn} ({clone_type})")
        return {"status": "dry_run", "source": source_fqn, "dest": dest_fqn}

    execute_sql(client, warehouse_id, create_catalog_sql)
    execute_sql(client, warehouse_id, create_schema_sql)

    sql = f"CREATE OR REPLACE TABLE `{dest_fqn}` {clone_type} CLONE `{source_fqn}`"
    execute_sql(client, warehouse_id, sql)

    return {"status": "success", "source": source_fqn, "dest": dest_fqn, "clone_type": clone_type}


def run_preflight_checks(
    source_catalog: str,
    dest_catalog: str,
    warehouse_id: str,
    host: str | None = None,
    token: str | None = None,
) -> dict:
    """Run pre-flight checks before cloning.

    Verifies: connectivity, warehouse status, catalog access, write permissions.

    Args:
        source_catalog: Source catalog name.
        dest_catalog: Destination catalog name.
        warehouse_id: SQL warehouse ID.
        host: Workspace URL (optional).
        token: PAT (optional).

    Returns:
        dict with checks list, passed/warnings/failed counts, and ready flag.

    Example:
        >>> result = run_preflight_checks("prod", "dev", "abc123")
        >>> if result["ready"]:
        ...     print("All checks passed!")
    """
    from src.preflight import run_preflight

    client = _get_client(host, token)
    return run_preflight(client, warehouse_id, source_catalog, dest_catalog)


def compare_catalogs(
    source_catalog: str,
    dest_catalog: str,
    warehouse_id: str,
    exclude_schemas: list[str] | None = None,
    host: str | None = None,
    token: str | None = None,
) -> dict:
    """Compare two catalogs and return a diff report.

    Shows objects only in source, only in dest, and in both — for schemas,
    tables, views, functions, and volumes.

    Args:
        source_catalog: Source catalog name.
        dest_catalog: Destination catalog name.
        warehouse_id: SQL warehouse ID.
        exclude_schemas: Schemas to skip.
        host: Workspace URL (optional).
        token: PAT (optional).

    Returns:
        dict keyed by object type with only_in_source, only_in_dest, in_both lists.

    Example:
        >>> diff = compare_catalogs("prod", "dev", "abc123")
        >>> print(f"Missing tables: {len(diff['tables']['only_in_source'])}")
    """
    from src.diff import compare_catalogs as _compare

    client = _get_client(host, token)
    exclude = exclude_schemas or ["information_schema", "default"]
    return _compare(client, warehouse_id, source_catalog, dest_catalog, exclude)


def validate_clone(
    source_catalog: str,
    dest_catalog: str,
    warehouse_id: str,
    exclude_schemas: list[str] | None = None,
    max_workers: int = 4,
    use_checksum: bool = False,
    host: str | None = None,
    token: str | None = None,
) -> dict:
    """Validate a clone by comparing row counts (and optionally checksums).

    Args:
        source_catalog: Source catalog name.
        dest_catalog: Destination catalog name.
        warehouse_id: SQL warehouse ID.
        exclude_schemas: Schemas to skip.
        max_workers: Parallel validation workers.
        use_checksum: Also verify data checksums (slower).
        host: Workspace URL (optional).
        token: PAT (optional).

    Returns:
        dict with total_tables, matched, mismatched, errors, and details list.

    Example:
        >>> result = validate_clone("prod", "dev", "abc123")
        >>> print(f"{result['matched']}/{result['total_tables']} tables match")
    """
    from src.validation import validate_catalog

    client = _get_client(host, token)
    exclude = exclude_schemas or ["information_schema", "default"]
    return validate_catalog(
        client, warehouse_id, source_catalog, dest_catalog,
        exclude, max_workers, use_checksum=use_checksum,
    )
