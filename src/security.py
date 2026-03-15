import logging

from databricks.sdk import WorkspaceClient

from src.client import execute_sql

logger = logging.getLogger(__name__)


def get_row_filters(
    client: WorkspaceClient, warehouse_id: str, catalog: str, schema: str, table_name: str
) -> list[dict]:
    """Get row filter policies applied to a table."""
    sql = f"""
        SELECT filter_name, filter_catalog, filter_schema, filter_condition
        FROM {catalog}.information_schema.row_filters
        WHERE table_schema = '{schema}'
        AND table_name = '{table_name}'
    """
    try:
        return execute_sql(client, warehouse_id, sql)
    except Exception as e:
        logger.debug(f"Could not fetch row filters for {schema}.{table_name}: {e}")
        return []


def get_column_masks(
    client: WorkspaceClient, warehouse_id: str, catalog: str, schema: str, table_name: str
) -> list[dict]:
    """Get column mask policies applied to a table."""
    sql = f"""
        SELECT column_name, mask_catalog, mask_schema, mask_name, mask_function_name
        FROM {catalog}.information_schema.column_masks
        WHERE table_schema = '{schema}'
        AND table_name = '{table_name}'
    """
    try:
        return execute_sql(client, warehouse_id, sql)
    except Exception as e:
        logger.debug(f"Could not fetch column masks for {schema}.{table_name}: {e}")
        return []


def copy_row_filters(
    client: WorkspaceClient, warehouse_id: str,
    source_catalog: str, dest_catalog: str, schema: str, table_name: str,
    dry_run: bool = False,
) -> None:
    """Copy row filter policies from source table to destination table."""
    filters = get_row_filters(client, warehouse_id, source_catalog, schema, table_name)
    if not filters:
        return

    for f in filters:
        # Replace source catalog reference with destination
        filter_ref = f.get("filter_name", "")
        filter_catalog = f.get("filter_catalog", source_catalog)
        filter_schema = f.get("filter_schema", schema)

        # If the filter function lives in the source catalog, point to dest
        if filter_catalog == source_catalog:
            filter_catalog = dest_catalog

        sql = (
            f"ALTER TABLE `{dest_catalog}`.`{schema}`.`{table_name}` "
            f"SET ROW FILTER `{filter_catalog}`.`{filter_schema}`.`{filter_ref}` ON ()"
        )
        try:
            execute_sql(client, warehouse_id, sql, dry_run=dry_run)
            logger.info(
                f"{'[DRY RUN] ' if dry_run else ''}"
                f"Applied row filter {filter_ref} to {dest_catalog}.{schema}.{table_name}"
            )
        except Exception as e:
            logger.error(
                f"Failed to apply row filter {filter_ref} on {schema}.{table_name}: {e}"
            )


def copy_column_masks(
    client: WorkspaceClient, warehouse_id: str,
    source_catalog: str, dest_catalog: str, schema: str, table_name: str,
    dry_run: bool = False,
) -> None:
    """Copy column mask policies from source table to destination table."""
    masks = get_column_masks(client, warehouse_id, source_catalog, schema, table_name)
    if not masks:
        return

    for mask in masks:
        col_name = mask["column_name"]
        mask_catalog = mask.get("mask_catalog", source_catalog)
        mask_schema = mask.get("mask_schema", schema)
        mask_func = mask.get("mask_function_name", mask.get("mask_name", ""))

        # If the mask function lives in the source catalog, point to dest
        if mask_catalog == source_catalog:
            mask_catalog = dest_catalog

        sql = (
            f"ALTER TABLE `{dest_catalog}`.`{schema}`.`{table_name}` "
            f"ALTER COLUMN `{col_name}` "
            f"SET MASK `{mask_catalog}`.`{mask_schema}`.`{mask_func}`"
        )
        try:
            execute_sql(client, warehouse_id, sql, dry_run=dry_run)
            logger.info(
                f"{'[DRY RUN] ' if dry_run else ''}"
                f"Applied column mask {mask_func} on {col_name} "
                f"to {dest_catalog}.{schema}.{table_name}"
            )
        except Exception as e:
            logger.error(
                f"Failed to apply column mask {mask_func} on "
                f"{schema}.{table_name}.{col_name}: {e}"
            )


def copy_table_security(
    client: WorkspaceClient, warehouse_id: str,
    source_catalog: str, dest_catalog: str, schema: str, table_name: str,
    dry_run: bool = False,
) -> None:
    """Copy both row filters and column masks for a table."""
    copy_row_filters(client, warehouse_id, source_catalog, dest_catalog, schema, table_name, dry_run)
    copy_column_masks(client, warehouse_id, source_catalog, dest_catalog, schema, table_name, dry_run)
