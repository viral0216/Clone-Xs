import logging

from databricks.sdk import WorkspaceClient

from src.client import execute_sql

logger = logging.getLogger(__name__)


def get_table_tags(
    client: WorkspaceClient, warehouse_id: str, catalog: str, schema: str, table_name: str
) -> list[dict]:
    """Get tags for a table."""
    sql = f"""
        SELECT tag_name, tag_value
        FROM {catalog}.information_schema.table_tags
        WHERE schema_name = '{schema}'
        AND table_name = '{table_name}'
    """
    try:
        return execute_sql(client, warehouse_id, sql)
    except Exception as e:
        logger.debug(f"Could not fetch table tags for {schema}.{table_name}: {e}")
        return []


def get_column_tags(
    client: WorkspaceClient, warehouse_id: str, catalog: str, schema: str, table_name: str
) -> list[dict]:
    """Get column-level tags for a table."""
    sql = f"""
        SELECT column_name, tag_name, tag_value
        FROM {catalog}.information_schema.column_tags
        WHERE schema_name = '{schema}'
        AND table_name = '{table_name}'
    """
    try:
        return execute_sql(client, warehouse_id, sql)
    except Exception as e:
        logger.debug(f"Could not fetch column tags for {schema}.{table_name}: {e}")
        return []


def get_schema_tags(
    client: WorkspaceClient, warehouse_id: str, catalog: str, schema: str
) -> list[dict]:
    """Get tags for a schema."""
    sql = f"""
        SELECT tag_name, tag_value
        FROM {catalog}.information_schema.schema_tags
        WHERE schema_name = '{schema}'
    """
    try:
        return execute_sql(client, warehouse_id, sql)
    except Exception as e:
        logger.debug(f"Could not fetch schema tags for {schema}: {e}")
        return []


def get_catalog_tags(
    client: WorkspaceClient, warehouse_id: str, catalog: str
) -> list[dict]:
    """Get tags for a catalog."""
    sql = f"""
        SELECT tag_name, tag_value
        FROM {catalog}.information_schema.catalog_tags
        WHERE catalog_name = '{catalog}'
    """
    try:
        return execute_sql(client, warehouse_id, sql)
    except Exception as e:
        logger.debug(f"Could not fetch catalog tags for {catalog}: {e}")
        return []


def copy_catalog_tags(
    client: WorkspaceClient, warehouse_id: str,
    source_catalog: str, dest_catalog: str, dry_run: bool = False,
) -> None:
    """Copy tags from source catalog to destination catalog."""
    tags = get_catalog_tags(client, warehouse_id, source_catalog)
    if not tags:
        return

    for tag in tags:
        sql = f"ALTER CATALOG `{dest_catalog}` SET TAGS ('{tag['tag_name']}' = '{tag['tag_value']}')"
        try:
            execute_sql(client, warehouse_id, sql, dry_run=dry_run)
        except Exception as e:
            logger.error(f"Failed to set catalog tag {tag['tag_name']}: {e}")

    logger.info(f"{'[DRY RUN] ' if dry_run else ''}Copied {len(tags)} catalog tags to {dest_catalog}")


def copy_schema_tags(
    client: WorkspaceClient, warehouse_id: str,
    source_catalog: str, dest_catalog: str, schema: str, dry_run: bool = False,
) -> None:
    """Copy tags from source schema to destination schema."""
    tags = get_schema_tags(client, warehouse_id, source_catalog, schema)
    if not tags:
        return

    for tag in tags:
        sql = (
            f"ALTER SCHEMA `{dest_catalog}`.`{schema}` "
            f"SET TAGS ('{tag['tag_name']}' = '{tag['tag_value']}')"
        )
        try:
            execute_sql(client, warehouse_id, sql, dry_run=dry_run)
        except Exception as e:
            logger.error(f"Failed to set schema tag {tag['tag_name']} on {schema}: {e}")

    logger.info(
        f"{'[DRY RUN] ' if dry_run else ''}Copied {len(tags)} schema tags to {dest_catalog}.{schema}"
    )


def copy_table_tags(
    client: WorkspaceClient, warehouse_id: str,
    source_catalog: str, dest_catalog: str, schema: str, table_name: str,
    dry_run: bool = False,
) -> None:
    """Copy table-level and column-level tags from source to destination."""
    # Table tags
    table_tags = get_table_tags(client, warehouse_id, source_catalog, schema, table_name)
    for tag in table_tags:
        sql = (
            f"ALTER TABLE `{dest_catalog}`.`{schema}`.`{table_name}` "
            f"SET TAGS ('{tag['tag_name']}' = '{tag['tag_value']}')"
        )
        try:
            execute_sql(client, warehouse_id, sql, dry_run=dry_run)
        except Exception as e:
            logger.error(f"Failed to set table tag {tag['tag_name']} on {schema}.{table_name}: {e}")

    if table_tags:
        logger.info(
            f"{'[DRY RUN] ' if dry_run else ''}"
            f"Copied {len(table_tags)} table tags to {dest_catalog}.{schema}.{table_name}"
        )

    # Column tags
    col_tags = get_column_tags(client, warehouse_id, source_catalog, schema, table_name)
    for tag in col_tags:
        sql = (
            f"ALTER TABLE `{dest_catalog}`.`{schema}`.`{table_name}` "
            f"ALTER COLUMN `{tag['column_name']}` "
            f"SET TAGS ('{tag['tag_name']}' = '{tag['tag_value']}')"
        )
        try:
            execute_sql(client, warehouse_id, sql, dry_run=dry_run)
        except Exception as e:
            logger.error(
                f"Failed to set column tag {tag['tag_name']} on "
                f"{schema}.{table_name}.{tag['column_name']}: {e}"
            )

    if col_tags:
        logger.info(
            f"{'[DRY RUN] ' if dry_run else ''}"
            f"Copied {len(col_tags)} column tags to {dest_catalog}.{schema}.{table_name}"
        )


def get_table_properties(
    client: WorkspaceClient, warehouse_id: str, catalog: str, schema: str, table_name: str
) -> list[dict]:
    """Get TBLPROPERTIES for a table."""
    sql = f"SHOW TBLPROPERTIES `{catalog}`.`{schema}`.`{table_name}`"
    try:
        rows = execute_sql(client, warehouse_id, sql)
        # Filter out internal/delta properties that shouldn't be copied
        internal_prefixes = ("delta.", "spark.", "option.", "transient_lastDdlTime")
        return [r for r in rows if not any(str(r.get("key", "")).startswith(p) for p in internal_prefixes)]
    except Exception as e:
        logger.debug(f"Could not fetch table properties for {schema}.{table_name}: {e}")
        return []


def copy_table_properties(
    client: WorkspaceClient, warehouse_id: str,
    source_catalog: str, dest_catalog: str, schema: str, table_name: str,
    dry_run: bool = False,
) -> None:
    """Copy TBLPROPERTIES from source table to destination table."""
    props = get_table_properties(client, warehouse_id, source_catalog, schema, table_name)
    if not props:
        return

    props_str = ", ".join(f"'{p['key']}' = '{p['value']}'" for p in props)
    sql = f"ALTER TABLE `{dest_catalog}`.`{schema}`.`{table_name}` SET TBLPROPERTIES ({props_str})"

    try:
        execute_sql(client, warehouse_id, sql, dry_run=dry_run)
        logger.info(
            f"{'[DRY RUN] ' if dry_run else ''}"
            f"Copied {len(props)} table properties to {dest_catalog}.{schema}.{table_name}"
        )
    except Exception as e:
        logger.error(f"Failed to copy table properties for {schema}.{table_name}: {e}")
