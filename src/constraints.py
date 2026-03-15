import logging

from databricks.sdk import WorkspaceClient

from src.client import execute_sql

logger = logging.getLogger(__name__)


def get_table_constraints(
    client: WorkspaceClient, warehouse_id: str, catalog: str, schema: str, table_name: str
) -> list[dict]:
    """Get CHECK constraints for a table."""
    sql = f"""
        SELECT constraint_name, constraint_type, check_clause
        FROM {catalog}.information_schema.table_constraints
        WHERE table_schema = '{schema}'
        AND table_name = '{table_name}'
        AND constraint_type = 'CHECK'
    """
    try:
        return execute_sql(client, warehouse_id, sql)
    except Exception as e:
        logger.debug(f"Could not fetch constraints for {schema}.{table_name}: {e}")
        return []


def copy_table_constraints(
    client: WorkspaceClient, warehouse_id: str,
    source_catalog: str, dest_catalog: str, schema: str, table_name: str,
    dry_run: bool = False,
) -> None:
    """Copy CHECK constraints from source to destination table."""
    constraints = get_table_constraints(client, warehouse_id, source_catalog, schema, table_name)
    if not constraints:
        return

    for c in constraints:
        constraint_name = c.get("constraint_name", "")
        check_clause = c.get("check_clause", "")
        if not check_clause:
            continue

        sql = (
            f"ALTER TABLE `{dest_catalog}`.`{schema}`.`{table_name}` "
            f"ADD CONSTRAINT `{constraint_name}` CHECK ({check_clause})"
        )
        try:
            execute_sql(client, warehouse_id, sql, dry_run=dry_run)
            logger.info(
                f"{'[DRY RUN] ' if dry_run else ''}"
                f"Copied constraint {constraint_name} to {dest_catalog}.{schema}.{table_name}"
            )
        except Exception as e:
            logger.error(
                f"Failed to copy constraint {constraint_name} "
                f"on {schema}.{table_name}: {e}"
            )

    if constraints:
        logger.info(
            f"{'[DRY RUN] ' if dry_run else ''}"
            f"Copied {len(constraints)} constraints to {dest_catalog}.{schema}.{table_name}"
        )


def get_column_comments(
    client: WorkspaceClient, warehouse_id: str, catalog: str, schema: str, table_name: str
) -> list[dict]:
    """Get column-level comments for a table."""
    sql = f"""
        SELECT column_name, comment
        FROM {catalog}.information_schema.columns
        WHERE table_schema = '{schema}'
        AND table_name = '{table_name}'
        AND comment IS NOT NULL
        AND comment != ''
    """
    try:
        return execute_sql(client, warehouse_id, sql)
    except Exception as e:
        logger.debug(f"Could not fetch column comments for {schema}.{table_name}: {e}")
        return []


def get_table_comment(
    client: WorkspaceClient, warehouse_id: str, catalog: str, schema: str, table_name: str
) -> str | None:
    """Get the table-level comment."""
    sql = f"""
        SELECT comment
        FROM {catalog}.information_schema.tables
        WHERE table_schema = '{schema}'
        AND table_name = '{table_name}'
        AND comment IS NOT NULL
        AND comment != ''
    """
    try:
        rows = execute_sql(client, warehouse_id, sql)
        if rows:
            return rows[0].get("comment")
    except Exception as e:
        logger.debug(f"Could not fetch table comment for {schema}.{table_name}: {e}")
    return None


def copy_table_comments(
    client: WorkspaceClient, warehouse_id: str,
    source_catalog: str, dest_catalog: str, schema: str, table_name: str,
    dry_run: bool = False,
) -> None:
    """Copy table and column comments from source to destination."""
    dest = f"`{dest_catalog}`.`{schema}`.`{table_name}`"

    # Table comment
    table_comment = get_table_comment(client, warehouse_id, source_catalog, schema, table_name)
    if table_comment:
        escaped = table_comment.replace("'", "\\'")
        sql = f"COMMENT ON TABLE {dest} IS '{escaped}'"
        try:
            execute_sql(client, warehouse_id, sql, dry_run=dry_run)
            logger.info(f"{'[DRY RUN] ' if dry_run else ''}Set table comment on {dest}")
        except Exception as e:
            logger.error(f"Failed to set table comment on {dest}: {e}")

    # Column comments
    col_comments = get_column_comments(client, warehouse_id, source_catalog, schema, table_name)
    for col in col_comments:
        col_name = col["column_name"]
        comment = col["comment"].replace("'", "\\'")
        sql = f"ALTER TABLE {dest} ALTER COLUMN `{col_name}` COMMENT '{comment}'"
        try:
            execute_sql(client, warehouse_id, sql, dry_run=dry_run)
        except Exception as e:
            logger.error(f"Failed to set column comment on {dest}.{col_name}: {e}")

    if col_comments:
        logger.info(
            f"{'[DRY RUN] ' if dry_run else ''}"
            f"Copied {len(col_comments)} column comments to {dest}"
        )
