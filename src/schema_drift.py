import logging

from databricks.sdk import WorkspaceClient

from src.client import execute_sql

logger = logging.getLogger(__name__)


def get_columns_info(
    client: WorkspaceClient, warehouse_id: str, catalog: str, schema: str, table_name: str
) -> list[dict]:
    """Get column metadata for a table."""
    sql = f"""
        SELECT column_name, data_type, is_nullable, column_default,
               ordinal_position, character_maximum_length, numeric_precision,
               numeric_scale, comment
        FROM {catalog}.information_schema.columns
        WHERE table_schema = '{schema}'
        AND table_name = '{table_name}'
        ORDER BY ordinal_position
    """
    return execute_sql(client, warehouse_id, sql)


def compare_table_schema(
    client: WorkspaceClient,
    warehouse_id: str,
    source_catalog: str,
    dest_catalog: str,
    schema: str,
    table_name: str,
) -> dict:
    """Compare column definitions between source and destination table.

    Returns a dict with added, removed, modified columns and order changes.
    """
    source_cols = get_columns_info(client, warehouse_id, source_catalog, schema, table_name)
    dest_cols = get_columns_info(client, warehouse_id, dest_catalog, schema, table_name)

    source_map = {c["column_name"]: c for c in source_cols}
    dest_map = {c["column_name"]: c for c in dest_cols}

    source_names = [c["column_name"] for c in source_cols]
    dest_names = [c["column_name"] for c in dest_cols]

    added = [name for name in source_names if name not in dest_map]
    removed = [name for name in dest_names if name not in source_map]

    modified = []
    for name in source_names:
        if name in dest_map:
            src_col = source_map[name]
            dst_col = dest_map[name]
            diffs = {}
            for field in ("data_type", "is_nullable", "column_default"):
                if str(src_col.get(field, "")) != str(dst_col.get(field, "")):
                    diffs[field] = {"source": src_col.get(field), "dest": dst_col.get(field)}
            if diffs:
                modified.append({"column": name, "differences": diffs})

    # Check column order
    common_order = [n for n in source_names if n in dest_map]
    dest_order = [n for n in dest_names if n in source_map]
    order_changed = common_order != dest_order

    return {
        "schema": schema,
        "table": table_name,
        "added_in_source": added,
        "removed_from_source": removed,
        "modified": modified,
        "order_changed": order_changed,
        "has_drift": bool(added or removed or modified or order_changed),
    }


def detect_schema_drift(
    client: WorkspaceClient,
    warehouse_id: str,
    source_catalog: str,
    dest_catalog: str,
    exclude_schemas: list[str],
    include_schemas: list[str] | None = None,
) -> dict:
    """Detect schema drift across all tables in the catalog."""
    logger.info(f"Detecting schema drift: {source_catalog} vs {dest_catalog}")

    # Get schemas
    if include_schemas:
        schemas = [s for s in include_schemas if s not in exclude_schemas]
    else:
        exclude_clause = ",".join(f"'{s}'" for s in exclude_schemas)
        sql = f"""
            SELECT schema_name
            FROM {dest_catalog}.information_schema.schemata
            WHERE schema_name NOT IN ({exclude_clause})
        """
        rows = execute_sql(client, warehouse_id, sql)
        schemas = [r["schema_name"] for r in rows]

    all_drifts = []
    for schema in schemas:
        sql = f"""
            SELECT table_name
            FROM {dest_catalog}.information_schema.tables
            WHERE table_schema = '{schema}'
            AND table_type IN ('MANAGED', 'EXTERNAL')
        """
        tables = execute_sql(client, warehouse_id, sql)

        for row in tables:
            table_name = row["table_name"]
            try:
                drift = compare_table_schema(
                    client, warehouse_id, source_catalog, dest_catalog, schema, table_name,
                )
                if drift["has_drift"]:
                    all_drifts.append(drift)
            except Exception as e:
                logger.warning(f"Could not compare {schema}.{table_name}: {e}")

    summary = {
        "total_tables_checked": sum(
            len(execute_sql(client, warehouse_id,
                f"SELECT table_name FROM {dest_catalog}.information_schema.tables "
                f"WHERE table_schema = '{s}' AND table_type IN ('MANAGED', 'EXTERNAL')"))
            for s in schemas
        ),
        "tables_with_drift": len(all_drifts),
        "drifts": all_drifts,
    }

    # Print summary
    logger.info("=" * 60)
    logger.info(f"SCHEMA DRIFT REPORT: {source_catalog} vs {dest_catalog}")
    logger.info("=" * 60)
    logger.info(f"  Tables checked:    {summary['total_tables_checked']}")
    logger.info(f"  Tables with drift: {summary['tables_with_drift']}")

    for d in all_drifts:
        logger.warning(f"\n  {d['schema']}.{d['table']}:")
        if d["added_in_source"]:
            logger.warning(f"    Columns in source only: {d['added_in_source']}")
        if d["removed_from_source"]:
            logger.warning(f"    Columns in dest only:   {d['removed_from_source']}")
        for m in d["modified"]:
            logger.warning(f"    Column '{m['column']}' modified: {m['differences']}")
        if d["order_changed"]:
            logger.warning("    Column order differs")

    logger.info("=" * 60)
    return summary
