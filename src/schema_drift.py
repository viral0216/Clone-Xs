import logging

from databricks.sdk import WorkspaceClient

from src.client import execute_sql

logger = logging.getLogger(__name__)

# Severity levels for drift changes
SEVERITY_BREAKING = "BREAKING"    # Will likely break downstream queries
SEVERITY_CAUTION = "CAUTION"      # May cause issues, needs review
SEVERITY_INFO = "INFO"            # Safe / cosmetic change


def classify_severity(change_type: str, field: str | None = None, source_val: str | None = None, dest_val: str | None = None) -> str:
    """Classify the severity of a schema change."""
    if change_type == "removed":
        return SEVERITY_BREAKING
    if change_type == "added":
        return SEVERITY_INFO
    if change_type == "order":
        return SEVERITY_INFO
    # Modified column
    if field == "data_type":
        return SEVERITY_BREAKING
    if field == "is_nullable":
        # Nullable → NOT NULL is breaking; NOT NULL → Nullable is safe
        if str(source_val).upper() == "YES" and str(dest_val).upper() == "NO":
            return SEVERITY_BREAKING
        return SEVERITY_CAUTION
    if field == "column_default":
        return SEVERITY_CAUTION
    if field == "comment":
        return SEVERITY_INFO
    return SEVERITY_CAUTION


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
            for field in ("data_type", "is_nullable", "column_default", "comment"):
                src_val = str(src_col.get(field, "") or "")
                dst_val = str(dst_col.get(field, "") or "")
                if src_val != dst_val:
                    diffs[field] = {
                        "source": src_col.get(field),
                        "dest": dst_col.get(field),
                        "severity": classify_severity("modified", field, src_col.get(field), dst_col.get(field)),
                    }
            if diffs:
                # Table-level severity is the worst among all field diffs
                severities = [v["severity"] for v in diffs.values()]
                worst = SEVERITY_BREAKING if SEVERITY_BREAKING in severities else (
                    SEVERITY_CAUTION if SEVERITY_CAUTION in severities else SEVERITY_INFO
                )
                modified.append({"column": name, "differences": diffs, "severity": worst})

    # Check column order
    common_order = [n for n in source_names if n in dest_map]
    dest_order = [n for n in dest_names if n in source_map]
    order_changed = common_order != dest_order

    # Compute overall table severity
    table_severity = SEVERITY_INFO
    if removed:
        table_severity = SEVERITY_BREAKING
    elif any(m.get("severity") == SEVERITY_BREAKING for m in modified):
        table_severity = SEVERITY_BREAKING
    elif any(m.get("severity") == SEVERITY_CAUTION for m in modified):
        table_severity = SEVERITY_CAUTION

    return {
        "schema": schema,
        "table": table_name,
        "added_in_source": added,
        "removed_from_source": removed,
        "modified": modified,
        "order_changed": order_changed,
        "has_drift": bool(added or removed or modified or order_changed),
        "severity": table_severity,
    }


def _list_schemas(client: WorkspaceClient, warehouse_id: str, catalog: str, exclude: list[str]) -> list[str]:
    """List schemas in a catalog, excluding system schemas."""
    exclude_clause = ",".join(f"'{s}'" for s in exclude)
    sql = f"""
        SELECT schema_name
        FROM {catalog}.information_schema.schemata
        WHERE schema_name NOT IN ({exclude_clause})
    """
    rows = execute_sql(client, warehouse_id, sql)
    return [r["schema_name"] for r in rows]


def _list_tables(client: WorkspaceClient, warehouse_id: str, catalog: str, schema: str) -> list[str]:
    """List managed/external table names in a schema."""
    sql = f"""
        SELECT table_name
        FROM {catalog}.information_schema.tables
        WHERE table_schema = '{schema}'
        AND table_type IN ('MANAGED', 'EXTERNAL')
    """
    rows = execute_sql(client, warehouse_id, sql)
    return [r["table_name"] for r in rows]


def detect_schema_drift(
    client: WorkspaceClient,
    warehouse_id: str,
    source_catalog: str,
    dest_catalog: str,
    exclude_schemas: list[str],
    include_schemas: list[str] | None = None,
) -> dict:
    """Detect 3-tier schema drift across catalogs.

    Tier 1: Schema existence — schemas missing from source or destination.
    Tier 2: Table existence — tables missing from source or destination.
    Tier 3: Column differences — added, removed, modified columns within matching tables.
    """
    logger.info(f"Detecting schema drift: {source_catalog} vs {dest_catalog}")

    # ── Tier 1: Schema-level existence ───────────────────
    if include_schemas:
        source_schemas = set(s for s in include_schemas if s not in exclude_schemas)
        dest_schemas = set(s for s in include_schemas if s not in exclude_schemas)
    else:
        source_schemas = set(_list_schemas(client, warehouse_id, source_catalog, exclude_schemas))
        dest_schemas = set(_list_schemas(client, warehouse_id, dest_catalog, exclude_schemas))

    schemas_only_in_source = sorted(source_schemas - dest_schemas)
    schemas_only_in_dest = sorted(dest_schemas - source_schemas)
    common_schemas = sorted(source_schemas & dest_schemas)

    # ── Tier 2 & 3: Table & column-level drift ──────────
    tables_only_in_source = []  # [{"schema": ..., "table": ...}]
    tables_only_in_dest = []
    column_drifts = []
    total_tables_checked = 0

    for schema in common_schemas:
        source_tables = set(_list_tables(client, warehouse_id, source_catalog, schema))
        dest_tables = set(_list_tables(client, warehouse_id, dest_catalog, schema))

        for t in sorted(source_tables - dest_tables):
            tables_only_in_source.append({"schema": schema, "table": t})
        for t in sorted(dest_tables - source_tables):
            tables_only_in_dest.append({"schema": schema, "table": t})

        common_tables = sorted(source_tables & dest_tables)
        total_tables_checked += len(common_tables)

        for table_name in common_tables:
            try:
                drift = compare_table_schema(
                    client, warehouse_id, source_catalog, dest_catalog, schema, table_name,
                )
                if drift["has_drift"]:
                    column_drifts.append(drift)
            except Exception as e:
                logger.warning(f"Could not compare {schema}.{table_name}: {e}")

    summary = {
        "total_tables_checked": total_tables_checked,
        "tables_with_drift": len(column_drifts),
        # Tier 1: schema existence
        "schemas_only_in_source": schemas_only_in_source,
        "schemas_only_in_dest": schemas_only_in_dest,
        # Tier 2: table existence
        "tables_only_in_source": tables_only_in_source,
        "tables_only_in_dest": tables_only_in_dest,
        # Tier 3: column drifts
        "drifts": column_drifts,
    }

    # Print summary
    logger.info("=" * 60)
    logger.info(f"SCHEMA DRIFT REPORT: {source_catalog} vs {dest_catalog}")
    logger.info("=" * 60)
    logger.info(f"  Schemas only in source: {schemas_only_in_source}")
    logger.info(f"  Schemas only in dest:   {schemas_only_in_dest}")
    logger.info(f"  Tables only in source:  {len(tables_only_in_source)}")
    logger.info(f"  Tables only in dest:    {len(tables_only_in_dest)}")
    logger.info(f"  Tables compared:        {total_tables_checked}")
    logger.info(f"  Tables with col drift:  {len(column_drifts)}")

    for d in column_drifts:
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
