import logging

from databricks.sdk import WorkspaceClient

from src.client import execute_sql

logger = logging.getLogger(__name__)


def apply_masking_rules(
    client: WorkspaceClient,
    warehouse_id: str,
    dest_catalog: str,
    schema: str,
    table_name: str,
    masking_rules: list[dict],
    dry_run: bool = False,
) -> int:
    """Apply data masking rules to a destination table after cloning.

    Each rule dict should have:
        - column: column name or regex pattern
        - strategy: masking strategy name
        - match_type: "exact" or "regex" (default: "exact")

    Strategies:
        - "hash": Replace with SHA2 hash
        - "redact": Replace with '***REDACTED***'
        - "null": Set to NULL
        - "email_mask": Mask email (j***@example.com)
        - "partial": Show first and last char only
        - custom SQL expression

    Returns number of columns masked.
    """
    import re

    dest = f"`{dest_catalog}`.`{schema}`.`{table_name}`"

    # Get columns for this table
    col_sql = f"""
        SELECT column_name, data_type
        FROM {dest_catalog}.information_schema.columns
        WHERE table_schema = '{schema}'
        AND table_name = '{table_name}'
    """
    columns = execute_sql(client, warehouse_id, col_sql)
    col_map = {c["column_name"]: c["data_type"] for c in columns}

    masked_count = 0
    update_parts = []

    for rule in masking_rules:
        column_pattern = rule.get("column", "")
        strategy = rule.get("strategy", "redact")
        match_type = rule.get("match_type", "exact")

        # Find matching columns
        matched_columns = []
        if match_type == "regex":
            for col_name in col_map:
                if re.search(column_pattern, col_name, re.IGNORECASE):
                    matched_columns.append(col_name)
        else:
            if column_pattern in col_map:
                matched_columns.append(column_pattern)

        for col_name in matched_columns:
            mask_expr = _get_mask_expression(col_name, strategy, col_map[col_name])
            if mask_expr:
                update_parts.append(f"`{col_name}` = {mask_expr}")
                masked_count += 1

    if not update_parts:
        return 0

    # Apply all masks in a single UPDATE
    sql = f"UPDATE {dest} SET {', '.join(update_parts)}"

    try:
        execute_sql(client, warehouse_id, sql, dry_run=dry_run)
        logger.info(
            f"{'[DRY RUN] ' if dry_run else ''}"
            f"Masked {masked_count} columns in {dest}"
        )
    except Exception as e:
        logger.error(f"Failed to apply masking to {dest}: {e}")

    return masked_count


def _get_mask_expression(column_name: str, strategy: str, data_type: str) -> str | None:
    """Get the SQL expression for a masking strategy."""
    dt = data_type.upper()

    if strategy == "hash":
        if "STRING" in dt or "VARCHAR" in dt or "CHAR" in dt:
            return f"SHA2(`{column_name}`, 256)"
        return f"CAST(SHA2(CAST(`{column_name}` AS STRING), 256) AS {data_type})"

    if strategy == "redact":
        if "STRING" in dt or "VARCHAR" in dt or "CHAR" in dt:
            return "'***REDACTED***'"
        if "INT" in dt or "LONG" in dt or "DOUBLE" in dt or "FLOAT" in dt or "DECIMAL" in dt:
            return "0"
        return "NULL"

    if strategy == "null":
        return "NULL"

    if strategy == "email_mask":
        return (
            f"CONCAT(SUBSTRING(`{column_name}`, 1, 1), '***@', "
            f"SUBSTRING_INDEX(`{column_name}`, '@', -1))"
        )

    if strategy == "partial":
        return (
            f"CONCAT(SUBSTRING(`{column_name}`, 1, 1), "
            f"REPEAT('*', LENGTH(`{column_name}`) - 2), "
            f"SUBSTRING(`{column_name}`, LENGTH(`{column_name}`), 1))"
        )

    # Treat as custom SQL expression
    return strategy


def mask_subject_rows(
    client: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
    schema: str,
    table_name: str,
    identifier_column: str,
    identifier_value: str,
    masking_rules: list[dict] | None = None,
    dry_run: bool = False,
) -> dict:
    """Apply masking to specific rows matching a subject identifier.

    Unlike apply_masking_rules() which masks entire columns, this function
    targets only rows where identifier_column = identifier_value.

    Args:
        client: Databricks workspace client.
        warehouse_id: SQL warehouse ID.
        catalog: Catalog name.
        schema: Schema name.
        table_name: Table name.
        identifier_column: Column used to identify the subject (e.g., "email").
        identifier_value: Value to match (e.g., "user@example.com").
        masking_rules: Optional explicit rules. If None, auto-detects PII columns.
        dry_run: If True, returns SQL without executing.

    Returns:
        Dict with columns_masked count and sql_executed.
    """
    import re

    dest = f"`{catalog}`.`{schema}`.`{table_name}`"

    # Get columns for this table
    col_sql = f"""
        SELECT column_name, data_type
        FROM {catalog}.information_schema.columns
        WHERE table_schema = '{schema}'
        AND table_name = '{table_name}'
    """
    columns = execute_sql(client, warehouse_id, col_sql)
    col_map = {c["column_name"]: c["data_type"] for c in columns}

    update_parts = []

    if masking_rules:
        # Use explicit rules
        for rule in masking_rules:
            col_name = rule.get("column", "")
            strategy = rule.get("strategy", "redact")
            if col_name in col_map:
                mask_expr = _get_mask_expression(col_name, strategy, col_map[col_name])
                if mask_expr:
                    update_parts.append(f"`{col_name}` = {mask_expr}")
    else:
        # Auto-detect PII columns using column name patterns
        from src.pii_detection import COLUMN_NAME_PATTERNS, SUGGESTED_MASKING

        for col_name, data_type in col_map.items():
            if col_name == identifier_column:
                continue
            for pattern, pii_type in COLUMN_NAME_PATTERNS.items():
                if re.search(pattern, col_name):
                    strategy = SUGGESTED_MASKING.get(pii_type, "redact")
                    mask_expr = _get_mask_expression(col_name, strategy, data_type)
                    if mask_expr:
                        update_parts.append(f"`{col_name}` = {mask_expr}")
                    break

        # Also mask the identifier column itself
        id_mask = _get_mask_expression(identifier_column, "hash", col_map.get(identifier_column, "STRING"))
        if id_mask:
            update_parts.append(f"`{identifier_column}` = {id_mask}")

    if not update_parts:
        return {"columns_masked": 0, "sql_executed": None}

    sql = f"UPDATE {dest} SET {', '.join(update_parts)} WHERE `{identifier_column}` = '{identifier_value}'"

    if dry_run:
        logger.info(f"[DRY RUN] Row-level masking SQL: {sql}")
        return {"columns_masked": len(update_parts), "sql_executed": sql, "dry_run": True}

    try:
        execute_sql(client, warehouse_id, sql)
        logger.info(f"Masked {len(update_parts)} columns for subject rows in {dest}")
    except Exception as e:
        logger.error(f"Failed to apply row-level masking to {dest}: {e}")
        raise

    return {"columns_masked": len(update_parts), "sql_executed": sql}
