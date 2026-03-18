"""Data lineage tracking — record source→dest mappings for clone operations."""

import logging
import os
from datetime import datetime

from src.client import execute_sql

logger = logging.getLogger(__name__)


def ensure_lineage_table(client, warehouse_id: str, lineage_catalog: str = "clone_audit") -> str:
    """Create the lineage tracking table if it doesn't exist.

    Returns:
        Fully qualified table name.
    """
    schema = "lineage"
    table = "clone_lineage"
    fqn = f"{lineage_catalog}.{schema}.{table}"

    execute_sql(client, warehouse_id, f"CREATE CATALOG IF NOT EXISTS {lineage_catalog}")
    execute_sql(client, warehouse_id, f"CREATE SCHEMA IF NOT EXISTS {lineage_catalog}.{schema}")

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {fqn} (
        lineage_id STRING,
        operation_id STRING,
        source_catalog STRING,
        source_schema STRING,
        source_table STRING,
        source_fqn STRING,
        dest_catalog STRING,
        dest_schema STRING,
        dest_table STRING,
        dest_fqn STRING,
        object_type STRING,
        clone_type STRING,
        cloned_at TIMESTAMP,
        cloned_by STRING,
        clone_status STRING,
        row_count BIGINT,
        size_bytes BIGINT,
        metadata MAP<STRING, STRING>
    )
    USING DELTA
    COMMENT 'Lineage tracking for catalog clone operations'
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'delta.autoOptimize.optimizeWrite' = 'true'
    )
    """
    execute_sql(client, warehouse_id, create_sql)
    logger.info(f"Lineage table ready: {fqn}")
    return fqn


def record_lineage(
    client,
    warehouse_id: str,
    operation_id: str,
    source_catalog: str,
    source_schema: str,
    source_table: str,
    dest_catalog: str,
    dest_schema: str,
    dest_table: str,
    object_type: str = "TABLE",
    clone_type: str = "DEEP",
    clone_status: str = "success",
    row_count: int | None = None,
    size_bytes: int | None = None,
    lineage_catalog: str = "clone_audit",
) -> None:
    """Record a single lineage entry for a cloned object."""
    import uuid
    fqn = f"{lineage_catalog}.lineage.clone_lineage"
    lineage_id = str(uuid.uuid4())
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    user = os.environ.get("USER", os.environ.get("USERNAME", "unknown"))

    source_fqn = f"{source_catalog}.{source_schema}.{source_table}"
    dest_fqn = f"{dest_catalog}.{dest_schema}.{dest_table}"

    row_count_val = str(row_count) if row_count is not None else "NULL"
    size_val = str(size_bytes) if size_bytes is not None else "NULL"

    sql = f"""
    INSERT INTO {fqn}
    (lineage_id, operation_id, source_catalog, source_schema, source_table,
     source_fqn, dest_catalog, dest_schema, dest_table, dest_fqn,
     object_type, clone_type, cloned_at, cloned_by, clone_status,
     row_count, size_bytes)
    VALUES
    ('{lineage_id}', '{operation_id}', '{source_catalog}', '{source_schema}', '{source_table}',
     '{source_fqn}', '{dest_catalog}', '{dest_schema}', '{dest_table}', '{dest_fqn}',
     '{object_type}', '{clone_type}', '{now}', '{user}', '{clone_status}',
     {row_count_val}, {size_val})
    """
    try:
        execute_sql(client, warehouse_id, sql)
    except Exception as e:
        logger.warning(f"Failed to record lineage for {source_fqn}: {e}")


def record_batch_lineage(
    client,
    warehouse_id: str,
    operation_id: str,
    source_catalog: str,
    dest_catalog: str,
    clone_type: str,
    cloned_objects: list[dict],
    lineage_catalog: str = "clone_audit",
) -> int:
    """Record lineage for a batch of cloned objects.

    Args:
        cloned_objects: List of dicts with keys: schema, table, object_type, status,
                        and optionally row_count, size_bytes.

    Returns:
        Number of lineage records written.
    """
    count = 0
    for obj in cloned_objects:
        record_lineage(
            client, warehouse_id, operation_id,
            source_catalog, obj["schema"], obj["table"],
            dest_catalog, obj["schema"], obj["table"],
            object_type=obj.get("object_type", "TABLE"),
            clone_type=clone_type,
            clone_status=obj.get("status", "success"),
            row_count=obj.get("row_count"),
            size_bytes=obj.get("size_bytes"),
            lineage_catalog=lineage_catalog,
        )
        count += 1

    logger.info(f"Recorded {count} lineage entries for operation {operation_id}")
    return count


def query_lineage(
    client,
    warehouse_id: str,
    table_fqn: str | None = None,
    operation_id: str | None = None,
    limit: int = 50,
    lineage_catalog: str = "clone_audit",
) -> list[dict]:
    """Query lineage history for a table or operation.

    Args:
        table_fqn: Fully qualified table name (searches both source and dest).
        operation_id: Filter by operation ID.
        limit: Max results.

    Returns:
        List of lineage records.
    """
    fqn = f"{lineage_catalog}.lineage.clone_lineage"
    where_clauses = []
    if table_fqn:
        where_clauses.append(f"(source_fqn = '{table_fqn}' OR dest_fqn = '{table_fqn}')")
    if operation_id:
        where_clauses.append(f"operation_id = '{operation_id}'")

    where = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

    sql = f"""
    SELECT lineage_id, operation_id, source_fqn, dest_fqn,
           object_type, clone_type, cloned_at, cloned_by, clone_status,
           row_count, size_bytes
    FROM {fqn}
    {where}
    ORDER BY cloned_at DESC
    LIMIT {limit}
    """
    rows = execute_sql(client, warehouse_id, sql)

    logger.info(f"Lineage query returned {len(rows)} records:")
    for row in rows:
        logger.info(
            f"  {row['source_fqn']} -> {row['dest_fqn']} | "
            f"{row['clone_type']} | {row['clone_status']} | {row['cloned_at']}"
        )

    return rows
