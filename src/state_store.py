"""State store — persistent tracking of clone state using Delta tables."""

import json
import logging
import os
from datetime import datetime

from src.client import execute_sql

logger = logging.getLogger(__name__)

DEFAULT_STATE_CATALOG = "clone_audit"
DEFAULT_STATE_SCHEMA = "state"


class StateStore:
    """Delta table-based state store for tracking clone operations."""

    def __init__(
        self,
        client,
        warehouse_id: str,
        state_catalog: str = DEFAULT_STATE_CATALOG,
        state_schema: str = DEFAULT_STATE_SCHEMA,
    ):
        self.client = client
        self.warehouse_id = warehouse_id
        self.state_catalog = state_catalog
        self.state_schema = state_schema
        self._clone_state_table = f"{state_catalog}.{state_schema}.clone_state"
        self._operations_table = f"{state_catalog}.{state_schema}.clone_operations"

    def init_tables(self) -> None:
        """Create the state tracking Delta tables if they don't exist."""
        execute_sql(self.client, self.warehouse_id,
                    f"CREATE CATALOG IF NOT EXISTS {self.state_catalog}")
        execute_sql(self.client, self.warehouse_id,
                    f"CREATE SCHEMA IF NOT EXISTS {self.state_catalog}.{self.state_schema}")

        # Clone state table — tracks individual table clone status
        execute_sql(self.client, self.warehouse_id, f"""
            CREATE TABLE IF NOT EXISTS {self._clone_state_table} (
                source_fqn STRING NOT NULL,
                dest_fqn STRING NOT NULL,
                object_type STRING NOT NULL,
                clone_type STRING,
                last_cloned_at TIMESTAMP,
                last_status STRING,
                source_row_count BIGINT,
                dest_row_count BIGINT,
                source_size_bytes BIGINT,
                source_version BIGINT,
                is_stale BOOLEAN DEFAULT false,
                error_message STRING
            )
            USING DELTA
            COMMENT 'Per-table clone state tracking'
            TBLPROPERTIES (
                'delta.enableChangeDataFeed' = 'true',
                'delta.autoOptimize.optimizeWrite' = 'true'
            )
        """)

        # Operations table — tracks overall clone operations
        execute_sql(self.client, self.warehouse_id, f"""
            CREATE TABLE IF NOT EXISTS {self._operations_table} (
                operation_id STRING,
                source_catalog STRING,
                dest_catalog STRING,
                clone_type STRING,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                status STRING,
                tables_total INT,
                tables_cloned INT,
                tables_failed INT,
                summary STRING
            )
            USING DELTA
            COMMENT 'Clone operation history'
            TBLPROPERTIES (
                'delta.enableChangeDataFeed' = 'true',
                'delta.autoOptimize.optimizeWrite' = 'true'
            )
        """)

        logger.info(f"State store tables ready in {self.state_catalog}.{self.state_schema}")

    def record_table_clone(
        self,
        source_fqn: str,
        dest_fqn: str,
        object_type: str = "TABLE",
        clone_type: str = "DEEP",
        status: str = "success",
        row_count: int | None = None,
        size_bytes: int | None = None,
        version: int | None = None,
        error_message: str | None = None,
    ) -> None:
        """Record a table clone result (upsert)."""
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        row_val = str(row_count) if row_count is not None else "NULL"
        size_val = str(size_bytes) if size_bytes is not None else "NULL"
        ver_val = str(version) if version is not None else "NULL"
        err_val = f"'{error_message}'" if error_message else "NULL"

        sql = f"""
        MERGE INTO {self._clone_state_table} AS target
        USING (SELECT '{source_fqn}' AS source_fqn, '{dest_fqn}' AS dest_fqn) AS source
        ON target.source_fqn = source.source_fqn AND target.dest_fqn = source.dest_fqn
        WHEN MATCHED THEN UPDATE SET
            clone_type = '{clone_type}',
            last_cloned_at = '{now}',
            last_status = '{status}',
            source_row_count = {row_val},
            dest_row_count = {row_val},
            source_size_bytes = {size_val},
            source_version = {ver_val},
            is_stale = false,
            error_message = {err_val}
        WHEN NOT MATCHED THEN INSERT
            (source_fqn, dest_fqn, object_type, clone_type, last_cloned_at,
             last_status, source_row_count, dest_row_count, source_size_bytes,
             source_version, is_stale, error_message)
        VALUES
            ('{source_fqn}', '{dest_fqn}', '{object_type}', '{clone_type}', '{now}',
             '{status}', {row_val}, {row_val}, {size_val},
             {ver_val}, false, {err_val})
        """
        try:
            execute_sql(self.client, self.warehouse_id, sql)
        except Exception as e:
            logger.warning(f"Failed to record clone state for {source_fqn}: {e}")

    def record_operation(
        self,
        operation_id: str,
        source_catalog: str,
        dest_catalog: str,
        clone_type: str,
        status: str = "running",
    ) -> None:
        """Record a clone operation start."""
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        sql = f"""
        INSERT INTO {self._operations_table}
        (operation_id, source_catalog, dest_catalog, clone_type, started_at, status)
        VALUES ('{operation_id}', '{source_catalog}', '{dest_catalog}', '{clone_type}', '{now}', '{status}')
        """
        try:
            execute_sql(self.client, self.warehouse_id, sql)
        except Exception as e:
            logger.warning(f"Failed to record operation: {e}")

    def complete_operation(
        self,
        operation_id: str,
        status: str,
        tables_total: int,
        tables_cloned: int,
        tables_failed: int,
        summary: dict | None = None,
    ) -> None:
        """Mark an operation as complete."""
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        summary_json = json.dumps(summary).replace("'", "\\\\'") if summary else ""

        sql = f"""
        UPDATE {self._operations_table}
        SET completed_at = '{now}',
            status = '{status}',
            tables_total = {tables_total},
            tables_cloned = {tables_cloned},
            tables_failed = {tables_failed},
            summary = '{summary_json}'
        WHERE operation_id = '{operation_id}'
        """
        try:
            execute_sql(self.client, self.warehouse_id, sql)
        except Exception as e:
            logger.warning(f"Failed to complete operation: {e}")

    def get_table_state(self, source_fqn: str, dest_fqn: str) -> dict | None:
        """Get the clone state of a specific table."""
        sql = f"""
        SELECT * FROM {self._clone_state_table}
        WHERE source_fqn = '{source_fqn}' AND dest_fqn = '{dest_fqn}'
        """
        rows = execute_sql(self.client, self.warehouse_id, sql)
        return rows[0] if rows else None

    def get_synced_tables(self, source_catalog: str, dest_catalog: str) -> list[dict]:
        """Get all successfully cloned tables for a catalog pair."""
        sql = f"""
        SELECT * FROM {self._clone_state_table}
        WHERE source_fqn LIKE '{source_catalog}.%'
          AND dest_fqn LIKE '{dest_catalog}.%'
          AND last_status = 'success'
          AND is_stale = false
        ORDER BY source_fqn
        """
        return execute_sql(self.client, self.warehouse_id, sql)

    def get_stale_tables(self, source_catalog: str, dest_catalog: str) -> list[dict]:
        """Get tables that are marked as stale (need re-clone)."""
        sql = f"""
        SELECT * FROM {self._clone_state_table}
        WHERE source_fqn LIKE '{source_catalog}.%'
          AND dest_fqn LIKE '{dest_catalog}.%'
          AND is_stale = true
        ORDER BY source_fqn
        """
        return execute_sql(self.client, self.warehouse_id, sql)

    def get_failed_tables(self, source_catalog: str, dest_catalog: str) -> list[dict]:
        """Get tables that failed to clone."""
        sql = f"""
        SELECT * FROM {self._clone_state_table}
        WHERE source_fqn LIKE '{source_catalog}.%'
          AND dest_fqn LIKE '{dest_catalog}.%'
          AND last_status = 'failed'
        ORDER BY source_fqn
        """
        return execute_sql(self.client, self.warehouse_id, sql)

    def mark_stale(self, source_fqn: str, dest_fqn: str) -> None:
        """Mark a table as stale (needs re-clone)."""
        sql = f"""
        UPDATE {self._clone_state_table}
        SET is_stale = true
        WHERE source_fqn = '{source_fqn}' AND dest_fqn = '{dest_fqn}'
        """
        execute_sql(self.client, self.warehouse_id, sql)

    def mark_all_stale(self, source_catalog: str, dest_catalog: str) -> None:
        """Mark all tables for a catalog pair as stale."""
        sql = f"""
        UPDATE {self._clone_state_table}
        SET is_stale = true
        WHERE source_fqn LIKE '{source_catalog}.%'
          AND dest_fqn LIKE '{dest_catalog}.%'
        """
        execute_sql(self.client, self.warehouse_id, sql)
        logger.info(f"Marked all tables as stale: {source_catalog} -> {dest_catalog}")

    def get_operations(self, limit: int = 20) -> list[dict]:
        """Get recent clone operations."""
        sql = f"""
        SELECT * FROM {self._operations_table}
        ORDER BY started_at DESC
        LIMIT {limit}
        """
        return execute_sql(self.client, self.warehouse_id, sql)

    def get_summary(self, source_catalog: str, dest_catalog: str) -> dict:
        """Get summary statistics for a catalog pair."""
        sql = f"""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN last_status = 'success' AND is_stale = false THEN 1 ELSE 0 END) AS synced,
            SUM(CASE WHEN is_stale = true THEN 1 ELSE 0 END) AS stale,
            SUM(CASE WHEN last_status = 'failed' THEN 1 ELSE 0 END) AS failed
        FROM {self._clone_state_table}
        WHERE source_fqn LIKE '{source_catalog}.%'
          AND dest_fqn LIKE '{dest_catalog}.%'
        """
        rows = execute_sql(self.client, self.warehouse_id, sql)
        stats = rows[0] if rows else {"total": 0, "synced": 0, "stale": 0, "failed": 0}

        summary = {
            "source_catalog": source_catalog,
            "dest_catalog": dest_catalog,
            "total_tracked": int(stats.get("total") or 0),
            "synced": int(stats.get("synced") or 0),
            "stale": int(stats.get("stale") or 0),
            "failed": int(stats.get("failed") or 0),
        }

        logger.info("=" * 60)
        logger.info(f"STATE STORE SUMMARY: {source_catalog} -> {dest_catalog}")
        logger.info("=" * 60)
        logger.info(f"Total tracked: {summary['total_tracked']}")
        logger.info(f"Synced:        {summary['synced']}")
        logger.info(f"Stale:         {summary['stale']}")
        logger.info(f"Failed:        {summary['failed']}")

        return summary
