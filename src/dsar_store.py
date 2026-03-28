"""DSAR (Data Subject Access Request) store — Delta table tracking for GDPR Article 15 requests."""

import json
import logging
from datetime import datetime, timezone

from src.client import execute_sql

logger = logging.getLogger(__name__)

DSAR_STATUSES = [
    "received", "discovering", "analyzed", "approved",
    "exporting", "exported", "delivered", "completed",
    "failed", "cancelled",
]

STATUS_TRANSITIONS = {
    "received": ["discovering", "cancelled"],
    "discovering": ["analyzed", "failed"],
    "analyzed": ["approved", "cancelled"],
    "approved": ["exporting", "cancelled"],
    "exporting": ["exported", "failed"],
    "exported": ["delivered", "failed"],
    "delivered": ["completed"],
    "failed": ["received", "cancelled"],
}


class DSARStore:
    """Delta table store for DSAR request lifecycle, actions, and exports."""

    def __init__(self, client, warehouse_id: str, state_catalog: str = "clone_audit", state_schema: str = "dsar"):
        self.client = client
        self.warehouse_id = warehouse_id
        self.state_catalog = state_catalog
        self.state_schema = state_schema
        self._requests_table = f"{state_catalog}.{state_schema}.dsar_requests"
        self._actions_table = f"{state_catalog}.{state_schema}.dsar_actions"
        self._exports_table = f"{state_catalog}.{state_schema}.dsar_exports"

    def init_tables(self) -> None:
        execute_sql(self.client, self.warehouse_id,
                    f"CREATE SCHEMA IF NOT EXISTS {self.state_catalog}.{self.state_schema}")

        execute_sql(self.client, self.warehouse_id, f"""
            CREATE TABLE IF NOT EXISTS {self._requests_table} (
                request_id STRING NOT NULL,
                subject_type STRING NOT NULL,
                subject_value_hash STRING NOT NULL,
                subject_column STRING,
                requester_email STRING,
                requester_name STRING,
                legal_basis STRING,
                export_format STRING,
                scope_catalogs STRING,
                status STRING,
                deadline TIMESTAMP NOT NULL,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                completed_at TIMESTAMP,
                created_by STRING,
                discovery_json STRING,
                affected_tables INT,
                affected_rows BIGINT,
                notes STRING,
                error_message STRING
            ) USING DELTA
            COMMENT 'DSAR / GDPR Article 15 access request lifecycle'
            TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'delta.autoOptimize.optimizeWrite' = 'true')
        """)

        execute_sql(self.client, self.warehouse_id, f"""
            CREATE TABLE IF NOT EXISTS {self._actions_table} (
                action_id STRING NOT NULL,
                request_id STRING NOT NULL,
                action_type STRING NOT NULL,
                catalog STRING,
                schema_name STRING,
                table_name STRING,
                column_name STRING,
                rows_found BIGINT,
                status STRING,
                executed_at TIMESTAMP,
                duration_seconds DOUBLE,
                error_message STRING
            ) USING DELTA
            COMMENT 'DSAR per-table discovery and export actions'
            TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'delta.autoOptimize.optimizeWrite' = 'true')
        """)

        execute_sql(self.client, self.warehouse_id, f"""
            CREATE TABLE IF NOT EXISTS {self._exports_table} (
                export_id STRING NOT NULL,
                request_id STRING NOT NULL,
                format STRING,
                file_path STRING,
                file_size_bytes BIGINT,
                total_rows BIGINT,
                total_tables INT,
                generated_at TIMESTAMP,
                generated_by STRING
            ) USING DELTA
            COMMENT 'DSAR exported data files'
            TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'delta.autoOptimize.optimizeWrite' = 'true')
        """)
        logger.info(f"DSAR store tables ready in {self.state_catalog}.{self.state_schema}")

    def save_request(self, **kwargs) -> None:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        scope_json = json.dumps(kwargs.get("scope_catalogs", []) or []).replace("'", "\\\\'")
        notes_escaped = (kwargs.get("notes", "") or "").replace("'", "\\\\'")
        subj_col = f"'{kwargs.get('subject_column')}'" if kwargs.get("subject_column") else "NULL"

        sql = f"""
        INSERT INTO {self._requests_table}
        (request_id, subject_type, subject_value_hash, subject_column,
         requester_email, requester_name, legal_basis, export_format,
         scope_catalogs, status, deadline, created_at, updated_at, created_by, notes)
        VALUES (
            '{kwargs["request_id"]}', '{kwargs["subject_type"]}', '{kwargs["subject_value_hash"]}', {subj_col},
            '{kwargs["requester_email"]}', '{kwargs["requester_name"]}', '{kwargs["legal_basis"]}',
            '{kwargs.get("export_format", "csv")}',
            '{scope_json}', 'received', '{kwargs["deadline"]}',
            '{now}', '{now}', '{kwargs.get("created_by", "")}', '{notes_escaped}'
        )
        """
        execute_sql(self.client, self.warehouse_id, sql)

    def update_request_status(self, request_id: str, status: str, **kwargs) -> None:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        sets = [f"status = '{status}'", f"updated_at = '{now}'"]
        if status == "completed":
            sets.append(f"completed_at = '{now}'")
        if kwargs.get("error_message"):
            sets.append(f"error_message = '{kwargs['error_message'].replace(chr(39), chr(92)+chr(39))}'")
        if kwargs.get("discovery_json"):
            sets.append(f"discovery_json = '{kwargs['discovery_json'].replace(chr(39), chr(92)+chr(39))}'")
        if kwargs.get("affected_tables") is not None:
            sets.append(f"affected_tables = {kwargs['affected_tables']}")
        if kwargs.get("affected_rows") is not None:
            sets.append(f"affected_rows = {kwargs['affected_rows']}")
        execute_sql(self.client, self.warehouse_id,
                    f"UPDATE {self._requests_table} SET {', '.join(sets)} WHERE request_id = '{request_id}'")

    def get_request(self, request_id: str) -> dict | None:
        try:
            rows = execute_sql(self.client, self.warehouse_id,
                               f"SELECT * FROM {self._requests_table} WHERE request_id = '{request_id}'")
            return rows[0] if rows else None
        except Exception:
            return None

    def list_requests(self, status: str | None = None, limit: int = 50) -> list[dict]:
        where = f"WHERE status = '{status}'" if status else ""
        try:
            return execute_sql(self.client, self.warehouse_id,
                               f"SELECT * FROM {self._requests_table} {where} ORDER BY created_at DESC LIMIT {limit}")
        except Exception:
            return []

    def get_overdue_requests(self) -> list[dict]:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        try:
            return execute_sql(self.client, self.warehouse_id, f"""
                SELECT * FROM {self._requests_table}
                WHERE deadline < '{now}' AND status NOT IN ('completed', 'cancelled')
                ORDER BY deadline ASC
            """)
        except Exception:
            return []

    def get_dashboard_stats(self) -> dict:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        try:
            rows = execute_sql(self.client, self.warehouse_id, f"""
                SELECT COUNT(*) AS total,
                    SUM(CASE WHEN status IN ('received','analyzed') THEN 1 ELSE 0 END) AS pending,
                    SUM(CASE WHEN status IN ('discovering','approved','exporting') THEN 1 ELSE 0 END) AS in_progress,
                    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed,
                    SUM(CASE WHEN deadline < '{now}' AND status NOT IN ('completed','cancelled') THEN 1 ELSE 0 END) AS overdue
                FROM {self._requests_table}
            """)
            return rows[0] if rows else {}
        except Exception:
            return {}

    def save_action(self, **kwargs) -> None:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        try:
            execute_sql(self.client, self.warehouse_id, f"""
                INSERT INTO {self._actions_table}
                (action_id, request_id, action_type, catalog, schema_name, table_name,
                 column_name, rows_found, status, executed_at, duration_seconds, error_message)
                VALUES ('{kwargs["action_id"]}', '{kwargs["request_id"]}', '{kwargs["action_type"]}',
                        '{kwargs.get("catalog","")}', '{kwargs.get("schema_name","")}',
                        '{kwargs.get("table_name","")}', '{kwargs.get("column_name","")}',
                        {kwargs.get("rows_found",0)}, '{kwargs.get("status","completed")}',
                        '{now}', {kwargs.get("duration_seconds",0)},
                        {f"'{kwargs['error_message']}'" if kwargs.get("error_message") else "NULL"})
            """)
        except Exception as e:
            logger.warning(f"Failed to save DSAR action: {e}")

    def save_export(self, **kwargs) -> None:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        try:
            execute_sql(self.client, self.warehouse_id, f"""
                INSERT INTO {self._exports_table}
                (export_id, request_id, format, file_path, file_size_bytes, total_rows, total_tables, generated_at, generated_by)
                VALUES ('{kwargs["export_id"]}', '{kwargs["request_id"]}', '{kwargs.get("format","csv")}',
                        '{kwargs.get("file_path","")}', {kwargs.get("file_size_bytes",0)},
                        {kwargs.get("total_rows",0)}, {kwargs.get("total_tables",0)},
                        '{now}', '{kwargs.get("generated_by","")}')
            """)
        except Exception as e:
            logger.warning(f"Failed to save DSAR export: {e}")

    def get_actions(self, request_id: str) -> list[dict]:
        try:
            return execute_sql(self.client, self.warehouse_id,
                               f"SELECT * FROM {self._actions_table} WHERE request_id = '{request_id}' ORDER BY executed_at")
        except Exception:
            return []

    def get_exports(self, request_id: str) -> list[dict]:
        try:
            return execute_sql(self.client, self.warehouse_id,
                               f"SELECT * FROM {self._exports_table} WHERE request_id = '{request_id}' ORDER BY generated_at DESC")
        except Exception:
            return []
