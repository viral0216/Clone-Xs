"""RTBF (Right to Be Forgotten) store — persistent tracking of erasure requests using Delta tables."""

import json
import logging
from datetime import datetime, timezone

from src.client import execute_sql

logger = logging.getLogger(__name__)

DEFAULT_STATE_CATALOG = "clone_audit"
DEFAULT_RTBF_SCHEMA = "rtbf"

# Valid RTBF request statuses and their allowed transitions
RTBF_STATUSES = [
    "received",
    "discovering",
    "analyzed",
    "approved",
    "on_hold",
    "executing",
    "deleted_pending_vacuum",
    "vacuuming",
    "vacuumed",
    "verifying",
    "verified",
    "completed",
    "failed",
    "cancelled",
]

STATUS_TRANSITIONS = {
    "received": ["discovering", "cancelled"],
    "discovering": ["analyzed", "failed"],
    "analyzed": ["approved", "on_hold", "cancelled"],
    "approved": ["executing", "cancelled"],
    "on_hold": ["approved", "cancelled"],
    "executing": ["deleted_pending_vacuum", "failed"],
    "deleted_pending_vacuum": ["vacuuming", "failed"],
    "vacuuming": ["vacuumed", "failed"],
    "vacuumed": ["verifying"],
    "verifying": ["verified", "failed"],
    "verified": ["completed"],
    "failed": ["received", "cancelled"],  # allow retry from start
}


class RTBFStore:
    """Delta table-based store for RTBF request lifecycle, actions, and certificates."""

    def __init__(
        self,
        client,
        warehouse_id: str,
        state_catalog: str = DEFAULT_STATE_CATALOG,
        state_schema: str = DEFAULT_RTBF_SCHEMA,
    ):
        self.client = client
        self.warehouse_id = warehouse_id
        self.state_catalog = state_catalog
        self.state_schema = state_schema
        self._requests_table = f"{state_catalog}.{state_schema}.rtbf_requests"
        self._actions_table = f"{state_catalog}.{state_schema}.rtbf_actions"
        self._certificates_table = f"{state_catalog}.{state_schema}.rtbf_certificates"

    def init_tables(self) -> None:
        """Create the RTBF Delta tables if they don't exist.

        The catalog must already exist (configured via Settings > Audit Catalog).
        Only creates the schema and tables within it.
        """
        execute_sql(
            self.client, self.warehouse_id,
            f"CREATE SCHEMA IF NOT EXISTS {self.state_catalog}.{self.state_schema}",
        )

        # RTBF requests — tracks the full lifecycle of each erasure request
        execute_sql(self.client, self.warehouse_id, f"""
            CREATE TABLE IF NOT EXISTS {self._requests_table} (
                request_id STRING NOT NULL,
                subject_type STRING NOT NULL,
                subject_value_hash STRING NOT NULL,
                subject_column STRING,
                requester_email STRING,
                requester_name STRING,
                legal_basis STRING,
                strategy STRING,
                scope_catalogs STRING,
                status STRING,
                grace_period_days INT,
                grace_period_ends TIMESTAMP,
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
            )
            USING DELTA
            COMMENT 'RTBF / GDPR Article 17 erasure request lifecycle'
            TBLPROPERTIES (
                'delta.enableChangeDataFeed' = 'true',
                'delta.autoOptimize.optimizeWrite' = 'true'
            )
        """)

        # RTBF actions — per-table actions (discover, delete, vacuum, verify)
        execute_sql(self.client, self.warehouse_id, f"""
            CREATE TABLE IF NOT EXISTS {self._actions_table} (
                action_id STRING NOT NULL,
                request_id STRING NOT NULL,
                action_type STRING NOT NULL,
                catalog STRING,
                schema_name STRING,
                table_name STRING,
                column_name STRING,
                rows_before BIGINT,
                rows_affected BIGINT,
                rows_after BIGINT,
                sql_executed STRING,
                status STRING,
                executed_at TIMESTAMP,
                completed_at TIMESTAMP,
                duration_seconds DOUBLE,
                executed_by STRING,
                error_message STRING
            )
            USING DELTA
            COMMENT 'RTBF per-table actions and execution log'
            TBLPROPERTIES (
                'delta.enableChangeDataFeed' = 'true',
                'delta.autoOptimize.optimizeWrite' = 'true'
            )
        """)

        # RTBF certificates — deletion evidence for compliance/legal
        execute_sql(self.client, self.warehouse_id, f"""
            CREATE TABLE IF NOT EXISTS {self._certificates_table} (
                certificate_id STRING NOT NULL,
                request_id STRING NOT NULL,
                generated_at TIMESTAMP,
                generated_by STRING,
                certificate_type STRING,
                summary_json STRING,
                tables_processed INT,
                rows_deleted BIGINT,
                verification_passed BOOLEAN,
                html_report STRING,
                json_report STRING
            )
            USING DELTA
            COMMENT 'RTBF deletion certificates and compliance evidence'
            TBLPROPERTIES (
                'delta.enableChangeDataFeed' = 'true',
                'delta.autoOptimize.optimizeWrite' = 'true'
            )
        """)

        logger.info(f"RTBF store tables ready in {self.state_catalog}.{self.state_schema}")

    # ── Request CRUD ──────────────────────────────────────────────────────

    def save_request(
        self,
        request_id: str,
        subject_type: str,
        subject_value_hash: str,
        requester_email: str,
        requester_name: str,
        legal_basis: str,
        deadline: str,
        strategy: str = "delete",
        scope_catalogs: list[str] | None = None,
        grace_period_days: int = 0,
        grace_period_ends: str | None = None,
        subject_column: str | None = None,
        created_by: str = "",
        notes: str | None = None,
    ) -> None:
        """Insert a new RTBF request."""
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        scope_json = json.dumps(scope_catalogs or []).replace("'", "\\\\'")
        notes_escaped = (notes or "").replace("'", "\\\\'")
        gp_ends = f"'{grace_period_ends}'" if grace_period_ends else "NULL"
        subj_col = f"'{subject_column}'" if subject_column else "NULL"

        sql = f"""
        INSERT INTO {self._requests_table}
        (request_id, subject_type, subject_value_hash, subject_column,
         requester_email, requester_name, legal_basis, strategy,
         scope_catalogs, status, grace_period_days, grace_period_ends,
         deadline, created_at, updated_at, created_by, notes)
        VALUES (
            '{request_id}', '{subject_type}', '{subject_value_hash}', {subj_col},
            '{requester_email}', '{requester_name}', '{legal_basis}', '{strategy}',
            '{scope_json}', 'received', {grace_period_days}, {gp_ends},
            '{deadline}', '{now}', '{now}', '{created_by}', '{notes_escaped}'
        )
        """
        try:
            execute_sql(self.client, self.warehouse_id, sql)
        except Exception as e:
            logger.warning(f"Failed to save RTBF request: {e}")
            raise

    def update_request_status(
        self,
        request_id: str,
        status: str,
        error_message: str | None = None,
        discovery_json: str | None = None,
        affected_tables: int | None = None,
        affected_rows: int | None = None,
    ) -> None:
        """Update an RTBF request status and optional metadata."""
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        sets = [f"status = '{status}'", f"updated_at = '{now}'"]

        if status == "completed":
            sets.append(f"completed_at = '{now}'")
        if error_message is not None:
            err_escaped = error_message.replace("'", "\\\\'")
            sets.append(f"error_message = '{err_escaped}'")
        if discovery_json is not None:
            disc_escaped = discovery_json.replace("'", "\\\\'")
            sets.append(f"discovery_json = '{disc_escaped}'")
        if affected_tables is not None:
            sets.append(f"affected_tables = {affected_tables}")
        if affected_rows is not None:
            sets.append(f"affected_rows = {affected_rows}")

        sql = f"""
        UPDATE {self._requests_table}
        SET {', '.join(sets)}
        WHERE request_id = '{request_id}'
        """
        try:
            execute_sql(self.client, self.warehouse_id, sql)
        except Exception as e:
            logger.warning(f"Failed to update RTBF request status: {e}")
            raise

    def get_request(self, request_id: str) -> dict | None:
        """Fetch a single RTBF request by ID."""
        sql = f"""
        SELECT * FROM {self._requests_table}
        WHERE request_id = '{request_id}'
        """
        try:
            rows = execute_sql(self.client, self.warehouse_id, sql)
            return rows[0] if rows else None
        except Exception as e:
            logger.warning(f"Failed to fetch RTBF request: {e}")
            return None

    def list_requests(
        self,
        status: str | None = None,
        from_date: str | None = None,
        to_date: str | None = None,
        limit: int = 50,
    ) -> list[dict]:
        """List RTBF requests with optional filters."""
        conditions = []
        if status:
            conditions.append(f"status = '{status}'")
        if from_date:
            conditions.append(f"created_at >= '{from_date}'")
        if to_date:
            conditions.append(f"created_at <= '{to_date}'")

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        sql = f"""
        SELECT * FROM {self._requests_table}
        {where}
        ORDER BY created_at DESC
        LIMIT {limit}
        """
        try:
            return execute_sql(self.client, self.warehouse_id, sql)
        except Exception as e:
            logger.warning(f"Failed to list RTBF requests: {e}")
            return []

    def get_overdue_requests(self) -> list[dict]:
        """Get requests that have passed their GDPR deadline without completion."""
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        sql = f"""
        SELECT * FROM {self._requests_table}
        WHERE deadline < '{now}'
          AND status NOT IN ('completed', 'cancelled')
        ORDER BY deadline ASC
        """
        try:
            return execute_sql(self.client, self.warehouse_id, sql)
        except Exception as e:
            logger.warning(f"Failed to fetch overdue RTBF requests: {e}")
            return []

    def get_dashboard_stats(self) -> dict:
        """Get summary statistics for the RTBF dashboard."""
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        sql = f"""
        SELECT
            COUNT(*) AS total_requests,
            SUM(CASE WHEN status = 'received' OR status = 'analyzed' THEN 1 ELSE 0 END) AS pending,
            SUM(CASE WHEN status IN ('discovering', 'approved', 'executing',
                'deleted_pending_vacuum', 'vacuuming', 'verifying') THEN 1 ELSE 0 END) AS in_progress,
            SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed,
            SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled,
            SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed,
            SUM(CASE WHEN deadline < '{now}' AND status NOT IN ('completed', 'cancelled')
                THEN 1 ELSE 0 END) AS overdue,
            AVG(CASE WHEN completed_at IS NOT NULL
                THEN DATEDIFF(completed_at, created_at) END) AS avg_processing_days
        FROM {self._requests_table}
        """
        try:
            rows = execute_sql(self.client, self.warehouse_id, sql)
            return rows[0] if rows else {}
        except Exception as e:
            logger.warning(f"Failed to fetch RTBF dashboard stats: {e}")
            return {}

    # ── Actions CRUD ──────────────────────────────────────────────────────

    def save_action(
        self,
        action_id: str,
        request_id: str,
        action_type: str,
        catalog: str = "",
        schema_name: str = "",
        table_name: str = "",
        column_name: str = "",
        rows_before: int = 0,
        rows_affected: int = 0,
        rows_after: int = 0,
        sql_executed: str = "",
        status: str = "started",
        executed_by: str = "",
        error_message: str | None = None,
        duration_seconds: float = 0.0,
    ) -> None:
        """Record an RTBF action (discover, delete, vacuum, verify)."""
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        sql_escaped = sql_executed.replace("'", "\\\\'")
        err_escaped = (error_message or "").replace("'", "\\\\'") if error_message else ""
        err_val = f"'{err_escaped}'" if error_message else "NULL"

        sql = f"""
        INSERT INTO {self._actions_table}
        (action_id, request_id, action_type, catalog, schema_name, table_name,
         column_name, rows_before, rows_affected, rows_after, sql_executed,
         status, executed_at, duration_seconds, executed_by, error_message)
        VALUES (
            '{action_id}', '{request_id}', '{action_type}',
            '{catalog}', '{schema_name}', '{table_name}', '{column_name}',
            {rows_before}, {rows_affected}, {rows_after},
            '{sql_escaped}', '{status}', '{now}',
            {duration_seconds}, '{executed_by}', {err_val}
        )
        """
        try:
            execute_sql(self.client, self.warehouse_id, sql)
        except Exception as e:
            logger.warning(f"Failed to save RTBF action: {e}")

    def complete_action(
        self,
        action_id: str,
        status: str = "completed",
        rows_affected: int = 0,
        rows_after: int = 0,
        duration_seconds: float = 0.0,
        error_message: str | None = None,
    ) -> None:
        """Mark an action as completed or failed."""
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        sets = [
            f"status = '{status}'",
            f"completed_at = '{now}'",
            f"rows_affected = {rows_affected}",
            f"rows_after = {rows_after}",
            f"duration_seconds = {duration_seconds}",
        ]
        if error_message:
            err_escaped = error_message.replace("'", "\\\\'")
            sets.append(f"error_message = '{err_escaped}'")

        sql = f"""
        UPDATE {self._actions_table}
        SET {', '.join(sets)}
        WHERE action_id = '{action_id}'
        """
        try:
            execute_sql(self.client, self.warehouse_id, sql)
        except Exception as e:
            logger.warning(f"Failed to complete RTBF action: {e}")

    def get_actions(self, request_id: str) -> list[dict]:
        """Get all actions for a given request."""
        sql = f"""
        SELECT * FROM {self._actions_table}
        WHERE request_id = '{request_id}'
        ORDER BY executed_at ASC
        """
        try:
            return execute_sql(self.client, self.warehouse_id, sql)
        except Exception as e:
            logger.warning(f"Failed to fetch RTBF actions: {e}")
            return []

    # ── Certificates CRUD ─────────────────────────────────────────────────

    def save_certificate(
        self,
        certificate_id: str,
        request_id: str,
        generated_by: str,
        summary_json: str,
        tables_processed: int,
        rows_deleted: int,
        verification_passed: bool,
        html_report: str = "",
        json_report: str = "",
    ) -> None:
        """Save a deletion certificate."""
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        summary_escaped = summary_json.replace("'", "\\\\'")
        html_escaped = html_report.replace("'", "\\\\'")
        json_escaped = json_report.replace("'", "\\\\'")

        sql = f"""
        INSERT INTO {self._certificates_table}
        (certificate_id, request_id, generated_at, generated_by, certificate_type,
         summary_json, tables_processed, rows_deleted, verification_passed,
         html_report, json_report)
        VALUES (
            '{certificate_id}', '{request_id}', '{now}', '{generated_by}', 'deletion',
            '{summary_escaped}', {tables_processed}, {rows_deleted},
            {str(verification_passed).lower()},
            '{html_escaped}', '{json_escaped}'
        )
        """
        try:
            execute_sql(self.client, self.warehouse_id, sql)
        except Exception as e:
            logger.warning(f"Failed to save RTBF certificate: {e}")

    def get_certificate(self, request_id: str) -> dict | None:
        """Get the latest certificate for a request."""
        sql = f"""
        SELECT * FROM {self._certificates_table}
        WHERE request_id = '{request_id}'
        ORDER BY generated_at DESC
        LIMIT 1
        """
        try:
            rows = execute_sql(self.client, self.warehouse_id, sql)
            return rows[0] if rows else None
        except Exception as e:
            logger.warning(f"Failed to fetch RTBF certificate: {e}")
            return None
