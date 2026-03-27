"""PII scan store — persistent tracking of PII scan results using Delta tables."""

import json
import logging
from datetime import datetime, timezone

from src.client import execute_sql

logger = logging.getLogger(__name__)

DEFAULT_STATE_CATALOG = "clone_audit"
DEFAULT_PII_SCHEMA = "pii"


class PIIScanStore:
    """Delta table-based store for PII scan history, detections, and remediation."""

    def __init__(
        self,
        client,
        warehouse_id: str,
        state_catalog: str = DEFAULT_STATE_CATALOG,
        state_schema: str = DEFAULT_PII_SCHEMA,
    ):
        self.client = client
        self.warehouse_id = warehouse_id
        self.state_catalog = state_catalog
        self.state_schema = state_schema
        self._scans_table = f"{state_catalog}.{state_schema}.pii_scans"
        self._detections_table = f"{state_catalog}.{state_schema}.pii_detections"
        self._remediation_table = f"{state_catalog}.{state_schema}.pii_remediation"

    def init_tables(self) -> None:
        """Create the PII tracking Delta tables if they don't exist.

        The catalog must already exist (configured via Settings > Audit Catalog).
        Only creates the schema and tables within it.
        """
        execute_sql(self.client, self.warehouse_id,
                    f"CREATE SCHEMA IF NOT EXISTS {self.state_catalog}.{self.state_schema}")

        execute_sql(self.client, self.warehouse_id, f"""
            CREATE TABLE IF NOT EXISTS {self._scans_table} (
                scan_id STRING NOT NULL,
                catalog STRING NOT NULL,
                scanned_at TIMESTAMP,
                scan_type STRING,
                total_columns_scanned INT,
                pii_columns_found INT,
                risk_level STRING,
                duration_seconds DOUBLE,
                config_json STRING,
                summary_json STRING
            )
            USING DELTA
            COMMENT 'PII scan history'
            TBLPROPERTIES (
                'delta.enableChangeDataFeed' = 'true',
                'delta.autoOptimize.optimizeWrite' = 'true'
            )
        """)

        execute_sql(self.client, self.warehouse_id, f"""
            CREATE TABLE IF NOT EXISTS {self._detections_table} (
                scan_id STRING NOT NULL,
                catalog STRING NOT NULL,
                schema_name STRING,
                table_name STRING,
                column_name STRING,
                data_type STRING,
                pii_type STRING,
                detection_method STRING,
                confidence STRING,
                confidence_score DOUBLE,
                match_rate DOUBLE,
                suggested_masking STRING,
                correlation_flags STRING,
                detected_at TIMESTAMP
            )
            USING DELTA
            COMMENT 'PII detection results'
            TBLPROPERTIES (
                'delta.enableChangeDataFeed' = 'true',
                'delta.autoOptimize.optimizeWrite' = 'true'
            )
        """)

        execute_sql(self.client, self.warehouse_id, f"""
            CREATE TABLE IF NOT EXISTS {self._remediation_table} (
                catalog STRING,
                schema_name STRING,
                table_name STRING,
                column_name STRING,
                pii_type STRING,
                status STRING,
                reviewed_by STRING,
                reviewed_at TIMESTAMP,
                masking_applied STRING,
                notes STRING
            )
            USING DELTA
            COMMENT 'PII remediation tracking'
            TBLPROPERTIES (
                'delta.enableChangeDataFeed' = 'true',
                'delta.autoOptimize.optimizeWrite' = 'true'
            )
        """)

        logger.info(f"PII scan store tables ready in {self.state_catalog}.{self.state_schema}")

    def save_scan(
        self,
        scan_id: str,
        catalog: str,
        result: dict,
        duration_seconds: float = 0.0,
        config_json: str = "",
    ) -> None:
        """Save a PII scan and its detections to Delta tables."""
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        summary = result.get("summary", {})
        summary_json = json.dumps(summary).replace("'", "\\\\'")

        sql = f"""
        INSERT INTO {self._scans_table}
        (scan_id, catalog, scanned_at, scan_type, total_columns_scanned,
         pii_columns_found, risk_level, duration_seconds, config_json, summary_json)
        VALUES (
            '{scan_id}', '{catalog}', '{now}', 'full',
            {summary.get('total_columns_scanned', 0)},
            {summary.get('pii_columns_found', 0)},
            '{summary.get('risk_level', 'NONE')}',
            {duration_seconds},
            '{config_json}',
            '{summary_json}'
        )
        """
        try:
            execute_sql(self.client, self.warehouse_id, sql)
        except Exception as e:
            logger.warning(f"Failed to save scan record: {e}")
            return

        # Batch insert detections (chunked to avoid SQL size limits)
        columns = result.get("columns", [])
        CHUNK_SIZE = 50
        for i in range(0, len(columns), CHUNK_SIZE):
            chunk = columns[i:i + CHUNK_SIZE]
            value_rows = []
            for d in chunk:
                flags = json.dumps(d.get("correlation_flags", [])).replace("'", "\\\\'")
                match_rate = d.get("match_rate", 0)
                mr_val = str(match_rate) if match_rate else "NULL"
                value_rows.append(
                    f"('{scan_id}', '{catalog}', '{d['schema']}', '{d['table']}', "
                    f"'{d['column']}', '{d.get('data_type', '')}', "
                    f"'{d['pii_type']}', '{d['detection_method']}', "
                    f"'{d.get('confidence', '')}', {d.get('confidence_score', 0)}, "
                    f"{mr_val}, '{d.get('suggested_masking', '')}', '{flags}', '{now}')"
                )
            if value_rows:
                det_sql = f"""
                INSERT INTO {self._detections_table}
                (scan_id, catalog, schema_name, table_name, column_name, data_type,
                 pii_type, detection_method, confidence, confidence_score, match_rate,
                 suggested_masking, correlation_flags, detected_at)
                VALUES {', '.join(value_rows)}
                """
                try:
                    execute_sql(self.client, self.warehouse_id, det_sql)
                except Exception as e:
                    logger.warning(f"Failed to batch-insert {len(chunk)} detections (chunk {i // CHUNK_SIZE + 1}): {e}")

    def get_scan_history(self, catalog: str, limit: int = 20) -> list[dict]:
        """Get recent scan history for a catalog."""
        sql = f"""
        SELECT * FROM {self._scans_table}
        WHERE catalog = '{catalog}'
        ORDER BY scanned_at DESC
        LIMIT {limit}
        """
        try:
            return execute_sql(self.client, self.warehouse_id, sql)
        except Exception as e:
            logger.warning(f"Failed to fetch scan history: {e}")
            return []

    def get_scan_detections(self, scan_id: str) -> list[dict]:
        """Get all detections for a specific scan."""
        sql = f"""
        SELECT * FROM {self._detections_table}
        WHERE scan_id = '{scan_id}'
        ORDER BY schema_name, table_name, column_name
        """
        try:
            return execute_sql(self.client, self.warehouse_id, sql)
        except Exception as e:
            logger.warning(f"Failed to fetch scan detections: {e}")
            return []

    def get_latest_scan_id(self, catalog: str) -> str | None:
        """Get the most recent scan_id for a catalog."""
        sql = f"""
        SELECT scan_id FROM {self._scans_table}
        WHERE catalog = '{catalog}'
        ORDER BY scanned_at DESC
        LIMIT 1
        """
        try:
            rows = execute_sql(self.client, self.warehouse_id, sql)
            return rows[0]["scan_id"] if rows else None
        except Exception:
            return None

    def diff_scans(self, scan_id_a: str, scan_id_b: str) -> dict:
        """Compare two scans and return new, removed, and changed detections."""
        dets_a = self.get_scan_detections(scan_id_a)
        dets_b = self.get_scan_detections(scan_id_b)

        def _key(d):
            return (d.get("schema_name", ""), d.get("table_name", ""), d.get("column_name", ""))

        map_a = {_key(d): d for d in dets_a}
        map_b = {_key(d): d for d in dets_b}

        keys_a = set(map_a.keys())
        keys_b = set(map_b.keys())

        new_keys = keys_b - keys_a
        removed_keys = keys_a - keys_b
        common_keys = keys_a & keys_b

        changed = []
        for k in common_keys:
            a, b = map_a[k], map_b[k]
            if a.get("pii_type") != b.get("pii_type") or a.get("confidence") != b.get("confidence"):
                changed.append({"before": a, "after": b})

        return {
            "scan_a": scan_id_a,
            "scan_b": scan_id_b,
            "new": [map_b[k] for k in new_keys],
            "removed": [map_a[k] for k in removed_keys],
            "changed": changed,
            "summary": {
                "new_count": len(new_keys),
                "removed_count": len(removed_keys),
                "changed_count": len(changed),
            },
        }

    def update_remediation(
        self,
        catalog: str,
        schema_name: str,
        table_name: str,
        column_name: str,
        pii_type: str,
        status: str,
        reviewed_by: str = "",
        notes: str = "",
    ) -> None:
        """Update remediation status for a PII column."""
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        notes_escaped = notes.replace("'", "\\\\'")

        sql = f"""
        MERGE INTO {self._remediation_table} AS target
        USING (SELECT '{catalog}' AS catalog, '{schema_name}' AS schema_name,
               '{table_name}' AS table_name, '{column_name}' AS column_name) AS source
        ON target.catalog = source.catalog
           AND target.schema_name = source.schema_name
           AND target.table_name = source.table_name
           AND target.column_name = source.column_name
        WHEN MATCHED THEN UPDATE SET
            pii_type = '{pii_type}',
            status = '{status}',
            reviewed_by = '{reviewed_by}',
            reviewed_at = '{now}',
            notes = '{notes_escaped}'
        WHEN NOT MATCHED THEN INSERT
            (catalog, schema_name, table_name, column_name, pii_type,
             status, reviewed_by, reviewed_at, notes)
        VALUES
            ('{catalog}', '{schema_name}', '{table_name}', '{column_name}',
             '{pii_type}', '{status}', '{reviewed_by}', '{now}', '{notes_escaped}')
        """
        try:
            execute_sql(self.client, self.warehouse_id, sql)
        except Exception as e:
            logger.warning(f"Failed to update remediation status: {e}")

    def get_remediation_status(self, catalog: str) -> list[dict]:
        """Get all remediation statuses for a catalog."""
        sql = f"""
        SELECT * FROM {self._remediation_table}
        WHERE catalog = '{catalog}'
        ORDER BY schema_name, table_name, column_name
        """
        try:
            return execute_sql(self.client, self.warehouse_id, sql)
        except Exception as e:
            logger.warning(f"Failed to fetch remediation status: {e}")
            return []
