"""MDM (Master Data Management) store — Delta table CRUD for golden records, matching, and stewardship."""

import json
import logging
from datetime import datetime, timezone

from src.client import execute_sql

logger = logging.getLogger(__name__)

DEFAULT_STATE_CATALOG = "clone_audit"
DEFAULT_MDM_SCHEMA = "mdm"


def _safe_query(client, warehouse_id, sql, default=None):
    """Execute SQL and return default on any failure (e.g., table not found)."""
    try:
        return execute_sql(client, warehouse_id, sql)
    except Exception as e:
        logger.debug(f"MDM query failed (tables may not exist yet): {e}")
        return default if default is not None else []


class MDMStore:
    """Delta table-based store for MDM entities, matching, stewardship, and hierarchies."""

    def __init__(self, client, warehouse_id: str, state_catalog: str = DEFAULT_STATE_CATALOG, state_schema: str = DEFAULT_MDM_SCHEMA):
        self.client = client
        self.warehouse_id = warehouse_id
        self.catalog = state_catalog
        self.schema = state_schema
        self._entities = f"{state_catalog}.{state_schema}.mdm_entities"
        self._source_records = f"{state_catalog}.{state_schema}.mdm_source_records"
        self._match_pairs = f"{state_catalog}.{state_schema}.mdm_match_pairs"
        self._matching_rules = f"{state_catalog}.{state_schema}.mdm_matching_rules"
        self._stewardship = f"{state_catalog}.{state_schema}.mdm_stewardship_queue"
        self._hierarchies = f"{state_catalog}.{state_schema}.mdm_hierarchies"

    def init_tables(self) -> None:
        """Create all MDM Delta tables if they don't exist."""
        execute_sql(self.client, self.warehouse_id, f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.schema}")

        execute_sql(self.client, self.warehouse_id, f"""
            CREATE TABLE IF NOT EXISTS {self._entities} (
                entity_id STRING NOT NULL,
                entity_type STRING NOT NULL,
                display_name STRING,
                attributes MAP<STRING, STRING>,
                source_count INT DEFAULT 0,
                confidence_score DOUBLE DEFAULT 0.0,
                status STRING DEFAULT 'active',
                created_by STRING,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            ) USING DELTA
            TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
        """)

        execute_sql(self.client, self.warehouse_id, f"""
            CREATE TABLE IF NOT EXISTS {self._source_records} (
                source_record_id STRING NOT NULL,
                entity_id STRING,
                entity_type STRING NOT NULL,
                source_system STRING NOT NULL,
                source_table STRING,
                source_key STRING,
                attributes MAP<STRING, STRING>,
                trust_score DOUBLE DEFAULT 1.0,
                ingested_at TIMESTAMP
            ) USING DELTA
        """)

        execute_sql(self.client, self.warehouse_id, f"""
            CREATE TABLE IF NOT EXISTS {self._match_pairs} (
                pair_id STRING NOT NULL,
                entity_type STRING NOT NULL,
                record_a_id STRING NOT NULL,
                record_b_id STRING NOT NULL,
                record_a_name STRING,
                record_b_name STRING,
                match_score DOUBLE NOT NULL,
                matched_rules STRING,
                status STRING DEFAULT 'pending',
                reviewed_by STRING,
                reviewed_at TIMESTAMP,
                created_at TIMESTAMP
            ) USING DELTA
        """)

        execute_sql(self.client, self.warehouse_id, f"""
            CREATE TABLE IF NOT EXISTS {self._matching_rules} (
                rule_id STRING NOT NULL,
                entity_type STRING NOT NULL,
                name STRING NOT NULL,
                field STRING NOT NULL,
                match_type STRING NOT NULL,
                weight DOUBLE DEFAULT 1.0,
                threshold DOUBLE DEFAULT 0.8,
                enabled BOOLEAN DEFAULT true,
                created_at TIMESTAMP
            ) USING DELTA
        """)

        execute_sql(self.client, self.warehouse_id, f"""
            CREATE TABLE IF NOT EXISTS {self._stewardship} (
                task_id STRING NOT NULL,
                task_type STRING NOT NULL,
                entity_type STRING,
                description STRING,
                priority STRING DEFAULT 'medium',
                assignee STRING,
                related_entity_id STRING,
                related_pair_id STRING,
                status STRING DEFAULT 'open',
                resolution STRING,
                created_at TIMESTAMP,
                resolved_at TIMESTAMP,
                resolved_by STRING
            ) USING DELTA
        """)

        execute_sql(self.client, self.warehouse_id, f"""
            CREATE TABLE IF NOT EXISTS {self._hierarchies} (
                hierarchy_id STRING NOT NULL,
                name STRING,
                entity_type STRING,
                node_id STRING NOT NULL,
                parent_node_id STRING,
                entity_id STRING,
                label STRING,
                level INT DEFAULT 0,
                path STRING,
                created_at TIMESTAMP
            ) USING DELTA
        """)
        logger.info("MDM tables initialized")

    # ---- Entities (Golden Records) ----

    def upsert_entity(self, entity_id: str, entity_type: str, display_name: str, attributes: dict, source_count: int, confidence_score: float, status: str = "active", created_by: str = "") -> None:
        now = datetime.now(timezone.utc).isoformat()
        attrs_sql = ", ".join(f"'{k}', '{v}'" for k, v in attributes.items()) if attributes else ""
        attrs_map = f"MAP({attrs_sql})" if attrs_sql else "MAP()"
        execute_sql(self.client, self.warehouse_id, f"""
            MERGE INTO {self._entities} t USING (SELECT '{entity_id}' AS entity_id) s ON t.entity_id = s.entity_id
            WHEN MATCHED THEN UPDATE SET display_name = '{display_name}', attributes = {attrs_map}, source_count = {source_count}, confidence_score = {confidence_score}, status = '{status}', updated_at = TIMESTAMP '{now}'
            WHEN NOT MATCHED THEN INSERT (entity_id, entity_type, display_name, attributes, source_count, confidence_score, status, created_by, created_at, updated_at)
            VALUES ('{entity_id}', '{entity_type}', '{display_name}', {attrs_map}, {source_count}, {confidence_score}, '{status}', '{created_by}', TIMESTAMP '{now}', TIMESTAMP '{now}')
        """)

    def get_entities(self, entity_type: str = None, status: str = None, limit: int = 100) -> list:
        where = ["1=1"]
        if entity_type:
            where.append(f"entity_type = '{entity_type}'")
        if status:
            where.append(f"status = '{status}'")
        return _safe_query(self.client, self.warehouse_id, f"SELECT * FROM {self._entities} WHERE {' AND '.join(where)} ORDER BY updated_at DESC LIMIT {limit}")

    def get_entity(self, entity_id: str) -> dict | None:
        rows = _safe_query(self.client, self.warehouse_id, f"SELECT * FROM {self._entities} WHERE entity_id = '{entity_id}'")
        return rows[0] if rows else None

    def delete_entity(self, entity_id: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        execute_sql(self.client, self.warehouse_id, f"UPDATE {self._entities} SET status = 'deleted', updated_at = TIMESTAMP '{now}' WHERE entity_id = '{entity_id}'")

    # ---- Source Records ----

    def insert_source_record(self, source_record_id: str, entity_id: str | None, entity_type: str, source_system: str, source_table: str, source_key: str, attributes: dict, trust_score: float = 1.0) -> None:
        now = datetime.now(timezone.utc).isoformat()
        attrs_sql = ", ".join(f"'{k}', '{str(v).replace(chr(39), chr(39)+chr(39))}'" for k, v in attributes.items()) if attributes else ""
        attrs_map = f"MAP({attrs_sql})" if attrs_sql else "MAP()"
        eid = f"'{entity_id}'" if entity_id else "NULL"
        execute_sql(self.client, self.warehouse_id, f"""
            INSERT INTO {self._source_records} (source_record_id, entity_id, entity_type, source_system, source_table, source_key, attributes, trust_score, ingested_at)
            VALUES ('{source_record_id}', {eid}, '{entity_type}', '{source_system}', '{source_table}', '{source_key}', {attrs_map}, {trust_score}, TIMESTAMP '{now}')
        """)

    def get_source_records(self, entity_id: str = None, entity_type: str = None, limit: int = 200) -> list:
        where = ["1=1"]
        if entity_id:
            where.append(f"entity_id = '{entity_id}'")
        if entity_type:
            where.append(f"entity_type = '{entity_type}'")
        return _safe_query(self.client, self.warehouse_id, f"SELECT * FROM {self._source_records} WHERE {' AND '.join(where)} ORDER BY ingested_at DESC LIMIT {limit}")

    def get_unmatched_source_records(self, entity_type: str, limit: int = 500) -> list:
        return _safe_query(self.client, self.warehouse_id, f"SELECT * FROM {self._source_records} WHERE entity_type = '{entity_type}' AND entity_id IS NULL LIMIT {limit}")

    def link_source_to_entity(self, source_record_id: str, entity_id: str) -> None:
        execute_sql(self.client, self.warehouse_id, f"UPDATE {self._source_records} SET entity_id = '{entity_id}' WHERE source_record_id = '{source_record_id}'")

    # ---- Match Pairs ----

    def insert_match_pair(self, pair_id: str, entity_type: str, record_a_id: str, record_b_id: str, record_a_name: str, record_b_name: str, match_score: float, matched_rules: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        execute_sql(self.client, self.warehouse_id, f"""
            INSERT INTO {self._match_pairs} (pair_id, entity_type, record_a_id, record_b_id, record_a_name, record_b_name, match_score, matched_rules, status, created_at)
            VALUES ('{pair_id}', '{entity_type}', '{record_a_id}', '{record_b_id}', '{record_a_name}', '{record_b_name}', {match_score}, '{matched_rules}', 'pending', TIMESTAMP '{now}')
        """)

    def get_match_pairs(self, entity_type: str = None, status: str = None, limit: int = 100) -> list:
        where = ["1=1"]
        if entity_type:
            where.append(f"entity_type = '{entity_type}'")
        if status:
            where.append(f"status = '{status}'")
        return _safe_query(self.client, self.warehouse_id, f"SELECT * FROM {self._match_pairs} WHERE {' AND '.join(where)} ORDER BY match_score DESC LIMIT {limit}")

    def update_pair_status(self, pair_id: str, status: str, reviewed_by: str = "") -> None:
        now = datetime.now(timezone.utc).isoformat()
        execute_sql(self.client, self.warehouse_id, f"UPDATE {self._match_pairs} SET status = '{status}', reviewed_by = '{reviewed_by}', reviewed_at = TIMESTAMP '{now}' WHERE pair_id = '{pair_id}'")

    # ---- Matching Rules ----

    def upsert_rule(self, rule_id: str, entity_type: str, name: str, field: str, match_type: str, weight: float, threshold: float, enabled: bool) -> None:
        now = datetime.now(timezone.utc).isoformat()
        execute_sql(self.client, self.warehouse_id, f"""
            MERGE INTO {self._matching_rules} t USING (SELECT '{rule_id}' AS rule_id) s ON t.rule_id = s.rule_id
            WHEN MATCHED THEN UPDATE SET name = '{name}', field = '{field}', match_type = '{match_type}', weight = {weight}, threshold = {threshold}, enabled = {str(enabled).lower()}
            WHEN NOT MATCHED THEN INSERT (rule_id, entity_type, name, field, match_type, weight, threshold, enabled, created_at)
            VALUES ('{rule_id}', '{entity_type}', '{name}', '{field}', '{match_type}', {weight}, {threshold}, {str(enabled).lower()}, TIMESTAMP '{now}')
        """)

    def get_rules(self, entity_type: str = None) -> list:
        where = f"WHERE entity_type = '{entity_type}'" if entity_type else ""
        return _safe_query(self.client, self.warehouse_id, f"SELECT * FROM {self._matching_rules} {where} ORDER BY weight DESC")

    def delete_rule(self, rule_id: str) -> None:
        execute_sql(self.client, self.warehouse_id, f"DELETE FROM {self._matching_rules} WHERE rule_id = '{rule_id}'")

    # ---- Stewardship Queue ----

    def insert_task(self, task_id: str, task_type: str, entity_type: str, description: str, priority: str, assignee: str = "", related_entity_id: str = "", related_pair_id: str = "") -> None:
        now = datetime.now(timezone.utc).isoformat()
        execute_sql(self.client, self.warehouse_id, f"""
            INSERT INTO {self._stewardship} (task_id, task_type, entity_type, description, priority, assignee, related_entity_id, related_pair_id, status, created_at)
            VALUES ('{task_id}', '{task_type}', '{entity_type}', '{description}', '{priority}', '{assignee}', '{related_entity_id}', '{related_pair_id}', 'open', TIMESTAMP '{now}')
        """)

    def get_tasks(self, status: str = None, priority: str = None, limit: int = 50) -> list:
        where = ["1=1"]
        if status:
            where.append(f"status = '{status}'")
        if priority:
            where.append(f"priority = '{priority}'")
        return _safe_query(self.client, self.warehouse_id, f"SELECT * FROM {self._stewardship} WHERE {' AND '.join(where)} ORDER BY CASE priority WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END, created_at ASC LIMIT {limit}")

    def resolve_task(self, task_id: str, resolution: str, resolved_by: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        execute_sql(self.client, self.warehouse_id, f"UPDATE {self._stewardship} SET status = 'resolved', resolution = '{resolution}', resolved_by = '{resolved_by}', resolved_at = TIMESTAMP '{now}' WHERE task_id = '{task_id}'")

    def get_stewardship_stats(self) -> dict:
        rows = _safe_query(self.client, self.warehouse_id, f"""
            SELECT status, priority, COUNT(*) as cnt FROM {self._stewardship} GROUP BY status, priority
        """)
        total = sum(r.get("cnt", 0) for r in rows)
        open_count = sum(r.get("cnt", 0) for r in rows if r.get("status") == "open")
        high = sum(r.get("cnt", 0) for r in rows if r.get("priority") == "high" and r.get("status") == "open")
        return {"total": total, "open": open_count, "high_priority": high, "resolved": total - open_count}

    # ---- Hierarchies ----

    def insert_hierarchy_node(self, hierarchy_id: str, name: str, entity_type: str, node_id: str, parent_node_id: str | None, entity_id: str | None, label: str, level: int, path: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        parent = f"'{parent_node_id}'" if parent_node_id else "NULL"
        eid = f"'{entity_id}'" if entity_id else "NULL"
        execute_sql(self.client, self.warehouse_id, f"""
            INSERT INTO {self._hierarchies} (hierarchy_id, name, entity_type, node_id, parent_node_id, entity_id, label, level, path, created_at)
            VALUES ('{hierarchy_id}', '{name}', '{entity_type}', '{node_id}', {parent}, {eid}, '{label}', {level}, '{path}', TIMESTAMP '{now}')
        """)

    def get_hierarchy(self, hierarchy_id: str) -> list:
        return _safe_query(self.client, self.warehouse_id, f"SELECT * FROM {self._hierarchies} WHERE hierarchy_id = '{hierarchy_id}' ORDER BY level, label")

    def get_all_hierarchies(self) -> list:
        return _safe_query(self.client, self.warehouse_id, f"""
            SELECT hierarchy_id, name, entity_type, COUNT(*) as node_count, MAX(level) as max_depth
            FROM {self._hierarchies} GROUP BY hierarchy_id, name, entity_type ORDER BY name
        """)

    def move_node(self, node_id: str, new_parent_id: str) -> None:
        execute_sql(self.client, self.warehouse_id, f"UPDATE {self._hierarchies} SET parent_node_id = '{new_parent_id}' WHERE node_id = '{node_id}'")

    # ---- Dashboard ----

    def get_dashboard_stats(self) -> dict:
        entities = _safe_query(self.client, self.warehouse_id, f"""
            SELECT entity_type, status, COUNT(*) as cnt, AVG(confidence_score) as avg_confidence
            FROM {self._entities} GROUP BY entity_type, status
        """)
        pairs = _safe_query(self.client, self.warehouse_id, f"""
            SELECT status, COUNT(*) as cnt FROM {self._match_pairs} GROUP BY status
        """)
        stewardship = self.get_stewardship_stats()
        return {"entities": entities, "pairs": pairs, "stewardship": stewardship}
