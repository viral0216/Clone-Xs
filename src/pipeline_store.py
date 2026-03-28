"""Pipeline store — Delta table tracking for clone pipeline definitions and runs."""

import json
import logging
from datetime import datetime, timezone

from src.client import execute_sql

logger = logging.getLogger(__name__)


class PipelineStore:
    """Delta table store for pipeline definitions, runs, and step results."""

    def __init__(self, client, warehouse_id: str, state_catalog: str = "clone_audit", state_schema: str = "pipelines"):
        self.client = client
        self.warehouse_id = warehouse_id
        self.state_catalog = state_catalog
        self._pipelines = f"{state_catalog}.{state_schema}.pipelines"
        self._runs = f"{state_catalog}.{state_schema}.pipeline_runs"
        self._steps = f"{state_catalog}.{state_schema}.pipeline_step_results"

    def init_tables(self) -> None:
        execute_sql(self.client, self.warehouse_id,
                    f"CREATE SCHEMA IF NOT EXISTS {self.state_catalog}.pipelines")

        execute_sql(self.client, self.warehouse_id, f"""
            CREATE TABLE IF NOT EXISTS {self._pipelines} (
                pipeline_id STRING NOT NULL, name STRING, description STRING,
                steps_json STRING, is_template BOOLEAN, template_name STRING,
                created_by STRING, created_at TIMESTAMP, updated_at TIMESTAMP
            ) USING DELTA COMMENT 'Clone pipeline definitions'
            TBLPROPERTIES ('delta.enableChangeDataFeed'='true','delta.autoOptimize.optimizeWrite'='true')
        """)

        execute_sql(self.client, self.warehouse_id, f"""
            CREATE TABLE IF NOT EXISTS {self._runs} (
                run_id STRING NOT NULL, pipeline_id STRING NOT NULL, pipeline_name STRING,
                status STRING, started_at TIMESTAMP, completed_at TIMESTAMP,
                triggered_by STRING, total_steps INT, completed_steps INT, error STRING
            ) USING DELTA COMMENT 'Pipeline execution runs'
            TBLPROPERTIES ('delta.enableChangeDataFeed'='true','delta.autoOptimize.optimizeWrite'='true')
        """)

        execute_sql(self.client, self.warehouse_id, f"""
            CREATE TABLE IF NOT EXISTS {self._steps} (
                result_id STRING NOT NULL, run_id STRING NOT NULL, step_index INT,
                step_type STRING, step_name STRING, status STRING,
                started_at TIMESTAMP, completed_at TIMESTAMP,
                duration_seconds DOUBLE, result_json STRING, error STRING
            ) USING DELTA COMMENT 'Pipeline step execution results'
            TBLPROPERTIES ('delta.enableChangeDataFeed'='true','delta.autoOptimize.optimizeWrite'='true')
        """)
        logger.info("Pipeline store tables ready")

    def save_pipeline(self, pipeline_id: str, name: str, description: str, steps: list, created_by: str, is_template: bool = False, template_name: str = "") -> None:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        steps_json = json.dumps(steps).replace("'", "\\\\'")
        execute_sql(self.client, self.warehouse_id, f"""
            INSERT INTO {self._pipelines}
            (pipeline_id, name, description, steps_json, is_template, template_name, created_by, created_at, updated_at)
            VALUES ('{pipeline_id}', '{name}', '{description}', '{steps_json}',
                    {str(is_template).lower()}, '{template_name}', '{created_by}', '{now}', '{now}')
        """)

    def get_pipeline(self, pipeline_id: str) -> dict | None:
        try:
            rows = execute_sql(self.client, self.warehouse_id,
                               f"SELECT * FROM {self._pipelines} WHERE pipeline_id = '{pipeline_id}'")
            return rows[0] if rows else None
        except Exception:
            return None

    def list_pipelines(self, templates_only: bool = False, limit: int = 50) -> list[dict]:
        where = "WHERE is_template = true" if templates_only else ""
        try:
            return execute_sql(self.client, self.warehouse_id,
                               f"SELECT * FROM {self._pipelines} {where} ORDER BY created_at DESC LIMIT {limit}")
        except Exception:
            return []

    def delete_pipeline(self, pipeline_id: str) -> None:
        execute_sql(self.client, self.warehouse_id,
                    f"DELETE FROM {self._pipelines} WHERE pipeline_id = '{pipeline_id}'")

    def save_run(self, run_id: str, pipeline_id: str, pipeline_name: str, total_steps: int, triggered_by: str) -> None:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        execute_sql(self.client, self.warehouse_id, f"""
            INSERT INTO {self._runs}
            (run_id, pipeline_id, pipeline_name, status, started_at, triggered_by, total_steps, completed_steps)
            VALUES ('{run_id}', '{pipeline_id}', '{pipeline_name}', 'running', '{now}', '{triggered_by}', {total_steps}, 0)
        """)

    def update_run(self, run_id: str, status: str, completed_steps: int = 0, error: str | None = None) -> None:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        sets = [f"status = '{status}'", f"completed_steps = {completed_steps}"]
        if status in ("completed", "failed", "cancelled"):
            sets.append(f"completed_at = '{now}'")
        if error:
            sets.append(f"error = '{error.replace(chr(39), chr(92)+chr(39))}'")
        execute_sql(self.client, self.warehouse_id,
                    f"UPDATE {self._runs} SET {', '.join(sets)} WHERE run_id = '{run_id}'")

    def get_run(self, run_id: str) -> dict | None:
        try:
            rows = execute_sql(self.client, self.warehouse_id,
                               f"SELECT * FROM {self._runs} WHERE run_id = '{run_id}'")
            return rows[0] if rows else None
        except Exception:
            return None

    def list_runs(self, pipeline_id: str | None = None, limit: int = 50) -> list[dict]:
        where = f"WHERE pipeline_id = '{pipeline_id}'" if pipeline_id else ""
        try:
            return execute_sql(self.client, self.warehouse_id,
                               f"SELECT * FROM {self._runs} {where} ORDER BY started_at DESC LIMIT {limit}")
        except Exception:
            return []

    def save_step_result(self, result_id: str, run_id: str, step_index: int, step_type: str,
                         step_name: str, status: str, duration: float = 0, result_json: str = "", error: str | None = None) -> None:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        res_escaped = result_json.replace("'", "\\\\'") if result_json else ""
        err_val = f"'{error.replace(chr(39), chr(92)+chr(39))}'" if error else "NULL"
        execute_sql(self.client, self.warehouse_id, f"""
            INSERT INTO {self._steps}
            (result_id, run_id, step_index, step_type, step_name, status, started_at, completed_at, duration_seconds, result_json, error)
            VALUES ('{result_id}', '{run_id}', {step_index}, '{step_type}', '{step_name}', '{status}',
                    '{now}', '{now}', {duration}, '{res_escaped}', {err_val})
        """)

    def get_step_results(self, run_id: str) -> list[dict]:
        try:
            return execute_sql(self.client, self.warehouse_id,
                               f"SELECT * FROM {self._steps} WHERE run_id = '{run_id}' ORDER BY step_index")
        except Exception:
            return []
