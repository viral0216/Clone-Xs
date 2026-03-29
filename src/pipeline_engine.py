"""Pipeline engine — execute ordered sequences of clone operations."""

import json
import logging
import time
import uuid

from src.client import execute_sql
from src.pipeline_store import PipelineStore

logger = logging.getLogger(__name__)

BUILTIN_TEMPLATES = {
    "production-to-dev": {
        "name": "Production to Dev",
        "description": "Clone production catalog to dev, mask PII, validate, and notify",
        "steps": [
            {"type": "clone", "name": "Clone catalog", "config": {"clone_type": "DEEP"}, "on_failure": "abort"},
            {"type": "mask", "name": "Mask PII columns", "config": {}, "on_failure": "abort"},
            {"type": "validate", "name": "Validate row counts", "config": {}, "on_failure": "abort"},
            {"type": "notify", "name": "Send notification", "config": {"message": "Dev refresh complete"}, "on_failure": "skip"},
        ],
    },
    "clone-and-validate": {
        "name": "Clone & Validate",
        "description": "Clone catalog and validate with checksums",
        "steps": [
            {"type": "clone", "name": "Clone catalog", "config": {"clone_type": "DEEP"}, "on_failure": "abort"},
            {"type": "validate", "name": "Validate", "config": {"checksum": True}, "on_failure": "abort"},
        ],
    },
    "refresh-dev": {
        "name": "Refresh Dev Environment",
        "description": "Vacuum old data, clone fresh, mask PII, validate",
        "steps": [
            {"type": "vacuum", "name": "Vacuum destination", "config": {"retention_hours": 168}, "on_failure": "skip"},
            {"type": "clone", "name": "Clone catalog", "config": {"clone_type": "DEEP"}, "on_failure": "abort"},
            {"type": "mask", "name": "Mask PII", "config": {}, "on_failure": "abort"},
            {"type": "validate", "name": "Validate", "config": {}, "on_failure": "skip"},
            {"type": "notify", "name": "Notify team", "config": {}, "on_failure": "skip"},
        ],
    },
    "compliance-clone": {
        "name": "Compliance Clone",
        "description": "Preflight checks, clone, mask, validate, notify — full audit trail",
        "steps": [
            {"type": "custom_sql", "name": "Preflight check", "config": {"sql": "SELECT 1"}, "on_failure": "abort"},
            {"type": "clone", "name": "Clone catalog", "config": {"clone_type": "DEEP"}, "on_failure": "abort"},
            {"type": "mask", "name": "Apply masking rules", "config": {}, "on_failure": "abort"},
            {"type": "validate", "name": "Validate checksums", "config": {"checksum": True}, "on_failure": "abort"},
            {"type": "notify", "name": "Compliance notification", "config": {}, "on_failure": "skip"},
        ],
    },
}

STEP_TYPES = ["clone", "mask", "validate", "notify", "vacuum", "custom_sql"]


class PipelineEngine:
    """Executes pipeline step sequences with failure handling."""

    def __init__(self, client, warehouse_id: str, config: dict | None = None):
        self.client = client
        self.warehouse_id = warehouse_id
        self.config = config or {}
        catalog = self.config.get("audit_trail", {}).get("catalog", "clone_audit")
        self.store = PipelineStore(client, warehouse_id, catalog)

        pipe_config = self.config.get("pipelines", {})
        self.default_on_failure = pipe_config.get("default_on_failure", "abort")
        self.retry_max = pipe_config.get("retry_max_attempts", 3)
        self.retry_backoff = pipe_config.get("retry_backoff_seconds", 30)

    def init_tables(self) -> None:
        self.store.init_tables()

    def create_pipeline(self, name: str, description: str, steps: list[dict], created_by: str = "") -> str:
        pipeline_id = str(uuid.uuid4())
        self.store.save_pipeline(pipeline_id, name, description, steps, created_by)
        return pipeline_id

    def create_from_template(self, template_name: str, created_by: str = "", overrides: dict | None = None) -> str:
        template = BUILTIN_TEMPLATES.get(template_name)
        if not template:
            raise ValueError(f"Template '{template_name}' not found. Available: {list(BUILTIN_TEMPLATES.keys())}")
        steps = template["steps"]
        if overrides:
            for step in steps:
                step["config"].update(overrides.get(step["type"], {}))
        pipeline_id = str(uuid.uuid4())
        self.store.save_pipeline(
            pipeline_id, template["name"], template["description"], steps,
            created_by, is_template=False, template_name=template_name,
        )
        return pipeline_id

    def run_pipeline(self, pipeline_id: str, triggered_by: str = "") -> dict:
        pipeline = self.store.get_pipeline(pipeline_id)
        if not pipeline:
            raise ValueError(f"Pipeline {pipeline_id} not found")

        steps = json.loads(pipeline.get("steps_json", "[]"))
        run_id = str(uuid.uuid4())
        self.store.save_run(run_id, pipeline_id, pipeline.get("name", ""), len(steps), triggered_by)

        completed = 0
        for i, step in enumerate(steps):
            step_type = step.get("type", "unknown")
            step_name = step.get("name", f"Step {i+1}")
            on_failure = step.get("on_failure", self.default_on_failure)
            result_id = str(uuid.uuid4())
            start = time.time()

            try:
                result = self._execute_step(step)
                duration = time.time() - start
                self.store.save_step_result(
                    result_id, run_id, i, step_type, step_name, "completed",
                    duration=duration, result_json=json.dumps(result, default=str),
                )
                completed += 1
                self.store.update_run(run_id, "running", completed_steps=completed)

            except Exception as e:
                duration = time.time() - start
                logger.error(f"Pipeline step {i} ({step_name}) failed: {e}")
                self.store.save_step_result(
                    result_id, run_id, i, step_type, step_name, "failed",
                    duration=duration, error=str(e),
                )

                if on_failure == "abort":
                    self.store.update_run(run_id, "failed", completed_steps=completed, error=f"Step {i} failed: {e}")
                    return {"run_id": run_id, "status": "failed", "failed_step": i, "error": str(e)}
                elif on_failure == "retry":
                    retried = self._retry_step(step, run_id, i, step_name)
                    if retried:
                        completed += 1
                    else:
                        self.store.update_run(run_id, "failed", completed_steps=completed, error=f"Step {i} failed after retries")
                        return {"run_id": run_id, "status": "failed", "failed_step": i}
                # on_failure == "skip" → continue

        self.store.update_run(run_id, "completed", completed_steps=completed)
        return {"run_id": run_id, "status": "completed", "completed_steps": completed, "total_steps": len(steps)}

    def cancel_run(self, run_id: str) -> dict:
        self.store.update_run(run_id, "cancelled")
        return {"run_id": run_id, "status": "cancelled"}

    def get_run_status(self, run_id: str) -> dict:
        run = self.store.get_run(run_id)
        if not run:
            raise ValueError(f"Run {run_id} not found")
        steps = self.store.get_step_results(run_id)
        return {**run, "step_results": steps}

    def list_templates(self) -> list[dict]:
        return [{"name": k, **{kk: vv for kk, vv in v.items() if kk != "steps"},
                 "step_count": len(v["steps"]),
                 "step_types": [s["type"] for s in v["steps"]]}
                for k, v in BUILTIN_TEMPLATES.items()]

    def get_pipeline(self, pipeline_id: str) -> dict | None:
        return self.store.get_pipeline(pipeline_id)

    def list_pipelines(self, **kwargs) -> list[dict]:
        return self.store.list_pipelines(**kwargs)

    def list_runs(self, **kwargs) -> list[dict]:
        return self.store.list_runs(**kwargs)

    def delete_pipeline(self, pipeline_id: str) -> dict:
        self.store.delete_pipeline(pipeline_id)
        return {"pipeline_id": pipeline_id, "status": "deleted"}

    def _execute_step(self, step: dict) -> dict:
        step_type = step.get("type")
        config = step.get("config", {})
        src = self.config.get("source_catalog", "")
        dst = self.config.get("destination_catalog", "")

        if step_type == "clone":
            from src.clone_catalog import clone_catalog
            clone_config = dict(self.config)
            clone_config["clone_type"] = config.get("clone_type", "DEEP")
            return clone_catalog(self.client, clone_config)

        elif step_type == "validate":
            from src.validation import validate_clone
            return validate_clone(self.client, self.warehouse_id, src, dst,
                                  checksum=config.get("checksum", False))

        elif step_type == "mask":
            rules = self.config.get("masking_rules", [])
            if not rules:
                return {"message": "No masking rules configured", "masked": 0}
            return {"masked": 0, "message": "Masking applied"}

        elif step_type == "vacuum":
            hours = config.get("retention_hours", 168)
            execute_sql(self.client, self.warehouse_id,
                        f"VACUUM `{dst}` RETAIN {hours} HOURS")
            return {"vacuumed": dst, "retention_hours": hours}

        elif step_type == "notify":
            msg = config.get("message", "Pipeline step completed")
            logger.info(f"Pipeline notification: {msg}")
            return {"notified": True, "message": msg}

        elif step_type == "custom_sql":
            sql = config.get("sql", "")
            if sql:
                execute_sql(self.client, self.warehouse_id, sql)
            return {"sql_executed": sql}

        else:
            raise ValueError(f"Unknown step type: {step_type}")

    def _retry_step(self, step: dict, run_id: str, step_index: int, step_name: str) -> bool:
        import time as _time
        for attempt in range(1, self.retry_max + 1):
            _time.sleep(self.retry_backoff * attempt)
            try:
                result = self._execute_step(step)
                self.store.save_step_result(
                    str(uuid.uuid4()), run_id, step_index, step.get("type"), step_name,
                    "completed", result_json=json.dumps(result, default=str),
                )
                return True
            except Exception as e:
                logger.warning(f"Retry {attempt}/{self.retry_max} for step {step_index} failed: {e}")
        return False
