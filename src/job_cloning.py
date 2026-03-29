"""Databricks Jobs cloning — clone, diff, backup, and restore job definitions across workspaces."""

import json
import logging

logger = logging.getLogger(__name__)

# Fields to strip from source job before cloning (runtime/identity fields)
STRIP_FIELDS = {"job_id", "created_time", "creator_user_name", "run_as_user_name", "run_as"}


class JobCloneManager:
    """Clone Databricks Jobs within or across workspaces."""

    def __init__(self, client):
        self.client = client

    def list_jobs(self, name_filter: str = "", limit: int = 100) -> list[dict]:
        """List all workspace jobs with optional name filter."""
        try:
            jobs = []
            for job in self.client.jobs.list(limit=limit, expand_tasks=False):
                info = {
                    "job_id": job.job_id,
                    "name": job.settings.name if job.settings else "",
                    "created_time": str(job.created_time) if job.created_time else "",
                    "creator_user_name": job.creator_user_name or "",
                    "tags": dict(job.settings.tags) if job.settings and job.settings.tags else {},
                }
                # Schedule
                if job.settings and job.settings.schedule:
                    info["schedule"] = job.settings.schedule.quartz_cron_expression or ""
                    info["schedule_paused"] = str(job.settings.schedule.pause_status) if job.settings.schedule.pause_status else ""
                else:
                    info["schedule"] = ""
                    info["schedule_paused"] = ""

                # Task count
                info["task_count"] = len(job.settings.tasks) if job.settings and job.settings.tasks else 0

                if name_filter and name_filter.lower() not in info["name"].lower():
                    continue
                jobs.append(info)
            return jobs
        except Exception as e:
            logger.error(f"Failed to list jobs: {e}")
            return []

    def get_job_details(self, job_id: int) -> dict:
        """Get full job configuration and recent runs."""
        try:
            job = self.client.jobs.get(job_id)
            settings = job.settings

            detail = {
                "job_id": job.job_id,
                "name": settings.name if settings else "",
                "created_time": str(job.created_time) if job.created_time else "",
                "creator_user_name": job.creator_user_name or "",
                "settings": json.loads(json.dumps(settings.as_dict())) if settings else {},
            }

            # Get recent runs
            try:
                runs = []
                for run in self.client.jobs.list_runs(job_id=job_id, limit=5):
                    runs.append({
                        "run_id": run.run_id,
                        "state": str(run.state.life_cycle_state) if run.state else "",
                        "result_state": str(run.state.result_state) if run.state and run.state.result_state else "",
                        "start_time": str(run.start_time) if run.start_time else "",
                        "end_time": str(run.end_time) if run.end_time else "",
                        "run_duration": run.run_duration or 0,
                    })
                detail["recent_runs"] = runs
            except Exception:
                detail["recent_runs"] = []

            return detail
        except Exception as e:
            logger.error(f"Failed to get job {job_id}: {e}")
            return {"error": str(e)}

    def clone_job(self, job_id: int, new_name: str, overrides: dict = None) -> dict:
        """Clone a job within the same workspace."""
        try:
            source = self.client.jobs.get(job_id)
            if not source.settings:
                return {"error": "Source job has no settings"}

            settings = json.loads(json.dumps(source.settings.as_dict()))

            # Strip runtime fields
            for field in STRIP_FIELDS:
                settings.pop(field, None)

            # Apply new name
            settings["name"] = new_name or f"{settings.get('name', 'job')}_clone"

            # Apply overrides
            if overrides:
                for key, value in overrides.items():
                    if value is not None:
                        settings[key] = value

            # Create the new job
            result = self.client.jobs.create(**settings)

            return {
                "source_job_id": job_id,
                "new_job_id": result.job_id,
                "new_name": settings["name"],
                "status": "cloned",
            }
        except Exception as e:
            logger.error(f"Failed to clone job {job_id}: {e}")
            return {"error": str(e)}

    def clone_job_cross_workspace(self, job_id: int, dest_host: str, dest_token: str, new_name: str = "") -> dict:
        """Clone a job to a different Databricks workspace."""
        try:
            from databricks.sdk import WorkspaceClient

            # Get source job
            source = self.client.jobs.get(job_id)
            if not source.settings:
                return {"error": "Source job has no settings"}

            settings = json.loads(json.dumps(source.settings.as_dict()))

            # Strip runtime fields
            for field in STRIP_FIELDS:
                settings.pop(field, None)

            settings["name"] = new_name or f"{settings.get('name', 'job')}_clone"

            # Remove workspace-specific references that won't exist in destination
            warnings = []
            if "existing_cluster_id" in settings:
                warnings.append(f"existing_cluster_id '{settings['existing_cluster_id']}' may not exist in destination")
            if settings.get("tasks"):
                for task in settings["tasks"]:
                    if "existing_cluster_id" in task:
                        warnings.append(f"Task '{task.get('task_key', '?')}' uses existing_cluster_id — may not exist in destination")

            # Create destination client
            dest_client = WorkspaceClient(host=dest_host, token=dest_token)
            result = dest_client.jobs.create(**settings)

            return {
                "source_job_id": job_id,
                "source_workspace": self.client.config.host,
                "dest_workspace": dest_host,
                "new_job_id": result.job_id,
                "new_name": settings["name"],
                "status": "cloned_cross_workspace",
                "warnings": warnings,
            }
        except Exception as e:
            logger.error(f"Failed to clone job {job_id} cross-workspace: {e}")
            return {"error": str(e)}

    def diff_jobs(self, job_id_a: int, job_id_b: int) -> dict:
        """Compare two job configurations side-by-side."""
        try:
            job_a = self.client.jobs.get(job_id_a)
            job_b = self.client.jobs.get(job_id_b)

            settings_a = json.loads(json.dumps(job_a.settings.as_dict())) if job_a.settings else {}
            settings_b = json.loads(json.dumps(job_b.settings.as_dict())) if job_b.settings else {}

            # Find differences
            all_keys = sorted(set(list(settings_a.keys()) + list(settings_b.keys())))
            diffs = []
            for key in all_keys:
                val_a = settings_a.get(key)
                val_b = settings_b.get(key)
                if val_a != val_b:
                    diffs.append({
                        "field": key,
                        "job_a": json.dumps(val_a, indent=2) if isinstance(val_a, (dict, list)) else str(val_a) if val_a is not None else "(missing)",
                        "job_b": json.dumps(val_b, indent=2) if isinstance(val_b, (dict, list)) else str(val_b) if val_b is not None else "(missing)",
                    })

            return {
                "job_a": {"job_id": job_id_a, "name": settings_a.get("name", "")},
                "job_b": {"job_id": job_id_b, "name": settings_b.get("name", "")},
                "total_fields": len(all_keys),
                "differences": len(diffs),
                "diffs": diffs,
            }
        except Exception as e:
            return {"error": str(e)}

    def backup_jobs(self, job_ids: list[int]) -> list[dict]:
        """Export job definitions as JSON for backup."""
        backups = []
        for job_id in job_ids:
            try:
                job = self.client.jobs.get(job_id)
                settings = json.loads(json.dumps(job.settings.as_dict())) if job.settings else {}
                for field in STRIP_FIELDS:
                    settings.pop(field, None)
                backups.append({"original_job_id": job_id, "name": settings.get("name", ""), "settings": settings})
            except Exception as e:
                backups.append({"original_job_id": job_id, "error": str(e)})
        return backups

    def restore_jobs(self, definitions: list[dict]) -> list[dict]:
        """Import job definitions from backup JSON."""
        results = []
        for defn in definitions:
            try:
                settings = defn.get("settings", {})
                if not settings:
                    results.append({"name": defn.get("name", "?"), "error": "No settings provided"})
                    continue
                result = self.client.jobs.create(**settings)
                results.append({"name": settings.get("name", ""), "new_job_id": result.job_id, "status": "restored"})
            except Exception as e:
                results.append({"name": defn.get("name", "?"), "error": str(e)})
        return results
