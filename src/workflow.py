import json
import logging
import os

import yaml

logger = logging.getLogger(__name__)


def generate_workflow(
    config: dict,
    output_path: str = "databricks_workflow.json",
    job_name: str | None = None,
    cluster_id: str | None = None,
    schedule_cron: str | None = None,
    notification_email: str | None = None,
) -> str:
    """Generate a Databricks Workflows job JSON from clone config.

    The generated job installs the wheel and runs the clone command.
    """
    source = config["source_catalog"]
    dest = config["destination_catalog"]

    if not job_name:
        job_name = f"Clone UC: {source} -> {dest}"

    # Build the CLI command
    cmd_parts = [
        "clone-catalog", "clone",
        "--source", source,
        "--dest", dest,
        "--clone-type", config["clone_type"],
        "--load-type", config["load_type"],
        "--warehouse-id", config["sql_warehouse_id"],
        "--max-workers", str(config["max_workers"]),
    ]

    if config.get("generate_report"):
        cmd_parts.append("--report")
    if config.get("enable_rollback"):
        cmd_parts.append("--enable-rollback")
    if not config.get("copy_permissions", True):
        cmd_parts.append("--no-permissions")
    if not config.get("copy_ownership", True):
        cmd_parts.append("--no-ownership")
    if not config.get("copy_tags", True):
        cmd_parts.append("--no-tags")
    if not config.get("copy_security", True):
        cmd_parts.append("--no-security")

    cmd_parts.append("-v")

    # Build job definition
    job_def = {
        "name": job_name,
        "tasks": [
            {
                "task_key": "clone_catalog",
                "description": f"Clone {source} to {dest}",
                "python_wheel_task": {
                    "package_name": "clone_xs",
                    "entry_point": "clone-catalog",
                    "parameters": cmd_parts[1:],  # Skip the binary name
                },
                "libraries": [
                    {"pypi": {"package": "clone-xs"}},
                ],
            }
        ],
        "max_concurrent_runs": 1,
    }

    # Add existing cluster if specified
    if cluster_id:
        job_def["tasks"][0]["existing_cluster_id"] = cluster_id

    # Add schedule if specified
    if schedule_cron:
        job_def["schedule"] = {
            "quartz_cron_expression": schedule_cron,
            "timezone_id": "UTC",
            "pause_status": "UNPAUSED",
        }

    # Add email notifications
    if notification_email:
        job_def["email_notifications"] = {
            "on_failure": [notification_email],
            "on_success": [notification_email],
        }

    # Write JSON
    with open(output_path, "w") as f:
        json.dump(job_def, f, indent=2)

    logger.info(f"Databricks Workflow job definition saved: {output_path}")
    logger.info(f"Deploy with: databricks jobs create --json @{output_path}")

    return output_path


def generate_workflow_yaml(
    config: dict,
    output_path: str = "databricks_workflow.yaml",
    job_name: str | None = None,
    schedule_cron: str | None = None,
) -> str:
    """Generate a Databricks Asset Bundle YAML for the clone job."""
    source = config["source_catalog"]
    dest = config["destination_catalog"]

    if not job_name:
        job_name = f"clone_{source}_to_{dest}"

    bundle = {
        "bundle": {
            "name": f"clone-{source}-to-{dest}",
        },
        "resources": {
            "jobs": {
                job_name: {
                    "name": f"Clone UC: {source} -> {dest}",
                    "tasks": [
                        {
                            "task_key": "clone_catalog",
                            "python_wheel_task": {
                                "package_name": "clone_xs",
                                "entry_point": "clone-catalog",
                                "parameters": [
                                    "clone",
                                    "--source", source,
                                    "--dest", dest,
                                    "--clone-type", config["clone_type"],
                                    "--warehouse-id", config["sql_warehouse_id"],
                                    "-v",
                                ],
                            },
                        }
                    ],
                }
            }
        },
    }

    if schedule_cron:
        bundle["resources"]["jobs"][job_name]["schedule"] = {
            "quartz_cron_expression": schedule_cron,
            "timezone_id": "UTC",
        }

    with open(output_path, "w") as f:
        yaml.dump(bundle, f, default_flow_style=False, sort_keys=False)

    logger.info(f"Databricks Asset Bundle YAML saved: {output_path}")
    logger.info(f"Deploy with: databricks bundle deploy")

    return output_path
