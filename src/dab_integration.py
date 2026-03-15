"""Databricks Asset Bundle (DAB) integration for scheduling clone jobs."""

import json
import logging
import os

import yaml

logger = logging.getLogger(__name__)


def generate_dab_bundle(
    config: dict,
    output_dir: str = "dab_bundle",
    job_name: str | None = None,
    schedule_cron: str | None = None,
    notification_email: str | None = None,
    git_url: str | None = None,
    git_branch: str = "main",
) -> str:
    """Generate a complete Databricks Asset Bundle for the clone job.

    Creates:
      - databricks.yml (bundle config)
      - resources/clone_job.yml (job definition)
      - src/run_clone.py (entry point script)

    Returns:
        Path to the generated bundle directory.
    """
    source = config.get("source_catalog", "source_catalog")
    dest = config.get("destination_catalog", "dest_catalog")
    job_name = job_name or f"clone_{source}_to_{dest}"

    os.makedirs(os.path.join(output_dir, "resources"), exist_ok=True)
    os.makedirs(os.path.join(output_dir, "src"), exist_ok=True)

    # --- databricks.yml ---
    bundle_config = {
        "bundle": {
            "name": job_name,
        },
        "workspace": {
            "host": config.get("databricks_host", "${var.DATABRICKS_HOST}"),
        },
        "variables": {
            "DATABRICKS_HOST": {
                "description": "Databricks workspace URL",
            },
            "WAREHOUSE_ID": {
                "description": "SQL Warehouse ID",
                "default": config.get("sql_warehouse_id", ""),
            },
        },
        "include": ["resources/*.yml"],
        "targets": {
            "dev": {
                "mode": "development",
                "default": True,
                "workspace": {
                    "host": config.get("databricks_host", "${var.DATABRICKS_HOST}"),
                },
            },
            "staging": {
                "mode": "development",
                "workspace": {
                    "host": config.get("databricks_host", "${var.DATABRICKS_HOST}"),
                },
            },
            "production": {
                "workspace": {
                    "host": config.get("databricks_host", "${var.DATABRICKS_HOST}"),
                },
            },
        },
    }

    bundle_path = os.path.join(output_dir, "databricks.yml")
    with open(bundle_path, "w") as f:
        yaml.dump(bundle_config, f, default_flow_style=False, sort_keys=False)

    # --- resources/clone_job.yml ---
    tasks = [
        {
            "task_key": "preflight_checks",
            "description": "Run pre-flight checks before cloning",
            "python_wheel_task": {
                "package_name": "clone_xs",
                "entry_point": "clone-catalog",
                "parameters": [
                    "preflight",
                    "--config", "/Workspace/${bundle.name}/config/clone_config.yaml",
                    "--source", source,
                    "--dest", dest,
                    "--warehouse-id", "${var.WAREHOUSE_ID}",
                ],
            },
            "libraries": [{"pypi": {"package": "clone-xs"}}],
        },
        {
            "task_key": "clone_catalog",
            "depends_on": [{"task_key": "preflight_checks"}],
            "description": f"Clone {source} -> {dest}",
            "python_wheel_task": {
                "package_name": "clone_xs",
                "entry_point": "clone-catalog",
                "parameters": [
                    "clone",
                    "--config", "/Workspace/${bundle.name}/config/clone_config.yaml",
                    "--source", source,
                    "--dest", dest,
                    "--warehouse-id", "${var.WAREHOUSE_ID}",
                    "--enable-rollback",
                    "--validate",
                ],
            },
            "libraries": [{"pypi": {"package": "clone-xs"}}],
        },
        {
            "task_key": "validate_clone",
            "depends_on": [{"task_key": "clone_catalog"}],
            "description": "Validate the clone with row counts",
            "python_wheel_task": {
                "package_name": "clone_xs",
                "entry_point": "clone-catalog",
                "parameters": [
                    "validate",
                    "--config", "/Workspace/${bundle.name}/config/clone_config.yaml",
                    "--source", source,
                    "--dest", dest,
                    "--warehouse-id", "${var.WAREHOUSE_ID}",
                ],
            },
            "libraries": [{"pypi": {"package": "clone-xs"}}],
        },
    ]

    # Use notebook task as alternative (no wheel needed)
    notebook_tasks = [
        {
            "task_key": "preflight_checks",
            "description": "Run pre-flight checks",
            "notebook_task": {
                "notebook_path": "${bundle.name}/notebooks/run_preflight",
                "base_parameters": {
                    "source_catalog": source,
                    "dest_catalog": dest,
                    "warehouse_id": "${var.WAREHOUSE_ID}",
                },
            },
        },
        {
            "task_key": "clone_catalog",
            "depends_on": [{"task_key": "preflight_checks"}],
            "description": f"Clone {source} -> {dest}",
            "notebook_task": {
                "notebook_path": "${bundle.name}/notebooks/run_clone",
                "base_parameters": {
                    "source_catalog": source,
                    "dest_catalog": dest,
                    "warehouse_id": "${var.WAREHOUSE_ID}",
                    "clone_type": config.get("clone_type", "DEEP"),
                },
            },
        },
        {
            "task_key": "validate_clone",
            "depends_on": [{"task_key": "clone_catalog"}],
            "description": "Validate clone results",
            "notebook_task": {
                "notebook_path": "${bundle.name}/notebooks/run_validate",
                "base_parameters": {
                    "source_catalog": source,
                    "dest_catalog": dest,
                    "warehouse_id": "${var.WAREHOUSE_ID}",
                },
            },
        },
    ]

    job_config = {
        "resources": {
            "jobs": {
                job_name: {
                    "name": job_name,
                    "description": f"Automated catalog clone: {source} -> {dest}",
                    "tasks": notebook_tasks,
                    "max_concurrent_runs": 1,
                    "queue": {"enabled": True},
                },
            },
        },
    }

    if schedule_cron:
        job_config["resources"]["jobs"][job_name]["schedule"] = {
            "quartz_cron_expression": schedule_cron,
            "timezone_id": "UTC",
        }

    if notification_email:
        job_config["resources"]["jobs"][job_name]["email_notifications"] = {
            "on_failure": [notification_email],
            "on_success": [notification_email],
        }

    job_path = os.path.join(output_dir, "resources", "clone_job.yml")
    with open(job_path, "w") as f:
        yaml.dump(job_config, f, default_flow_style=False, sort_keys=False)

    # --- src/run_clone.py (notebook entry point) ---
    run_script = '''# Databricks notebook source
import sys
import os

# Add repo root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.client import get_workspace_client
from src.clone_catalog import clone_catalog
from src.config import load_config

# Get parameters from workflow
dbutils.widgets.text("source_catalog", "")
dbutils.widgets.text("dest_catalog", "")
dbutils.widgets.text("warehouse_id", "")
dbutils.widgets.text("clone_type", "DEEP")
dbutils.widgets.text("config_path", "config/clone_config.yaml")

source = dbutils.widgets.get("source_catalog")
dest = dbutils.widgets.get("dest_catalog")
warehouse_id = dbutils.widgets.get("warehouse_id")
clone_type = dbutils.widgets.get("clone_type")
config_path = dbutils.widgets.get("config_path")

config = load_config(config_path)
config["source_catalog"] = source
config["destination_catalog"] = dest
config["sql_warehouse_id"] = warehouse_id
config["clone_type"] = clone_type

client = get_workspace_client()
summary = clone_catalog(client, config)

total_failed = sum(summary[t]["failed"] for t in ("tables", "views", "functions", "volumes"))
if total_failed > 0:
    raise Exception(f"Clone completed with {total_failed} failures")

print(f"Clone completed successfully: {summary}")
'''
    run_path = os.path.join(output_dir, "src", "run_clone.py")
    with open(run_path, "w") as f:
        f.write(run_script)

    logger.info(f"DAB bundle generated at: {output_dir}/")
    logger.info(f"  databricks.yml — bundle configuration")
    logger.info(f"  resources/clone_job.yml — job definition with 3 tasks")
    logger.info(f"  src/run_clone.py — notebook entry point")
    logger.info(f"")
    logger.info(f"Deploy with:")
    logger.info(f"  cd {output_dir}")
    logger.info(f"  databricks bundle deploy --target dev")
    logger.info(f"  databricks bundle run {job_name} --target dev")

    return output_dir
