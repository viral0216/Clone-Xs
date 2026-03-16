"""Create a persistent Databricks Job for scheduled catalog cloning.

Unlike `serverless.submit_clone_job()` which submits a one-off run,
this module creates a reusable job that appears in the Databricks Jobs UI
and can be scheduled, triggered, or run manually.
"""

from __future__ import annotations

import json
import logging

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import (
    CronSchedule,
    JobEmailNotifications,
    NotebookTask,
    PauseStatus,
    Source,
    Task,
)

from src.serverless import (
    _ensure_uploaded,
    build_job_config,
    select_volume,
)

logger = logging.getLogger(__name__)


def create_persistent_job(
    client: WorkspaceClient,
    config: dict,
    job_name: str | None = None,
    volume_path: str | None = None,
    wheel_path: str | None = None,
    schedule_cron: str | None = None,
    schedule_timezone: str = "UTC",
    notification_emails: list[str] | None = None,
    max_retries: int = 0,
    timeout_seconds: int = 7200,
    tags: dict[str, str] | None = None,
    update_job_id: int | None = None,
) -> dict:
    """Create (or update) a persistent Databricks Job for catalog cloning.

    Args:
        client: Authenticated WorkspaceClient.
        config: Clone configuration dict.
        job_name: Display name for the job.
        volume_path: UC Volume path for wheel upload.
        wheel_path: Path to .whl file (auto-detected if not provided).
        schedule_cron: Quartz cron expression (e.g. "0 0 6 * * ?").
        schedule_timezone: Timezone for the schedule (default: UTC).
        notification_emails: Email addresses for job notifications.
        max_retries: Number of retries on failure.
        timeout_seconds: Job timeout in seconds.
        tags: Job tags as key-value pairs.
        update_job_id: If provided, update this existing job instead of creating.

    Returns:
        Dict with job_id, job_url, notebook_path, volume_wheel_path.
    """
    source = config.get("source_catalog", "")
    dest = config.get("destination_catalog", "")

    if not job_name:
        job_name = f"Clone-Xs: {source} -> {dest}"

    # Resolve volume path
    if not volume_path:
        volume_path = config.get("volume_path") or select_volume(client)

    # Upload wheel and create clone notebook
    vol_wheel, nb_path = _ensure_uploaded(client, volume_path, wheel_path)

    # Build the config that gets passed to the notebook
    job_config = build_job_config(config)
    config_json = json.dumps(job_config)

    logger.info("Creating Databricks Job: %s", job_name)
    logger.info("Wheel: %s", vol_wheel)
    logger.info("Notebook: %s", nb_path)

    # Build task
    task = Task(
        task_key="clone_catalog",
        description=f"Clone {source} to {dest}",
        notebook_task=NotebookTask(
            notebook_path=nb_path,
            base_parameters={"config": config_json},
            source=Source.WORKSPACE,
        ),
        max_retries=max_retries,
        timeout_seconds=timeout_seconds,
    )

    # Build schedule
    schedule = None
    if schedule_cron:
        schedule = CronSchedule(
            quartz_cron_expression=schedule_cron,
            timezone_id=schedule_timezone,
            pause_status=PauseStatus.UNPAUSED,
        )
        logger.info("Schedule: %s (%s)", schedule_cron, schedule_timezone)

    # Build notifications
    email_notifications = None
    if notification_emails:
        email_notifications = JobEmailNotifications(
            on_failure=notification_emails,
            on_success=notification_emails,
        )
        logger.info("Notifications: %s", ", ".join(notification_emails))

    # Job tags
    job_tags = {"created_by": "clone-xs"}
    if tags:
        job_tags.update(tags)

    # Create or update
    host = client.config.host.rstrip("/")

    if update_job_id:
        from databricks.sdk.service.jobs import JobSettings

        new_settings = JobSettings(
            name=job_name,
            tasks=[task],
            schedule=schedule,
            email_notifications=email_notifications,
            max_concurrent_runs=1,
            timeout_seconds=timeout_seconds,
            tags=job_tags,
        )
        client.jobs.reset(job_id=update_job_id, new_settings=new_settings)
        job_id = update_job_id
        logger.info("Job updated (job_id=%s)", job_id)
    else:
        response = client.jobs.create(
            name=job_name,
            tasks=[task],
            schedule=schedule,
            email_notifications=email_notifications,
            max_concurrent_runs=1,
            timeout_seconds=timeout_seconds,
            tags=job_tags,
        )
        job_id = response.job_id
        logger.info("Job created (job_id=%s)", job_id)

    job_url = f"{host}/#job/{job_id}"

    return {
        "job_id": job_id,
        "job_url": job_url,
        "job_name": job_name,
        "notebook_path": nb_path,
        "volume_wheel_path": vol_wheel,
        "schedule": schedule_cron,
    }
