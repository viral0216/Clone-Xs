"""Clone queue with priority — queue multiple clone requests and process with configurable concurrency."""

import heapq
import json
import logging
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum

logger = logging.getLogger(__name__)


class Priority(Enum):
    CRITICAL = 1
    HIGH = 2
    NORMAL = 3
    LOW = 4
    BACKGROUND = 5


class JobStatus(Enum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass(order=True)
class CloneJob:
    """A queued clone operation."""
    priority: int
    job_id: str = field(compare=False)
    source_catalog: str = field(compare=False)
    dest_catalog: str = field(compare=False)
    config: dict = field(compare=False, repr=False)
    status: JobStatus = field(default=JobStatus.QUEUED, compare=False)
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat(), compare=False)
    started_at: str | None = field(default=None, compare=False)
    completed_at: str | None = field(default=None, compare=False)
    result: dict | None = field(default=None, compare=False)
    error: str | None = field(default=None, compare=False)
    submitted_by: str = field(default="unknown", compare=False)


class CloneQueue:
    """Priority queue for clone operations."""

    def __init__(self, max_concurrent: int = 2, state_file: str | None = None):
        self._queue: list[CloneJob] = []
        self._jobs: dict[str, CloneJob] = {}
        self._lock = threading.Lock()
        self._max_concurrent = max_concurrent
        self._running_count = 0
        self._state_file = state_file or "clone_queue_state.json"
        self._executor = ThreadPoolExecutor(max_workers=max_concurrent)
        self._running = False

    def submit(
        self,
        source_catalog: str,
        dest_catalog: str,
        config: dict,
        priority: Priority = Priority.NORMAL,
        submitted_by: str = "cli",
    ) -> str:
        """Submit a new clone job to the queue.

        Returns:
            Job ID.
        """
        job_id = str(uuid.uuid4())[:8]
        job = CloneJob(
            priority=priority.value,
            job_id=job_id,
            source_catalog=source_catalog,
            dest_catalog=dest_catalog,
            config=config,
            submitted_by=submitted_by,
        )

        with self._lock:
            heapq.heappush(self._queue, job)
            self._jobs[job_id] = job
            self._save_state()

        logger.info(
            f"Job {job_id} queued: {source_catalog} -> {dest_catalog} "
            f"(priority: {Priority(priority.value).name})"
        )
        return job_id

    def cancel(self, job_id: str) -> bool:
        """Cancel a queued job. Cannot cancel running jobs."""
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                logger.error(f"Job {job_id} not found")
                return False
            if job.status == JobStatus.RUNNING:
                logger.error(f"Cannot cancel running job {job_id}")
                return False
            if job.status != JobStatus.QUEUED:
                logger.error(f"Job {job_id} is already {job.status.value}")
                return False

            job.status = JobStatus.CANCELLED
            self._save_state()
            logger.info(f"Job {job_id} cancelled")
            return True

    def get_status(self, job_id: str) -> dict | None:
        """Get the status of a job."""
        job = self._jobs.get(job_id)
        if not job:
            return None
        return {
            "job_id": job.job_id,
            "source": job.source_catalog,
            "dest": job.dest_catalog,
            "status": job.status.value,
            "priority": Priority(job.priority).name,
            "created_at": job.created_at,
            "started_at": job.started_at,
            "completed_at": job.completed_at,
            "error": job.error,
            "submitted_by": job.submitted_by,
        }

    def list_jobs(self, status: JobStatus | None = None) -> list[dict]:
        """List all jobs, optionally filtered by status."""
        jobs = []
        for job in self._jobs.values():
            if status and job.status != status:
                continue
            jobs.append(self.get_status(job.job_id))
        return sorted(jobs, key=lambda j: j["created_at"], reverse=True)

    def process_queue(self) -> None:
        """Process the queue — run jobs up to max_concurrent."""
        self._running = True
        logger.info(f"Queue processor started (max concurrent: {self._max_concurrent})")

        while self._running:
            with self._lock:
                # Find next job to run
                if self._running_count >= self._max_concurrent:
                    pass
                elif self._queue:
                    job = heapq.heappop(self._queue)
                    if job.status == JobStatus.CANCELLED:
                        continue
                    job.status = JobStatus.RUNNING
                    job.started_at = datetime.now(timezone.utc).isoformat()
                    self._running_count += 1
                    self._save_state()

                    # Submit to executor
                    self._executor.submit(self._run_job, job)

            time.sleep(1)  # Check every second

    def _run_job(self, job: CloneJob) -> None:
        """Execute a single clone job."""
        logger.info(f"Job {job.job_id} started: {job.source_catalog} -> {job.dest_catalog}")

        try:
            from src.client import get_workspace_client
            from src.clone_catalog import clone_catalog

            client = get_workspace_client()
            summary = clone_catalog(client, job.config)

            with self._lock:
                job.status = JobStatus.COMPLETED
                job.completed_at = datetime.now(timezone.utc).isoformat()
                job.result = summary
                self._running_count -= 1
                self._save_state()

            total_failed = sum(summary[t]["failed"] for t in ("tables", "views", "functions", "volumes"))
            if total_failed > 0:
                logger.warning(f"Job {job.job_id} completed with {total_failed} failures")
            else:
                logger.info(f"Job {job.job_id} completed successfully")

        except Exception as e:
            with self._lock:
                job.status = JobStatus.FAILED
                job.completed_at = datetime.now(timezone.utc).isoformat()
                job.error = str(e)
                self._running_count -= 1
                self._save_state()

            logger.error(f"Job {job.job_id} failed: {e}")

    def stop(self) -> None:
        """Stop the queue processor."""
        self._running = False
        self._executor.shutdown(wait=False)
        logger.info("Queue processor stopped")

    def _save_state(self) -> None:
        """Persist queue state to disk."""
        state = {
            "jobs": {},
        }
        for job_id, job in self._jobs.items():
            state["jobs"][job_id] = {
                "job_id": job.job_id,
                "priority": job.priority,
                "source_catalog": job.source_catalog,
                "dest_catalog": job.dest_catalog,
                "status": job.status.value,
                "created_at": job.created_at,
                "started_at": job.started_at,
                "completed_at": job.completed_at,
                "error": job.error,
                "submitted_by": job.submitted_by,
            }

        try:
            with open(self._state_file, "w") as f:
                json.dump(state, f, indent=2)
        except Exception as e:
            logger.warning(f"Failed to save queue state: {e}")

    def print_queue(self) -> None:
        """Print the current queue status."""
        jobs = self.list_jobs()
        queued = [j for j in jobs if j["status"] == "queued"]
        running = [j for j in jobs if j["status"] == "running"]
        completed = [j for j in jobs if j["status"] == "completed"]
        failed = [j for j in jobs if j["status"] == "failed"]

        logger.info("=" * 60)
        logger.info("CLONE QUEUE STATUS")
        logger.info("=" * 60)
        logger.info(f"Queued:    {len(queued)}")
        logger.info(f"Running:   {len(running)}")
        logger.info(f"Completed: {len(completed)}")
        logger.info(f"Failed:    {len(failed)}")
        logger.info("-" * 60)

        for job in jobs:
            status_icon = {
                "queued": "⏳", "running": "🔄", "completed": "✅",
                "failed": "❌", "cancelled": "🚫",
            }.get(job["status"], "?")
            logger.info(
                f"  {status_icon} {job['job_id']} | {job['source']} -> {job['dest']} | "
                f"{job['priority']} | {job['status']} | {job['submitted_by']}"
            )
