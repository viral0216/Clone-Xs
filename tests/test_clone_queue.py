"""Tests for the clone queue module."""

from src.clone_queue import CloneJob, CloneQueue, JobStatus, Priority


class TestPriority:
    def test_enum_values(self):
        assert Priority.CRITICAL.value == 1
        assert Priority.HIGH.value == 2
        assert Priority.NORMAL.value == 3
        assert Priority.LOW.value == 4
        assert Priority.BACKGROUND.value == 5

    def test_ordering(self):
        assert Priority.CRITICAL.value < Priority.HIGH.value
        assert Priority.HIGH.value < Priority.NORMAL.value


class TestJobStatus:
    def test_enum_values(self):
        assert JobStatus.QUEUED.value == "queued"
        assert JobStatus.RUNNING.value == "running"
        assert JobStatus.COMPLETED.value == "completed"
        assert JobStatus.FAILED.value == "failed"
        assert JobStatus.CANCELLED.value == "cancelled"


class TestCloneJob:
    def test_creation(self):
        job = CloneJob(
            priority=Priority.NORMAL.value,
            job_id="abc123",
            source_catalog="src",
            dest_catalog="dst",
            config={"clone_type": "DEEP"},
        )
        assert job.priority == 3
        assert job.job_id == "abc123"
        assert job.status == JobStatus.QUEUED
        assert job.started_at is None

    def test_ordering_by_priority(self):
        high = CloneJob(priority=2, job_id="high", source_catalog="s", dest_catalog="d", config={})
        low = CloneJob(priority=4, job_id="low", source_catalog="s", dest_catalog="d", config={})
        assert high < low  # Lower priority value = higher priority


class TestCloneQueue:
    def test_submit_returns_job_id(self):
        queue = CloneQueue(max_concurrent=2)
        job_id = queue.submit("src", "dst", {"clone_type": "DEEP"})
        assert isinstance(job_id, str)
        assert len(job_id) == 8

    def test_submit_adds_to_jobs(self):
        queue = CloneQueue(max_concurrent=2)
        job_id = queue.submit("src", "dst", {})
        assert job_id in queue._jobs

    def test_get_status(self):
        queue = CloneQueue(max_concurrent=2)
        job_id = queue.submit("src", "dst", {}, priority=Priority.HIGH)
        status = queue.get_status(job_id)

        assert status["job_id"] == job_id
        assert status["source"] == "src"
        assert status["dest"] == "dst"
        assert status["status"] == "queued"
        assert status["priority"] == "HIGH"

    def test_get_status_nonexistent(self):
        queue = CloneQueue(max_concurrent=2)
        assert queue.get_status("nonexistent") is None

    def test_cancel_queued_job(self):
        queue = CloneQueue(max_concurrent=2)
        job_id = queue.submit("src", "dst", {})
        assert queue.cancel(job_id) is True
        assert queue._jobs[job_id].status == JobStatus.CANCELLED

    def test_cancel_nonexistent_job(self):
        queue = CloneQueue(max_concurrent=2)
        assert queue.cancel("nonexistent") is False

    def test_list_jobs(self):
        queue = CloneQueue(max_concurrent=2)
        queue.submit("src1", "dst1", {})
        queue.submit("src2", "dst2", {})
        jobs = queue.list_jobs()
        assert len(jobs) == 2

    def test_list_jobs_filtered(self):
        queue = CloneQueue(max_concurrent=2)
        job_id = queue.submit("src1", "dst1", {})
        queue.cancel(job_id)
        queue.submit("src2", "dst2", {})

        queued = queue.list_jobs(status=JobStatus.QUEUED)
        cancelled = queue.list_jobs(status=JobStatus.CANCELLED)
        assert len(queued) == 1
        assert len(cancelled) == 1

    def test_priority_ordering(self):
        queue = CloneQueue(max_concurrent=2)
        queue.submit("low", "dst", {}, priority=Priority.LOW)
        queue.submit("critical", "dst", {}, priority=Priority.CRITICAL)
        queue.submit("normal", "dst", {}, priority=Priority.NORMAL)

        # Queue should have critical first
        assert queue._queue[0].source_catalog == "critical"
