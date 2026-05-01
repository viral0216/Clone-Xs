"""Tests for JobManager auto-retry on transient clone failures.

Covers the helpers added for Tier 1 #2:
  - `_is_transient_error` classification (transient vs logical)
  - `_execute_clone_with_retry` retry loop, backoff, attempt tracking
"""

import asyncio

import pytest

from api.queue.job_manager import JobManager, _is_transient_error


class TestIsTransientError:
    @pytest.mark.parametrize(
        "exc",
        [
            ValueError("bad input"),
            KeyError("missing"),
            TypeError("wrong type"),
            AttributeError("no attr"),
            AssertionError("nope"),
        ],
    )
    def test_logical_errors_never_retry(self, exc):
        assert _is_transient_error(exc) is False

    @pytest.mark.parametrize(
        "exc",
        [
            TimeoutError("took too long"),
            ConnectionError("reset by peer"),
        ],
    )
    def test_network_classes_retry(self, exc):
        assert _is_transient_error(exc) is True

    @pytest.mark.parametrize(
        "msg",
        [
            "HTTP 429 rate limit",
            "503 Service Unavailable",
            "504 Gateway Timeout",
            "connection reset by peer",
            "request throttled",
            "operation timed out",
            "service temporarily unavailable",
        ],
    )
    def test_transient_substrings_retry(self, msg):
        assert _is_transient_error(Exception(msg)) is True

    @pytest.mark.parametrize(
        "msg",
        [
            "table not found",
            "permission denied",
            "schema mismatch",
            "invalid SQL syntax",
        ],
    )
    def test_unknown_messages_do_not_retry(self, msg):
        # Conservative default — unknown errors don't auto-retry so logical
        # bugs aren't masked by retries that hide the real failure.
        assert _is_transient_error(Exception(msg)) is False


def _stub_job(jm: JobManager, job_id: str) -> None:
    jm.jobs[job_id] = {
        "job_id": job_id,
        "status": "running",
        "logs": [],
        "attempt": 1,
        "max_attempts": 1,
        "retry_history": [],
    }


class TestExecuteCloneWithRetry:
    def setup_method(self):
        self.jm = JobManager()
        self.loop = asyncio.new_event_loop()

    def teardown_method(self):
        self.loop.close()

    def test_succeeds_after_transient_failures(self):
        _stub_job(self.jm, "j1")
        calls = {"n": 0}

        def flaky():
            calls["n"] += 1
            if calls["n"] < 3:
                raise Exception("HTTP 429 rate limit")
            return {"ok": True}

        result = self.jm._execute_clone_with_retry(
            flaky,
            "j1",
            {"max_retries": 5, "enable_retry": True},
            self.loop,
            self.jm.jobs["j1"]["logs"],
            "test",
        )
        assert result == {"ok": True}
        assert self.jm.jobs["j1"]["attempt"] == 3
        assert len(self.jm.jobs["j1"]["retry_history"]) == 2

    def test_logical_error_raises_without_retry(self):
        _stub_job(self.jm, "j2")
        calls = {"n": 0}

        def buggy():
            calls["n"] += 1
            raise ValueError("schema mismatch")

        with pytest.raises(ValueError):
            self.jm._execute_clone_with_retry(
                buggy,
                "j2",
                {"max_retries": 5, "enable_retry": True},
                self.loop,
                self.jm.jobs["j2"]["logs"],
                "test",
            )
        assert calls["n"] == 1
        assert self.jm.jobs["j2"]["retry_history"] == []

    def test_enable_retry_false_forces_single_attempt(self):
        _stub_job(self.jm, "j3")
        calls = {"n": 0}

        def transient():
            calls["n"] += 1
            raise Exception("HTTP 503")

        with pytest.raises(Exception, match="503"):
            self.jm._execute_clone_with_retry(
                transient,
                "j3",
                {"max_retries": 5, "enable_retry": False},
                self.loop,
                self.jm.jobs["j3"]["logs"],
                "test",
            )
        assert calls["n"] == 1

    def test_exhausts_max_attempts_on_persistent_transient(self, monkeypatch):
        # Patch sleep so the test doesn't actually wait through backoff delays.
        import api.queue.job_manager as jm_mod

        monkeypatch.setattr(jm_mod.time, "sleep", lambda _s: None)

        _stub_job(self.jm, "j4")
        calls = {"n": 0}

        def always_throttled():
            calls["n"] += 1
            raise Exception("429 throttled")

        with pytest.raises(Exception, match="429"):
            self.jm._execute_clone_with_retry(
                always_throttled,
                "j4",
                {"max_retries": 3, "enable_retry": True},
                self.loop,
                self.jm.jobs["j4"]["logs"],
                "test",
            )
        assert calls["n"] == 3
        assert len(self.jm.jobs["j4"]["retry_history"]) == 2

    def test_max_attempts_reflected_on_job_dict(self, monkeypatch):
        import api.queue.job_manager as jm_mod

        monkeypatch.setattr(jm_mod.time, "sleep", lambda _s: None)
        _stub_job(self.jm, "j5")

        def ok():
            return {"done": True}

        self.jm._execute_clone_with_retry(
            ok,
            "j5",
            {"max_retries": 7, "enable_retry": True},
            self.loop,
            self.jm.jobs["j5"]["logs"],
            "test",
        )
        assert self.jm.jobs["j5"]["max_attempts"] == 7
