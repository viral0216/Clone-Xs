"""Throttle controls for managing clone resource consumption."""

import logging
import time
import threading
from dataclasses import dataclass
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass
class ThrottleProfile:
    """Defines resource limits for clone operations."""
    name: str
    max_concurrent_deep_clones: int
    max_tables_per_minute: int
    max_rps: float
    max_parallel_queries: int
    max_workers: int


PRESET_PROFILES = {
    "low": ThrottleProfile(
        name="low",
        max_concurrent_deep_clones=1,
        max_tables_per_minute=5,
        max_rps=2.0,
        max_parallel_queries=3,
        max_workers=1,
    ),
    "medium": ThrottleProfile(
        name="medium",
        max_concurrent_deep_clones=2,
        max_tables_per_minute=15,
        max_rps=5.0,
        max_parallel_queries=6,
        max_workers=2,
    ),
    "high": ThrottleProfile(
        name="high",
        max_concurrent_deep_clones=4,
        max_tables_per_minute=60,
        max_rps=15.0,
        max_parallel_queries=10,
        max_workers=4,
    ),
    "max": ThrottleProfile(
        name="max",
        max_concurrent_deep_clones=8,
        max_tables_per_minute=0,  # unlimited
        max_rps=0,  # unlimited
        max_parallel_queries=20,
        max_workers=8,
    ),
}


class ThrottleSchedule:
    """Time-based throttle profile switching."""

    def __init__(self, schedule_config: list[dict]):
        """Initialize with schedule config.

        schedule_config: [
            {"hours": "0-6", "profile": "high"},
            {"hours": "9-17", "profile": "low"},
            {"hours": "17-24", "profile": "medium"},
        ]
        """
        self.schedule = schedule_config

    def get_current_profile(self) -> ThrottleProfile:
        """Return the profile for the current hour."""
        current_hour = datetime.now().hour

        for entry in self.schedule:
            hours_str = entry.get("hours", "")
            profile_name = entry.get("profile", "medium")

            if "-" in hours_str:
                start, end = hours_str.split("-")
                start_h, end_h = int(start), int(end)
                if start_h <= current_hour < end_h:
                    return PRESET_PROFILES.get(profile_name, PRESET_PROFILES["medium"])

        # Default to medium if no schedule matches
        return PRESET_PROFILES["medium"]


class TableRateLimiter:
    """Rate limiter that tracks tables-per-minute using a sliding window."""

    def __init__(self, max_tables_per_minute: int):
        self.max_tpm = max_tables_per_minute
        self._lock = threading.Lock()
        self._timestamps: list[float] = []

    def acquire(self) -> None:
        """Block until a table clone is allowed under the TPM limit."""
        if self.max_tpm <= 0:
            return  # unlimited

        while True:
            with self._lock:
                now = time.time()
                # Remove timestamps older than 60 seconds
                self._timestamps = [t for t in self._timestamps if now - t < 60]

                if len(self._timestamps) < self.max_tpm:
                    self._timestamps.append(now)
                    return

            # Wait before retrying
            time.sleep(0.5)

    def record_completion(self) -> None:
        """Record that a table clone finished (for tracking only)."""
        pass  # Tracking handled by acquire()


def apply_throttle_profile(profile: ThrottleProfile, config: dict) -> None:
    """Apply a throttle profile to the config dict."""
    logger.info(f"Applying throttle profile: {profile.name}")

    config["max_workers"] = profile.max_workers
    config["max_parallel_queries"] = profile.max_parallel_queries

    if profile.max_rps > 0:
        config["max_rps"] = profile.max_rps

    if profile.max_tables_per_minute > 0:
        config["max_tables_per_minute"] = profile.max_tables_per_minute

    if profile.max_concurrent_deep_clones > 0:
        config["parallel_tables"] = profile.max_concurrent_deep_clones

    logger.info(
        f"  max_workers={config['max_workers']}, "
        f"max_parallel_queries={config['max_parallel_queries']}, "
        f"max_rps={config.get('max_rps', 0)}"
    )


def resolve_throttle(config: dict) -> ThrottleProfile | None:
    """Resolve throttle settings from config. Returns profile or None."""
    # Check for throttle schedule first
    schedule_config = config.get("throttle_schedule")
    if schedule_config:
        schedule = ThrottleSchedule(schedule_config)
        profile = schedule.get_current_profile()
        logger.info(f"Using scheduled throttle profile: {profile.name}")
        return profile

    # Check for preset profile
    throttle = config.get("throttle")
    if throttle and throttle in PRESET_PROFILES:
        return PRESET_PROFILES[throttle]

    return None
