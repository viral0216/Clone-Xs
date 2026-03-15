import logging
import sys
import threading
import time

logger = logging.getLogger(__name__)


class ProgressTracker:
    """Thread-safe progress tracker with console display."""

    def __init__(self, total: int, description: str = "Processing"):
        self.total = total
        self.description = description
        self.completed = 0
        self.failed = 0
        self.skipped = 0
        self.lock = threading.Lock()
        self.start_time = time.time()
        self._running = False
        self._thread = None

    def start(self) -> None:
        """Start the progress display."""
        self._running = True
        self._thread = threading.Thread(target=self._display_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop the progress display."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=2)
        self._print_bar(final=True)

    def update(self, success: bool = True, skipped: bool = False) -> None:
        """Record a completed item."""
        with self.lock:
            if skipped:
                self.skipped += 1
            elif success:
                self.completed += 1
            else:
                self.failed += 1

    def get_counts(self) -> dict:
        """Get current counts."""
        with self.lock:
            return {
                "completed": self.completed,
                "failed": self.failed,
                "skipped": self.skipped,
                "total": self.total,
            }

    def _display_loop(self) -> None:
        """Background thread that updates the progress bar."""
        while self._running:
            self._print_bar()
            time.sleep(0.5)

    def _print_bar(self, final: bool = False) -> None:
        """Print a progress bar to stderr."""
        with self.lock:
            done = self.completed + self.failed + self.skipped
            elapsed = time.time() - self.start_time

            if self.total > 0:
                pct = done / self.total * 100
                bar_len = 30
                filled = int(bar_len * done / self.total)
                bar = "█" * filled + "░" * (bar_len - filled)
            else:
                pct = 0
                bar = "░" * 30

            # ETA calculation
            if done > 0 and done < self.total:
                rate = done / elapsed
                eta = (self.total - done) / rate
                eta_str = _format_time(eta)
            elif done >= self.total:
                eta_str = "done"
            else:
                eta_str = "..."

            status = (
                f"\r  {self.description} |{bar}| "
                f"{done}/{self.total} ({pct:.0f}%) "
                f"[{self.completed}ok/{self.failed}fail/{self.skipped}skip] "
                f"ETA: {eta_str}  "
            )

            sys.stderr.write(status)
            sys.stderr.flush()

            if final:
                sys.stderr.write("\n")
                sys.stderr.flush()


class SchemaProgressTracker:
    """Track progress across schemas with per-object-type tracking."""

    def __init__(self, schemas: list[str], show_progress: bool = True):
        self.schemas = schemas
        self.show_progress = show_progress
        self.schema_tracker = None
        self.object_trackers: dict[str, ProgressTracker] = {}

        if show_progress and schemas:
            self.schema_tracker = ProgressTracker(len(schemas), "Schemas")

    def start(self) -> None:
        if self.schema_tracker:
            self.schema_tracker.start()

    def stop(self) -> None:
        if self.schema_tracker:
            self.schema_tracker.stop()

    def schema_done(self, schema_results: dict) -> None:
        """Mark a schema as completed."""
        if not self.schema_tracker:
            return

        has_error = "error" in schema_results
        self.schema_tracker.update(success=not has_error)

    def get_summary(self) -> dict | None:
        if self.schema_tracker:
            return self.schema_tracker.get_counts()
        return None


def _format_time(seconds: float) -> str:
    """Format seconds into a human-readable string."""
    if seconds < 60:
        return f"{seconds:.0f}s"
    elif seconds < 3600:
        m, s = divmod(int(seconds), 60)
        return f"{m}m{s}s"
    else:
        h, remainder = divmod(int(seconds), 3600)
        m, s = divmod(remainder, 60)
        return f"{h}h{m}m"
