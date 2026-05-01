import logging
import sys
import threading
import time
from typing import Callable

from src.log_formatter import IS_TTY as _IS_TTY

logger = logging.getLogger(__name__)


class ProgressTracker:
    """Thread-safe progress tracker with console display."""

    def __init__(
        self,
        total: int,
        description: str = "Processing",
        extra_info_fn: Callable[[], str] | None = None,
    ):
        self.total = total
        self.description = description
        self.completed = 0
        self.failed = 0
        self.skipped = 0
        self.lock = threading.Lock()
        self.start_time = time.time()
        self._running = False
        self._thread = None
        # Optional callable — its return value is appended to the status line,
        # used e.g. to show a secondary counter (Tables) alongside the primary one.
        self._extra_info_fn = extra_info_fn

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
                bar_fill = "█" * filled
                bar_empty = "░" * (bar_len - filled)
                if _IS_TTY:
                    # Color the bar: green for success, red portion for failures
                    if self.failed > 0:
                        fail_len = max(1, int(bar_len * self.failed / self.total))
                        ok_len = filled - fail_len
                        bar = f"\033[32m{'█' * max(0, ok_len)}\033[31m{'█' * fail_len}\033[0m{bar_empty}"
                    else:
                        bar = f"\033[32m{bar_fill}\033[0m{bar_empty}"
                else:
                    bar = bar_fill + bar_empty
            else:
                pct = 0
                bar = "░" * 30

            # ETA calculation
            if done > 0 and done < self.total:
                rate = done / elapsed
                eta = (self.total - done) / rate
                eta_str = _format_time(eta)
            elif done >= self.total:
                eta_str = "\033[32mdone\033[0m" if _IS_TTY else "done"
            else:
                eta_str = "..."

            # Color-coded stats
            if _IS_TTY:
                ok_s = f"\033[32m{self.completed}ok\033[0m"
                fail_s = f"\033[31m{self.failed}fail\033[0m" if self.failed else f"{self.failed}fail"
                skip_s = f"\033[33m{self.skipped}skip\033[0m" if self.skipped else f"{self.skipped}skip"
            else:
                ok_s = f"{self.completed}ok"
                fail_s = f"{self.failed}fail"
                skip_s = f"{self.skipped}skip"

            extra = ""
            if self._extra_info_fn:
                try:
                    extra = self._extra_info_fn() or ""
                except Exception:
                    extra = ""

            status = (
                f"\r  {self.description} |{bar}| "
                f"{done}/{self.total} ({pct:.0f}%) "
                f"[{ok_s}/{fail_s}/{skip_s}] "
                f"ETA: {eta_str}{extra}  "
            )

            sys.stderr.write(status)
            sys.stderr.flush()

            if final:
                sys.stderr.write("\n")
                sys.stderr.flush()


class SchemaProgressTracker:
    """Track progress across schemas with per-object-type tracking.

    Also tracks catalog-level table counts and renders them inline on the
    Schemas progress bar as ``· Tables 120/611 [118ok/1fail/1skip]`` so the
    user can see both denominators at a glance.
    """

    def __init__(
        self,
        schemas: list[str],
        show_progress: bool = True,
        tables_total: int = 0,
    ):
        self.schemas = schemas
        self.show_progress = show_progress
        self.schema_tracker = None
        self.object_trackers: dict[str, ProgressTracker] = {}

        # Catalog-level table counters (live across all schemas)
        self.tables_total = tables_total
        self._tables_done = 0
        self._tables_failed = 0
        self._tables_skipped = 0
        self._tables_lock = threading.Lock()

        if show_progress and schemas:
            self.schema_tracker = ProgressTracker(
                len(schemas),
                "Schemas",
                extra_info_fn=self._render_tables_suffix if tables_total else None,
            )

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

    def tables_update(self, success: int = 0, failed: int = 0, skipped: int = 0) -> None:
        """Bump the catalog-level Tables counters. Called mid-schema as tables finish."""
        if success or failed or skipped:
            with self._tables_lock:
                self._tables_done += success
                self._tables_failed += failed
                self._tables_skipped += skipped

    def _render_tables_suffix(self) -> str:
        with self._tables_lock:
            done = self._tables_done + self._tables_failed + self._tables_skipped
            if _IS_TTY:
                ok = f"\033[32m{self._tables_done}ok\033[0m"
                fl = f"\033[31m{self._tables_failed}fail\033[0m" if self._tables_failed else f"{self._tables_failed}fail"
                sk = f"\033[33m{self._tables_skipped}skip\033[0m" if self._tables_skipped else f"{self._tables_skipped}skip"
            else:
                ok = f"{self._tables_done}ok"
                fl = f"{self._tables_failed}fail"
                sk = f"{self._tables_skipped}skip"
            return f" · Tables {done}/{self.tables_total} [{ok}/{fl}/{sk}]"

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
