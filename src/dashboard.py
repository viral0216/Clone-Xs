import logging
import sys
import threading
import time

logger = logging.getLogger(__name__)


class Dashboard:
    """Interactive TUI dashboard showing live clone status across schemas.

    Uses simple ANSI escape codes for terminal rendering — no external dependencies.
    """

    def __init__(self, schemas: list[str]):
        self.schemas = schemas
        self.lock = threading.Lock()
        self.start_time = time.time()
        self._running = False
        self._thread = None

        # Per-schema status
        self.schema_status: dict[str, dict] = {}
        for schema in schemas:
            self.schema_status[schema] = {
                "status": "pending",  # pending, running, done, error
                "tables": {"success": 0, "failed": 0, "skipped": 0, "total": 0},
                "views": {"success": 0, "failed": 0, "skipped": 0},
                "functions": {"success": 0, "failed": 0, "skipped": 0},
                "volumes": {"success": 0, "failed": 0, "skipped": 0},
                "error": None,
            }

    def start(self) -> None:
        """Start the dashboard display."""
        self._running = True
        self._thread = threading.Thread(target=self._render_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop the dashboard and print final state."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=2)
        self._render(final=True)

    def schema_started(self, schema: str) -> None:
        """Mark a schema as running."""
        with self.lock:
            if schema in self.schema_status:
                self.schema_status[schema]["status"] = "running"

    def schema_completed(self, schema: str, results: dict) -> None:
        """Mark a schema as completed with results."""
        with self.lock:
            if schema not in self.schema_status:
                return

            if "error" in results:
                self.schema_status[schema]["status"] = "error"
                self.schema_status[schema]["error"] = results.get("error")
            else:
                self.schema_status[schema]["status"] = "done"
                for obj_type in ("tables", "views", "functions", "volumes"):
                    if obj_type in results:
                        self.schema_status[schema][obj_type] = results[obj_type]

    def update_table_count(self, schema: str, total: int) -> None:
        """Set the total table count for a schema."""
        with self.lock:
            if schema in self.schema_status:
                self.schema_status[schema]["tables"]["total"] = total

    def _render_loop(self) -> None:
        """Background render loop."""
        while self._running:
            self._render()
            time.sleep(1)

    def _render(self, final: bool = False) -> None:
        """Render the dashboard to stderr."""
        with self.lock:
            elapsed = time.time() - self.start_time
            elapsed_str = _format_duration(elapsed)

            total_schemas = len(self.schemas)
            done = sum(1 for s in self.schema_status.values() if s["status"] in ("done", "error"))
            running = sum(1 for s in self.schema_status.values() if s["status"] == "running")
            pending = sum(1 for s in self.schema_status.values() if s["status"] == "pending")

            lines = []
            lines.append("")
            lines.append(f"  Clone Dashboard  |  Elapsed: {elapsed_str}  |  "
                         f"Schemas: {done}/{total_schemas} done, {running} running, {pending} pending")
            lines.append("  " + "-" * 78)

            # Status indicators
            status_icons = {
                "pending": ".",
                "running": ">",
                "done": "+",
                "error": "!",
            }

            for schema in self.schemas:
                s = self.schema_status[schema]
                icon = status_icons.get(s["status"], "?")
                tables = s["tables"]
                t_info = f"T:{tables['success']}/{tables['failed']}/{tables['skipped']}"

                if s["status"] == "error":
                    line = f"  [{icon}] {schema:30s} ERROR: {s.get('error', 'unknown')}"
                elif s["status"] == "done":
                    v = s["views"]
                    f_info = f"V:{v['success']}/{v['failed']}"
                    line = f"  [{icon}] {schema:30s} {t_info}  {f_info}  DONE"
                elif s["status"] == "running":
                    line = f"  [{icon}] {schema:30s} {t_info}  running..."
                else:
                    line = f"  [{icon}] {schema:30s} pending"

                lines.append(line)

            lines.append("  " + "-" * 78)

            # Totals
            total_t = sum(s["tables"]["success"] for s in self.schema_status.values())
            total_f = sum(s["tables"]["failed"] for s in self.schema_status.values())
            lines.append(f"  Totals: {total_t} tables cloned, {total_f} failed")
            lines.append("")

            output = "\n".join(lines)

            if not final:
                # Move cursor up to overwrite previous output
                num_lines = len(lines)
                sys.stderr.write(f"\033[{num_lines}A\033[J")

            sys.stderr.write(output + "\n")
            sys.stderr.flush()


def _format_duration(seconds: float) -> str:
    """Format seconds as human-readable duration."""
    if seconds < 60:
        return f"{seconds:.0f}s"
    m, s = divmod(int(seconds), 60)
    if m < 60:
        return f"{m}m{s}s"
    h, m = divmod(m, 60)
    return f"{h}h{m}m{s}s"
