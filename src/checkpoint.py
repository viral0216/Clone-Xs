"""Checkpoint management for resumable clone operations."""

import json
import logging
import os
import time
import threading
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

CHECKPOINT_DIR = "checkpoints"


class CheckpointManager:
    """Manages periodic checkpointing during clone operations.

    Thread-safe: multiple schema processing threads can record completions.
    Auto-saves when interval_tables or interval_minutes is reached.
    """

    def __init__(
        self,
        config: dict,
        interval_tables: int = 50,
        interval_minutes: float = 5.0,
    ):
        self.config = config
        self.interval_tables = interval_tables
        self.interval_minutes = interval_minutes
        self._lock = threading.Lock()
        self._tables_since_save = 0
        self._last_save_time = time.time()
        self._checkpoint_path = self._generate_path(config)
        self._state = {
            "source_catalog": config.get("source_catalog", ""),
            "destination_catalog": config.get("destination_catalog", ""),
            "clone_type": config.get("clone_type", "DEEP"),
            "started_at": datetime.now(timezone.utc).isoformat(),
            "completed_schemas": [],
            "completed_tables": [],
            "completed_views": [],
            "completed_functions": [],
            "completed_volumes": [],
            "current_schema": None,
            "last_updated": None,
        }

        # Ensure checkpoint directory exists
        os.makedirs(CHECKPOINT_DIR, exist_ok=True)
        logger.info(f"Checkpoint manager initialized: {self._checkpoint_path}")

    def _generate_path(self, config: dict) -> str:
        """Generate checkpoint file path."""
        dest = config.get("destination_catalog", "unknown")
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        return os.path.join(CHECKPOINT_DIR, f"chk_{dest}_{ts}.json")

    @property
    def checkpoint_path(self) -> str:
        return self._checkpoint_path

    def record_table(self, schema: str, table_name: str) -> None:
        """Record a completed table clone. Auto-saves if interval reached."""
        with self._lock:
            self._state["completed_tables"].append(f"{schema}.{table_name}")
            self._state["current_schema"] = schema
            self._tables_since_save += 1
            if self._should_save():
                self._save()

    def record_view(self, schema: str, view_name: str) -> None:
        """Record a completed view clone."""
        with self._lock:
            self._state["completed_views"].append(f"{schema}.{view_name}")
            if self._should_save():
                self._save()

    def record_function(self, schema: str, func_name: str) -> None:
        """Record a completed function clone."""
        with self._lock:
            self._state["completed_functions"].append(f"{schema}.{func_name}")

    def record_volume(self, schema: str, vol_name: str) -> None:
        """Record a completed volume clone."""
        with self._lock:
            self._state["completed_volumes"].append(f"{schema}.{vol_name}")

    def record_schema_complete(self, schema: str) -> None:
        """Record a fully completed schema."""
        with self._lock:
            if schema not in self._state["completed_schemas"]:
                self._state["completed_schemas"].append(schema)
            self._save()

    def _should_save(self) -> bool:
        """Check if it's time to save a checkpoint."""
        if self._tables_since_save >= self.interval_tables:
            return True
        elapsed_minutes = (time.time() - self._last_save_time) / 60
        if elapsed_minutes >= self.interval_minutes:
            return True
        return False

    def _save(self) -> None:
        """Write checkpoint JSON to disk."""
        self._state["last_updated"] = datetime.now(timezone.utc).isoformat()
        with open(self._checkpoint_path, "w") as f:
            json.dump(self._state, f, indent=2)
        self._tables_since_save = 0
        self._last_save_time = time.time()
        logger.debug(
            f"Checkpoint saved: {len(self._state['completed_tables'])} tables, "
            f"{len(self._state['completed_schemas'])} schemas"
        )

    def save_final(self) -> None:
        """Force save checkpoint (called at end of clone or on error)."""
        with self._lock:
            self._state["last_updated"] = datetime.now(timezone.utc).isoformat()
            self._state["completed_at"] = datetime.now(timezone.utc).isoformat()
            with open(self._checkpoint_path, "w") as f:
                json.dump(self._state, f, indent=2)
        logger.info(f"Final checkpoint saved: {self._checkpoint_path}")

    @staticmethod
    def load(checkpoint_path: str) -> dict:
        """Load checkpoint state from file."""
        with open(checkpoint_path) as f:
            return json.load(f)

    @staticmethod
    def get_completed_from_checkpoint(checkpoint_path: str) -> dict:
        """Convert checkpoint state to the format expected by resume.get_completed_objects().

        Returns dict with sets: {tables: set, views: set, functions: set, volumes: set, schemas: set}
        """
        state = CheckpointManager.load(checkpoint_path)

        def _parse_entries(entries: list[str]) -> set[tuple[str, str]]:
            result = set()
            for entry in entries:
                parts = entry.split(".", 1)
                if len(parts) == 2:
                    result.add((parts[0], parts[1]))
            return result

        return {
            "tables": _parse_entries(state.get("completed_tables", [])),
            "views": _parse_entries(state.get("completed_views", [])),
            "functions": _parse_entries(state.get("completed_functions", [])),
            "volumes": _parse_entries(state.get("completed_volumes", [])),
            "schemas": set(state.get("completed_schemas", [])),
        }
