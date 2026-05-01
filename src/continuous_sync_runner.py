"""Continuous sync executor — lifecycle for long-running CDC streaming jobs.

The roadmap (Feature 6) called for moving `continuous_sync.py` from a plan-
generator to an actual executor. This module is the runtime side: it submits
the plan as a Databricks Jobs run, tracks the run-id in a process-local
registry, classifies its health from the Databricks run state, and exposes
start/stop/restart controls.

Why process-local state instead of Delta-backed? Two reasons:

1. The stream registry is small (a customer with 50+ continuous syncs is an
   anomaly worth a phone call). The cost of an in-memory dict is trivial.
2. Restart semantics are intentionally non-persistent: when the API server
   restarts, all known streams are re-discovered from `client.jobs.list_runs`
   filtered by run-name prefix. Persistence in a Delta table would be
   misleading — it'd show streams as RUNNING that are no longer monitored.
   Re-discovery from the source of truth is more correct.

Health classification — these are the user-facing states:

- ``starting``   — submit() issued, run-id assigned, run hasn't started yet
- ``running``    — job's latest run state is RUNNING / IN_PROGRESS
- ``failed``     — latest run terminated with FAILED / TIMEDOUT
- ``stopped``    — latest run terminated via cancel_run() or manual STOP
- ``idle``       — no runs found (job exists but never ran)
- ``unknown``    — Databricks returned an unrecognised state (defensive)

The mapping from Databricks run state to user-facing state is in
`_classify_run_state`. Tests cover every life_cycle_state value the SDK
documents.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)

# Naming prefix for run-name on submitted jobs. Lets the runner re-discover
# streams it submitted (across API server restarts) by filtering jobs.list_runs
# on this prefix.
_RUN_NAME_PREFIX = "clxs-continuous-sync"


@dataclass
class StreamRecord:
    """Process-local registry entry for one continuous sync stream.

    Held in `_REGISTRY` (a dict). Never persisted — see module docstring
    for why. The minimal authoritative state is `run_id`; everything else
    is decorative metadata for the UI.
    """
    stream_id: str
    source_catalog: str
    destination_catalog: str
    schema: str | None
    tables: list[str]
    trigger_ms: int
    run_id: int | None = None
    submitted_at: float = field(default_factory=time.time)
    last_status: str = "starting"
    last_error: str | None = None
    # Cached run state — updated on each refresh_stream_status() call. Avoids
    # hammering the Databricks API on every list endpoint hit.
    last_polled_at: float = 0.0


_REGISTRY: dict[str, StreamRecord] = {}
_REGISTRY_LOCK = threading.Lock()


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


def start_stream(
    client: "WorkspaceClient",
    *,
    source_catalog: str,
    destination_catalog: str,
    tables: list[str] | None = None,
    schema: str | None = None,
    trigger_ms: int = 30_000,
    checkpoint_root: str | None = None,
) -> StreamRecord:
    """Submit a continuous-sync streaming job to Databricks and register it.

    Returns the StreamRecord with run_id populated. On submit failure the
    record carries `last_status="failed"` and `last_error` set; caller can
    inspect or retry.

    The submission uses `client.jobs.submit()` which creates a one-off run
    that persists across API server restarts (the run lives on Databricks,
    not on the API process). On API server restart, callers can invoke
    `discover_existing_streams(client)` to re-attach to running runs.
    """
    from src.continuous_sync import build_streaming_plan

    # Plan generation is shared between preview and execution paths.
    plan = build_streaming_plan(
        source_catalog=source_catalog,
        destination_catalog=destination_catalog,
        tables=tables,
        schema=schema,
        trigger_ms=trigger_ms,
        checkpoint_root=checkpoint_root,
    )
    spec = plan["job_spec"]

    stream_id = _make_stream_id(source_catalog, destination_catalog, schema, tables)
    record = StreamRecord(
        stream_id=stream_id,
        source_catalog=source_catalog,
        destination_catalog=destination_catalog,
        schema=schema,
        tables=tables or [],
        trigger_ms=trigger_ms,
    )

    run_name = f"{_RUN_NAME_PREFIX}-{stream_id}"
    try:
        # Submit issues a one-off run on Databricks. The actual streaming
        # runtime is the Python file embedded in plan["job_spec"]["inline_python"]
        # — caller must already have ensured the source tables have CDF
        # enabled and target tables have a primary key (the plan's
        # `prerequisites` block enumerates these).
        submit_response = client.jobs.submit(
            run_name=run_name,
            tasks=spec.get("tasks", []),
        )
        run_id = getattr(submit_response, "run_id", None)
        if run_id is None and isinstance(submit_response, dict):
            run_id = submit_response.get("run_id")
        record.run_id = int(run_id) if run_id is not None else None
        record.last_status = "starting"
        logger.info(
            "Continuous sync started: stream_id=%s run_id=%s source=%s dest=%s",
            stream_id, record.run_id, source_catalog, destination_catalog,
        )
    except Exception as e:
        record.last_status = "failed"
        record.last_error = str(e)
        logger.exception(f"Continuous sync submit failed for {stream_id}: {e}")

    with _REGISTRY_LOCK:
        _REGISTRY[stream_id] = record
    return record


def stop_stream(client: "WorkspaceClient", stream_id: str) -> StreamRecord:
    """Cancel the streaming run and mark the record stopped.

    Idempotent — calling stop on an already-stopped stream is a no-op (the
    final state is reported either way). Returns the updated record. Raises
    KeyError if the stream_id isn't known.
    """
    with _REGISTRY_LOCK:
        record = _REGISTRY.get(stream_id)
        if record is None:
            raise KeyError(f"unknown stream_id: {stream_id}")

    if record.run_id is None:
        # Submit failed earlier — nothing to cancel. Mark stopped for the UI.
        record.last_status = "stopped"
        return record

    try:
        client.jobs.cancel_run(run_id=record.run_id)
        record.last_status = "stopped"
        record.last_error = None
        logger.info(f"Continuous sync stopped: stream_id={stream_id} run_id={record.run_id}")
    except Exception as e:
        # Cancel failures are typically benign — the run may have already
        # ended. Mark the record's last error so the UI shows the SDK message.
        record.last_status = "stopped"
        record.last_error = str(e)
        logger.warning(f"cancel_run for {stream_id} returned: {e}")
    return record


def restart_stream(client: "WorkspaceClient", stream_id: str) -> StreamRecord:
    """Stop the current run and submit a fresh one with the same parameters.

    Used when the streaming task crashes or when source schema drift
    requires a clean restart. The new run gets a new `run_id`; the same
    `stream_id` is reused so UI users don't lose track.
    """
    with _REGISTRY_LOCK:
        existing = _REGISTRY.get(stream_id)
        if existing is None:
            raise KeyError(f"unknown stream_id: {stream_id}")

    # Best-effort stop the old run. If it already ended, cancel_run no-ops.
    try:
        stop_stream(client, stream_id)
    except Exception as e:
        logger.warning(f"restart_stream: stop failed for {stream_id}: {e}")

    return start_stream(
        client,
        source_catalog=existing.source_catalog,
        destination_catalog=existing.destination_catalog,
        tables=existing.tables or None,
        schema=existing.schema,
        trigger_ms=existing.trigger_ms,
    )


# ---------------------------------------------------------------------------
# Status / health
# ---------------------------------------------------------------------------


_LIFE_CYCLE_TO_STATUS = {
    "PENDING":     "starting",
    "RUNNING":     "running",
    "TERMINATING": "stopping",
    "TERMINATED":  None,  # depends on result_state
    "SKIPPED":     "stopped",
    "INTERNAL_ERROR": "failed",
    "BLOCKED":     "running",
    "WAITING_FOR_RETRY": "running",
}

_RESULT_STATE_TO_STATUS = {
    "SUCCESS":  "stopped",  # streaming usually shouldn't reach SUCCESS — it ran to completion when streams are infinite
    "FAILED":   "failed",
    "TIMEDOUT": "failed",
    "CANCELED": "stopped",
}


def _classify_run_state(life_cycle_state: str | None, result_state: str | None) -> str:
    """Map Databricks run state pair → user-facing health string.

    See the module docstring for the user-facing categories. Returns
    'unknown' for any state combination not in the table — defensive
    rather than raising, since Databricks may add new states over time.
    """
    if not life_cycle_state:
        return "unknown"

    lcs = life_cycle_state.upper()
    if lcs == "TERMINATED":
        rs = (result_state or "").upper()
        return _RESULT_STATE_TO_STATUS.get(rs, "stopped")

    return _LIFE_CYCLE_TO_STATUS.get(lcs, "unknown")


def refresh_stream_status(client: "WorkspaceClient", stream_id: str) -> StreamRecord:
    """Poll Databricks for the latest state of a stream's run and update
    the record's `last_status`. Cheap to call (one SDK call per stream).

    Caller-controlled rate: the registry's `last_polled_at` field lets
    callers throttle (e.g. "skip refresh if polled within last 10 seconds").
    """
    with _REGISTRY_LOCK:
        record = _REGISTRY.get(stream_id)
        if record is None:
            raise KeyError(f"unknown stream_id: {stream_id}")

    if record.run_id is None:
        # Submit failed; nothing to poll. The status is already 'failed'.
        return record

    try:
        run = client.jobs.get_run(run_id=record.run_id)
    except Exception as e:
        record.last_error = str(e)
        record.last_polled_at = time.time()
        return record

    # The SDK returns a Run object; extract the nested state. Defensive
    # because attribute paths have shifted across SDK versions.
    state = getattr(run, "state", None)
    lcs = getattr(state, "life_cycle_state", None) if state else None
    rs = getattr(state, "result_state", None) if state else None
    if hasattr(lcs, "value"):
        lcs = lcs.value
    if hasattr(rs, "value"):
        rs = rs.value

    record.last_status = _classify_run_state(lcs, rs)
    record.last_polled_at = time.time()
    if record.last_status == "failed":
        record.last_error = (
            getattr(state, "state_message", None) if state else None
        ) or record.last_error
    return record


def list_streams(client: "WorkspaceClient | None" = None, refresh: bool = False) -> list[dict]:
    """Return all known stream records, optionally polling each for fresh
    status. The `refresh=False` default avoids hammering Databricks on every
    UI list-page hit; the dedicated detail endpoint can call with refresh=True.
    """
    out = []
    with _REGISTRY_LOCK:
        records = list(_REGISTRY.values())

    for r in records:
        if refresh and client is not None:
            try:
                refresh_stream_status(client, r.stream_id)
            except Exception as e:
                logger.debug(f"refresh failed for {r.stream_id}: {e}")
        out.append(_record_to_dict(r))
    return out


def get_stream(stream_id: str) -> StreamRecord:
    """Lookup a stream by id. Raises KeyError when unknown."""
    with _REGISTRY_LOCK:
        record = _REGISTRY.get(stream_id)
    if record is None:
        raise KeyError(f"unknown stream_id: {stream_id}")
    return record


def discover_existing_streams(client: "WorkspaceClient") -> int:
    """Re-populate the registry from Databricks runs whose `run_name` starts
    with the runner's prefix. Run on API server startup so a restart doesn't
    lose track of streams the user submitted before the restart.

    Returns the count of streams discovered.
    """
    discovered = 0
    try:
        runs = client.jobs.list_runs(active_only=True)
    except Exception as e:
        logger.warning(f"discover_existing_streams: list_runs failed: {e}")
        return 0

    for run in runs:
        name = getattr(run, "run_name", None) or ""
        if not name.startswith(_RUN_NAME_PREFIX):
            continue
        run_id = getattr(run, "run_id", None)
        if run_id is None:
            continue
        # Best-effort stream_id extraction from run_name. Run name format is
        # `<prefix>-<stream_id>`; we keep stream_id stable across restarts so
        # the UI's URL paths don't break.
        stream_id = name[len(_RUN_NAME_PREFIX) + 1:] or f"discovered-{run_id}"
        with _REGISTRY_LOCK:
            if stream_id in _REGISTRY:
                continue
            _REGISTRY[stream_id] = StreamRecord(
                stream_id=stream_id,
                source_catalog="(rediscovered)",
                destination_catalog="(rediscovered)",
                schema=None,
                tables=[],
                trigger_ms=0,
                run_id=int(run_id),
                last_status="running",
            )
        discovered += 1
    if discovered:
        logger.info(f"Continuous sync: rediscovered {discovered} running stream(s) from Databricks")
    return discovered


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------


def _make_stream_id(
    source_catalog: str,
    destination_catalog: str,
    schema: str | None,
    tables: list[str] | None,
) -> str:
    """Stable stream_id derived from the (source, dest, scope) tuple. So
    starting "the same" sync twice without an intervening stop gets the
    same stream_id — UI users see one record, not two."""
    import hashlib
    payload = "|".join([
        source_catalog,
        destination_catalog,
        schema or "",
        ",".join(sorted(tables or [])),
    ])
    suffix = hashlib.sha1(payload.encode(), usedforsecurity=False).hexdigest()[:10]
    return f"sync-{suffix}"


def _record_to_dict(r: StreamRecord) -> dict:
    return {
        "stream_id": r.stream_id,
        "source_catalog": r.source_catalog,
        "destination_catalog": r.destination_catalog,
        "schema": r.schema,
        "tables": list(r.tables),
        "trigger_ms": r.trigger_ms,
        "run_id": r.run_id,
        "submitted_at": r.submitted_at,
        "status": r.last_status,
        "error": r.last_error,
        "last_polled_at": r.last_polled_at,
    }


def _reset_registry_for_tests() -> None:
    """Clear the in-process registry. Test-only; not part of the public API."""
    with _REGISTRY_LOCK:
        _REGISTRY.clear()
