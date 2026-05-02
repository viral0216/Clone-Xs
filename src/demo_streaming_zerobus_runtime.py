"""Runtime seam for the Zerobus destination.

Phase 2 of the Zerobus integration. The destination dispatch in
``src/demo_streaming.py`` calls into this module when the user picks
``destination="zerobus"``.

The Zerobus Python SDK is GA — package
`databricks-zerobus-ingest-sdk` (https://github.com/databricks/zerobus-sdk).
This module probes for it at import time and flips
``ZEROBUS_AVAILABLE`` to True when:

  1. The package is installed (``pip install databricks-zerobus-ingest-sdk``).
  2. Both required symbols import: ``zerobus.sdk.sync.ZerobusSdk``
     and ``zerobus.sdk.shared.{RecordType, StreamConfigurationOptions, TableProperties}``.

Per-request, an actual emission also needs three caller-supplied
secrets that the form collects when ``destination="zerobus"`` is
picked:

  - ``zerobus_server_endpoint`` — region-specific gRPC URL, format
    ``https://<workspace_id>.zerobus.<region>.cloud.databricks.com``
  - ``zerobus_client_id``       — service-principal app ID
  - ``zerobus_client_secret``   — service-principal secret

Stream lifecycle is **open once, ingest many, close at end** — opening
a fresh stream per batch defeats the point of Zerobus. The dispatch in
``demo_streaming.py`` calls :func:`open_zerobus_stream` before the
emission loop starts, hands the resulting handle to
:func:`ingest_batch_zerobus` for each tick, and calls
:func:`close_zerobus_stream` in a ``finally`` so streams don't leak
when the loop is interrupted.
"""

from __future__ import annotations

import logging
from typing import Any

from databricks.sdk import WorkspaceClient

from src.client import execute_sql
from src.demo_streaming import DEVICE_PROFILES

logger = logging.getLogger(__name__)


# Probe for the real SDK at module import. The package has prebuilt
# wheels for Linux/macOS/Windows (PyO3 bindings to a Rust core), so a
# successful import on a supported platform is the right signal.
ZEROBUS_AVAILABLE: bool = False
_UNAVAILABLE_REASON: str | None = (
    "The `databricks-zerobus-ingest-sdk` Python SDK is not installed in "
    "this environment. Install it (`pip install databricks-zerobus-ingest-sdk`) "
    "and restart the API server to enable the runtime destination. In the "
    "meantime, use the 'Try with Zerobus' code snippet below to run Zerobus "
    "from your own environment."
)
try:
    # Real SDK API — both modules must import cleanly.
    from zerobus.sdk.shared import (
        RecordType,
        StreamConfigurationOptions,
        TableProperties,
    )
    from zerobus.sdk.sync import ZerobusSdk

    ZEROBUS_AVAILABLE = True
    _UNAVAILABLE_REASON = None
except Exception:
    # SDK not installed (or a partial install). Phase 1's snippet
    # panel still gives the user a working code path.
    ZEROBUS_AVAILABLE = False
    # Late binding to satisfy type checkers when SDK isn't installed.
    RecordType = None  # type: ignore[assignment]
    StreamConfigurationOptions = None  # type: ignore[assignment]
    TableProperties = None  # type: ignore[assignment]
    ZerobusSdk = None  # type: ignore[assignment]


def is_available() -> tuple[bool, str | None]:
    """Return (available, reason_if_not).

    The two-tuple shape lets the API expose a human-readable reason
    string in ``GET /demo-data/zerobus/availability`` so the UI can
    explain *why* the destination radio is disabled rather than just
    greying it out silently.
    """
    return ZEROBUS_AVAILABLE, _UNAVAILABLE_REASON


def ensure_zerobus_table(
    client: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
    schema: str,
    profile: str,
    table_name: str,
) -> str:
    """Create catalog + schema + Delta table if missing for Zerobus emission.

    Schema mirrors :func:`src.demo_streaming._ensure_direct_bronze_table` —
    Zerobus appends to a regular Delta table, so the DDL is identical.
    Kept as a separate function so a future Zerobus-specific schema
    change (e.g. required ``__zb_seq`` column) only touches one file.
    """
    if profile not in DEVICE_PROFILES:
        raise ValueError(f"Unknown profile: {profile!r}")
    cols = DEVICE_PROFILES[profile]["columns"]
    col_ddl = ", ".join(f"`{name}` {sql_type}" for name, sql_type in cols)
    fqn = f"`{catalog}`.`{schema}`.`{table_name}`"
    execute_sql(client, warehouse_id, f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
    execute_sql(client, warehouse_id, f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
    comment = DEVICE_PROFILES[profile]["comment"]
    execute_sql(
        client,
        warehouse_id,
        f"CREATE TABLE IF NOT EXISTS {fqn} ({col_ddl}) "
        f"USING DELTA COMMENT 'Streaming demo events (Zerobus) — {comment}'",
    )
    return f"{catalog}.{schema}.{table_name}"


def open_zerobus_stream(
    workspace_url: str,
    server_endpoint: str,
    client_id: str,
    client_secret: str,
    table_fqn: str,
):
    """Open a long-lived Zerobus stream against ``table_fqn``.

    Returns the SDK's stream object (a `zerobus.sdk.sync.Stream`) which
    supports ``ingest_record_offset(record)`` per tick and ``close()``
    at the end. The caller is responsible for ``close()``-ing in a
    ``finally`` so the stream doesn't leak.

    Raises ``NotImplementedError`` if the SDK isn't installed — the
    caller (dispatch) checks ``is_available()`` upfront, but defending
    here too keeps the runtime contract explicit.
    """
    if not ZEROBUS_AVAILABLE:
        raise NotImplementedError(_UNAVAILABLE_REASON or "Zerobus runtime not available")

    sdk = ZerobusSdk(server_endpoint, workspace_url)
    stream = sdk.create_stream(
        client_id,
        client_secret,
        TableProperties(table_fqn),
        StreamConfigurationOptions(record_type=RecordType.JSON),
    )
    logger.info(f"Zerobus stream opened against {table_fqn}")
    return stream


def ingest_batch_zerobus(stream: Any, batch: list[dict[str, Any]]) -> int:
    """Ingest one batch of records into an open Zerobus stream.

    Calls ``stream.ingest_record_offset(record)`` for each record.
    Returns the count of records ingested. Per-record exceptions
    propagate — the caller decides whether to retry the batch.
    """
    if stream is None:
        raise RuntimeError("Zerobus stream is None — open_zerobus_stream() must be called first.")
    for record in batch:
        stream.ingest_record_offset(record)
    return len(batch)


def close_zerobus_stream(stream: Any) -> None:
    """Best-effort close. Swallows exceptions so the dispatch's
    ``finally`` block never raises secondarily.
    """
    if stream is None:
        return
    try:
        stream.close()
        logger.info("Zerobus stream closed")
    except Exception as e:
        logger.warning(f"Failed to close Zerobus stream cleanly: {e}")
