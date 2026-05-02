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

Even when the SDK is installed, an actual emission also needs three
caller-supplied secrets that the demo-data form does NOT yet collect:

  - ``ZEROBUS_SERVER_ENDPOINT`` (region-specific gRPC endpoint)
  - ``DATABRICKS_CLIENT_ID``    (service-principal app ID)
  - ``DATABRICKS_CLIENT_SECRET``

Until the form learns to ask for those, the runtime call still raises
``NotImplementedError`` pointing the user at the Phase 1 snippet (which
documents exactly which env vars to set). The credential-plumbing PR
is the natural next step.
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
    from zerobus.sdk.shared import (  # noqa: F401
        RecordType,
        StreamConfigurationOptions,
        TableProperties,
    )
    from zerobus.sdk.sync import ZerobusSdk  # noqa: F401

    ZEROBUS_AVAILABLE = True
    _UNAVAILABLE_REASON = None
except Exception:
    # SDK not installed (or a partial install). Phase 1's snippet
    # panel still gives the user a working code path.
    ZEROBUS_AVAILABLE = False


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


def insert_batch_zerobus(
    client: WorkspaceClient,
    table_fqn: str,
    profile: str,
    batch: list[dict[str, Any]],
) -> int:
    """Append ``batch`` to ``table_fqn`` via Zerobus. Returns rows appended.

    Mirrors :func:`src.demo_streaming.insert_batch_direct` — same
    signature shape so the dispatch code in ``demo_streaming.py`` can
    call either path interchangeably.

    Today this raises ``NotImplementedError`` because the SDK is not
    yet released. When the SDK lands, replace the body with the real
    Zerobus call (see :func:`_real_zerobus_append` below).
    """
    if not ZEROBUS_AVAILABLE:
        raise NotImplementedError(_UNAVAILABLE_REASON or "Zerobus runtime not available")
    return _real_zerobus_append(client, table_fqn, profile, batch)


def _real_zerobus_append(
    client: WorkspaceClient,
    table_fqn: str,
    profile: str,
    batch: list[dict[str, Any]],
) -> int:
    """Real Zerobus append — pending credential-plumbing PR.

    The SDK requires three caller-supplied secrets that the demo-data
    form does NOT yet collect: a region-specific gRPC server endpoint,
    a service-principal client_id, and the corresponding client_secret.
    See https://github.com/databricks/zerobus-sdk/blob/main/python/README.md.

    Once the form learns to ask for those (next Phase 2 PR), the body
    becomes::

        from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties
        from zerobus.sdk.sync import ZerobusSdk

        sdk = ZerobusSdk(server_endpoint, workspace_url)
        stream = sdk.create_stream(
            client_id, client_secret,
            TableProperties(table_fqn),
            StreamConfigurationOptions(record_type=RecordType.JSON),
        )
        try:
            for record in batch:
                stream.ingest_record_offset(record)
        finally:
            stream.close()
        return len(batch)

    For now we raise so the dispatch fails fast with a helpful message
    rather than silently dropping events.
    """
    raise NotImplementedError(
        "Zerobus SDK is installed, but the demo-data form does not yet "
        "collect the required credentials (server_endpoint, client_id, "
        "client_secret). Use the 'Try with Zerobus' snippet on the demo-"
        "data page in the meantime — it includes the env vars to set."
    )
