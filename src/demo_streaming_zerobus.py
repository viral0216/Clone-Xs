"""Render a copy-pastable Python snippet that emits demo events via Zerobus.

Phase 1 of the Zerobus integration: produce a self-contained Python
script that the user can paste into their own environment to push
events directly to a Delta table using the Databricks Zerobus
ingestion SDK. No runtime dependency on the SDK — the snippet is just
text.

Targets the GA Python SDK ``databricks-zerobus-ingest-sdk`` (see
https://github.com/databricks/zerobus-sdk/tree/main/python). API used
in the snippet:

    from zerobus.sdk.sync import ZerobusSdk
    from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties

    sdk = ZerobusSdk(server_endpoint, workspace_url)
    stream = sdk.create_stream(client_id, client_secret,
                               TableProperties(table_fqn),
                               StreamConfigurationOptions(record_type=RecordType.JSON))
    stream.ingest_record_offset(record)

Single source of truth for the per-profile event generator is
``src.demo_streaming_schedule._PROFILE_GENERATORS_SOURCE`` — same
strings already inlined into the scheduled-Job notebook, so behaviour
is identical across every emission path.
"""

from __future__ import annotations

from src.demo_streaming import DEVICE_PROFILES
from src.demo_streaming_schedule import _PROFILE_GENERATORS_SOURCE


def render_zerobus_snippet(
    profile: str,
    catalog: str,
    schema: str,
    table: str | None = None,
    events_per_batch: int = 100,
    interval_seconds: float = 5.0,
    num_devices: int = 10,
) -> str:
    """Return a Python script that streams `profile` events to Delta via Zerobus.

    Args:
        profile: Device profile name from ``DEVICE_PROFILES``.
        catalog, schema: Target Unity Catalog catalog + schema.
        table: Target table name. Defaults to ``bronze_<profile>`` to match
            the convention used by the in-process Bronze auto-create path
            ([src/demo_streaming.py:create_bronze_streaming_table]).
        events_per_batch, interval_seconds, num_devices: Match the
            corresponding fields on the streaming wizard so the snippet
            mirrors what the user just configured.

    Raises:
        ValueError: if ``profile`` is not a known device profile.
    """
    if profile not in DEVICE_PROFILES:
        raise ValueError(f"Unknown profile: {profile!r}. Known: {sorted(DEVICE_PROFILES)}")
    if profile not in _PROFILE_GENERATORS_SOURCE:
        # Defensive: should never happen because the registry and the
        # source map are kept in sync by the test suite, but raise a
        # crisp error if drift slips through.
        raise ValueError(
            f"Generator source missing for profile {profile!r} — "
            f"src.demo_streaming_schedule._PROFILE_GENERATORS_SOURCE is out of sync."
        )

    table_name = (table or f"bronze_{profile}").strip()
    generator_src = _PROFILE_GENERATORS_SOURCE[profile].strip()
    fqn_display = f"{catalog}.{schema}.{table_name}"

    return f'''# pip install databricks-zerobus-ingest-sdk
#
# Sends {profile!r} events directly to {fqn_display}
# using Databricks Zerobus — the low-latency Delta ingest API.
# No SQL warehouse, no Volume, no Auto Loader.
#
# Prerequisites (see https://github.com/databricks/zerobus-sdk):
#   1. The destination Delta table must already exist with a matching
#      schema (this snippet's generator emits the exact columns from
#      DEVICE_PROFILES["{profile}"]).
#   2. A Service Principal with USE_CATALOG, USE_SCHEMA, MODIFY+SELECT
#      grants on the table.
#   3. The Zerobus server endpoint for your workspace, format:
#         https://<workspace_id>.zerobus.<region>.cloud.databricks.com

import os
import random
import time
from datetime import datetime, timezone

from zerobus.sdk.sync import ZerobusSdk
from zerobus.sdk.shared import (
    RecordType,
    StreamConfigurationOptions,
    TableProperties,
)

# --- Config (matches your demo-data wizard selection) ----------
WORKSPACE_URL    = os.environ["DATABRICKS_HOST"]
SERVER_ENDPOINT  = os.environ["ZEROBUS_SERVER_ENDPOINT"]  # https://<wsid>.zerobus.<region>.cloud.databricks.com
CLIENT_ID        = os.environ["DATABRICKS_CLIENT_ID"]     # service principal app ID
CLIENT_SECRET    = os.environ["DATABRICKS_CLIENT_SECRET"] # service principal secret
TABLE            = "{fqn_display}"
EVENTS_PER_BATCH = {int(events_per_batch)}
INTERVAL_SECONDS = {float(interval_seconds)}
NUM_DEVICES      = {int(num_devices)}

# --- Per-profile generator (verbatim from DEVICE_PROFILES["{profile}"]) ---
{generator_src}

# --- Open one stream against the table ---
sdk = ZerobusSdk(SERVER_ENDPOINT, WORKSPACE_URL)
stream = sdk.create_stream(
    CLIENT_ID,
    CLIENT_SECRET,
    TableProperties(TABLE),
    StreamConfigurationOptions(record_type=RecordType.JSON),
)

# --- Emission loop ---
# Delivery is at-least-once: ingest_record_offset returns immediately
# with the offset Zerobus assigned, but the record is only durable
# once wait_for_offset() returns. We call wait_for_offset on the
# LAST offset in each batch — durability is monotonic, so confirming
# the last offset implicitly confirms every prior offset in the batch.
state = init_state(NUM_DEVICES)
seq = 0
print(f"Streaming {profile} events to {{TABLE}} via Zerobus...")
try:
    while True:
        now = datetime.now(timezone.utc)
        last_offset = None
        for i in range(EVENTS_PER_BATCH):
            record = generate_event(state, seq + i, now)
            # ingest_record_offset returns the durable offset assigned
            # by Zerobus. Capture the LAST one in the batch so we can
            # block on its durability acknowledgement below.
            last_offset = stream.ingest_record_offset(record)
        # Block until Zerobus acknowledges the last record is durable.
        # Production code that prefers throughput over per-batch
        # confirmation can drop this and use AckCallback instead.
        if last_offset is not None:
            stream.wait_for_offset(last_offset)
        seq += EVENTS_PER_BATCH
        print(f"Ingested {{EVENTS_PER_BATCH}} records (total: {{seq}}, durable up to offset {{last_offset}})")
        time.sleep(INTERVAL_SECONDS)
except KeyboardInterrupt:
    print(f"Stopped. {{seq}} records ingested in total.")
finally:
    stream.close()
'''
