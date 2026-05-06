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
    from zerobus.sdk.shared.headers_provider import HeadersProvider
    from zerobus.sdk.sync import ZerobusSdk

    ZEROBUS_AVAILABLE = True
    _UNAVAILABLE_REASON = None

    class _PatHeadersProvider(HeadersProvider):
        """Bearer-token headers provider for PAT auth.

        The Zerobus SDK calls ``get_headers()`` on every gRPC request,
        so re-fetching the token here would let a refreshed PAT
        propagate without restarting the stream. Today we just store
        the token at construction — adequate for the demo's
        bounded-duration runs (≤ 1 h). For long-running production
        streams a callable token-source would be the right shape.
        """

        def __init__(self, pat: str):
            super().__init__()
            self._pat = pat

        def get_headers(self):
            return [("authorization", f"Bearer {self._pat}")]
except Exception:
    # SDK not installed (or a partial install). Phase 1's snippet
    # panel still gives the user a working code path.
    ZEROBUS_AVAILABLE = False
    # Late binding to satisfy type checkers when SDK isn't installed.
    RecordType = None  # type: ignore[assignment]
    StreamConfigurationOptions = None  # type: ignore[assignment]
    TableProperties = None  # type: ignore[assignment]
    ZerobusSdk = None  # type: ignore[assignment]
    HeadersProvider = None  # type: ignore[assignment]
    _PatHeadersProvider = None  # type: ignore[assignment]


def is_available() -> tuple[bool, str | None]:
    """Return (available, reason_if_not).

    The two-tuple shape lets the API expose a human-readable reason
    string in ``GET /demo-data/zerobus/availability`` so the UI can
    explain *why* the destination radio is disabled rather than just
    greying it out silently.
    """
    return ZEROBUS_AVAILABLE, _UNAVAILABLE_REASON


def _catalog_exists(client: WorkspaceClient, warehouse_id: str, catalog: str) -> bool:
    """Best-effort check via SHOW CATALOGS. Returns False on any
    error so the caller falls back to attempting CREATE — preserves
    the original behaviour for workspaces where SHOW is restricted.

    The result column name varies across runtime versions (``catalog``
    vs ``catalog_name``); we accept both.
    """
    try:
        rows = execute_sql(client, warehouse_id, "SHOW CATALOGS")
    except Exception as e:
        logger.warning(f"SHOW CATALOGS failed; falling back to CREATE: {e}")
        return False
    target = catalog.lower()
    for r in rows or []:
        name = (r.get("catalog") or r.get("catalog_name") or "").lower()
        if name == target:
            return True
    return False


def _schema_exists(client: WorkspaceClient, warehouse_id: str, catalog: str, schema: str) -> bool:
    """Best-effort check via SHOW SCHEMAS. Returns False on any error
    (same fallback rationale as ``_catalog_exists``).
    """
    try:
        # LIKE filter scopes the result to the target schema name —
        # cheaper than scanning every schema in the catalog.
        rows = execute_sql(client, warehouse_id, f"SHOW SCHEMAS IN `{catalog}` LIKE '{schema}'")
    except Exception as e:
        logger.warning(f"SHOW SCHEMAS IN `{catalog}` failed; falling back to CREATE: {e}")
        return False
    return bool(rows)


def ensure_zerobus_table(
    client: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
    schema: str,
    profile: str,
    table_name: str,
    service_principal_id: str | None = None,
    catalog_storage_location: str = "",
) -> str:
    """Create catalog + schema + managed Delta table for Zerobus.

    Per the Zerobus connector docs, only **managed** Delta tables
    in non-default storage are supported:

        > "The connector supports writing only to managed Delta tables."
        > "Writing to default storage is not supported."
        — https://docs.databricks.com/aws/en/ingestion/zerobus-limits

    To satisfy both, the schema (or catalog) must have its own
    managed storage location configured. The user does that **once
    per schema** as a workspace admin, before any Zerobus run:

        ALTER SCHEMA `<cat>`.`<sch>`
          SET MANAGED LOCATION 's3://<bucket>/<path>';

    Once that's in place, every CREATE TABLE in that schema lands in
    the configured location and Zerobus accepts it. If the schema
    has no managed location, the CREATE TABLE here succeeds (table
    lands in metastore default storage) but the Zerobus
    ``create_stream`` call later fails with
    ``Error Code: 4024  Unsupported table kind``.

    Returns the fully-qualified table name (no backticks).
    """
    if profile not in DEVICE_PROFILES:
        raise ValueError(f"Unknown profile: {profile!r}")
    cols = DEVICE_PROFILES[profile]["columns"]
    col_ddl = ", ".join(f"`{name}` {sql_type}" for name, sql_type in cols)
    fqn = f"`{catalog}`.`{schema}`.`{table_name}`"
    # Existence-check before CREATE. On workspaces whose metastore has
    # no default storage root, CREATE CATALOG IF NOT EXISTS validates
    # the storage prerequisite *before* the IF-NOT-EXISTS short-circuit
    # — so it fails with INVALID_STATE even when the catalog already
    # exists. Doing SHOW CATALOGS first lets us skip the CREATE entirely
    # in the common idempotent case.
    if not _catalog_exists(client, warehouse_id, catalog):
        # Optional MANAGED LOCATION when we actually need to create.
        # Single quotes inside the location are doubled to defend
        # against SQL injection from form input.
        cat_loc_clause = ""
        if (catalog_storage_location or "").strip():
            loc_escaped = catalog_storage_location.strip().replace("'", "''")
            cat_loc_clause = f" MANAGED LOCATION '{loc_escaped}'"
        execute_sql(
            client, warehouse_id, f"CREATE CATALOG IF NOT EXISTS `{catalog}`{cat_loc_clause}"
        )
    else:
        logger.info(f"Catalog `{catalog}` already exists — skipping CREATE")

    # Same logic for the schema. Schema inherits the catalog's MANAGED
    # LOCATION (UC default). No separate schema-location knob — if a
    # caller needs one, they can ALTER it post-create.
    if not _schema_exists(client, warehouse_id, catalog, schema):
        execute_sql(client, warehouse_id, f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
    else:
        logger.info(f"Schema `{catalog}`.`{schema}` already exists — skipping CREATE")
    comment = DEVICE_PROFILES[profile]["comment"]
    execute_sql(
        client,
        warehouse_id,
        f"CREATE TABLE IF NOT EXISTS {fqn} ({col_ddl}) "
        f"USING DELTA "
        f"COMMENT 'Streaming demo events (Zerobus) — {comment}'",
    )

    # Auto-grant the service principal everything Zerobus needs.
    # The streaming-emit caller has already authenticated as a workspace
    # admin (or someone with manage privileges); the SP that's about to
    # ingest via Zerobus has no privileges yet by default. Without these
    # grants Databricks rejects the create-stream call with
    # `invalid_authorization_details`.
    #
    # Each GRANT runs in its own try/except so a partial-permission
    # caller (e.g. table owner but not catalog admin) gets as far as
    # they can — the runner will surface the same auth error from
    # Zerobus if a critical grant didn't take, but at least the
    # successful grants stick.
    if service_principal_id and (sp := service_principal_id.strip()):
        _grant_zerobus_perms(
            client,
            warehouse_id,
            sp=sp,
            catalog=catalog,
            schema=schema,
            fqn=fqn,
        )

    return f"{catalog}.{schema}.{table_name}"


def _grant_zerobus_perms(
    client: WorkspaceClient,
    warehouse_id: str,
    *,
    sp: str,
    catalog: str,
    schema: str,
    fqn: str,
) -> None:
    """Grant the SP every privilege Zerobus needs to ingest, plus
    CREATE TABLE on the schema so future runs against *new* tables in
    the same schema don't need re-granting.

    Per the official docs (https://docs.databricks.com/aws/en/ingestion/zerobus-overview)
    the SP needs three grants for ingestion against a single table:
      - USE CATALOG on the catalog
      - USE SCHEMA on the schema
      - MODIFY, SELECT on the table

    The doc explicitly notes: "You must grant MODIFY and SELECT
    privileges on the table, even for tables with ALL PRIVILEGES granted."

    Beyond the doc's minimum, we add ``CREATE TABLE ON SCHEMA`` so the
    SP can create *additional* tables in the schema for follow-up
    Zerobus runs without re-granting. Stops short of ``ALL PRIVILEGES``
    on the schema (which would also let the SP drop the schema —
    broader than needed and harder to audit).

    We swallow per-grant exceptions so a caller without manage perms
    on (say) the catalog still gets the schema / table grants applied.
    Failures are logged so they're discoverable in the job log.
    """
    grants = [
        ("catalog", f"GRANT USE CATALOG ON CATALOG `{catalog}` TO `{sp}`"),
        ("schema-use", f"GRANT USE SCHEMA ON SCHEMA `{catalog}`.`{schema}` TO `{sp}`"),
        # Lets the SP create additional tables in this schema for
        # future Zerobus runs without re-granting per-table. Narrower
        # than ALL PRIVILEGES — no DROP / ALTER on the schema itself.
        ("schema-create-table", f"GRANT CREATE TABLE ON SCHEMA `{catalog}`.`{schema}` TO `{sp}`"),
        ("table", f"GRANT MODIFY, SELECT ON TABLE {fqn} TO `{sp}`"),
    ]
    for level, sql in grants:
        try:
            execute_sql(client, warehouse_id, sql)
            logger.info(f"Granted Zerobus {level} perms to SP {sp}")
        except Exception as e:
            logger.warning(
                f"Could not grant {level} perms to SP {sp} "
                f"(continuing — Zerobus may still reject the run): {e}"
            )


def open_zerobus_stream(
    workspace_url: str,
    server_endpoint: str,
    client_id: str,
    client_secret: str,
    table_fqn: str,
    pat: str | None = None,
):
    """Open a long-lived Zerobus stream against ``table_fqn``.

    Two auth paths, picked by which arguments the caller fills:

    - **PAT** — when ``pat`` is non-empty, the SDK is given a custom
      ``HeadersProvider`` that emits ``Authorization: Bearer <pat>``
      on every gRPC request. ``client_id`` / ``client_secret`` are
      ignored by the SDK in this mode (see
      ``zerobus/sdk/sync/zerobus_sdk.py:create_stream`` line 282 —
      ``headers_provider`` overrides OAuth when set). Caveat: the
      Zerobus *server* may still reject PATs that lack the right
      scopes; the workspace tier matters more than the token type.
    - **OAuth (default)** — original path. SDK runs the
      client-credentials exchange itself using the SP creds.

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
    if pat:
        # PAT path — bearer-token headers provider overrides OAuth.
        # client_id/client_secret are positional-required by the SDK
        # signature but ignored when headers_provider is set.
        stream = sdk.create_stream(
            "",
            "",
            TableProperties(table_fqn),
            StreamConfigurationOptions(record_type=RecordType.JSON),
            headers_provider=_PatHeadersProvider(pat),
        )
        logger.info(f"Zerobus stream opened against {table_fqn} (PAT auth)")
    else:
        stream = sdk.create_stream(
            client_id,
            client_secret,
            TableProperties(table_fqn),
            StreamConfigurationOptions(record_type=RecordType.JSON),
        )
        logger.info(f"Zerobus stream opened against {table_fqn} (OAuth/SP auth)")
    return stream


def encode_record_for_zerobus(
    record: dict[str, Any], columns: list[tuple[str, str]]
) -> dict[str, Any]:
    """Transform a record dict to match Zerobus's JSON type conventions.

    Per the Delta → Zerobus type mappings table in the upstream README
    (https://github.com/databricks/zerobus-sdk/blob/main/README.md):

        TIMESTAMP, TIMESTAMP_NTZ → int64 (microseconds since epoch)
        DATE                    → int32 (days since 1970-01-01)

    Sending the column as an ISO 8601 string instead surfaces server-side
    as ``ZerobusException: Invalid argument: Record decoder/encoder
    error: invalid digit found in string`` — the JSON parser reads the
    ``T`` in ``2025-11-05T...`` while trying to decode an int64 and bails.

    The shared ``DEVICE_PROFILES`` generators emit ISO strings because
    that shape works for the volume_bronze and direct_table paths
    (Auto Loader / INSERT VALUES handle the parsing). The Zerobus
    runner converts at the SDK boundary so the generators stay shared.

    All other types pass through unchanged.
    """
    from datetime import date, datetime, timezone

    type_by_field = {name: dtype.upper() for name, dtype in columns}
    out: dict[str, Any] = {}
    for k, v in record.items():
        t = type_by_field.get(k, "")
        if v is None:
            out[k] = None
            continue
        if t in {"TIMESTAMP", "TIMESTAMP_NTZ"} and isinstance(v, str):
            dt = datetime.fromisoformat(v)
            # Naive datetimes mean "local time" by Python convention,
            # but our generators all emit timezone-aware UTC. Defend
            # by assuming UTC for any naive input rather than letting
            # `.timestamp()` apply the runner's local TZ silently.
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            out[k] = int(dt.timestamp() * 1_000_000)
        elif t == "DATE" and isinstance(v, str):
            d = date.fromisoformat(v)
            out[k] = (d - date(1970, 1, 1)).days
        else:
            out[k] = v
    return out


def ingest_batch_zerobus(stream: Any, batch: list[dict[str, Any]]) -> int:
    """Ingest one batch of records into an open Zerobus stream and
    block until the server acknowledges the last offset.

    The SDK's ``ingest_record_offset`` payload type depends on the
    stream's ``RecordType``:

    - ``RecordType.JSON`` (what we use): pass a **Python dict**
      directly. The SDK serialises internally. Per the Databricks
      Zerobus SDK docs:
      https://docs.databricks.com/aws/en/ingestion/zerobus-ingest

          record_dict = {"device_name": f"sensor-{i}", "temp": 20, ...}
          offset = stream.ingest_record_offset(record_dict)

    - ``RecordType.PROTO``: pass Protobuf-encoded bytes whose schema
      matches the table's column descriptor. The ``b"record_data"``
      example in the SDK's own module-level docstring is for this
      mode, not JSON.

    All ``DEVICE_PROFILES`` generators emit dicts whose values are
    already JSON-serialisable — strings, numbers, ISO-formatted
    timestamps. No transformation needed before passing to the SDK.

    **Why we wait_for_offset per batch.** ``ingest_record_offset`` is
    fire-and-buffer: it queues the record locally and returns an
    offset immediately without waiting for the server to commit. If
    the server tears down the stream a few seconds later (the
    ``Stream is closed: Internal`` we observe in practice), every
    record still sitting in the local buffer is lost — the runner
    reports "600 rows inserted" but the destination table is empty.
    Blocking on ``wait_for_offset(last_offset)`` after each batch
    guarantees the records actually committed before we report
    success and trades raw throughput for at-least-once semantics
    on per-batch granularity, which matches what the runner exposes
    via its tick-level error reporting.

    Returns the count of records ingested. Per-record exceptions
    propagate — the caller decides whether to retry the batch.
    """
    if stream is None:
        raise RuntimeError("Zerobus stream is None — open_zerobus_stream() must be called first.")
    last_offset = None
    for record in batch:
        last_offset = stream.ingest_record_offset(record)
    if last_offset is not None:
        # Block until the server acks the last record. If the server
        # closes the stream mid-flight (the very failure we're trying
        # to detect), this raises and the runner's except block sees
        # the failure on the *committing* batch instead of silently
        # losing rows that "succeeded" optimistically.
        stream.wait_for_offset(last_offset)
    return len(batch)


def close_zerobus_stream(stream: Any) -> None:
    """Best-effort flush + close. Swallows exceptions so the dispatch's
    ``finally`` block never raises secondarily.

    The SDK's ``ingest_record_offset`` is fire-and-buffer: it returns
    an offset immediately and queues the record locally. Without an
    explicit ``flush()`` before ``close()``, records sitting in the
    local buffer are dropped — which is why "the table is empty after
    a successful run" is the canonical Zerobus footgun. Flush first,
    close second; both are best-effort.
    """
    if stream is None:
        return
    try:
        stream.flush()
        logger.info("Zerobus stream flushed")
    except Exception as e:
        # Don't bail on flush failure — still attempt close so the
        # gRPC connection doesn't leak.
        logger.warning(f"Zerobus flush failed (records may be lost): {e}")
    try:
        stream.close()
        logger.info("Zerobus stream closed")
    except Exception as e:
        logger.warning(f"Failed to close Zerobus stream cleanly: {e}")
