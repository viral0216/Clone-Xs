"""Pydantic models for the Demo Data Generator."""

from typing import Literal

from pydantic import BaseModel, Field, field_validator, model_validator


# ── Streaming-emit field bounds ─────────────────────────────────────
# Bounds for `events_per_batch`, `interval_seconds`, and
# `total_duration_seconds` come from `streaming_limits` in
# clone_config.yaml (see src.config.get_streaming_limits) so workspace
# admins can widen / narrow the form without editing code. The lookup
# is cached (60s TTL) inside src.config, so per-request validation is
# a dict access — no file I/O on the hot path.
def _streaming_default(field_name: str, fallback):
    """Resolve a default value from configured limits, with a fallback.

    Used as ``default_factory=lambda: _streaming_default("events_per_batch", 100)``
    so the API's default tracks YAML edits without a server restart.
    """
    try:
        from src.config import get_streaming_limits

        return get_streaming_limits()[field_name]["default"]
    except Exception:
        return fallback


def _check_streaming_bound(field_name: str, value):
    """Enforce min/max from configured limits. Used by @field_validator."""
    try:
        from src.config import get_streaming_limits

        bounds = get_streaming_limits().get(field_name)
    except Exception:
        bounds = None
    if not bounds:
        return value
    lo, hi = bounds["min"], bounds["max"]
    if value < lo or value > hi:
        raise ValueError(f"{field_name} must be in [{lo}, {hi}], got {value}")
    return value


class DemoDataRequest(BaseModel):
    catalog_name: str = Field(..., description="Name of the catalog to create")
    industries: list[str] = Field(
        default=["healthcare", "financial", "retail", "telecom", "manufacturing"],
        description="Industries to generate",
    )
    owner: str | None = Field(default=None, description="Set as catalog owner")
    scale_factor: float = Field(
        default=1.0, description="Row multiplier. 1.0 = ~1B rows, 0.1 = ~100M"
    )
    batch_size: int = Field(default=5_000_000, description="Rows per INSERT batch")
    max_workers: int = Field(default=4, description="Parallel SQL workers")
    storage_location: str | None = Field(default=None, description="Optional managed location")
    warehouse_id: str | None = Field(default=None, description="Override SQL warehouse ID")
    drop_existing: bool = Field(default=False, description="Drop and recreate if catalog exists")
    medallion: bool = Field(
        default=True, description="Generate bronze/silver/gold medallion schemas"
    )
    uc_best_practices: bool = Field(
        default=True, description="UC naming: bronze/silver/gold (not industry_bronze)"
    )
    create_functions: bool = Field(default=True, description="Create UDFs (20 per industry)")
    create_volumes: bool = Field(
        default=True, description="Create managed volumes with sample CSV files"
    )
    start_date: str = Field(default="2020-01-01", description="Data start date (YYYY-MM-DD)")
    end_date: str = Field(default="2025-01-01", description="Data end date (YYYY-MM-DD)")
    dest_catalog: str | None = Field(
        default=None, description="If set, clone the generated catalog to this destination"
    )
    # When true: create catalog, schemas, tables, views, UDFs, volumes, and
    # column masks — but skip every INSERT/UPDATE/DELETE. Drops generation
    # time from minutes/hours to seconds; useful for verifying DDL templates,
    # CI smoke runs, and YAML custom-industry validation.
    schema_only: bool = Field(default=False, description="DDL only — skip data INSERT statements")
    # Realistic-data toggle. When True, the generator rewrites the small
    # static pools embedded in INSERT expressions (e.g. "James", "Mary", and
    # `concat('patient',id,'@example.com')`) to sample from Faker-generated
    # locale-aware pools. Off by default to preserve existing test fixtures
    # that match the legacy values.
    realistic_data: bool = Field(
        default=False, description="Use Faker for realistic synthetic names/emails/phones"
    )
    locale: str = Field(
        default="en_US",
        description="Faker locale (e.g. en_US, en_GB, de_DE) — used when realistic_data=True",
    )
    seed: int | None = Field(
        default=None, description="Seed for deterministic Faker output. None = non-deterministic."
    )
    # Referential integrity audit. After generation, the orchestrator runs a
    # sampled LEFT JOIN orphan check across the FK relationship registry and
    # surfaces the report on the result. Skipped automatically when
    # schema_only=True.
    validate_referential_integrity: bool = Field(
        default=True, description="Run a post-generation FK orphan audit"
    )
    # Theme 2 (DQ profiles + ML labels). `dq_profile` is a named bundle of
    # null/dup/outlier rates ('clean', 'realistic', 'dirty'); see
    # `src/demo_anomalies.py:DQ_PROFILES`. `anomaly_rate` drives the
    # positive-class proportion on labeled training columns
    # (`is_fraud`/`churn_risk`/`is_anomaly`) added when `inject_anomalies`
    # is true.
    dq_profile: str = Field(
        default="realistic", description="DQ noise profile: clean | realistic | dirty"
    )
    anomaly_rate: float = Field(
        default=0.02, description="Positive-class rate for labeled training columns (0.0..1.0)"
    )
    inject_anomalies: bool = Field(
        default=True, description="Add labeled training columns (is_fraud, churn_risk, is_anomaly)"
    )
    # Theme 4 — custom industry YAML templates. Each entry is the path to
    # a YAML file matching the schema documented in
    # ``src/demo_industry_loader.py``. The orchestrator validates and
    # merges these on top of the built-in INDUSTRIES dict at run start.
    custom_industries: list[str] | None = Field(
        default=None, description="Paths to YAML industry templates"
    )
    # Data modeling pattern. "flat" (default) preserves today's behaviour —
    # only the original per-industry schema is generated. "star_schema"
    # additionally builds a `<industry>_star` schema with fct_/dim_ tables
    # following DBT-style naming. Future: data_vault_2 / one_big_table /
    # snowflake — see src/demo_models.py STAR_SCHEMA_REGISTRY for the v1
    # registry surface.
    data_model: Literal["flat", "star_schema"] = Field(
        default="flat", description="Data modeling pattern overlay"
    )

    @field_validator("dq_profile")
    @classmethod
    def _dq_profile_must_be_known(cls, v: str) -> str:
        # Lazy-import to avoid a load-time dep on src.* from api.models.
        from src.demo_anomalies import DQ_PROFILES

        if v not in DQ_PROFILES:
            raise ValueError(f"dq_profile must be one of {sorted(DQ_PROFILES)}, got {v!r}")
        return v

    @field_validator("anomaly_rate")
    @classmethod
    def _anomaly_rate_in_range(cls, v: float) -> float:
        if not 0 <= v <= 1:
            raise ValueError(f"anomaly_rate must be in [0.0, 1.0], got {v}")
        return v


class StreamingEmissionRequest(BaseModel):
    """Request to start a file-based streaming emission to a UC Volume.

    The runner emits JSON event batches at `interval_seconds` cadence
    for `total_duration_seconds`. Each batch is one file under
    `/Volumes/<catalog>/<schema>/<volume>/<profile>/`. Optionally
    creates a streaming Bronze Delta table consuming the Volume via
    Auto Loader. See `src/demo_streaming.py` for the device profile
    registry and emission semantics.
    """

    model_config = {"populate_by_name": True}

    catalog: str = Field(..., description="Target catalog (created if missing)")
    # Pydantic reserves `.schema` on BaseModel — use `schema_name`
    # internally and accept `schema` from the wire via the alias.
    schema_name: str = Field(..., alias="schema", description="Target schema")
    # UC Volume name — created if missing. Default preserves the legacy
    # path `/Volumes/<catalog>/<schema>/events_volume/<profile>/` for
    # any existing API callers.
    volume: str = Field(default="events_volume", description="UC Volume name (created if missing)")
    # Keep this list in sync with `src.demo_streaming.DEVICE_PROFILES`
    # — the registry is the source of truth, but Pydantic needs the
    # explicit Literal for OpenAPI / 422 validation. The
    # `tests/test_demo_streaming.py:test_pydantic_literal_matches_registry`
    # check guards against drift.
    profile: Literal[
        "generic_sensor",
        "industrial_machine",
        "car_obd2",
        "smart_meter",
        "wearable_health",
        "pos_terminal",
        "wind_turbine",
        "atm_transaction",
        "server_metrics",
        "clickstream",
    ] = Field(..., description="Built-in device profile")
    # Bounds + default come from clone_config.yaml `streaming_limits`
    # (see _check_streaming_bound / _streaming_default at the top of
    # this module). Edits to the YAML are picked up on the next
    # request without restarting the API.
    events_per_batch: int = Field(
        default_factory=lambda: _streaming_default("events_per_batch", 100)
    )
    interval_seconds: float = Field(
        default_factory=lambda: _streaming_default("interval_seconds", 5.0)
    )
    # 1-hour cap on v1 — bounds the maximum demo session length and
    # limits storage growth in shared workspaces.
    total_duration_seconds: int = Field(
        default_factory=lambda: _streaming_default("total_duration_seconds", 60)
    )
    num_devices: int | None = Field(
        default=None,
        ge=1,
        le=100000,
        description="Override the profile's default device count",
    )
    warehouse_id: str | None = Field(default=None, description="Override SQL warehouse ID")
    # Destination mode for emitted events:
    #   "volume"        — write JSON files to the Volume only
    #   "volume_bronze" — files + auto-create STREAMING TABLE Bronze
    #   "direct_table"  — INSERT INTO Delta table directly (no Volume,
    #                     no Auto Loader; works on Free Edition / any tier)
    #   "zerobus"       — direct gRPC append via Databricks Zerobus.
    #                     Requires the official `databricks-zerobus`
    #                     Python SDK to be installed; the runner returns
    #                     a 503 when it isn't (today). See
    #                     src/demo_streaming_zerobus_runtime.py.
    # Default preserves legacy behaviour by deferring to
    # `auto_create_bronze` when destination is unset (see runner).
    destination: Literal["volume", "volume_bronze", "direct_table", "zerobus"] | None = Field(
        default=None,
        description="Destination mode (volume | volume_bronze | direct_table | zerobus)",
    )
    # Bronze table name for direct_table mode. Empty → defaults to
    # `bronze_<profile>` at runtime.
    bronze_table: str = Field(
        default="", description="Bronze table name (default: bronze_<profile>)"
    )
    # Auto Loader Bronze: when true, runs CREATE OR REFRESH STREAMING
    # TABLE so files land in a Delta table automatically. Requires
    # DBSQL Serverless on the warehouse + CREATE TABLE on the schema.
    # Kept for backwards compatibility — superseded by `destination`
    # when set.
    auto_create_bronze: bool = Field(default=False)
    bronze_refresh_minutes: int = Field(default=5, ge=1, le=60)

    # ── Zerobus (only relevant when destination="zerobus") ──
    # Zerobus uses a region-specific gRPC endpoint that is NOT the
    # workspace URL — format e.g.
    # https://<workspace_id>.zerobus.<region>.cloud.databricks.com
    zerobus_server_endpoint: str | None = Field(
        default=None,
        description="Zerobus gRPC endpoint URL. Required when destination='zerobus'.",
    )
    # Optional MANAGED LOCATION for the catalog when the runner has to
    # create it. Required on metastores with no default storage root —
    # CREATE CATALOG IF NOT EXISTS fails there with INVALID_STATE
    # unless a location is supplied. Cloud-agnostic: s3://, abfss://,
    # gs:// all work as long as a UC external location / storage
    # credential covers the path.
    zerobus_catalog_location: str | None = Field(
        default=None,
        description="Optional MANAGED LOCATION for new catalogs (e.g. abfss://… or s3://…). Required only on metastores without a default storage root.",
    )
    # Auth mode for the Zerobus SDK call. "oauth" (default) uses the SP
    # client_id/client_secret below. "pat" lifts the token off the
    # logged-in user's WorkspaceClient and passes it via a custom
    # HeadersProvider — no SP fields needed. Caveat: Zerobus' server
    # may still reject PATs that lack the right scopes; this is a
    # convenience for users who already have a working PAT, not a
    # blanket replacement for the SP path.
    zerobus_auth_mode: Literal["oauth", "pat"] = Field(
        default="oauth",
        description="Zerobus auth mode. 'oauth' (default) uses the SP creds below; 'pat' uses the logged-in user's PAT.",
    )
    # Service-principal credentials. Required only when zerobus_auth_mode='oauth'.
    # Left blank in 'pat' mode — the runner ignores them.
    zerobus_client_id: str | None = Field(
        default=None,
        description="Service-principal client_id for Zerobus OAuth. Required when destination='zerobus' and auth_mode='oauth'.",
    )
    zerobus_client_secret: str | None = Field(
        default=None,
        description="Service-principal client_secret for Zerobus OAuth. Required when destination='zerobus' and auth_mode='oauth'.",
    )

    @field_validator("events_per_batch")
    @classmethod
    def _check_events_per_batch(cls, v: int) -> int:
        return _check_streaming_bound("events_per_batch", v)

    @field_validator("interval_seconds")
    @classmethod
    def _check_interval_seconds(cls, v: float) -> float:
        return _check_streaming_bound("interval_seconds", v)

    @field_validator("total_duration_seconds")
    @classmethod
    def _check_total_duration_seconds(cls, v: int) -> int:
        return _check_streaming_bound("total_duration_seconds", v)

    @model_validator(mode="after")
    def _zerobus_requires_credentials(self) -> "StreamingEmissionRequest":
        """When destination='zerobus', the required field set depends
        on auth mode:
          - oauth: server_endpoint + client_id + client_secret
          - pat:   server_endpoint only (PAT comes from the logged-in
                   client at runtime; not collected via the form)

        Validating here rather than in the runner means the form gets
        a clean 422 with field paths instead of a 500 mid-stream.
        """
        if self.destination != "zerobus":
            return self
        required = [("zerobus_server_endpoint", self.zerobus_server_endpoint)]
        if self.zerobus_auth_mode == "oauth":
            required += [
                ("zerobus_client_id", self.zerobus_client_id),
                ("zerobus_client_secret", self.zerobus_client_secret),
            ]
        missing = [name for name, val in required if not (val or "").strip()]
        if missing:
            raise ValueError(
                f"destination='zerobus' (auth_mode={self.zerobus_auth_mode!r}) requires: {', '.join(missing)}"
            )
        return self


class StreamingScheduleRequest(StreamingEmissionRequest):
    """Schedule a streaming-emit job on Databricks.

    Inherits every field from ``StreamingEmissionRequest`` (catalog,
    schema, volume, profile, cadence, num_devices, destination,
    auto-create-bronze) and adds the Job-creation specifics: Quartz
    cron, name, timezone, notebook path, and a serverless-vs-cluster
    toggle.

    Unlike the in-process ``POST /demo-data/streaming`` path, this
    creates a real Databricks Job — emission runs on Databricks
    compute and survives API restarts. Tagged ``created_by=clone-xs``
    so the existing ``GET /clone-jobs`` listing picks it up.
    """

    name: str = Field(default="", description="Databricks Job name; empty → auto-generated")
    schedule_quartz_cron: str = Field(
        default="0 */5 * * * ?",
        description="Quartz cron expression (e.g. '0 */5 * * * ?' for every 5 min)",
    )
    timezone_id: str = Field(default="UTC", description="IANA timezone for the schedule")
    notebook_path: str | None = Field(
        default=None,
        description="Workspace path for the generated notebook; None → auto-generated under /Users/<me>/clxs/",
    )
    use_serverless: bool = Field(
        default=True,
        description="Use Serverless compute (recommended). Falls back to a Single-Node job cluster when False.",
    )

    @field_validator("schedule_quartz_cron")
    @classmethod
    def _cron_shape(cls, v: str) -> str:
        # Basic Quartz validation — Quartz cron has 6 or 7 fields
        # (sec min hour dom mon dow [year]). Reject empty / obviously
        # wrong shapes; full validation happens server-side when
        # Databricks rejects the create_job call.
        v = (v or "").strip()
        if not v:
            raise ValueError("schedule_quartz_cron must not be empty")
        parts = v.split()
        if len(parts) not in (6, 7):
            raise ValueError(
                f"schedule_quartz_cron must have 6 or 7 fields (Quartz format), "
                f"got {len(parts)}: {v!r}"
            )
        return v


class ZerobusSnippetRequest(BaseModel):
    """Request to render a Python snippet that emits via Databricks Zerobus.

    Pure render — produces text only. No backend dependency on the
    (unreleased) Zerobus SDK; the user runs the snippet in their own
    environment. See ``src.demo_streaming_zerobus.render_zerobus_snippet``.
    """

    model_config = {"populate_by_name": True}

    profile: Literal[
        "generic_sensor",
        "industrial_machine",
        "car_obd2",
        "smart_meter",
        "wearable_health",
        "pos_terminal",
        "wind_turbine",
        "atm_transaction",
        "server_metrics",
        "clickstream",
    ] = Field(..., description="Built-in device profile (matches StreamingEmissionRequest)")
    catalog: str = Field(..., description="Target Unity Catalog catalog")
    # Same Pydantic-reserved-attribute trick as StreamingEmissionRequest —
    # accept `schema` on the wire, store as `schema_name` internally.
    schema_name: str = Field(..., alias="schema", description="Target schema")
    table: str | None = Field(
        default=None,
        description="Target table name. Defaults to bronze_<profile>.",
    )
    # Same config-driven bounds as StreamingEmissionRequest so the
    # snippet-render path doesn't 422 with values that the form
    # accepted. See _check_streaming_bound at the top of the module.
    events_per_batch: int = Field(
        default_factory=lambda: _streaming_default("events_per_batch", 100)
    )
    interval_seconds: float = Field(
        default_factory=lambda: _streaming_default("interval_seconds", 5.0)
    )
    num_devices: int = Field(default=10, ge=1, le=100000)

    @field_validator("events_per_batch")
    @classmethod
    def _check_events_per_batch(cls, v: int) -> int:
        return _check_streaming_bound("events_per_batch", v)

    @field_validator("interval_seconds")
    @classmethod
    def _check_interval_seconds(cls, v: float) -> float:
        return _check_streaming_bound("interval_seconds", v)


class ZerobusSnippetResponse(BaseModel):
    snippet: str
    language: str = "python"
    filename_suggestion: str
