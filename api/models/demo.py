"""Pydantic models for the Demo Data Generator."""

from typing import Literal

from pydantic import BaseModel, Field, field_validator


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
    events_per_batch: int = Field(default=100, ge=1, le=10000)
    interval_seconds: float = Field(default=5.0, ge=0.1, le=300.0)
    # 1-hour cap on v1 — bounds the maximum demo session length and
    # limits storage growth in shared workspaces.
    total_duration_seconds: int = Field(default=60, ge=1, le=3600)
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
    # Default preserves legacy behaviour by deferring to
    # `auto_create_bronze` when destination is unset (see runner).
    destination: Literal["volume", "volume_bronze", "direct_table"] | None = Field(
        default=None,
        description="Destination mode (volume | volume_bronze | direct_table)",
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
