"""Shared type definitions for Clone-Xs.

Provides TypedDict classes for common data structures passed between modules.
Use these for type hints to improve IDE support and catch bugs early.
"""

from __future__ import annotations
from typing import TypedDict


class AuditTrailConfig(TypedDict, total=False):
    catalog: str
    schema: str
    table: str


class TablesConfig(TypedDict, total=False):
    catalog: str
    schemas: dict[str, str]


class CloneConfig(TypedDict, total=False):
    """Configuration dict for clone operations — mirrors clone_config.yaml."""
    source_catalog: str
    destination_catalog: str
    sql_warehouse_id: str
    clone_type: str
    load_type: str
    catalog_location: str
    max_workers: int
    max_parallel_queries: int
    parallel_tables: int
    batch_insert_size: int
    include_schemas: list[str]
    exclude_schemas: list[str]
    exclude_tables: list[str]
    copy_permissions: bool
    copy_ownership: bool
    copy_tags: bool
    dry_run: bool
    validate_after_clone: bool
    enable_rollback: bool
    audit: dict | None
    audit_trail: AuditTrailConfig
    tables: TablesConfig
    metrics_enabled: bool
    metrics_table: str
    save_run_logs: bool


class ObjectCounts(TypedDict):
    """Counts for a single object type (tables, views, etc.)."""
    success: int
    failed: int
    skipped: int


class CloneSummary(TypedDict, total=False):
    """Summary returned by clone_catalog()."""
    schemas_processed: int
    tables: ObjectCounts
    views: ObjectCounts
    functions: ObjectCounts
    volumes: ObjectCounts
    errors: list[str]
    duration_seconds: float


class MetricRecord(TypedDict, total=False):
    """A single metric measurement from anomaly detection."""
    id: str
    table_fqn: str
    column_name: str
    metric_name: str
    value: float
    measured_at: str
    baseline_mean: float
    baseline_stddev: float
    z_score: float
    is_anomaly: bool
    severity: str
