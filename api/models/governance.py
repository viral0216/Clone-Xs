"""Pydantic models for the Governance portal."""

from pydantic import BaseModel, Field


# --- Data Dictionary ---

class GlossaryTermCreate(BaseModel):
    name: str = Field(..., description="Business term name")
    abbreviation: str = Field(default="", description="Short abbreviation (e.g., CLV)")
    definition: str = Field(..., description="Business definition")
    domain: str = Field(default="General", description="Business domain (e.g., Marketing, Finance)")
    owner: str = Field(default="", description="Term owner email")
    tags: list[str] = Field(default=[], description="Tags for categorization")
    status: str = Field(default="draft", description="draft, approved, deprecated")


class GlossaryLinkRequest(BaseModel):
    term_id: str = Field(..., description="Glossary term ID")
    column_fqns: list[str] = Field(..., description="List of catalog.schema.table.column FQNs to link")


class MetadataSearchRequest(BaseModel):
    query: str = Field(..., description="Search query")
    catalogs: list[str] = Field(default=[], description="Catalogs to search (empty = all)")
    search_type: str = Field(default="all", description="all, tables, columns, terms, tags")
    limit: int = Field(default=50, description="Max results")


# --- Data Quality Rules ---

class DQRuleCreate(BaseModel):
    name: str = Field(..., description="Rule name")
    table_fqn: str = Field(..., description="Fully qualified table name (catalog.schema.table)")
    column: str = Field(default="", description="Column name (empty for table-level rules)")
    rule_type: str = Field(..., description="not_null, unique, range, regex, custom_sql, freshness, row_count, referential")
    expression: str = Field(default="", description="Custom SQL expression or regex pattern")
    params: dict = Field(default={}, description="Rule parameters: min, max, pattern, expected_count, etc.")
    threshold: float = Field(default=0.0, description="Max allowed failure rate (0.0 = zero tolerance)")
    severity: str = Field(default="warning", description="critical, warning, info")
    schedule: str = Field(default="manual", description="manual, daily, hourly, weekly")
    enabled: bool = Field(default=True)


class DQRunRequest(BaseModel):
    rule_ids: list[str] = Field(default=[], description="Rule IDs to run (empty = all enabled)")
    catalog: str = Field(default="", description="Filter by catalog")
    table_fqn: str = Field(default="", description="Filter by table")


# --- Certifications ---

class CertificationCreate(BaseModel):
    table_fqn: str = Field(..., description="Fully qualified table name")
    status: str = Field(..., description="certified, deprecated, draft, pending_review, under_investigation")
    notes: str = Field(default="", description="Certification notes")
    expiry_date: str = Field(default="", description="Certification expiry (YYYY-MM-DD)")
    review_frequency: str = Field(default="quarterly", description="quarterly, monthly, annually")


class CertificationApproval(BaseModel):
    cert_id: str = Field(..., description="Certification ID")
    action: str = Field(..., description="approve or reject")
    reviewer_notes: str = Field(default="")


# --- SLA Rules ---

class SLARuleCreate(BaseModel):
    table_fqn: str = Field(..., description="Fully qualified table name")
    metric: str = Field(..., description="freshness, row_count, schema_stability")
    threshold_hours: int = Field(default=24, description="Max hours threshold")
    threshold_value: float = Field(default=0, description="Numeric threshold for row_count etc.")
    severity: str = Field(default="warning", description="critical, warning, info")
    owner_team: str = Field(default="", description="Responsible team")
    notification_channel: str = Field(default="", description="slack, email, webhook")


# --- Data Contracts ---

class DataContractCreate(BaseModel):
    name: str = Field(..., description="Contract name")
    table_fqn: str = Field(..., description="Table this contract covers")
    producer_team: str = Field(..., description="Team that produces the data")
    consumer_teams: list[str] = Field(default=[], description="Teams that consume the data")
    expected_columns: list[dict] = Field(default=[], description="Expected column definitions [{name, type, nullable}]")
    quality_rules: list[str] = Field(default=[], description="DQ rule IDs that must pass")
    freshness_sla_hours: int = Field(default=24, description="Max hours since last update")
    row_count_min: int = Field(default=0, description="Minimum expected row count")
    row_count_max: int = Field(default=0, description="Maximum expected row count (0 = no limit)")
    effective_date: str = Field(default="", description="Contract effective date")
    status: str = Field(default="draft", description="draft, active, expired, violated")


# --- DQX Checks ---

class DQXCheckCreate(BaseModel):
    table_fqn: str = Field(..., description="Fully qualified table name")
    name: str = Field(default="", description="Check name")
    check_function: str = Field(..., description="DQX function: is_not_null, is_in_range, regex_match, etc.")
    arguments: dict = Field(default={}, description="Function arguments: {column, min_value, max_value, pattern, etc.}")
    criticality: str = Field(default="error", description="error or warn")
    filter_expr: str = Field(default="", description="Optional SQL filter applied before check")


class DQXRunRequest(BaseModel):
    table_fqn: str = Field(..., description="Table to check")
    check_ids: list[str] = Field(default=[], description="Specific check IDs to run (empty = all enabled)")


class DQXProfileRequest(BaseModel):
    table_fqn: str = Field(..., description="Table to profile")
    sample_fraction: float = Field(default=0.3, description="Fraction of data to sample (0.05-1.0)")
    max_in_count: int = Field(default=10, description="Generate is_in_list when distinct values below this count")
    max_null_ratio: float = Field(default=0.01, description="Generate is_not_null when null ratio below this (0.0-1.0)")
    remove_outliers: bool = Field(default=True, description="Remove statistical outliers from min/max calculations")
    max_parallelism: int = Field(default=4, description="Max tables to profile in parallel (1-16)")
    auto_generate_checks: bool = Field(default=True, description="Auto-generate checks from profile results")


# --- Change History ---

class ChangeTrackRequest(BaseModel):
    entity_type: str = Field(..., description="glossary, certification, dq_rule, sla_rule, contract, table, column")
    entity_id: str = Field(..., description="Entity identifier")
    change_type: str = Field(..., description="created, updated, deleted, approved, rejected")
    details: dict = Field(default={}, description="Change details (old/new values)")
