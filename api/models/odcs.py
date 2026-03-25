"""Pydantic models for ODCS (Open Data Contract Standard) v3.1.0.

Implements the full ODCS v3.1.0 specification covering all 11 sections:
Fundamentals, Schema, References, Data Quality, Support, Pricing,
Team, Roles, SLA, Infrastructure/Servers, and Custom Properties.

Spec: https://bitol-io.github.io/open-data-contract-standard/v3.1.0/
"""

from __future__ import annotations

import re
import uuid
from datetime import datetime
from typing import Any, Optional, Union

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator


# ---------------------------------------------------------------------------
# Reusable building blocks
# ---------------------------------------------------------------------------

class ODCSCustomProperty(BaseModel):
    """Key-value custom property usable at any level."""
    property: str = Field(..., description="Property name")
    value: Any = Field(..., description="Property value (string, number, list, etc.)")


class ODCSReference(BaseModel):
    """Authoritative definition / external reference."""
    type: str = Field(default="", description="Reference type: canonical, businessDefinition, videoTutorial, privacy-statement, transformationImplementation, implementation")
    url: str = Field(default="", description="URL to the reference")
    description: str = Field(default="", description="Human-readable description")


# ---------------------------------------------------------------------------
# Description
# ---------------------------------------------------------------------------

class ODCSDescription(BaseModel):
    """Contract-level description block."""
    purpose: str = Field(default="", description="Intended purpose of the data")
    limitations: str = Field(default="", description="Technical, compliance, and legal limitations")
    usage: str = Field(default="", description="Recommended usage")
    authoritativeDefinitions: list[ODCSReference] = Field(default_factory=list)
    customProperties: list[ODCSCustomProperty] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Quality
# ---------------------------------------------------------------------------

class ODCSQualityRule(BaseModel):
    """Data quality rule — can appear at contract, schema-object, or property level."""
    id: str = Field(default="", description="Unique rule identifier")
    name: str = Field(default="", description="Short descriptive name")
    description: str = Field(default="", description="Detailed explanation")
    type: str = Field(default="library", description="library, text, sql, custom")
    metric: str = Field(default="", description="Library metric: nullValues, missingValues, invalidValues, duplicateValues, rowCount")
    dimension: str = Field(default="", description="Quality dimension: accuracy, completeness, conformity, consistency, coverage, timeliness, uniqueness")
    method: str = Field(default="", description="Validation method, e.g. reconciliation")
    severity: str = Field(default="error", description="Failure severity: critical, error, warning, info")
    businessImpact: str = Field(default="", description="Impact description: operational, financial, regulatory")

    # Comparison operators (only one should be set per rule)
    mustBe: Optional[Union[float, int]] = Field(default=None)
    mustNotBe: Optional[Union[float, int]] = Field(default=None)
    mustBeGreaterThan: Optional[Union[float, int]] = Field(default=None)
    mustBeGreaterOrEqualTo: Optional[Union[float, int]] = Field(default=None)
    mustBeLessThan: Optional[Union[float, int]] = Field(default=None)
    mustBeLessOrEqualTo: Optional[Union[float, int]] = Field(default=None)
    mustBeBetween: Optional[list[Union[float, int]]] = Field(default=None, description="[min, max]")
    mustNotBeBetween: Optional[list[Union[float, int]]] = Field(default=None, description="[min, max]")

    # SQL / custom type fields
    query: str = Field(default="", description="SQL query (required for type=sql)")
    engine: str = Field(default="", description="Engine name (required for type=custom, e.g. soda, great_expectations, dbt, dqx)")
    implementation: str = Field(default="", description="Non-parsed code block for custom implementations")

    # Arguments for library metrics
    arguments: Optional[dict[str, Any]] = Field(default=None, description="Metric-specific parameters: validValues, pattern, missingValues, properties")
    unit: str = Field(default="", description="Unit: rows or percent")

    # Scheduling
    scheduler: str = Field(default="", description="Scheduler tool name: cron, airflow, etc.")
    schedule: str = Field(default="", description="Schedule expression, e.g. cron: '0 20 * * *'")

    # DQX integration
    dqx_function: str = Field(default="", description="DQX check function name, e.g. is_not_null, is_in_range")
    dqx_arguments: Optional[dict[str, Any]] = Field(default=None, description="DQX check function arguments")
    dqx_criticality: str = Field(default="error", description="DQX criticality: error or warn")

    tags: list[str] = Field(default_factory=list)
    customProperties: list[ODCSCustomProperty] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Schema — Properties (columns) and Objects (tables/views)
# ---------------------------------------------------------------------------

class ODCSRelationship(BaseModel):
    """Relationship definition (e.g. foreign key)."""
    type: str = Field(default="foreignKey", description="Relationship type")
    from_: list[str] = Field(default_factory=list, alias="from", description="Source columns (dot notation)")
    to: list[str] = Field(default_factory=list, description="Target columns (dot notation)")
    customProperties: list[ODCSCustomProperty] = Field(default_factory=list)

    model_config = {"populate_by_name": True}


class ODCSProperty(BaseModel):
    """Schema property (column) definition."""
    id: str = Field(default="", description="Unique property identifier")
    name: str = Field(..., description="Column name")
    physicalName: str = Field(default="", description="Physical column name in the data source")
    businessName: str = Field(default="", description="Business-facing column name")
    logicalType: str = Field(default="string", description="Logical type: string, date, timestamp, time, number, integer, object, array, boolean")
    physicalType: str = Field(default="", description="Physical data type, e.g. VARCHAR(255), DOUBLE, INT")
    logicalTypeOptions: Optional[dict[str, Any]] = Field(default=None, description="Type-specific metadata: format, min, max, timezone, pattern, etc.")
    description: str = Field(default="")
    required: bool = Field(default=False, description="Whether null values are allowed")
    unique: bool = Field(default=False, description="Whether values must be unique")
    primaryKey: bool = Field(default=False, description="Whether this is a primary key column")
    primaryKeyPosition: int = Field(default=-1, description="Position in composite primary key (-1 = not a PK)")
    partitioned: bool = Field(default=False, description="Whether column is used for partitioning")
    partitionKeyPosition: int = Field(default=-1, description="Position in partition key (-1 = not partitioned)")
    classification: str = Field(default="", description="Confidentiality: public, internal, restricted, confidential")
    criticalDataElement: bool = Field(default=False, description="Whether this is a Critical Data Element")
    encryptedName: str = Field(default="", description="Name of the encrypted version of this column")
    transformSourceObjects: list[str] = Field(default_factory=list, description="Source objects used in transformation")
    transformLogic: str = Field(default="", description="SQL or transformation logic")
    transformDescription: str = Field(default="", description="Business-friendly transformation description")
    examples: list[str] = Field(default_factory=list, description="Example values")
    tags: list[str] = Field(default_factory=list)
    authoritativeDefinitions: list[ODCSReference] = Field(default_factory=list)
    quality: list[ODCSQualityRule] = Field(default_factory=list, description="Column-level quality rules")
    relationships: list[ODCSRelationship] = Field(default_factory=list)
    customProperties: list[ODCSCustomProperty] = Field(default_factory=list)


class ODCSSchemaObject(BaseModel):
    """Schema object (table, view, topic, file)."""
    id: str = Field(default="", description="Unique object identifier")
    name: str = Field(..., description="Object name")
    physicalName: str = Field(default="", description="Physical name in the data source")
    physicalType: str = Field(default="table", description="Object type: table, view, topic, file")
    businessName: str = Field(default="", description="Business-facing name")
    description: str = Field(default="")
    tags: list[str] = Field(default_factory=list)
    dataGranularityDescription: str = Field(default="", description="Granularity level of data in this object")
    authoritativeDefinitions: list[ODCSReference] = Field(default_factory=list)
    relationships: list[ODCSRelationship] = Field(default_factory=list)
    properties: list[ODCSProperty] = Field(default_factory=list, description="Columns / properties")
    quality: list[ODCSQualityRule] = Field(default_factory=list, description="Object-level quality rules")
    customProperties: list[ODCSCustomProperty] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Support & Communication
# ---------------------------------------------------------------------------

class ODCSSupportChannel(BaseModel):
    """Support / communication channel."""
    channel: str = Field(..., description="Channel name or identifier")
    tool: str = Field(default="", description="Tool: slack, email, teams, jira, etc.")
    url: str = Field(default="", description="URL or mailto link")
    scope: str = Field(default="", description="Channel scope: issues, announcements, etc.")
    description: str = Field(default="")
    customProperties: list[ODCSCustomProperty] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Pricing
# ---------------------------------------------------------------------------

class ODCSPrice(BaseModel):
    """Data pricing information."""
    priceAmount: float = Field(default=0.0, description="Price amount")
    priceCurrency: str = Field(default="USD", description="ISO currency code")
    priceUnit: str = Field(default="", description="Pricing unit: megabyte, record, query, etc.")


# ---------------------------------------------------------------------------
# Team
# ---------------------------------------------------------------------------

class ODCSTeamMember(BaseModel):
    """Team member definition."""
    username: str = Field(..., description="Username or email")
    role: str = Field(default="", description="Role in the team")
    description: str = Field(default="")
    dateIn: str = Field(default="", description="Start date (YYYY-MM-DD)")
    dateOut: str = Field(default="", description="End date (YYYY-MM-DD), empty if current")
    replacedByUsername: str = Field(default="", description="Username of replacement")


class ODCSTeam(BaseModel):
    """Team owning the data contract."""
    name: str = Field(default="", description="Team name")
    description: str = Field(default="")
    members: list[ODCSTeamMember] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Roles
# ---------------------------------------------------------------------------

class ODCSRole(BaseModel):
    """Access role definition."""
    role: str = Field(..., description="Role name / identifier")
    access: str = Field(default="read", description="Access level: read, write")
    firstLevelApprovers: str = Field(default="", description="First-level approver(s)")
    secondLevelApprovers: str = Field(default="", description="Second-level approver(s)")


# ---------------------------------------------------------------------------
# SLA Properties
# ---------------------------------------------------------------------------

class ODCSSLAProperty(BaseModel):
    """Service-level agreement property."""
    id: str = Field(default="", description="Unique SLA property identifier")
    property: str = Field(..., description="SLA property: latency, availability, throughput, errorRate, generalAvailability, endOfSupport, endOfLife, retention, frequency, timeOfAvailability, timeToDetect, timeToNotify, timeToRepair")
    value: Any = Field(..., description="Agreement value")
    valueExt: Optional[Any] = Field(default=None, description="Extended agreement value")
    unit: str = Field(default="", description="ISO unit: d/day/days, h/hour/hours, m/min/minutes, y/yr/years")
    element: str = Field(default="", description="Target element(s) using dot notation")
    driver: str = Field(default="", description="SLA importance: regulatory, analytics, operational")
    description: str = Field(default="")
    scheduler: str = Field(default="", description="Scheduler tool name")
    schedule: str = Field(default="", description="Schedule expression")


# ---------------------------------------------------------------------------
# Infrastructure & Servers
# ---------------------------------------------------------------------------

class ODCSServer(BaseModel):
    """Server / infrastructure definition."""
    server: str = Field(..., description="Server identifier")
    id: str = Field(default="", description="Unique server ID")
    type: str = Field(default="custom", description="Server type: databricks, postgres, snowflake, bigquery, kafka, s3, mysql, oracle, redshift, trino, custom, etc.")
    description: str = Field(default="")
    environment: str = Field(default="", description="Deployment environment: prod, dev, staging, uat")
    roles: list[str] = Field(default_factory=list, description="Access roles for this server")

    # Common connection fields (type-specific, all optional)
    host: str = Field(default="", description="Hostname or IP")
    port: Optional[int] = Field(default=None, description="Port number")
    account: str = Field(default="", description="Account identifier (Snowflake)")
    database: str = Field(default="", description="Database name")
    schema_: str = Field(default="", alias="schema", description="Schema name")
    catalog: str = Field(default="", description="Catalog name (Databricks, Unity Catalog)")
    warehouse: str = Field(default="", description="Warehouse name")
    dataset: str = Field(default="", description="Dataset name (BigQuery)")
    project: str = Field(default="", description="Project name (BigQuery, GCP)")
    topic: str = Field(default="", description="Topic name (Kafka, Pub/Sub)")
    location: str = Field(default="", description="Storage location (S3, ADLS)")
    path: str = Field(default="", description="File path")
    region: str = Field(default="", description="Cloud region")
    stream: str = Field(default="", description="Stream name (Kinesis)")

    customProperties: list[ODCSCustomProperty] = Field(default_factory=list)

    model_config = {"populate_by_name": True}


# ---------------------------------------------------------------------------
# Top-level ODCS Contract
# ---------------------------------------------------------------------------

_SEMVER_RE = re.compile(r"^\d+\.\d+\.\d+(-[a-zA-Z0-9.]+)?(\+[a-zA-Z0-9.]+)?$")


class ODCSContract(BaseModel):
    """Full Open Data Contract Standard v3.1.0 document."""

    # --- Fundamentals (required) ---
    apiVersion: str = Field(default="v3.1.0", description="ODCS standard version")
    kind: str = Field(default="DataContract", description="Document kind")
    id: str = Field(default_factory=lambda: str(uuid.uuid4()), description="Unique contract identifier (UUID)")
    name: str = Field(default="", description="Contract name")
    version: str = Field(default="1.0.0", description="Contract version (semver)")
    status: str = Field(default="draft", description="Status: proposed, draft, active, deprecated, retired")

    # --- Fundamentals (optional) ---
    tenant: str = Field(default="", description="Tenant / organization")
    domain: str = Field(default="", description="Logical data domain")
    dataProduct: str = Field(default="", description="Data product name")
    tags: list[str] = Field(default_factory=list)
    description: ODCSDescription = Field(default_factory=ODCSDescription)
    contractCreatedTs: str = Field(default="", description="Contract creation timestamp (ISO 8601)")

    # --- Schema ---
    schema_: list[ODCSSchemaObject] = Field(default_factory=list, alias="schema", description="Schema objects (tables, views, etc.)")

    # --- References ---
    authoritativeDefinitions: list[ODCSReference] = Field(default_factory=list)

    # --- Data Quality (contract-level) ---
    quality: list[ODCSQualityRule] = Field(default_factory=list)

    # --- Support ---
    support: list[ODCSSupportChannel] = Field(default_factory=list)

    # --- Pricing ---
    price: Optional[ODCSPrice] = Field(default=None)

    # --- Team ---
    team: Optional[ODCSTeam] = Field(default=None)

    # --- Roles ---
    roles: list[ODCSRole] = Field(default_factory=list)

    # --- SLA ---
    slaProperties: list[ODCSSLAProperty] = Field(default_factory=list)

    # --- Servers ---
    servers: list[ODCSServer] = Field(default_factory=list)

    # --- Custom Properties ---
    customProperties: list[ODCSCustomProperty] = Field(default_factory=list)

    model_config = {"populate_by_name": True}

    @field_validator("version")
    @classmethod
    def validate_semver(cls, v: str) -> str:
        if v and not _SEMVER_RE.match(v):
            raise ValueError(f"Version must be semver format (e.g. 1.0.0), got: {v}")
        return v

    @field_validator("status")
    @classmethod
    def validate_status(cls, v: str) -> str:
        allowed = {"proposed", "draft", "active", "deprecated", "retired"}
        if v and v not in allowed:
            raise ValueError(f"Status must be one of {allowed}, got: {v}")
        return v

    @model_validator(mode="after")
    def set_defaults(self):
        if not self.contractCreatedTs:
            self.contractCreatedTs = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S+00:00")
        return self

    # --- YAML serialization ---

    def to_yaml(self) -> str:
        """Serialize this contract to ODCS-compliant YAML."""
        data = self.model_dump(by_alias=True, exclude_none=True, exclude_defaults=False)
        # Remove empty strings and empty lists for cleaner YAML
        data = _strip_empty(data)
        return yaml.dump(data, default_flow_style=False, sort_keys=False, allow_unicode=True)

    @classmethod
    def from_yaml(cls, yaml_content: str) -> "ODCSContract":
        """Parse an ODCS YAML document into this model."""
        data = yaml.safe_load(yaml_content)
        if not isinstance(data, dict):
            raise ValueError("YAML content must be a mapping/dictionary")
        return cls.model_validate(data)

    def to_dict(self) -> dict:
        """Serialize to a Python dict with ODCS field names."""
        return self.model_dump(by_alias=True, exclude_none=True)


# ---------------------------------------------------------------------------
# API Request / Response models
# ---------------------------------------------------------------------------

class ODCSContractCreate(BaseModel):
    """Request body for creating a new ODCS contract.
    id and contractCreatedTs are auto-generated if not provided.
    """
    apiVersion: str = Field(default="v3.1.0")
    kind: str = Field(default="DataContract")
    id: str = Field(default="")
    name: str = Field(..., description="Contract name")
    version: str = Field(default="1.0.0")
    status: str = Field(default="draft")
    tenant: str = Field(default="")
    domain: str = Field(default="")
    dataProduct: str = Field(default="")
    tags: list[str] = Field(default_factory=list)
    description: ODCSDescription = Field(default_factory=ODCSDescription)
    schema_: list[ODCSSchemaObject] = Field(default_factory=list, alias="schema")
    authoritativeDefinitions: list[ODCSReference] = Field(default_factory=list)
    quality: list[ODCSQualityRule] = Field(default_factory=list)
    support: list[ODCSSupportChannel] = Field(default_factory=list)
    price: Optional[ODCSPrice] = Field(default=None)
    team: Optional[ODCSTeam] = Field(default=None)
    roles: list[ODCSRole] = Field(default_factory=list)
    slaProperties: list[ODCSSLAProperty] = Field(default_factory=list)
    servers: list[ODCSServer] = Field(default_factory=list)
    customProperties: list[ODCSCustomProperty] = Field(default_factory=list)

    model_config = {"populate_by_name": True}


class ODCSContractUpdate(BaseModel):
    """Partial update model — all fields optional."""
    name: Optional[str] = None
    version: Optional[str] = None
    status: Optional[str] = None
    tenant: Optional[str] = None
    domain: Optional[str] = None
    dataProduct: Optional[str] = None
    tags: Optional[list[str]] = None
    description: Optional[ODCSDescription] = None
    schema_: Optional[list[ODCSSchemaObject]] = Field(default=None, alias="schema")
    authoritativeDefinitions: Optional[list[ODCSReference]] = None
    quality: Optional[list[ODCSQualityRule]] = None
    support: Optional[list[ODCSSupportChannel]] = None
    price: Optional[ODCSPrice] = None
    team: Optional[ODCSTeam] = None
    roles: Optional[list[ODCSRole]] = None
    slaProperties: Optional[list[ODCSSLAProperty]] = None
    servers: Optional[list[ODCSServer]] = None
    customProperties: Optional[list[ODCSCustomProperty]] = None

    model_config = {"populate_by_name": True}


class ODCSImportRequest(BaseModel):
    """Request body for importing a contract from YAML."""
    yaml_content: str = Field(..., description="ODCS YAML document content")


class ODCSGenerateRequest(BaseModel):
    """Request body for generating a contract from a UC table."""
    table_fqn: str = Field(..., description="Fully qualified table name: catalog.schema.table")
    include_quality_rules: bool = Field(default=True, description="Auto-generate quality rules from column metadata")
    include_dqx_profiling: bool = Field(default=False, description="Run DQX Profiler for data-driven quality rules (slower)")
    include_lineage: bool = Field(default=True, description="Query system.access lineage tables")
    include_sla: bool = Field(default=True, description="Compute freshness and frequency SLA")
    include_tags: bool = Field(default=True, description="Read UC tags (table + column)")
    include_properties: bool = Field(default=True, description="Read TBLPROPERTIES")
    include_masks: bool = Field(default=True, description="Read column mask policies")
    include_row_filters: bool = Field(default=True, description="Read row filter policies → roles")
    include_history: bool = Field(default=True, description="Read table history for retention/frequency")
    auto_save: bool = Field(default=False, description="Automatically save to Delta after generation")


class ODCSGenerateSchemaRequest(BaseModel):
    """Request body for generating contracts for all tables in a schema."""
    catalog: str = Field(..., description="Catalog name")
    schema_name: str = Field(..., description="Schema name")
    include_quality_rules: bool = Field(default=True)
    include_dqx_profiling: bool = Field(default=False)
    include_lineage: bool = Field(default=True)
    include_sla: bool = Field(default=True)
    include_tags: bool = Field(default=True)
    auto_save: bool = Field(default=False, description="Automatically save all contracts to Delta")


class ODCSGenerateCatalogRequest(BaseModel):
    """Request body for generating contracts for all tables in a catalog."""
    catalog: str = Field(..., description="Catalog name")
    exclude_schemas: list[str] = Field(default=["information_schema"], description="Schemas to exclude")
    include_quality_rules: bool = Field(default=True)
    include_dqx_profiling: bool = Field(default=False)
    include_lineage: bool = Field(default=True)
    auto_save: bool = Field(default=False, description="Automatically save all contracts to Delta")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _strip_empty(obj: Any) -> Any:
    """Recursively remove empty strings, empty lists, and None values for cleaner YAML."""
    if isinstance(obj, dict):
        cleaned = {}
        for k, v in obj.items():
            v = _strip_empty(v)
            # Keep required ODCS fields even if empty
            if k in ("apiVersion", "kind", "id", "version", "status", "name"):
                cleaned[k] = v
            elif v is not None and v != "" and v != [] and v != {}:
                cleaned[k] = v
        return cleaned
    elif isinstance(obj, list):
        return [_strip_empty(item) for item in obj if _strip_empty(item) not in (None, "", {}, [])]
    return obj
