"""System Insights request/response models."""

from pydantic import BaseModel, Field


class SystemInsightsRequest(BaseModel):
    """Request for system table insights."""
    catalog: str = ""
    days: int = Field(default=30, ge=1, le=365)
    warehouse_id: str | None = None
    job_name_filter: str = ""


class BillingRequest(BaseModel):
    """Request for billing usage data."""
    catalog: str = ""
    days: int = Field(default=30, ge=1, le=365)
    warehouse_id: str | None = None


class OptimizationRequest(BaseModel):
    """Request for predictive optimization recommendations."""
    catalog: str = ""
    warehouse_id: str | None = None


class JobRunsRequest(BaseModel):
    """Request for job run timeline data."""
    days: int = Field(default=30, ge=1, le=365)
    warehouse_id: str | None = None
    job_name_filter: str = ""


# --- SDK-only request models ---

class WarehouseHealthRequest(BaseModel):
    """Request for warehouse health data. No params — lists all warehouses."""
    pass


class ClusterHealthRequest(BaseModel):
    """Request for cluster health data."""
    max_events: int = Field(default=100, ge=0, le=500)


class DltPipelineHealthRequest(BaseModel):
    """Request for DLT pipeline health data."""
    max_events_per_pipeline: int = Field(default=20, ge=0, le=100)


class QueryPerformanceRequest(BaseModel):
    """Request for query performance data."""
    days: int = Field(default=30, ge=1, le=365)
    max_results: int = Field(default=200, ge=1, le=1000)


class MetastoreSummaryRequest(BaseModel):
    """Request for metastore summary. No params."""
    pass


class SqlAlertsRequest(BaseModel):
    """Request for SQL alerts. No params."""
    pass


class TableUsageRequest(BaseModel):
    """Request for table usage summary."""
    catalog: str
    days: int = Field(default=90, ge=1, le=365)
    warehouse_id: str | None = None
