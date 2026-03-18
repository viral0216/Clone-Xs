"""Pydantic models for the Demo Data Generator."""

from pydantic import BaseModel, Field


class DemoDataRequest(BaseModel):
    catalog_name: str = Field(..., description="Name of the catalog to create")
    industries: list[str] = Field(
        default=["healthcare", "financial", "retail", "telecom", "manufacturing"],
        description="Industries to generate",
    )
    owner: str | None = Field(default=None, description="Set as catalog owner")
    scale_factor: float = Field(default=1.0, description="Row multiplier. 1.0 = ~1B rows, 0.1 = ~100M")
    batch_size: int = Field(default=5_000_000, description="Rows per INSERT batch")
    max_workers: int = Field(default=4, description="Parallel SQL workers")
    storage_location: str | None = Field(default=None, description="Optional managed location")
    warehouse_id: str | None = Field(default=None, description="Override SQL warehouse ID")
    drop_existing: bool = Field(default=False, description="Drop and recreate if catalog exists")
    medallion: bool = Field(default=True, description="Generate bronze/silver/gold medallion schemas")
    uc_best_practices: bool = Field(default=True, description="UC naming: bronze/silver/gold (not industry_bronze)")
    create_functions: bool = Field(default=True, description="Create UDFs (20 per industry)")
    create_volumes: bool = Field(default=True, description="Create managed volumes with sample CSV files")
    start_date: str = Field(default="2020-01-01", description="Data start date (YYYY-MM-DD)")
    end_date: str = Field(default="2025-01-01", description="Data end date (YYYY-MM-DD)")
    dest_catalog: str | None = Field(default=None, description="If set, clone the generated catalog to this destination")
