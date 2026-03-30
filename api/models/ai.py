"""Pydantic models for AI-powered features."""

from pydantic import BaseModel, Field
from typing import Optional


class AISummarizeRequest(BaseModel):
    """Request to generate an AI narrative summary."""
    context_type: str = Field(..., description="Type of summary: dashboard, audit, report, profiling, pii")
    data: dict = Field(default_factory=dict, description="Raw data to summarize")


class AISummarizeResponse(BaseModel):
    """AI-generated summary response."""
    summary: str = ""
    available: bool = True
    reason: Optional[str] = None


class CloneBuilderRequest(BaseModel):
    """Natural language clone request."""
    query: str = Field(..., description="Natural language description of clone operation")
    available_catalogs: list[str] = Field(default_factory=list, description="Available catalog names for context")


class CloneBuilderResponse(BaseModel):
    """Parsed clone configuration from natural language."""
    config: dict = Field(default_factory=dict, description="Structured clone config")
    explanation: str = ""
    available: bool = True
    reason: Optional[str] = None


class DQSuggestionRequest(BaseModel):
    """Request to suggest data quality rules from profiling results."""
    profiling_results: dict = Field(default_factory=dict)
    table_name: str = ""


class DQSuggestionResponse(BaseModel):
    """Suggested DQ rules."""
    suggestions: list[dict] = Field(default_factory=list)
    available: bool = True
    reason: Optional[str] = None


class PIIRemediationRequest(BaseModel):
    """Request PII remediation insights."""
    scan_results: dict = Field(default_factory=dict)


class PIIRemediationResponse(BaseModel):
    """PII remediation suggestions."""
    recommendations: list[dict] = Field(default_factory=list)
    summary: str = ""
    available: bool = True
    reason: Optional[str] = None


class AIStatusResponse(BaseModel):
    """AI feature availability status."""
    available: bool
    reason: Optional[str] = None
    model: Optional[str] = None
    backend: Optional[str] = None  # "anthropic", "databricks", "none"
