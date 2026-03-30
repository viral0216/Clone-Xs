"""AI-powered features — summaries, clone builder, DQ suggestions, PII remediation.

Supports dual backend: Anthropic API (direct) or Databricks Model Serving endpoints.
The active backend is determined by the X-Databricks-Model header — if present, calls go
through Databricks; otherwise, falls back to Anthropic API.
"""

import logging

from fastapi import APIRouter, Header, Depends
from typing import Optional

from api.dependencies import get_db_client, get_credentials
from api.models.ai import (
    AISummarizeRequest,
    AISummarizeResponse,
    AIStatusResponse,
    CloneBuilderRequest,
    CloneBuilderResponse,
    DQSuggestionRequest,
    DQSuggestionResponse,
    PIIRemediationRequest,
    PIIRemediationResponse,
)

logger = logging.getLogger(__name__)
router = APIRouter()


def _get_service():
    from src.ai_service import get_ai_service
    return get_ai_service()


async def _resolve_client(x_databricks_model: Optional[str], creds: tuple):
    """Only resolve the Databricks client when a model endpoint is specified."""
    if not x_databricks_model:
        return None
    try:
        return await get_db_client(creds)
    except Exception:
        return None


@router.get("/status", response_model=AIStatusResponse)
async def ai_status(
    x_databricks_model: Optional[str] = Header(None, alias="X-Databricks-Model"),
    creds: tuple = Depends(get_credentials),
):
    """Check if AI features are available."""
    svc = _get_service()

    if x_databricks_model:
        return AIStatusResponse(available=True, model=x_databricks_model, backend="databricks")

    if svc.available:
        return AIStatusResponse(available=True, model="claude-sonnet-4-20250514", backend="anthropic")

    return AIStatusResponse(available=False, reason="No AI backend configured. Set ANTHROPIC_API_KEY or select a Databricks Model Serving endpoint in Settings.", backend="none")


@router.post("/summarize", response_model=AISummarizeResponse)
async def ai_summarize(
    request: AISummarizeRequest,
    x_databricks_model: Optional[str] = Header(None, alias="X-Databricks-Model"),
    creds: tuple = Depends(get_credentials),
):
    """Generate an AI narrative summary."""
    svc = _get_service()
    if not svc.is_available(x_databricks_model):
        return AISummarizeResponse(available=False, reason="No AI backend configured")
    try:
        client = await _resolve_client(x_databricks_model, creds)
        summary = svc.summarize(request.context_type, request.data, endpoint_name=x_databricks_model, client=client)
        return AISummarizeResponse(summary=summary)
    except Exception as e:
        logger.exception("AI summarize failed")
        return AISummarizeResponse(available=True, summary="", reason=str(e))


@router.post("/clone-builder", response_model=CloneBuilderResponse)
async def ai_clone_builder(
    request: CloneBuilderRequest,
    x_databricks_model: Optional[str] = Header(None, alias="X-Databricks-Model"),
    creds: tuple = Depends(get_credentials),
):
    """Parse a natural language clone request into structured config."""
    svc = _get_service()
    if not svc.is_available(x_databricks_model):
        return CloneBuilderResponse(available=False, reason="No AI backend configured")
    try:
        client = await _resolve_client(x_databricks_model, creds)
        result = svc.parse_clone_query(request.query, request.available_catalogs, endpoint_name=x_databricks_model, client=client)
        explanation = result.pop("explanation", "")
        return CloneBuilderResponse(config=result, explanation=explanation)
    except Exception as e:
        logger.exception("AI clone builder failed")
        return CloneBuilderResponse(available=True, reason=str(e))


@router.post("/dq-suggestions", response_model=DQSuggestionResponse)
async def ai_dq_suggestions(
    request: DQSuggestionRequest,
    x_databricks_model: Optional[str] = Header(None, alias="X-Databricks-Model"),
    creds: tuple = Depends(get_credentials),
):
    """Suggest data quality rules based on profiling results."""
    svc = _get_service()
    if not svc.is_available(x_databricks_model):
        return DQSuggestionResponse(available=False, reason="No AI backend configured")
    try:
        client = await _resolve_client(x_databricks_model, creds)
        suggestions = svc.suggest_dq_rules(request.profiling_results, request.table_name, endpoint_name=x_databricks_model, client=client)
        return DQSuggestionResponse(suggestions=suggestions)
    except Exception as e:
        logger.exception("AI DQ suggestions failed")
        return DQSuggestionResponse(available=True, reason=str(e))


@router.post("/pii-remediation", response_model=PIIRemediationResponse)
async def ai_pii_remediation(
    request: PIIRemediationRequest,
    x_databricks_model: Optional[str] = Header(None, alias="X-Databricks-Model"),
    creds: tuple = Depends(get_credentials),
):
    """Get AI-powered PII remediation recommendations."""
    svc = _get_service()
    if not svc.is_available(x_databricks_model):
        return PIIRemediationResponse(available=False, reason="No AI backend configured")
    try:
        client = await _resolve_client(x_databricks_model, creds)
        result = svc.suggest_pii_remediation(request.scan_results, endpoint_name=x_databricks_model, client=client)
        return PIIRemediationResponse(
            summary=result.get("summary", ""),
            recommendations=result.get("recommendations", []),
        )
    except Exception as e:
        logger.exception("AI PII remediation failed")
        return PIIRemediationResponse(available=True, reason=str(e))
