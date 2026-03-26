"""AI-powered features — summaries, clone builder, DQ suggestions, PII remediation."""

import logging

from fastapi import APIRouter

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

_UNAVAILABLE = {"available": False, "reason": "ANTHROPIC_API_KEY not configured"}


def _get_service():
    from src.ai_service import get_ai_service
    return get_ai_service()


@router.get("/status", response_model=AIStatusResponse)
async def ai_status():
    """Check if AI features are available (API key configured)."""
    svc = _get_service()
    if not svc.available:
        return AIStatusResponse(**_UNAVAILABLE)
    return AIStatusResponse(available=True, model="claude-sonnet-4-20250514")


@router.post("/summarize", response_model=AISummarizeResponse)
async def ai_summarize(request: AISummarizeRequest):
    """Generate an AI narrative summary for dashboard, audit, report, etc."""
    svc = _get_service()
    if not svc.available:
        return AISummarizeResponse(**_UNAVAILABLE)
    try:
        summary = svc.summarize(request.context_type, request.data)
        return AISummarizeResponse(summary=summary)
    except Exception as e:
        logger.exception("AI summarize failed")
        return AISummarizeResponse(available=True, summary="", reason=str(e))


@router.post("/clone-builder", response_model=CloneBuilderResponse)
async def ai_clone_builder(request: CloneBuilderRequest):
    """Parse a natural language clone request into structured config."""
    svc = _get_service()
    if not svc.available:
        return CloneBuilderResponse(**_UNAVAILABLE)
    try:
        result = svc.parse_clone_query(request.query, request.available_catalogs)
        explanation = result.pop("explanation", "")
        return CloneBuilderResponse(config=result, explanation=explanation)
    except Exception as e:
        logger.exception("AI clone builder failed")
        return CloneBuilderResponse(available=True, reason=str(e))


@router.post("/dq-suggestions", response_model=DQSuggestionResponse)
async def ai_dq_suggestions(request: DQSuggestionRequest):
    """Suggest data quality rules based on profiling results."""
    svc = _get_service()
    if not svc.available:
        return DQSuggestionResponse(**_UNAVAILABLE)
    try:
        suggestions = svc.suggest_dq_rules(request.profiling_results, request.table_name)
        return DQSuggestionResponse(suggestions=suggestions)
    except Exception as e:
        logger.exception("AI DQ suggestions failed")
        return DQSuggestionResponse(available=True, reason=str(e))


@router.post("/pii-remediation", response_model=PIIRemediationResponse)
async def ai_pii_remediation(request: PIIRemediationRequest):
    """Get AI-powered PII remediation recommendations."""
    svc = _get_service()
    if not svc.available:
        return PIIRemediationResponse(**_UNAVAILABLE)
    try:
        result = svc.suggest_pii_remediation(request.scan_results)
        return PIIRemediationResponse(
            summary=result.get("summary", ""),
            recommendations=result.get("recommendations", []),
        )
    except Exception as e:
        logger.exception("AI PII remediation failed")
        return PIIRemediationResponse(available=True, reason=str(e))
