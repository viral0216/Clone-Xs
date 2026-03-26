"""AI service for Clone-Xs — Claude API integration for intelligent insights."""

import os
import json
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# System prompts per context type
_SYSTEM_PROMPTS = {
    "dashboard": (
        "You are an AI assistant for Clone-Xs, a Databricks Unity Catalog cloning toolkit. "
        "Analyze the dashboard data and provide a concise 2-3 sentence executive summary. "
        "Highlight key metrics, trends (week-over-week changes), and any concerns (failed jobs, low health scores). "
        "Be specific with numbers. Use a professional tone."
    ),
    "audit": (
        "You are an AI assistant for Clone-Xs. Analyze the audit trail data and summarize recent operations. "
        "Note patterns (most active catalogs, common job types), any failures and their causes, "
        "and recommend actions if issues are detected. Keep it to 3-4 sentences."
    ),
    "report": (
        "You are an AI assistant for Clone-Xs. Analyze the clone/sync report data and provide insights. "
        "Summarize what was cloned, data volumes, success rates, and any anomalies. "
        "Suggest optimizations if applicable. Keep it to 2-3 sentences."
    ),
    "profiling": (
        "You are an AI assistant for Clone-Xs. Analyze the data profiling results and suggest data quality rules. "
        "Look for columns with high null rates, low distinct counts, or unusual distributions. "
        "Suggest specific rule types (not_null, unique, range, regex) with thresholds. "
        "Return suggestions as a JSON array of objects with keys: column, rule_type, threshold, rationale."
    ),
    "pii": (
        "You are an AI assistant for Clone-Xs. Analyze the PII scan results and recommend remediation actions. "
        "Prioritize by risk level and data sensitivity. Suggest masking strategies (hash, redact, tokenize) "
        "for each PII type detected. Note any false positives based on column names vs detected patterns. "
        "Keep recommendations actionable and specific."
    ),
    "clone_builder": (
        "You are an AI assistant for Clone-Xs. Parse the user's natural language request into a structured "
        "clone configuration. Extract: source_catalog, destination_catalog, clone_type (deep/shallow/schema_only), "
        "include_schemas (list), exclude_schemas (list), include_tables (list), exclude_tables (list), "
        "copy_permissions (bool), copy_tags (bool). If information is ambiguous, make reasonable defaults. "
        "Return ONLY a JSON object with these keys, plus an 'explanation' key describing what you understood."
    ),
}


class AIService:
    """Claude API integration for intelligent insights."""

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("ANTHROPIC_API_KEY", "")
        self._client = None

    @property
    def available(self) -> bool:
        return bool(self.api_key)

    def _get_client(self):
        if self._client is None:
            try:
                import anthropic
                self._client = anthropic.Anthropic(api_key=self.api_key)
            except ImportError:
                raise RuntimeError("anthropic package not installed. Run: pip install anthropic>=0.30.0")
        return self._client

    def _call_claude(self, system_prompt: str, user_message: str, max_tokens: int = 1024) -> str:
        """Make a Claude API call and return the text response."""
        client = self._get_client()
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=max_tokens,
            system=system_prompt,
            messages=[{"role": "user", "content": user_message}],
        )
        return response.content[0].text

    def summarize(self, context_type: str, data: dict) -> str:
        """Generate a narrative summary for the given context."""
        system_prompt = _SYSTEM_PROMPTS.get(context_type, _SYSTEM_PROMPTS["dashboard"])
        user_message = f"Here is the {context_type} data to analyze:\n\n{json.dumps(data, indent=2, default=str)}"
        return self._call_claude(system_prompt, user_message)

    def parse_clone_query(self, query: str, available_catalogs: list[str]) -> dict:
        """Parse a natural language clone request into structured config."""
        system_prompt = _SYSTEM_PROMPTS["clone_builder"]
        user_message = (
            f"Available catalogs: {', '.join(available_catalogs) if available_catalogs else 'unknown'}\n\n"
            f"User request: {query}"
        )
        response = self._call_claude(system_prompt, user_message)
        # Try to parse JSON from response
        try:
            # Handle markdown code blocks
            text = response.strip()
            if text.startswith("```"):
                text = text.split("\n", 1)[1].rsplit("```", 1)[0]
            return json.loads(text)
        except (json.JSONDecodeError, IndexError):
            return {"explanation": response, "error": "Could not parse structured config"}

    def suggest_dq_rules(self, profiling_results: dict, table_name: str = "") -> list[dict]:
        """Suggest data quality rules from profiling results."""
        system_prompt = _SYSTEM_PROMPTS["profiling"]
        user_message = f"Table: {table_name}\n\nProfiling results:\n{json.dumps(profiling_results, indent=2, default=str)}"
        response = self._call_claude(system_prompt, user_message)
        try:
            text = response.strip()
            if text.startswith("```"):
                text = text.split("\n", 1)[1].rsplit("```", 1)[0]
            return json.loads(text)
        except (json.JSONDecodeError, IndexError):
            return [{"raw_response": response}]

    def suggest_pii_remediation(self, scan_results: dict) -> dict:
        """Suggest PII remediation actions."""
        system_prompt = _SYSTEM_PROMPTS["pii"]
        user_message = f"PII scan results:\n{json.dumps(scan_results, indent=2, default=str)}"
        response = self._call_claude(system_prompt, user_message)
        return {"summary": response, "recommendations": []}


# Singleton instance
_ai_service: Optional[AIService] = None


def get_ai_service() -> AIService:
    """Get or create the singleton AI service instance."""
    global _ai_service
    if _ai_service is None:
        _ai_service = AIService()
    return _ai_service
