"""AI service for Clone-Xs — dual-backend: Anthropic API or Databricks Model Serving."""

import os
import json
import logging
import requests
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
    "query_explain": (
        "You are a data analyst assistant. Given a SQL query, its result column metadata, "
        "summary statistics, and a sample of rows, explain what the data shows in plain English. "
        "Use this EXACT markdown format:\n\n"
        "## What This Data Shows\n- 1-2 bullets describing what the query returns\n\n"
        "## Key Findings\n- Specific numbers, trends, distributions from the stats\n\n"
        "## Notable Patterns\n- Any outliers, correlations, or anomalies\n\n"
        "## Recommendations\n- What to investigate further or actions to consider\n\n"
        "Be specific with numbers from the provided statistics. Keep each bullet to 1 sentence. Max 3 bullets per section."
    ),
    "ai_viz_suggest": (
        "You are a data visualization expert. Given column names, types, cardinality, and sample data, "
        "recommend the single best chart type from: bar, hbar, stacked, line, area, scatter, pie, radar, treemap, funnel, composed. "
        "Return ONLY a JSON object: {\"chartType\": \"...\", \"xCol\": \"...\", \"yCol\": \"...\", \"reason\": \"...\"}. "
        "No markdown, no explanation outside the JSON."
    ),
}


class AIService:
    """Dual-backend AI service: Anthropic API or Databricks Model Serving."""

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("ANTHROPIC_API_KEY", "")
        self._client = None

    @property
    def available(self) -> bool:
        return bool(self.api_key)

    def is_available(self, endpoint_name: str = None) -> bool:
        """Check if AI is available — either via Anthropic key or Databricks endpoint."""
        if endpoint_name:
            return True  # Databricks endpoint doesn't need Anthropic key
        return self.available

    def _get_client(self):
        if self._client is None:
            try:
                import anthropic
                self._client = anthropic.Anthropic(api_key=self.api_key)
            except ImportError:
                raise RuntimeError("anthropic package not installed. Run: pip install anthropic>=0.30.0")
        return self._client

    def _call_claude(self, system_prompt: str, user_message: str, max_tokens: int = 1024) -> str:
        """Make a Claude API call via Anthropic directly."""
        client = self._get_client()
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=max_tokens,
            system=system_prompt,
            messages=[{"role": "user", "content": user_message}],
        )
        return response.content[0].text

    def _call_databricks_model(self, endpoint_name: str, system_prompt: str, user_message: str, max_tokens: int, client) -> str:
        """Call a Databricks Model Serving endpoint (OpenAI chat format)."""
        config = client.config
        host = (config.host or "").rstrip("/")
        if not host:
            raise RuntimeError("Databricks host not configured. Please log in first.")

        # Build auth headers from SDK config
        headers = {"Content-Type": "application/json"}
        has_auth = False
        try:
            auth_headers = {}
            config.authenticate(auth_headers)
            headers.update(auth_headers)
            has_auth = bool(auth_headers)
        except Exception:
            pass
        if not has_auth:
            token = getattr(config, "token", None)
            if token:
                headers["Authorization"] = f"Bearer {token}"
                has_auth = True
        if not has_auth:
            raise RuntimeError("No authentication credentials available for Databricks. Please log in.")

        url = f"{host}/serving-endpoints/{endpoint_name}/invocations"
        payload = {
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message},
            ],
            "max_tokens": max_tokens,
        }

        logger.info(f"Calling Databricks model: {endpoint_name} at {host}")
        try:
            resp = requests.post(url, json=payload, headers=headers, timeout=120)
        except requests.ConnectionError:
            raise RuntimeError(f"Cannot connect to Databricks at {host}. Check your network connection.")
        except requests.Timeout:
            raise RuntimeError(f"Request to {endpoint_name} timed out after 120 seconds. The model may be starting up — try again.")

        if resp.status_code == 401:
            raise RuntimeError("Authentication failed. Your session may have expired — please log in again.")
        if resp.status_code == 404:
            raise RuntimeError(f"Serving endpoint '{endpoint_name}' not found. Check that the endpoint exists and is active.")
        if resp.status_code == 429:
            raise RuntimeError(f"Rate limited by {endpoint_name}. Please wait a moment and try again.")
        if resp.status_code >= 400:
            detail = resp.text[:200] if resp.text else f"HTTP {resp.status_code}"
            raise RuntimeError(f"Model serving error ({resp.status_code}): {detail}")

        data = resp.json()

        # OpenAI-compatible response format
        if "choices" in data and len(data.get("choices", [])) > 0:
            choice = data["choices"][0]
            if isinstance(choice, dict) and "message" in choice:
                return choice["message"].get("content", "")
            if isinstance(choice, dict) and "text" in choice:
                return choice["text"]
        # Fallback: some endpoints return differently
        if "predictions" in data and data["predictions"]:
            return str(data["predictions"][0])
        # Last resort
        logger.warning(f"Unexpected response format from {endpoint_name}: {list(data.keys())}")
        return str(data)

    def _call_llm(self, system_prompt: str, user_message: str, max_tokens: int = 1024, endpoint_name: str = None, client=None) -> str:
        """Route to Databricks or Anthropic based on configuration."""
        if endpoint_name and client:
            return self._call_databricks_model(endpoint_name, system_prompt, user_message, max_tokens, client)
        return self._call_claude(system_prompt, user_message, max_tokens)

    def summarize(self, context_type: str, data: dict, endpoint_name: str = None, client=None) -> str:
        """Generate a narrative summary for the given context."""
        system_prompt = _SYSTEM_PROMPTS.get(context_type, _SYSTEM_PROMPTS["dashboard"])
        user_message = f"Here is the {context_type} data to analyze:\n\n{json.dumps(data, indent=2, default=str)}"
        return self._call_llm(system_prompt, user_message, endpoint_name=endpoint_name, client=client)

    def parse_clone_query(self, query: str, available_catalogs: list[str], endpoint_name: str = None, client=None) -> dict:
        """Parse a natural language clone request into structured config."""
        system_prompt = _SYSTEM_PROMPTS["clone_builder"]
        user_message = (
            f"Available catalogs: {', '.join(available_catalogs) if available_catalogs else 'unknown'}\n\n"
            f"User request: {query}"
        )
        response = self._call_llm(system_prompt, user_message, endpoint_name=endpoint_name, client=client)
        try:
            text = response.strip()
            if text.startswith("```"):
                text = text.split("\n", 1)[1].rsplit("```", 1)[0]
            return json.loads(text)
        except (json.JSONDecodeError, IndexError):
            return {"explanation": response, "error": "Could not parse structured config"}

    def suggest_dq_rules(self, profiling_results: dict, table_name: str = "", endpoint_name: str = None, client=None) -> list[dict]:
        """Suggest data quality rules from profiling results."""
        system_prompt = _SYSTEM_PROMPTS["profiling"]
        user_message = f"Table: {table_name}\n\nProfiling results:\n{json.dumps(profiling_results, indent=2, default=str)}"
        response = self._call_llm(system_prompt, user_message, endpoint_name=endpoint_name, client=client)
        try:
            text = response.strip()
            if text.startswith("```"):
                text = text.split("\n", 1)[1].rsplit("```", 1)[0]
            return json.loads(text)
        except (json.JSONDecodeError, IndexError):
            return [{"raw_response": response}]

    def suggest_pii_remediation(self, scan_results: dict, endpoint_name: str = None, client=None) -> dict:
        """Suggest PII remediation actions."""
        system_prompt = _SYSTEM_PROMPTS["pii"]
        user_message = f"PII scan results:\n{json.dumps(scan_results, indent=2, default=str)}"
        response = self._call_llm(system_prompt, user_message, endpoint_name=endpoint_name, client=client)
        return {"summary": response, "recommendations": []}


# Singleton instance
_ai_service: Optional[AIService] = None


def get_ai_service() -> AIService:
    """Get or create the singleton AI service instance."""
    global _ai_service
    if _ai_service is None:
        _ai_service = AIService()
    return _ai_service
