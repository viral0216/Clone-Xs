"""Shared AI-drafting adapter for the unstructured demo generators.

Wraps :class:`src.ai_service.AIService` with a single
``draft(prompt, fallback)`` method that returns a one-or-two-sentence
narrative blurb. Routes through the user-picked Databricks Model
Serving endpoint when one is available (``X-Databricks-Model`` header
is the source of truth — the UI's api-client at
``ui/src/lib/api-client.ts`` sets it from ``localStorage.dbx_model``);
otherwise falls back to the Anthropic API path.

Used by all five unstructured-generator modules:

  * ``src.demo_documents``
  * ``src.demo_media``
  * ``src.demo_knowledge``
  * ``src.demo_logs``
  * ``src.demo_code``

Each orchestrator constructs one adapter per job and threads it into
every per-type generator. Generators call ``ai_client.draft(prompt,
fallback)``; the adapter is responsible for backend selection, error
handling, and the per-job token budget.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


_SYSTEM_PROMPT = (
    "You are a synthetic content generator for demo data. Output ONLY "
    "the requested content with no preamble, no explanation, no markdown "
    "formatting, no quotation marks. Keep any names, amounts, and dates "
    "that appear in the prompt verbatim. Output must be safe for direct "
    "insertion into a generated document or data record — plain text only."
)


class AIDrafter:
    """Thin narrative-text wrapper around ``AIService._call_llm``.

    On every ``draft()`` call:
      * If the per-job token budget has been exhausted, return the
        fallback immediately (no LLM call).
      * Otherwise call ``_call_llm`` (Databricks endpoint preferred;
        Anthropic fallback). On exception OR empty response, return the
        fallback.

    Per-job state (``_used`` token counter) is intentionally stored on
    the adapter — generators don't need to know it exists; they keep
    calling ``.draft()`` and the adapter degrades gracefully when the
    budget is hit.
    """

    def __init__(
        self,
        svc: Any,
        endpoint_name: str | None = None,
        sdk_client: Any | None = None,
        token_budget: int = 50_000,
    ) -> None:
        self._svc = svc
        self._endpoint = endpoint_name
        self._sdk_client = sdk_client
        self._budget = token_budget
        self._used = 0
        self._calls = 0
        self._fallbacks = 0

    @property
    def tokens_used(self) -> int:
        return self._used

    @property
    def calls_made(self) -> int:
        return self._calls

    @property
    def fallbacks(self) -> int:
        return self._fallbacks

    @property
    def backend(self) -> str:
        return f"databricks:{self._endpoint}" if self._endpoint else "anthropic"

    def draft(self, prompt: str, fallback: str, max_tokens: int = 200) -> str:
        if self._used >= self._budget:
            self._fallbacks += 1
            return fallback
        try:
            text = self._svc._call_llm(
                system_prompt=_SYSTEM_PROMPT,
                user_message=prompt,
                max_tokens=max_tokens,
                endpoint_name=self._endpoint,
                client=self._sdk_client,
            )
        except Exception as e:
            logger.debug("AI draft failed (will use fallback): %s", e)
            self._fallbacks += 1
            return fallback
        self._calls += 1
        # Approximate accounting — _call_llm doesn't surface usage,
        # so charge the full budget request. Errs toward stopping
        # early, which is the safe direction for cost control.
        self._used += max_tokens
        text = (text or "").strip()
        if not text:
            self._fallbacks += 1
            return fallback
        return text


def build_drafter(
    config: dict,
    sdk_client: Any | None = None,
) -> AIDrafter | None:
    """Construct an :class:`AIDrafter` from job config, or return None
    if AI mode is disabled / no backend is configured.

    Reads three keys from ``config``:

      * ``realistic_content`` (bool) — gate. If False → returns None.
      * ``ai_endpoint_name`` (str | None) — Databricks Model Serving
        endpoint (forwarded by routers from the ``X-Databricks-Model``
        header). When set, the Databricks path is used.
      * ``ai_token_budget`` (int) — soft cap, default 50,000.

    Falls back to None on any configuration / availability failure
    so callers can stay on the templated path.
    """
    if not bool(config.get("realistic_content", False)):
        return None
    try:
        from src.ai_service import get_ai_service

        svc = get_ai_service()
        endpoint_name = config.get("ai_endpoint_name") or None
        if not svc.is_available(endpoint_name):
            logger.warning(
                "realistic_content=True but no AI backend configured "
                "(no Databricks model picked in Settings + no "
                "ANTHROPIC_API_KEY set) — falling back to templates."
            )
            return None
        budget = int(config.get("ai_token_budget", 50_000))
        if budget <= 0:
            return None
        drafter = AIDrafter(
            svc,
            endpoint_name=endpoint_name,
            sdk_client=sdk_client,
            token_budget=budget,
        )
        logger.info(
            "AI mode ON: backend=%s, budget=%d tokens",
            drafter.backend,
            drafter._budget,
        )
        return drafter
    except Exception as e:
        logger.warning("AI client load failed, falling back to templates: %s", e)
        return None


def maybe_ai(
    drafter: AIDrafter | None,
    prompt: str,
    fallback: str,
    max_tokens: int = 150,
) -> str:
    """Single-line AI call with template fallback. Used pervasively
    in generators to keep the call sites terse."""
    if drafter is None:
        return fallback
    return drafter.draft(prompt, fallback, max_tokens=max_tokens)


def adapter_summary(drafter: AIDrafter | None) -> dict:
    """Return the AI-mode telemetry block to merge into the orchestrator
    result dict. Empty dict when AI mode wasn't engaged."""
    if drafter is None:
        return {}
    return {
        "ai_mode": True,
        "ai_backend": drafter.backend,
        "ai_calls": drafter.calls_made,
        "ai_tokens_used": drafter.tokens_used,
        "ai_fallbacks": drafter.fallbacks,
    }
