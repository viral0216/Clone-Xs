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
import time
from typing import Any

logger = logging.getLogger(__name__)

# Retry tuning for rate-limit errors. Three attempts with exponential
# backoff (0.5s, 1.5s, 4.5s) — total worst-case ~6.5s before falling
# back. Only triggers on "Rate limited" exceptions; other failures
# (auth, 404, network) fall back immediately.
_RATE_LIMIT_BACKOFF_S: tuple[float, ...] = (0.5, 1.5, 4.5)


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

    def draft(
        self,
        prompt: str,
        fallback: str,
        max_tokens: int = 200,
        *,
        image_bytes: bytes | None = None,
        image_mime: str | None = None,
    ) -> str:
        if self._used >= self._budget:
            self._fallbacks += 1
            logger.warning(
                "AI fallback (budget exhausted, %d/%d tokens used)",
                self._used,
                self._budget,
            )
            return fallback

        # Retry loop — only re-tries on rate-limit (429) errors. The
        # serving endpoint surfaces these as RuntimeError("Rate limited
        # by …"); we sleep with exponential backoff and try again. Any
        # other exception (auth, 404, network, model error) breaks
        # the loop immediately and falls back.
        text: str | None = None
        last_exc: Exception | None = None
        for attempt, backoff in enumerate((0.0, *_RATE_LIMIT_BACKOFF_S)):
            if backoff > 0.0:
                time.sleep(backoff)
            try:
                text = self._svc._call_llm(
                    system_prompt=_SYSTEM_PROMPT,
                    user_message=prompt,
                    max_tokens=max_tokens,
                    endpoint_name=self._endpoint,
                    client=self._sdk_client,
                    image_bytes=image_bytes,
                    image_mime=image_mime,
                )
                last_exc = None
                break
            except Exception as e:
                last_exc = e
                if "Rate limited" in str(e) and attempt < len(_RATE_LIMIT_BACKOFF_S):
                    logger.info(
                        "AI rate-limited (attempt %d), backing off %.1fs",
                        attempt + 1,
                        _RATE_LIMIT_BACKOFF_S[attempt],
                    )
                    continue
                break

        if last_exc is not None:
            logger.warning(
                "AI fallback (exception: %s): prompt[:80]=%r",
                last_exc,
                prompt[:80],
            )
            self._fallbacks += 1
            return fallback
        self._calls += 1
        # Approximate accounting — _call_llm doesn't surface usage,
        # so charge the full budget request. Errs toward stopping
        # early, which is the safe direction for cost control.
        self._used += max_tokens
        text = (text or "").strip()
        if not text:
            logger.warning(
                "AI fallback (empty response from %s): prompt[:80]=%r, max_tokens=%d",
                self.backend,
                prompt[:80],
                max_tokens,
            )
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
    # When a deterministic faker_seed is set, skip AI entirely so test
    # runs and reproducibility scenarios stay byte-stable. Production
    # runs (no seed) get the full AI experience.
    if config.get("faker_seed") is not None:
        logger.info(
            "faker_seed=%s set — skipping AI for reproducibility, using templates instead",
            config.get("faker_seed"),
        )
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
    *,
    image_bytes: bytes | None = None,
    image_mime: str | None = None,
) -> str:
    """Single-line AI call with template fallback. Used pervasively
    in generators to keep the call sites terse.

    Pass ``image_bytes`` for image-grounded calls (the Databricks
    Llama 4 / Claude vision endpoints accept inline images via the
    OpenAI ``image_url`` content block). Ignored when no AI backend
    is configured or the model is text-only.
    """
    if drafter is None:
        return fallback
    return drafter.draft(
        prompt,
        fallback,
        max_tokens=max_tokens,
        image_bytes=image_bytes,
        image_mime=image_mime,
    )


def maybe_ai_json(
    drafter: AIDrafter | None,
    prompt: str,
    fallback_dict: dict,
    max_tokens: int = 400,
    *,
    image_bytes: bytes | None = None,
    image_mime: str | None = None,
) -> dict:
    """Single multimodal AI call that returns a parsed JSON object.

    Mirrors ``maybe_ai`` for the JSON-output case: one call, parse the
    response, fill any missing keys from ``fallback_dict``. On any
    failure (no drafter, exception, non-JSON response, parse error)
    returns ``fallback_dict`` unchanged so callers always get the full
    expected key set.

    Strips common LLM noise (markdown code fences, leading/trailing
    prose) before parsing — Foundation Models occasionally wrap JSON
    in ```json ... ``` even when told not to.
    """
    if drafter is None:
        return dict(fallback_dict)

    raw = drafter.draft(
        prompt,
        fallback="",
        max_tokens=max_tokens,
        image_bytes=image_bytes,
        image_mime=image_mime,
    )
    if not raw:
        return dict(fallback_dict)

    import json
    import re

    text = raw.strip()
    # Strip ```json ... ``` or ``` ... ``` fences if present.
    fence = re.match(r"^```(?:json)?\s*(.*?)\s*```$", text, re.DOTALL)
    if fence:
        text = fence.group(1).strip()
    # If the model prefixed prose, slice from the first { to the last }.
    first = text.find("{")
    last = text.rfind("}")
    if first != -1 and last > first:
        text = text[first : last + 1]

    try:
        parsed = json.loads(text)
    except (ValueError, TypeError) as e:
        logger.warning("maybe_ai_json: parse failed (%s); raw[:120]=%r", e, raw[:120])
        return dict(fallback_dict)
    if not isinstance(parsed, dict):
        logger.warning("maybe_ai_json: response was not an object; got %s", type(parsed).__name__)
        return dict(fallback_dict)

    # Merge: parsed wins for keys it provides, fallback fills the rest.
    merged = dict(fallback_dict)
    for k, v in parsed.items():
        if v is None:
            continue
        merged[k] = v
    return merged


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
