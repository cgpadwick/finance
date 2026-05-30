"""Thin LLM client — the MLE-Beast pattern, trimmed to what this app needs.

One OpenAI-SDK client pointed at OpenRouter's base_url, so any model is just a
slug (e.g. "deepseek/deepseek-v4-flash", "anthropic/claude-sonnet-4-6"). Append
":online" to a slug for OpenRouter's built-in web-search grounding.

We deliberately skip MLE-Beast's instructor/structured-output + node-graph
machinery: this feature returns free-form markdown, so a plain chat completion
is the faithful, minimal port. (Adding `instructor` + a Pydantic response_model
is the upgrade path if we ever want typed research output.)
"""

from __future__ import annotations

import time

import httpx

from finance.config import Settings, get_settings

OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
# Per-phase timeouts: a single scalar timeout can fail to bound the socket read
# and hang for minutes (a bug MLE-Beast hit). Bound each phase explicitly.
_HTTP_TIMEOUT = httpx.Timeout(connect=10.0, read=180.0, write=60.0, pool=10.0)
_RETRIES = 3


def get_client(settings: Settings | None = None):
    """OpenAI client configured for OpenRouter. Raises if no key."""
    from openai import OpenAI

    settings = settings or get_settings()
    if not settings.openrouter_api_key:
        raise RuntimeError("OPENROUTER_API_KEY not set")
    return OpenAI(
        api_key=settings.openrouter_api_key,
        base_url=OPENROUTER_BASE_URL,
        timeout=_HTTP_TIMEOUT,
        max_retries=2,
    )


def complete(
    messages: list[dict],
    model: str,
    max_tokens: int = 1500,
    settings: Settings | None = None,
    **kwargs,
) -> str:
    """Single-shot chat completion → text content, with retry + backoff."""
    client = get_client(settings)
    last_err: Exception | None = None
    for attempt in range(_RETRIES):
        try:
            resp = client.chat.completions.create(
                model=model, messages=messages, max_tokens=max_tokens, **kwargs
            )
            return (resp.choices[0].message.content or "").strip()
        except Exception as e:  # noqa: BLE001 - retry transient failures
            last_err = e
            if attempt < _RETRIES - 1:
                time.sleep(2**attempt)
    raise last_err  # type: ignore[misc]
