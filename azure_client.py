"""
Enpro Filtration Mastermind Portal — Azure OpenAI Client
Async wrapper for gpt-4.1-mini (router) and gpt-4.1 (reasoning) deployments.
"""

import json
import logging
from typing import AsyncIterator, Optional

import httpx

from config import settings

logger = logging.getLogger("enpro.azure_client")

# Shared async client — initialized on first use, closed on shutdown
_client: Optional[httpx.AsyncClient] = None


def _get_base_url() -> str:
    """Build base URL from endpoint."""
    endpoint = settings.AZURE_OPENAI_ENDPOINT.rstrip("/")
    return f"{endpoint}/openai/deployments"


def _get_headers() -> dict:
    """Auth headers for Azure OpenAI."""
    return {
        "api-key": settings.AZURE_OPENAI_KEY,
        "Content-Type": "application/json",
    }


async def get_client() -> httpx.AsyncClient:
    """Get or create the shared async HTTP client.

    Uses an unbounded read timeout so streaming responses don't get killed
    mid-stream by Azure pauses; non-streaming calls still set their own
    timeout via the per-request post() call when needed.
    """
    global _client
    if _client is None or _client.is_closed:
        _client = httpx.AsyncClient(
            timeout=httpx.Timeout(None, connect=10.0)
        )
    return _client


async def close_client():
    """Close the shared async HTTP client."""
    global _client
    if _client and not _client.is_closed:
        await _client.aclose()
        _client = None


async def chat_completion(
    deployment: str,
    messages: list[dict],
    temperature: float = 0.3,
    max_tokens: int = 1024,
) -> dict:
    """
    Call Azure OpenAI Chat Completions API.

    Args:
        deployment: Model deployment name (e.g., 'gpt-4.1-mini', 'gpt-4.1').
        messages: List of message dicts with 'role' and 'content'.
        temperature: Sampling temperature.
        max_tokens: Max response tokens.

    Returns:
        Full API response dict.

    Raises:
        httpx.HTTPStatusError: On non-2xx response.
    """
    client = await get_client()
    url = f"{_get_base_url()}/{deployment}/chat/completions?api-version={settings.AZURE_OPENAI_API_VERSION}"

    payload = {
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
    }

    logger.debug(f"Azure OpenAI request: deployment={deployment}, messages={len(messages)}")

    response = await client.post(url, headers=_get_headers(), json=payload)
    response.raise_for_status()

    data = response.json()
    logger.debug(f"Azure OpenAI response: tokens={data.get('usage', {})}")
    return data


async def route_message(system_prompt: str, user_message: str) -> str:
    """
    Quick classification via gpt-4.1-mini (router).
    Returns the raw text content of the response.
    """
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_message},
    ]
    data = await chat_completion(
        deployment=settings.AZURE_DEPLOYMENT_ROUTER,
        messages=messages,
        temperature=0.0,
        max_tokens=64,
    )
    return data["choices"][0]["message"]["content"].strip()


async def reason(
    system_prompt: str,
    messages: list[dict],
    temperature: float = 0.3,
    max_tokens: int = 2048,
) -> str:
    """
    Full reasoning via gpt-4.1 deployment.
    Returns the raw text content of the response.
    """
    full_messages = [{"role": "system", "content": system_prompt}] + messages
    data = await chat_completion(
        deployment=settings.AZURE_DEPLOYMENT_REASONING,
        messages=full_messages,
        temperature=temperature,
        max_tokens=max_tokens,
    )
    return data["choices"][0]["message"]["content"].strip()


async def reason_stream(
    system_prompt: str,
    messages: list[dict],
    temperature: float = 0.3,
    max_tokens: int = 2048,
) -> AsyncIterator[str]:
    """
    Stream raw token deltas from the reasoning deployment (gpt-52-instant).

    Yields incremental content strings as they arrive from Azure OpenAI.
    Caller is responsible for buffering the full response and parsing it
    as JSON at end-of-stream. Uses response_format=json_object so the
    final concatenated buffer is guaranteed to be a syntactically valid
    JSON object (verified V2.14 ground-truth test).
    """
    client = await get_client()
    url = (
        f"{_get_base_url()}/{settings.AZURE_DEPLOYMENT_REASONING}"
        f"/chat/completions?api-version={settings.AZURE_OPENAI_API_VERSION}"
    )
    payload = {
        "messages": [{"role": "system", "content": system_prompt}] + messages,
        "temperature": temperature,
        "max_completion_tokens": max_tokens,
        "stream": True,
        "response_format": {"type": "json_object"},
    }

    logger.debug(
        f"Azure OpenAI stream: deployment={settings.AZURE_DEPLOYMENT_REASONING}, "
        f"messages={len(messages)}"
    )

    async with client.stream(
        "POST", url, headers=_get_headers(), json=payload
    ) as response:
        response.raise_for_status()
        async for line in response.aiter_lines():
            if not line.startswith("data: "):
                continue
            data_str = line[6:]
            if data_str == "[DONE]":
                break
            try:
                chunk = json.loads(data_str)
                delta = chunk["choices"][0]["delta"].get("content", "")
                if delta:
                    yield delta
            except (json.JSONDecodeError, KeyError, IndexError):
                continue


async def health_check() -> dict:
    """
    Check Azure OpenAI connectivity.
    Returns dict with 'healthy' bool and 'detail' string.
    """
    if not settings.AZURE_OPENAI_ENDPOINT or not settings.AZURE_OPENAI_KEY:
        return {"healthy": False, "detail": "Azure OpenAI credentials not configured"}

    try:
        data = await chat_completion(
            deployment=settings.AZURE_DEPLOYMENT_ROUTER,
            messages=[{"role": "user", "content": "ping"}],
            temperature=0.0,
            max_tokens=4,
        )
        return {"healthy": True, "detail": "Azure OpenAI responding"}
    except Exception as e:
        logger.warning(f"Azure OpenAI health check failed: {e}")
        return {"healthy": False, "detail": str(e)}
