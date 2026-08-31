import asyncio
import logging
from typing import Any, Literal

import httpx

from app.config import settings

logger = logging.getLogger(__name__)

ResponseFormat = Literal["json", "text"]


class OllamaClient:
    def __init__(self) -> None:
        self._base_url = settings.ollama_base_url.rstrip("/")
        self._model = settings.ollama_model
        self._num_ctx = settings.ollama_num_ctx
        self._timeout = settings.ollama_timeout_seconds
        self._temperature = settings.ollama_temperature
        self._think = settings.ollama_think
        self._retries = settings.ollama_retries
        self._retry_delay_seconds = settings.ollama_retry_delay_seconds

    async def is_available(self) -> bool:
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get(f"{self._base_url}/api/tags")
                return response.status_code == 200
        except httpx.HTTPError:
            return False

    async def wait_until_available(
        self,
        *,
        timeout_seconds: float | None = None,
        poll_interval_seconds: float = 2.0,
    ) -> bool:
        """Poll Ollama until /api/tags succeeds or timeout."""
        timeout = (
            settings.ollama_startup_timeout_seconds
            if timeout_seconds is None
            else timeout_seconds
        )
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        attempt = 0
        while True:
            attempt += 1
            if await self.is_available():
                logger.info("[OLLAMA] available at %s (attempt=%s)", self._base_url, attempt)
                return True
            remaining = deadline - loop.time()
            if remaining <= 0:
                logger.error(
                    "[OLLAMA] not available at %s after %.0fs",
                    self._base_url,
                    timeout,
                )
                return False
            logger.warning(
                "[OLLAMA] waiting for %s (attempt=%s, %.0fs left)...",
                self._base_url,
                attempt,
                remaining,
            )
            await asyncio.sleep(min(poll_interval_seconds, max(remaining, 0.1)))

    async def chat_text(self, messages: list[dict[str, Any]]) -> dict[str, Any]:
        """OCR phase: plain text response, no JSON format constraint."""
        return await self._chat(messages, response_format="text", phase="OCR")

    async def chat_json(self, messages: list[dict[str, Any]]) -> dict[str, Any]:
        """Compare phase: force JSON response."""
        return await self._chat(messages, response_format="json", phase="COMPARE")

    async def _chat(
        self,
        messages: list[dict[str, Any]],
        *,
        response_format: ResponseFormat,
        phase: str,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "model": self._model,
            "messages": messages,
            "stream": False,
            # Always set think explicitly. Gemma4 defaults to thinking ON when the
            # key is omitted; vision then often returns empty message.content
            # (tokens spent in message.thinking, done_reason=length).
            "think": self._think,
            "options": {
                "temperature": self._temperature,
                "num_ctx": self._num_ctx,
            },
        }
        if response_format == "json":
            payload["format"] = "json"

        image_count = sum(
            1
            for msg in messages
            if isinstance(msg, dict) and msg.get("images")
        )
        logger.info(
            "[OLLAMA] %s request model=%s messages=%s images=%s "
            "temperature=%s num_ctx=%s think=%s format=%s",
            phase,
            self._model,
            len(messages),
            image_count,
            self._temperature,
            self._num_ctx,
            self._think,
            response_format,
        )

        last_error: Exception | None = None
        attempts = max(1, self._retries + 1)

        for attempt in range(1, attempts + 1):
            try:
                async with httpx.AsyncClient(timeout=self._timeout) as client:
                    response = await client.post(
                        f"{self._base_url}/api/chat",
                        json=payload,
                    )
                    if response.status_code >= 400:
                        body_preview = (response.text or "")[:500]
                        raise httpx.HTTPStatusError(
                            f"Ollama {response.status_code}: {body_preview}",
                            request=response.request,
                            response=response,
                        )
                    response.raise_for_status()
                    result = response.json()

                message = result.get("message") or {}
                content = message.get("content") or ""
                thinking = message.get("thinking") or ""
                if not isinstance(content, str):
                    content = ""
                if not isinstance(thinking, str):
                    thinking = ""

                logger.info(
                    "[OLLAMA] %s response status=%s done_reason=%s content_len=%s "
                    "thinking_len=%s preview=%s",
                    phase,
                    response.status_code,
                    result.get("done_reason"),
                    len(content),
                    len(thinking),
                    content[:200] + "..." if len(content) > 200 else content,
                )

                # Empty final answer after a thinking-only budget burn: retry once
                # with think forced off (covers misconfigured OLLAMA_THINK=true).
                if (
                    phase == "OCR"
                    and not content.strip()
                    and thinking.strip()
                    and payload.get("think") is True
                    and attempt < attempts
                ):
                    logger.warning(
                        "[OLLAMA] OCR got empty content with thinking_len=%s "
                        "(done_reason=%s) — retry with think=false",
                        len(thinking),
                        result.get("done_reason"),
                    )
                    payload["think"] = False
                    delay = self._retry_delay_seconds * attempt
                    await asyncio.sleep(delay)
                    continue

                return result
            except (
                httpx.ConnectError,
                httpx.ConnectTimeout,
                httpx.ReadTimeout,
                httpx.HTTPStatusError,
            ) as exc:
                last_error = exc
                status = getattr(getattr(exc, "response", None), "status_code", None)
                # Do not retry client errors (4xx), only transport / 5xx.
                if isinstance(exc, httpx.HTTPStatusError) and status is not None and status < 500:
                    raise
                if attempt >= attempts:
                    break
                delay = self._retry_delay_seconds * attempt
                logger.warning(
                    "[OLLAMA] %s failed (attempt %s/%s): %s — retry in %.1fs",
                    phase,
                    attempt,
                    attempts,
                    exc,
                    delay,
                )
                await asyncio.sleep(delay)

        assert last_error is not None
        raise last_error
