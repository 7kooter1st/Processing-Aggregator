import logging
from typing import Any

import httpx

from app.config import settings

logger = logging.getLogger(__name__)


class OllamaClient:
    def __init__(self) -> None:
        self._base_url = settings.ollama_base_url.rstrip("/")
        self._model = settings.ollama_model
        self._timeout = settings.ollama_timeout_seconds
        self._temperature = settings.ollama_temperature
        self._think = settings.ollama_think

    async def is_available(self) -> bool:
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get(f"{self._base_url}/api/tags")
                return response.status_code == 200
        except httpx.HTTPError:
            return False

    async def chat(self, messages: list[dict[str, Any]]) -> dict[str, Any]:
        payload = {
            "model": self._model,
            "messages": messages,
            "stream": False,
            "format": "json",
            "think": self._think,
            "options": {
                "temperature": self._temperature,
            },
        }

        logger.info(
            "[OLLAMA] request job chunk model=%s messages=%s temperature=%s think=%s",
            self._model,
            len(messages),
            self._temperature,
            self._think,
        )

        async with httpx.AsyncClient(timeout=self._timeout) as client:
            response = await client.post(
                f"{self._base_url}/api/chat",
                json=payload,
            )
            response.raise_for_status()
            result = response.json()

        content = ((result.get("message") or {}).get("content")) or ""
        logger.info(
            "[OLLAMA] response status=%s content_len=%s preview=%s",
            response.status_code,
            len(content),
            content[:200] + "..." if len(content) > 200 else content,
        )
        return result
