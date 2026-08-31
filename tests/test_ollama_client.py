import unittest
from unittest.mock import patch

import httpx

from app.config import settings
from app.services.ollama_client import OllamaClient


class OllamaClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_sends_configured_context_size(self) -> None:
        captured: dict = {}

        class FakeAsyncClient:
            def __init__(self, *args, **kwargs) -> None:
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, traceback) -> None:
                pass

            async def post(self, url: str, json: dict) -> httpx.Response:
                captured["payload"] = json
                return httpx.Response(
                    200,
                    request=httpx.Request("POST", url),
                    json={"message": {"content": "ok"}},
                )

        with patch(
            "app.services.ollama_client.httpx.AsyncClient",
            FakeAsyncClient,
        ):
            await OllamaClient().chat_text([])

        self.assertEqual(
            captured["payload"]["options"]["num_ctx"],
            settings.ollama_num_ctx,
        )
        self.assertEqual(settings.ollama_num_ctx, 8192)

    async def test_includes_ollama_body_in_client_error(self) -> None:
        class FakeAsyncClient:
            def __init__(self, *args, **kwargs) -> None:
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, traceback) -> None:
                pass

            async def post(self, url: str, json: dict) -> httpx.Response:
                return httpx.Response(
                    400,
                    request=httpx.Request("POST", url),
                    text='{"error":"request exceeds context size"}',
                )

        with patch(
            "app.services.ollama_client.httpx.AsyncClient",
            FakeAsyncClient,
        ):
            with self.assertRaisesRegex(
                httpx.HTTPStatusError,
                "request exceeds context size",
            ):
                await OllamaClient().chat_text([])


if __name__ == "__main__":
    unittest.main()
