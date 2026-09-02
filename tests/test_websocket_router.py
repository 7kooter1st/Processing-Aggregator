import unittest

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.websocket import create_websocket_router


class FakeHub:
    def __init__(self) -> None:
        self.connected: list[str] = []
        self.disconnected: list[str] = []

    async def connect(self, job_id: str, websocket) -> None:
        await websocket.accept()
        self.connected.append(job_id)

    async def disconnect(self, job_id: str, websocket) -> None:
        self.disconnected.append(job_id)

    async def send_result(self, *_args, **_kwargs) -> None:
        return None


class FakeAggregator:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.calls = 0

    async def get_result(self, _job_id: str):
        self.calls += 1
        if self.error is not None:
            raise self.error
        return None


class WebSocketRouterTests(unittest.TestCase):
    def test_factory_returns_independent_routers(self) -> None:
        first = create_websocket_router(FakeHub(), FakeAggregator())
        second = create_websocket_router(FakeHub(), FakeAggregator())
        self.assertIsNot(first, second)

    def test_get_result_failure_keeps_socket_and_disconnects(self) -> None:
        hub = FakeHub()
        aggregator = FakeAggregator(
            RuntimeError("PostgreSQL OCR store is not started")
        )
        app = FastAPI()
        app.include_router(create_websocket_router(hub, aggregator))

        with TestClient(app) as client:
            with client.websocket_connect("/ws/jobs/job-1") as websocket:
                websocket.send_text("ping")

        self.assertEqual(aggregator.calls, 1)
        self.assertEqual(hub.connected, ["job-1"])
        self.assertEqual(hub.disconnected, ["job-1"])
