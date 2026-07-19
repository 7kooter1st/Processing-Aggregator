import asyncio
import logging
from typing import Any

from fastapi import WebSocket
from starlette.websockets import WebSocketDisconnect

from app.logging_utils import summarize_for_log
from app.models.schemas import ComparisonResult, StatusUpdateMessage, WebSocketEvent

logger = logging.getLogger(__name__)


class WebSocketHub:
    """Manages frontend WebSocket connections per job_id."""

    def __init__(self) -> None:
        self._connections: dict[str, set[WebSocket]] = {}
        self._lock = asyncio.Lock()

    async def connect(self, job_id: str, websocket: WebSocket) -> None:
        await websocket.accept()
        async with self._lock:
            self._connections.setdefault(job_id, set()).add(websocket)
            clients_count = len(self._connections[job_id])
        logger.info(
            "[WS] connected job=%s (clients: %s, hub_id=%s)",
            job_id,
            clients_count,
            id(self),
        )

    async def disconnect(self, job_id: str, websocket: WebSocket) -> None:
        async with self._lock:
            clients = self._connections.get(job_id)
            if clients is None:
                return
            clients.discard(websocket)
            if not clients:
                del self._connections[job_id]
        logger.info("[WS] disconnected job=%s", job_id)

    async def broadcast(self, job_id: str, event: WebSocketEvent) -> None:
        async with self._lock:
            clients = list(self._connections.get(job_id, set()))

        if not clients:
            logger.warning(
                "[WS] no clients for job=%s — event type=%s not delivered "
                "(frontend not connected? hub_id=%s tracked_jobs=%s)",
                job_id,
                event.type,
                id(self),
                list(self._connections.keys()),
            )
            return

        payload = event.model_dump()
        logger.info(
            "[WS] send job=%s type=%s clients=%s hub_id=%s payload=%s",
            job_id,
            event.type,
            len(clients),
            id(self),
            summarize_for_log(payload),
        )
        dead: list[WebSocket] = []

        for ws in clients:
            try:
                await ws.send_json(payload)
            except (WebSocketDisconnect, RuntimeError):
                dead.append(ws)
            except Exception:
                logger.exception("Failed to send WebSocket message for job=%s", job_id)
                dead.append(ws)

        for ws in dead:
            await self.disconnect(job_id, ws)

    async def send_status(self, status: StatusUpdateMessage) -> None:
        await self.broadcast(
            status.job_id,
            WebSocketEvent(
                type="status",
                job_id=status.job_id,
                data=status.model_dump(),
            ),
        )

    async def send_result(self, job_id: str, comparison: ComparisonResult) -> None:
        await self.broadcast(
            job_id,
            WebSocketEvent(
                type="result",
                job_id=job_id,
                data={"comparison": comparison.model_dump()},
            ),
        )

    async def send_error(self, job_id: str, message: str, details: dict[str, Any] | None = None) -> None:
        await self.broadcast(
            job_id,
            WebSocketEvent(
                type="error",
                job_id=job_id,
                data={"message": message, "details": details or {}},
            ),
        )
