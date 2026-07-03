import logging

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.services.result_aggregator import ResultAggregator
from app.services.websocket_hub import WebSocketHub

logger = logging.getLogger(__name__)

router = APIRouter()


def create_websocket_router(ws_hub: WebSocketHub, aggregator: ResultAggregator) -> APIRouter:
    @router.websocket("/ws/jobs/{job_id}")
    async def job_websocket(websocket: WebSocket, job_id: str) -> None:
        logger.info("[WS] incoming connection job=%s", job_id)
        await ws_hub.connect(job_id, websocket)

        existing = await aggregator.get_result(job_id)
        if existing is not None:
            logger.info("[WS] sending cached result to job=%s", job_id)
            await ws_hub.send_result(job_id, existing.comparison)

        try:
            while True:
                await websocket.receive_text()
        except WebSocketDisconnect:
            pass
        finally:
            await ws_hub.disconnect(job_id, websocket)

    return router
