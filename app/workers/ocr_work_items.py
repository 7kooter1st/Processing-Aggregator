from __future__ import annotations

import asyncio
import json
import logging
import os

from app.config import settings
from app.workers.ocr_worker import OcrStageHandler
from app.workflow.repository import WorkflowRepository

logger = logging.getLogger(__name__)


class OcrWorkItemWorker:
    """PG-backed OCR dispatch so a Kafka delay does not freeze ready pages."""

    def __init__(self, repo: WorkflowRepository, handler: OcrStageHandler) -> None:
        self._repo = repo
        self._handler = handler
        self._task: asyncio.Task | None = None
        self._running = False
        self._owner = settings.worker_id or f"ocr-{os.getpid()}"

    async def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._run())
        logger.info("[OCR WORK ITEMS] worker started")

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _run(self) -> None:
        try:
            while self._running:
                item = await self._repo.lease_work_item(
                    stage="ocr",
                    owner=self._owner,
                    lease_seconds=settings.lease_seconds,
                )
                if item is None:
                    await asyncio.sleep(settings.work_item_poll_interval_sec)
                    continue
                payload = item["payload_json"]
                if isinstance(payload, str):
                    payload = json.loads(payload)
                try:
                    await self._handler(payload)
                    await self._repo.complete_work_item(
                        work_item_id=str(item["id"]),
                        lease_token=str(item["lease_token"]),
                        lease_epoch=int(item["lease_epoch"]),
                        outcome="succeeded",
                    )
                except Exception as exc:
                    logger.exception("[OCR WORK ITEMS] failed task=%s", item.get("task_id"))
                    await self._repo.retry_work_item(
                        work_item_id=str(item["id"]),
                        lease_token=str(item["lease_token"]),
                        lease_epoch=int(item["lease_epoch"]),
                        delay_seconds=min(60, 2 ** int(item["attempt"])),
                        error=str(exc),
                    )
        except asyncio.CancelledError:
            logger.info("[OCR WORK ITEMS] cancelled")
