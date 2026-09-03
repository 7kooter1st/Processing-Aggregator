from __future__ import annotations

import asyncio
import logging
from typing import Any

from app.config import settings
from app.kafka.producer import KafkaPublisher
from app.metrics import metrics
from app.workflow.repository import WorkflowRepository

logger = logging.getLogger(__name__)


class OutboxPublisher:
    def __init__(self, repo: WorkflowRepository, publisher: KafkaPublisher) -> None:
        self._repo = repo
        self._publisher = publisher
        self._task: asyncio.Task | None = None
        self._running = False

    async def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._run())
        logger.info("[OUTBOX] publisher started")

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
                rows = await self._repo.claim_unpublished_outbox()
                if not rows:
                    metrics.set_gauge(
                        "outbox_oldest_age_seconds",
                        await self._repo.oldest_outbox_age_seconds(),
                    )
                    await asyncio.sleep(settings.outbox_poll_interval_sec)
                    continue
                for row in rows:
                    await self._publish_one(row)
        except asyncio.CancelledError:
            logger.info("[OUTBOX] publisher cancelled")

    async def _publish_one(self, row: dict[str, Any]) -> None:
        payload = row["payload_json"]
        if isinstance(payload, str):
            import json

            payload = json.loads(payload)
        try:
            await self._publisher.publish_raw(
                row["topic"],
                row["message_key"],
                payload,
            )
            await self._repo.mark_outbox_published(str(row["id"]))
            metrics.inc("outbox_published")
        except Exception as exc:
            logger.exception("[OUTBOX] publish failed id=%s topic=%s", row["id"], row["topic"])
            await self._repo.mark_outbox_error(str(row["id"]), str(exc))
            metrics.inc("outbox_errors")
            await asyncio.sleep(1)
