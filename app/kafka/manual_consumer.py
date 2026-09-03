from __future__ import annotations

import json
import logging
from typing import Any, Awaitable, Callable

from aiokafka import AIOKafkaConsumer, ConsumerRecord, TopicPartition

from app.config import settings
from app.logging_utils import summarize_for_log

logger = logging.getLogger(__name__)

Handler = Callable[[dict[str, Any]], Awaitable[None]]


class ManualCommitConsumer:
    def __init__(
        self,
        topic: str,
        group_id: str,
        handler: Handler,
        name: str,
        *,
        auto_offset_reset: str = "earliest",
    ) -> None:
        self._topic = topic
        self._group_id = group_id
        self._handler = handler
        self._name = name
        self._auto_offset_reset = auto_offset_reset
        self._consumer: AIOKafkaConsumer | None = None
        self._task = None
        self._running = False
        self._busy = None

    @property
    def is_connected(self) -> bool:
        return self._consumer is not None and self._running

    async def start(self) -> None:
        import asyncio

        self._busy = asyncio.Event()
        self._busy.set()
        self._consumer = AIOKafkaConsumer(
            self._topic,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=self._group_id,
            value_deserializer=lambda value: json.loads(value.decode("utf-8")),
            auto_offset_reset=self._auto_offset_reset,
            enable_auto_commit=False,
            isolation_level="read_committed",
            max_poll_records=settings.kafka_max_poll_records,
            max_poll_interval_ms=settings.kafka_max_poll_interval_ms,
            session_timeout_ms=settings.kafka_session_timeout_ms,
        )
        await self._consumer.start()
        self._running = True
        self._task = asyncio.create_task(self._consume_loop())
        logger.info("%s subscribed to %s (manual commit)", self._name, self._topic)

    async def stop(self) -> None:
        import asyncio

        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._busy is not None:
            await self._busy.wait()
        if self._consumer:
            await self._consumer.stop()
            self._consumer = None

    async def _consume_loop(self) -> None:
        import asyncio

        assert self._consumer is not None
        try:
            async for record in self._consumer:
                if not self._running:
                    break
                logger.info(
                    "[KAFKA IN] %s topic=%s partition=%s offset=%s payload=%s",
                    self._name,
                    record.topic,
                    record.partition,
                    record.offset,
                    summarize_for_log(record.value),
                )
                self._busy.clear()
                try:
                    await self._handler(record.value if isinstance(record.value, dict) else {})
                    await self._commit(record)
                except Exception:
                    logger.exception("%s failed to handle message; offset not committed", self._name)
                    await asyncio.sleep(2)
                finally:
                    self._busy.set()
        except asyncio.CancelledError:
            logger.info("%s consumer loop cancelled", self._name)
        except Exception:
            logger.exception("%s consumer loop failed", self._name)
            self._running = False

    async def _commit(self, record: ConsumerRecord) -> None:
        assert self._consumer is not None
        tp = TopicPartition(record.topic, record.partition)
        await self._consumer.commit({tp: record.offset + 1})
