import asyncio
import json
import logging
from typing import Any

from aiokafka import AIOKafkaConsumer
from pydantic import ValidationError

from app.config import settings
from app.logging_utils import summarize_for_log
from app.models.schemas import RawChunkMessage
from app.services.chunk_processor import ChunkProcessor

logger = logging.getLogger(__name__)


class KafkaConsumerWorker:
    def __init__(self, processor: ChunkProcessor, publisher) -> None:
        self._processor = processor
        self._publisher = publisher
        self._consumer: AIOKafkaConsumer | None = None
        self._task: asyncio.Task | None = None
        self._semaphore = asyncio.Semaphore(settings.consumer_max_concurrent)
        self._running = False

    @property
    def is_connected(self) -> bool:
        return self._consumer is not None and self._running

    async def start(self) -> None:
        self._consumer = AIOKafkaConsumer(
            settings.kafka_topic_raw_chunks,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=settings.kafka_consumer_group,
            value_deserializer=lambda value: json.loads(value.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
        )
        await self._consumer.start()
        self._running = True
        self._task = asyncio.create_task(self._consume_loop())
        logger.info(
            "Kafka consumer subscribed to %s (group=%s)",
            settings.kafka_topic_raw_chunks,
            settings.kafka_consumer_group,
        )

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._consumer:
            await self._consumer.stop()
            self._consumer = None

    async def _consume_loop(self) -> None:
        assert self._consumer is not None

        try:
            async for record in self._consumer:
                if not self._running:
                    break
                logger.info(
                    "[KAFKA IN] topic=%s partition=%s offset=%s key=%s payload=%s",
                    record.topic,
                    record.partition,
                    record.offset,
                    record.key.decode() if record.key else None,
                    summarize_for_log(record.value),
                )
                asyncio.create_task(self._handle_message(record.value))
        except asyncio.CancelledError:
            logger.info("Consumer loop cancelled")
        except Exception:
            logger.exception("Consumer loop failed")
            self._running = False

    async def _handle_message(self, payload: dict[str, Any]) -> None:
        job_id = payload.get("job_id", "?")
        chunk_index = payload.get("chunk_index", "?")
        total_chunks = payload.get("total_chunks", "?")
        logger.info(
            "[PROCESS] start job=%s chunk=%s/%s",
            job_id,
            chunk_index,
            total_chunks,
        )
        async with self._semaphore:
            try:
                message = RawChunkMessage.model_validate(payload)
                await self._processor.process(message)
                logger.info(
                    "[PROCESS] done job=%s chunk=%s/%s",
                    message.job_id,
                    message.chunk_index,
                    message.total_chunks,
                )
            except ValidationError as exc:
                logger.error(
                    "[PROCESS] validation error job=%s chunk=%s: %s",
                    job_id,
                    chunk_index,
                    exc,
                )
                await self._publisher.publish_to_dlt(payload, str(exc))
                await self._processor.handle_error(payload, f"Validation error: {exc}")
            except Exception as exc:
                # Do not log full exception with request bodies; keep message short.
                logger.error(
                    "[PROCESS] failed job=%s chunk=%s: %s",
                    job_id,
                    chunk_index,
                    exc,
                )
                await self._publisher.publish_to_dlt(payload, str(exc))
                await self._processor.handle_error(payload, str(exc))
