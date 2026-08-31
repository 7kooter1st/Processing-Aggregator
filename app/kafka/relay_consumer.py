import asyncio
import json
import logging
from typing import Any, Callable, Awaitable

from aiokafka import AIOKafkaConsumer
from pydantic import ValidationError

from app.config import settings
from app.logging_utils import summarize_for_log
from app.models.schemas import ProcessedResultMessage, StatusUpdateMessage
from app.services.result_aggregator import ResultAggregator
from app.services.websocket_hub import WebSocketHub

logger = logging.getLogger(__name__)


class KafkaTopicConsumer:
    def __init__(
        self,
        topic: str,
        group_id: str,
        handler: Callable[[dict[str, Any]], Awaitable[None]],
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
        self._task: asyncio.Task | None = None
        self._running = False

    @property
    def is_connected(self) -> bool:
        return self._consumer is not None and self._running

    async def start(self) -> None:
        self._consumer = AIOKafkaConsumer(
            self._topic,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=self._group_id,
            value_deserializer=lambda value: json.loads(value.decode("utf-8")),
            auto_offset_reset=self._auto_offset_reset,
            enable_auto_commit=True,
        )
        await self._consumer.start()
        self._running = True
        self._task = asyncio.create_task(self._consume_loop())
        logger.info(
            "%s subscribed to %s (group=%s)",
            self._name,
            self._topic,
            self._group_id,
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
                    "[KAFKA IN] %s topic=%s partition=%s offset=%s key=%s payload=%s",
                    self._name,
                    record.topic,
                    record.partition,
                    record.offset,
                    record.key.decode() if record.key else None,
                    summarize_for_log(record.value),
                )
                try:
                    await self._handler(record.value)
                except Exception:
                    logger.exception("%s failed to handle message", self._name)
        except asyncio.CancelledError:
            logger.info("%s consumer loop cancelled", self._name)
        except Exception:
            logger.exception("%s consumer loop failed", self._name)
            self._running = False


class AggregatorConsumers:
    """Kafka relay: processed_results → aggregator, status_updates → WebSocket."""

    def __init__(self, aggregator: ResultAggregator, ws_hub: WebSocketHub) -> None:
        self._processed_consumer = KafkaTopicConsumer(
            topic=settings.kafka_topic_processed_results,
            group_id=settings.kafka_consumer_group_aggregator,
            handler=self._handle_processed_result,
            name="Aggregator",
        )
        self._status_consumer = KafkaTopicConsumer(
            topic=settings.kafka_topic_status_updates,
            group_id=f"{settings.kafka_consumer_group_aggregator}-status",
            handler=self._handle_status_update,
            name="StatusRelay",
            auto_offset_reset="latest",
        )
        self._aggregator = aggregator
        self._ws_hub = ws_hub

    @property
    def is_connected(self) -> bool:
        return self._processed_consumer.is_connected and self._status_consumer.is_connected

    async def start(self) -> None:
        await self._processed_consumer.start()
        await self._status_consumer.start()

    async def stop(self) -> None:
        await self._processed_consumer.stop()
        await self._status_consumer.stop()

    async def _handle_processed_result(self, payload: dict[str, Any]) -> None:
        try:
            message = ProcessedResultMessage.model_validate(payload)
            logger.info(
                "[AGGREGATOR] received job=%s chunk=%s/%s fragment=%s",
                message.job_id,
                message.chunk_index,
                message.total_chunks,
                "yes" if message.comparison_fragment else "no",
            )
            await self._aggregator.handle_processed_result(message)
        except ValidationError as exc:
            logger.error("[AGGREGATOR] invalid processed_results: %s", exc)

    async def _handle_status_update(self, payload: dict[str, Any]) -> None:
        try:
            status = StatusUpdateMessage.model_validate(payload)
            logger.info(
                "[STATUS RELAY] job=%s status=%s progress=%s/%s msg=%s",
                status.job_id,
                status.status,
                status.processed_chunks,
                status.total_chunks,
                status.message,
            )
            await self._ws_hub.send_status(status)
        except ValidationError as exc:
            logger.error("[STATUS RELAY] invalid status_updates: %s", exc)
