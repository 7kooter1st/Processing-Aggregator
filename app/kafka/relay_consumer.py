import logging
from typing import Any, Awaitable, Callable

from pydantic import ValidationError

from app.config import settings
from app.kafka.manual_consumer import ManualCommitConsumer
from app.models.schemas import ProcessedResultMessage, StatusUpdateMessage
from app.services.result_aggregator import ResultAggregator
from app.services.websocket_hub import WebSocketHub

logger = logging.getLogger(__name__)


class AggregatorConsumers:
    """Kafka relay: processed_results → aggregator, status_updates → WebSocket."""

    def __init__(self, aggregator: ResultAggregator, ws_hub: WebSocketHub) -> None:
        self._processed_consumer = ManualCommitConsumer(
            topic=settings.kafka_topic_processed_results,
            group_id=settings.kafka_consumer_group_aggregator,
            handler=self._handle_processed_result,
            name="Aggregator",
        )
        self._status_consumer = ManualCommitConsumer(
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
