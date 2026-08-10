import json
import logging
from typing import Any

from aiokafka import AIOKafkaProducer

from app.config import settings
from app.logging_utils import summarize_for_log, strip_payload_for_dlt
from app.models.schemas import ProcessedResultMessage, StatusUpdateMessage

logger = logging.getLogger(__name__)


class KafkaPublisher:
    def __init__(self) -> None:
        self._producer: AIOKafkaProducer | None = None

    @property
    def is_connected(self) -> bool:
        return self._producer is not None

    async def start(self) -> None:
        self._producer = AIOKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            key_serializer=lambda key: key.encode("utf-8") if key else None,
            max_request_size=settings.kafka_max_request_size_bytes,
        )
        await self._producer.start()
        logger.info("Kafka producer connected to %s", settings.kafka_bootstrap_servers)

    async def stop(self) -> None:
        if self._producer:
            await self._producer.stop()
            self._producer = None

    async def publish_processed_result(self, result: ProcessedResultMessage) -> None:
        await self._send(
            topic=settings.kafka_topic_processed_results,
            key=result.job_id,
            value=result.model_dump(),
        )

    async def publish_status_update(self, status: StatusUpdateMessage) -> None:
        await self._send(
            topic=settings.kafka_topic_status_updates,
            key=status.job_id,
            value=status.model_dump(),
        )

    async def publish_to_dlt(self, payload: dict[str, Any], error: str) -> None:
        # Never push full page images into DLT — that floods logs and can exceed Kafka limits.
        safe_payload = strip_payload_for_dlt(payload)
        await self._send(
            topic=settings.kafka_topic_dlt,
            key=payload.get("job_id"),
            value={"original": safe_payload, "error": error[:500]},
        )

    async def _send(
        self,
        topic: str,
        key: str | None,
        value: dict[str, Any],
    ) -> None:
        if not self._producer:
            raise RuntimeError("Kafka producer is not started")

        await self._producer.send_and_wait(topic, value=value, key=key)
        logger.info(
            "[KAFKA OUT] topic=%s key=%s payload=%s",
            topic,
            key,
            summarize_for_log(value),
        )
