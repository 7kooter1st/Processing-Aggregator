from __future__ import annotations

import logging

from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError

from app.config import settings

logger = logging.getLogger(__name__)


def versioned_topics() -> list[str]:
    return [
        settings.kafka_topic_raw_chunks,
        settings.kafka_topic_processed_results,
        settings.kafka_topic_status_updates,
        settings.kafka_topic_dlt,
        settings.kafka_topic_ocr_cmd,
        settings.kafka_topic_diff_cmd,
        settings.kafka_topic_classify_cmd,
        settings.kafka_topic_finalize_cmd,
        settings.kafka_topic_stage_event,
        settings.kafka_topic_job_event,
        settings.kafka_topic_ocr_retry,
        settings.kafka_topic_ocr_dlt,
        "cmp.prepare.word.cmd.v1",
        "cmp.prepare.pdf.cmd.v1",
        "cmp.prepare.word.retry.v1",
        "cmp.prepare.pdf.retry.v1",
        "cmp.diff.retry.v1",
        "cmp.classify.retry.v1",
        "cmp.finalize.retry.v1",
        "cmp.prepare.word.dlt.v1",
        "cmp.prepare.pdf.dlt.v1",
        "cmp.diff.dlt.v1",
        "cmp.classify.dlt.v1",
        "cmp.finalize.dlt.v1",
    ]


async def ensure_topics() -> None:
    admin = AIOKafkaAdminClient(bootstrap_servers=settings.kafka_bootstrap_servers)
    await admin.start()
    try:
        cluster = await admin.describe_cluster()
        brokers = cluster.get("brokers") or cluster.get("nodes") or []
        broker_count = max(1, len(brokers) if isinstance(brokers, list) else 1)
        replication = max(1, min(settings.kafka_replication_factor, broker_count))
        created = False
        for rf in (replication, 1):
            topics = [
                NewTopic(name=name, num_partitions=3, replication_factor=rf)
                for name in versioned_topics()
            ]
            try:
                await admin.create_topics(topics)
                logger.info("[KAFKA] created %s topics rf=%s", len(topics), rf)
                created = True
                break
            except TopicAlreadyExistsError:
                logger.info("[KAFKA] topics already exist")
                created = True
                break
            except Exception:
                logger.exception("[KAFKA] topic create failed rf=%s", rf)
        if not created:
            logger.warning("[KAFKA] continuing without explicit topic create")
    finally:
        await admin.stop()
