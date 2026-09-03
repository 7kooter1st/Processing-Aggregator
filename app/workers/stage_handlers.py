from __future__ import annotations

import logging
from typing import Any

from app.config import settings
from app.metrics import metrics
from app.services.document_comparator import DocumentComparator
from app.workflow.repository import WorkflowRepository

logger = logging.getLogger(__name__)


class DiffStageHandler:
    def __init__(self, comparator: DocumentComparator, workflow: WorkflowRepository) -> None:
        self._comparator = comparator
        self._workflow = workflow

    async def __call__(self, payload: dict[str, Any]) -> None:
        job_id = str(payload.get("job_id") or "")
        event_id = str(payload.get("event_id") or payload.get("task_id") or "")
        if event_id:
            first = await self._workflow.claim_inbox(
                consumer_group=f"{settings.kafka_consumer_group}-diff",
                event_id=event_id,
                job_id=job_id,
                topic=settings.kafka_topic_diff_cmd,
            )
            if not first:
                metrics.inc("inbox_duplicates")
                return
        await self._comparator.compare_if_ready(job_id)
        metrics.inc("diff_jobs_processed")


class ClassifyStageHandler:
    def __init__(self, comparator: DocumentComparator, workflow: WorkflowRepository) -> None:
        self._comparator = comparator
        self._workflow = workflow

    async def __call__(self, payload: dict[str, Any]) -> None:
        job_id = str(payload.get("job_id") or "")
        event_id = str(payload.get("event_id") or payload.get("task_id") or "")
        if event_id:
            first = await self._workflow.claim_inbox(
                consumer_group=f"{settings.kafka_consumer_group}-classify",
                event_id=event_id,
                job_id=job_id,
                topic=settings.kafka_topic_classify_cmd,
            )
            if not first:
                metrics.inc("inbox_duplicates")
                return
        await self._comparator.compare_if_ready(job_id)
        metrics.inc("classify_jobs_processed")


class FinalizeStageHandler:
    def __init__(self, comparator: DocumentComparator, workflow: WorkflowRepository) -> None:
        self._comparator = comparator
        self._workflow = workflow

    async def __call__(self, payload: dict[str, Any]) -> None:
        job_id = str(payload.get("job_id") or "")
        event_id = str(payload.get("event_id") or payload.get("task_id") or "")
        if event_id:
            first = await self._workflow.claim_inbox(
                consumer_group=f"{settings.kafka_consumer_group}-finalize",
                event_id=event_id,
                job_id=job_id,
                topic=settings.kafka_topic_finalize_cmd,
            )
            if not first:
                metrics.inc("inbox_duplicates")
                return
        await self._comparator.compare_if_ready(job_id)
        metrics.inc("finalize_jobs_processed")
