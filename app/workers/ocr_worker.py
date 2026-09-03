from __future__ import annotations

import base64
import logging
from typing import Any

from app.config import settings
from app.metrics import metrics
from app.models.schemas import ChunkContent, ContentType, RawChunkMessage
from app.services.chunk_processor import ChunkProcessor
from app.services.object_store import get_object_store
from app.workflow.repository import WorkflowRepository

logger = logging.getLogger(__name__)


class OcrStageHandler:
    def __init__(
        self,
        processor: ChunkProcessor,
        workflow: WorkflowRepository,
        compare_scheduler,
        publisher,
    ) -> None:
        self._processor = processor
        self._workflow = workflow
        self._compare_scheduler = compare_scheduler
        self._publisher = publisher
        self._store = get_object_store()

    async def __call__(self, payload: dict[str, Any]) -> None:
        event_id = str(payload.get("event_id") or payload.get("task_id") or "")
        job_id = str(payload.get("job_id") or "")
        if event_id:
            first = await self._workflow.claim_inbox(
                consumer_group=f"{settings.kafka_consumer_group}-ocr-v2",
                event_id=event_id,
                job_id=job_id,
                topic=settings.kafka_topic_ocr_cmd,
            )
            if not first:
                logger.info("[OCR V2] duplicate event=%s job=%s", event_id, job_id)
                metrics.inc("inbox_duplicates")
                return
        inner = payload.get("payload") if isinstance(payload.get("payload"), dict) else payload
        image_keys = _image_object_keys(inner)
        message = await self._to_raw_chunk(inner, payload)
        ready = await self._processor.process(message)
        await self._discard_page_images(image_keys)
        metrics.inc("ocr_pages_processed")
        if ready:
            await self._compare_scheduler.submit(message.job_id)

    async def _to_raw_chunk(self, inner: dict[str, Any], envelope: dict[str, Any]) -> RawChunkMessage:
        if inner.get("file1") or inner.get("file2"):
            if not _is_object_ref(inner.get("file1")) and not _is_object_ref(inner.get("file2")):
                return RawChunkMessage.model_validate(inner)
        job_id = str(inner.get("job_id") or envelope.get("job_id"))
        chunk_index = int(inner.get("chunk_index") or 1)
        total_chunks = int(inner.get("total_chunks") or 1)
        return RawChunkMessage(
            job_id=job_id,
            document_id=str(inner.get("document_id") or job_id),
            chunk_index=chunk_index,
            total_chunks=total_chunks,
            file1=await self._load_side(inner.get("file1")),
            file2=await self._load_side(inner.get("file2")),
        )

    async def _load_side(self, spec: dict[str, Any] | None) -> ChunkContent | None:
        if spec is None:
            return None
        if spec.get("content") and not spec.get("object_key"):
            return ChunkContent.model_validate(spec)
        key = spec.get("object_key") or spec.get("key")
        if not key:
            return None
        data = await self._store.get_bytes(key)
        content_type = spec.get("content_type") or "image/png"
        filename = spec.get("filename") or key.rsplit("/", 1)[-1]
        fmt = spec.get("format") or ("pdf" if "png" in content_type else "docx")
        if content_type.startswith("text/"):
            return ChunkContent(
                filename=filename,
                format=fmt,
                content_type=ContentType.TEXT,
                content=data.decode("utf-8"),
            )
        return ChunkContent(
            filename=filename,
            format=fmt,
            content_type=ContentType.IMAGE,
            content=base64.b64encode(data).decode("ascii"),
        )

    async def _discard_page_images(self, keys: list[str]) -> None:
        for key in keys:
            try:
                await self._store.delete(key)
            except Exception:
                logger.exception("[OCR] failed to delete page object key=%s", key)
            try:
                async with self._workflow._pool.acquire() as conn:
                    await conn.execute(
                        "DELETE FROM object_assets WHERE object_key = $1",
                        key,
                    )
                    await conn.execute(
                        """
                        UPDATE document_pages
                        SET object_key = NULL
                        WHERE object_key = $1
                        """,
                        key,
                    )
            except Exception:
                logger.exception("[OCR] failed to forget page asset key=%s", key)


def _image_object_keys(inner: dict[str, Any]) -> list[str]:
    keys: list[str] = []
    for spec in (inner.get("file1"), inner.get("file2")):
        if not isinstance(spec, dict):
            continue
        key = spec.get("object_key") or spec.get("key")
        content_type = str(spec.get("content_type") or "")
        if key and not content_type.startswith("text/"):
            keys.append(str(key))
    return keys


def _is_object_ref(value: Any) -> bool:
    return isinstance(value, dict) and bool(value.get("object_key") or value.get("key"))
