import asyncio
import logging

from app.models.schemas import (
    ChunkContent,
    ContentType,
    RawChunkMessage,
    StatusUpdateMessage,
)
from app.services.document_comparator import DocumentComparator
from app.services.image_prep import downscale_image_b64
from app.services.ocr_store import OcrStore
from app.services.ollama_client import OllamaClient
from app.services.prompt_builder import (
    build_ocr_messages,
    extract_ocr_text,
)
from app.services.state_manager import StateManager

logger = logging.getLogger(__name__)


class EmptyOcrError(RuntimeError):
    """Raised when OCR returns no usable text."""


class ChunkProcessor:
    def __init__(
        self,
        ollama: OllamaClient,
        state: StateManager,
        store: OcrStore,
        comparator: DocumentComparator,
        publish_status,
    ) -> None:
        self._ollama = ollama
        self._state = state
        self._store = store
        self._comparator = comparator
        self._publish_status = publish_status

    async def process(self, message: RawChunkMessage) -> None:
        await self._state.register_chunk(message)
        db_job = await self._store.get_job(message.job_id)
        if db_job is not None and db_job["status"] == "completed":
            logger.info(
                "[OCR] skip completed job=%s chunk=%s",
                message.job_id,
                message.chunk_index,
            )
            return

        already_stored = await self._store.chunk_is_stored(
            message.job_id,
            message.chunk_index,
        )
        if already_stored:
            stored_chunks = await self._store.get_stored_count(message.job_id)
            ready = (
                message.total_chunks > 0
                and stored_chunks >= message.total_chunks
            )
            logger.info(
                "[OCR] skip persisted job=%s chunk=%s/%s",
                message.job_id,
                message.chunk_index,
                message.total_chunks,
            )
        else:
            needs_ocr = self._needs_ocr(message.file1) or self._needs_ocr(
                message.file2
            )
            if needs_ocr:
                await self._publish_status(
                    StatusUpdateMessage(
                        job_id=message.job_id,
                        document_id=message.document_id,
                        status="processing",
                        processed_chunks=await self._store.get_stored_count(
                            message.job_id
                        ),
                        total_chunks=message.total_chunks,
                        message=(
                            f"Выполняется сканирование файлов: "
                            f"{message.chunk_index} из {message.total_chunks}"
                        ),
                    )
                )

            if self._needs_ocr(message.file1) and self._needs_ocr(message.file2):
                file1, file2 = await asyncio.gather(
                    self._to_text(message.file1, message),
                    self._to_text(message.file2, message),
                )
            else:
                file1 = await self._to_text(message.file1, message)
                file2 = await self._to_text(message.file2, message)

            stored_chunks, ready = await self._store.save_ocr_pair(
                message,
                file1,
                file2,
            )

        progress, _ = await self._state.mark_chunk_processed(
            message.job_id,
            message.chunk_index,
        )
        await self._publish_status(
            StatusUpdateMessage(
                job_id=message.job_id,
                document_id=message.document_id,
                status="processing",
                processed_chunks=stored_chunks,
                total_chunks=message.total_chunks,
                message=(
                    f"Выполняется сканирование файлов: "
                    f"{stored_chunks} из {message.total_chunks}"
                ),
            )
        )

        if not ready:
            return

        if progress is not None:
            await self._state.set_status(
                message.job_id,
                status="ocr_ready",
                message="Сканирование завершено, начинается сравнение",
            )
        await self._comparator.compare_if_ready(message.job_id)

    async def _to_text(
        self,
        chunk: ChunkContent | None,
        message: RawChunkMessage,
    ) -> ChunkContent:
        if chunk is None:
            return ChunkContent(
                filename="unknown",
                format="unknown",
                content_type=ContentType.TEXT,
                content="",
            )

        if chunk.content_type == ContentType.TEXT:
            return chunk

        logger.info(
            "[OCR] job=%s chunk=%s/%s file=%s content_chars=%s",
            message.job_id,
            message.chunk_index,
            message.total_chunks,
            chunk.filename,
            len(chunk.content or ""),
        )

        prepared = ChunkContent(
            filename=chunk.filename,
            format=chunk.format,
            content_type=chunk.content_type,
            content=downscale_image_b64(chunk.content),
        )
        ocr_messages = build_ocr_messages(prepared)
        ocr_response = await self._ollama.chat_text(ocr_messages)
        text = extract_ocr_text(ocr_response)

        if not text:
            # Blank scan pages are rare; more often a model burned tokens on
            # thinking. A genuinely blank page is stored as an empty string.
            thinking = ((ocr_response.get("message") or {}).get("thinking") or "")
            if isinstance(thinking, str) and thinking.strip():
                raise EmptyOcrError(
                    f"OCR вернул пустой текст для файла {chunk.filename!r} "
                    f"(chunk {message.chunk_index}/{message.total_chunks}); "
                    f"модель ответила только thinking "
                    f"(done_reason={ocr_response.get('done_reason')})"
                )
            logger.warning(
                "[OCR] empty page treated as blank job=%s chunk=%s/%s file=%s",
                message.job_id,
                message.chunk_index,
                message.total_chunks,
                chunk.filename,
            )
            text = ""

        logger.info(
            "[OCR] done job=%s chunk=%s/%s file=%s chars=%s",
            message.job_id,
            message.chunk_index,
            message.total_chunks,
            chunk.filename,
            len(text),
        )

        return ChunkContent(
            filename=chunk.filename,
            format=chunk.format,
            content_type=ContentType.TEXT,
            content=text,
        )

    @staticmethod
    def _needs_ocr(chunk: ChunkContent | None) -> bool:
        return chunk is not None and chunk.content_type == ContentType.IMAGE

    async def handle_error(self, raw_payload: dict, error: str) -> None:
        job_id = raw_payload.get("job_id", "unknown")
        document_id = raw_payload.get("document_id", "unknown")
        total_chunks = raw_payload.get("total_chunks", 0)
        chunk_index = raw_payload.get("chunk_index", 0)
        user_error = (
            f"Не удалось обработать страницу {chunk_index}. "
            "Попробуйте загрузить документы ещё раз."
        )

        await self._state.mark_failed(job_id, user_error)
        await self._store.mark_failed(job_id, user_error)
        logger.error(
            "[PROCESS] error job=%s chunk=%s: %s",
            job_id,
            chunk_index,
            error,
        )

        await self._publish_status(
            StatusUpdateMessage(
                job_id=job_id,
                document_id=document_id,
                status="failed",
                processed_chunks=0,
                total_chunks=total_chunks,
                message=user_error,
            )
        )
