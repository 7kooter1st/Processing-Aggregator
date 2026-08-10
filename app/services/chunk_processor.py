import asyncio
import logging

from app.models.schemas import (
    ChunkContent,
    ContentType,
    ProcessedResultMessage,
    RawChunkMessage,
    StatusUpdateMessage,
)
from app.services.image_prep import downscale_image_b64
from app.services.ollama_client import OllamaClient
from app.services.prompt_builder import (
    build_compare_messages,
    build_ocr_messages,
    extract_comparison_fragment,
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
        publish_result,
        publish_status,
    ) -> None:
        self._ollama = ollama
        self._state = state
        self._publish_result = publish_result
        self._publish_status = publish_status

    async def process(self, message: RawChunkMessage) -> None:
        await self._state.register_chunk(message)

        needs_ocr = self._needs_ocr(message.file1) or self._needs_ocr(message.file2)
        processed_chunks = len(
            (await self._state.get_job(message.job_id)).processed_chunks
        )

        if needs_ocr:
            await self._publish_status(
                StatusUpdateMessage(
                    job_id=message.job_id,
                    document_id=message.document_id,
                    status="processing",
                    processed_chunks=processed_chunks,
                    total_chunks=message.total_chunks,
                    message=(
                        f"Распознавание chunk "
                        f"{message.chunk_index}/{message.total_chunks}..."
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
        else:
            file1 = await self._to_text(message.file1, message)
            file2 = await self._to_text(message.file2, message)

        await self._publish_status(
            StatusUpdateMessage(
                job_id=message.job_id,
                document_id=message.document_id,
                status="processing",
                processed_chunks=processed_chunks,
                total_chunks=message.total_chunks,
                message=(
                    f"Сравнение chunk "
                    f"{message.chunk_index}/{message.total_chunks}..."
                ),
            )
        )

        compare_messages = build_compare_messages(
            chunk_index=message.chunk_index,
            total_chunks=message.total_chunks,
            file1=file1,
            file2=file2,
        )

        logger.info(
            "[COMPARE] job=%s chunk=%s/%s",
            message.job_id,
            message.chunk_index,
            message.total_chunks,
        )

        ollama_response = await self._ollama.chat_json(compare_messages)
        comparison = extract_comparison_fragment(ollama_response)
        logger.info(
            "[COMPARE] parsed job=%s chunk=%s/%s identical=%s diffs=%s",
            message.job_id,
            message.chunk_index,
            message.total_chunks,
            comparison.get("identical") if comparison else None,
            len((comparison or {}).get("differences") or []),
        )

        result = ProcessedResultMessage(
            job_id=message.job_id,
            document_id=message.document_id,
            chunk_index=message.chunk_index,
            total_chunks=message.total_chunks,
            ollama=ollama_response,
            comparison_fragment=comparison,
        )
        await self._publish_result(result)

        progress, newly_completed = await self._state.mark_chunk_processed(
            message.job_id,
            message.chunk_index,
        )

        if progress is None:
            return

        status_message = (
            "Все фрагменты обработаны, сборка результата..."
            if newly_completed
            else progress.last_message
        )

        await self._publish_status(
            StatusUpdateMessage(
                job_id=message.job_id,
                document_id=message.document_id,
                status="processing",
                processed_chunks=len(progress.processed_chunks),
                total_chunks=progress.total_chunks,
                message=status_message,
            )
        )

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
                content="(пусто)",
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
            # Blank scan pages are rare; more often Gemma burned tokens on
            # thinking. Keep a stable placeholder so one empty page does not
            # fail the whole job after think=false is already enforced upstream.
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
            text = "[пусто]"

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

        await self._state.mark_failed(job_id, error)
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
                message=f"Ошибка chunk {chunk_index}: {error}",
            )
        )
