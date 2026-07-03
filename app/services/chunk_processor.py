import logging

from app.models.schemas import (
    ProcessedResultMessage,
    RawChunkMessage,
    StatusUpdateMessage,
)
from app.services.ollama_client import OllamaClient
from app.services.prompt_builder import build_ollama_messages, extract_comparison_fragment
from app.services.state_manager import StateManager

logger = logging.getLogger(__name__)


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

        await self._publish_status(
            StatusUpdateMessage(
                job_id=message.job_id,
                document_id=message.document_id,
                status="processing",
                processed_chunks=len(
                    (await self._state.get_job(message.job_id)).processed_chunks
                ),
                total_chunks=message.total_chunks,
                message=f"Анализ chunk {message.chunk_index}/{message.total_chunks}...",
            )
        )

        messages = build_ollama_messages(
            chunk_index=message.chunk_index,
            total_chunks=message.total_chunks,
            file1=message.file1,
            file2=message.file2,
        )

        logger.info(
            "Calling Ollama for job=%s chunk=%s/%s",
            message.job_id,
            message.chunk_index,
            message.total_chunks,
        )

        ollama_response = await self._ollama.chat(messages)
        comparison = extract_comparison_fragment(ollama_response)
        logger.info(
            "[OLLAMA] parsed job=%s chunk=%s/%s identical=%s diffs=%s",
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

        status = "processing"
        status_message = (
            "Все фрагменты обработаны, сборка результата..."
            if newly_completed
            else progress.last_message
        )

        await self._publish_status(
            StatusUpdateMessage(
                job_id=message.job_id,
                document_id=message.document_id,
                status=status,
                processed_chunks=len(progress.processed_chunks),
                total_chunks=progress.total_chunks,
                message=status_message,
            )
        )

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
