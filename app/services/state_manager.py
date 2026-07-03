import asyncio
import logging
from typing import Callable

from app.models.schemas import JobProgress, RawChunkMessage

logger = logging.getLogger(__name__)


class StateManager:
    """In-memory job progress tracker (idempotent by chunk_index)."""

    def __init__(self) -> None:
        self._jobs: dict[str, JobProgress] = {}
        self._lock = asyncio.Lock()
        self._completion_callbacks: list[Callable[[JobProgress], None]] = []

    def on_job_complete(self, callback: Callable[[JobProgress], None]) -> None:
        self._completion_callbacks.append(callback)

    async def register_chunk(self, message: RawChunkMessage) -> JobProgress:
        key = message.job_id
        async with self._lock:
            if key not in self._jobs:
                self._jobs[key] = JobProgress(
                    job_id=message.job_id,
                    document_id=message.document_id,
                    total_chunks=message.total_chunks,
                )
            return self._jobs[key]

    async def mark_chunk_processed(
        self,
        job_id: str,
        chunk_index: int,
    ) -> tuple[JobProgress | None, bool]:
        """Mark chunk as done. Returns (progress, newly_completed)."""
        async with self._lock:
            progress = self._jobs.get(job_id)
            if progress is None:
                return None, False

            if chunk_index in progress.processed_chunks:
                return progress, False

            progress.processed_chunks.add(chunk_index)
            progress.last_message = (
                f"Обработка {progress.progress_text()} завершена для chunk {chunk_index}"
            )

            if progress.is_complete and progress.status != "completed":
                progress.status = "completed"
                for callback in self._completion_callbacks:
                    callback(progress)
                return progress, True

            return progress, False

    async def mark_failed(self, job_id: str, error: str) -> JobProgress | None:
        async with self._lock:
            progress = self._jobs.get(job_id)
            if progress is None:
                return None
            progress.status = "failed"
            progress.last_message = error
            return progress

    async def get_job(self, job_id: str) -> JobProgress | None:
        async with self._lock:
            return self._jobs.get(job_id)

    async def list_jobs(self) -> list[JobProgress]:
        async with self._lock:
            return list(self._jobs.values())
