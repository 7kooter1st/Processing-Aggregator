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

    async def ensure_job(
        self,
        *,
        job_id: str,
        document_id: str | None = None,
        total_chunks: int = 0,
        status: str = "queued",
        message: str = "Ожидание чанков из Kafka...",
    ) -> JobProgress:
        """Create or refresh a job before Kafka chunks arrive."""
        async with self._lock:
            existing = self._jobs.get(job_id)
            if existing is None:
                progress = JobProgress(
                    job_id=job_id,
                    document_id=document_id or job_id,
                    total_chunks=max(0, total_chunks),
                    status=status,
                    last_message=message,
                )
                self._jobs[job_id] = progress
                logger.info(
                    "[STATE] registered job=%s status=%s total_chunks=%s",
                    job_id,
                    status,
                    progress.total_chunks,
                )
                return progress

            if document_id:
                existing.document_id = document_id
            if total_chunks > 0:
                existing.total_chunks = total_chunks
            # Do not downgrade an active/completed job back to queued/preparing.
            if status == "failed" or existing.status in {"queued", "preparing"} or (
                status == "processing" and existing.status == "queued"
            ):
                if not (
                    existing.status in {"processing", "completed", "failed"}
                    and status in {"queued", "preparing"}
                ):
                    existing.status = status
            if message:
                existing.last_message = message
            return existing

    async def register_chunk(self, message: RawChunkMessage) -> JobProgress:
        key = message.job_id
        async with self._lock:
            if key not in self._jobs:
                self._jobs[key] = JobProgress(
                    job_id=message.job_id,
                    document_id=message.document_id,
                    total_chunks=message.total_chunks,
                )
            else:
                job = self._jobs[key]
                if message.total_chunks > 0:
                    job.total_chunks = message.total_chunks
                if job.status in {"queued", "preparing"}:
                    job.status = "processing"
                    job.last_message = (
                        f"Обработка chunk {message.chunk_index}/"
                        f"{message.total_chunks}..."
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
                progress = JobProgress(
                    job_id=job_id,
                    document_id=job_id,
                    total_chunks=0,
                )
                self._jobs[job_id] = progress
            progress.status = "failed"
            progress.last_message = error
            return progress

    async def get_job(self, job_id: str) -> JobProgress | None:
        async with self._lock:
            return self._jobs.get(job_id)

    async def list_jobs(self) -> list[JobProgress]:
        async with self._lock:
            return list(self._jobs.values())
