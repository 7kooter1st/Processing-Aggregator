import asyncio
import logging

from app.config import settings
from app.services.document_comparator import DocumentComparator

logger = logging.getLogger(__name__)


class CompareScheduler:
    """Bounded queue for document-level comparison, separate from OCR intake."""

    def __init__(self, comparator: DocumentComparator) -> None:
        self._comparator = comparator
        self._queue: asyncio.Queue[str] = asyncio.Queue()
        self._semaphore = asyncio.Semaphore(max(1, settings.compare_max_concurrent))
        self._task: asyncio.Task | None = None
        self._workers: set[asyncio.Task] = set()
        self._running = False
        self._inflight: set[str] = set()
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._run())
        logger.info(
            "Compare scheduler started (max_concurrent=%s)",
            settings.compare_max_concurrent,
        )

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._workers:
            await asyncio.gather(*self._workers, return_exceptions=True)
        while not self._queue.empty():
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break

    async def submit(self, job_id: str) -> None:
        async with self._lock:
            if job_id in self._inflight:
                return
            self._inflight.add(job_id)
        await self._queue.put(job_id)

    async def recover(self, job_ids: list[str]) -> None:
        for job_id in job_ids:
            logger.info("[RECOVERY] enqueue compare job=%s", job_id)
            await self.submit(job_id)

    async def _run(self) -> None:
        try:
            while self._running:
                job_id = await self._queue.get()
                task = asyncio.create_task(self._run_one(job_id))
                self._workers.add(task)
                task.add_done_callback(self._workers.discard)
        except asyncio.CancelledError:
            logger.info("Compare scheduler cancelled")

    async def _run_one(self, job_id: str) -> None:
        try:
            async with self._semaphore:
                logger.info("[COMPARE QUEUE] start job=%s", job_id)
                await self._comparator.compare_if_ready(job_id)
        except Exception:
            logger.exception("[COMPARE QUEUE] failed job=%s", job_id)
        finally:
            async with self._lock:
                self._inflight.discard(job_id)
