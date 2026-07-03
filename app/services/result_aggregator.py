import asyncio
import logging

from app.models.schemas import (
    ComparisonResult,
    LineDifference,
    ProcessedResultMessage,
    ResultResponse,
    StatusUpdateMessage,
)
from app.services.websocket_hub import WebSocketHub

logger = logging.getLogger(__name__)


class _JobAggregation:
    def __init__(self, job_id: str, document_id: str, total_chunks: int) -> None:
        self.job_id = job_id
        self.document_id = document_id
        self.total_chunks = total_chunks
        self.fragments: dict[int, dict] = {}
        self.received_chunks: set[int] = set()


class ResultAggregator:
    """Collects processed_results and builds final comparison for the frontend."""

    def __init__(self, ws_hub: WebSocketHub, publish_status) -> None:
        self._ws_hub = ws_hub
        self._publish_status = publish_status
        self._jobs: dict[str, _JobAggregation] = {}
        self._final_results: dict[str, ResultResponse] = {}
        self._lock = asyncio.Lock()

    async def get_result(self, job_id: str) -> ResultResponse | None:
        async with self._lock:
            return self._final_results.get(job_id)

    async def handle_processed_result(self, message: ProcessedResultMessage) -> None:
        async with self._lock:
            if message.job_id in self._final_results:
                logger.info("[AGGREGATOR] skip job=%s — result already finalized", message.job_id)
                return

            job = self._jobs.get(message.job_id)
            if job is None:
                job = _JobAggregation(
                    job_id=message.job_id,
                    document_id=message.document_id,
                    total_chunks=message.total_chunks,
                )
                self._jobs[message.job_id] = job

            if message.chunk_index in job.received_chunks:
                logger.info(
                    "[AGGREGATOR] skip duplicate job=%s chunk=%s",
                    message.job_id,
                    message.chunk_index,
                )
                return

            job.received_chunks.add(message.chunk_index)
            job.fragments[message.chunk_index] = message.comparison_fragment or {
                "identical": True,
                "differences": [],
            }

            received = len(job.received_chunks)
            logger.info(
                "[AGGREGATOR] collected job=%s chunk=%s/%s (total received: %s/%s)",
                message.job_id,
                message.chunk_index,
                message.total_chunks,
                received,
                job.total_chunks,
            )

            if len(job.received_chunks) < job.total_chunks:
                return

            fragments = dict(job.fragments)
            total_chunks = job.total_chunks
            document_id = job.document_id

        comparison = self._merge_fragments(fragments, total_chunks)
        result = ResultResponse(comparison=comparison)

        async with self._lock:
            if message.job_id in self._final_results:
                return
            self._final_results[message.job_id] = result

        logger.info(
            "[AGGREGATOR] complete job=%s identical=%s differences=%s",
            message.job_id,
            comparison.identical,
            len(comparison.differences),
        )

        await self._ws_hub.send_result(message.job_id, comparison)

        await self._publish_status(
            StatusUpdateMessage(
                job_id=message.job_id,
                document_id=document_id,
                status="completed",
                processed_chunks=total_chunks,
                total_chunks=total_chunks,
                message=f"Document {document_id}: готово! Результат готов.",
            )
        )

    @staticmethod
    def _merge_fragments(fragments: dict[int, dict], total_chunks: int) -> ComparisonResult:
        all_differences: list[LineDifference] = []
        all_identical = True

        for index in sorted(fragments.keys()):
            fragment = fragments[index]
            if not fragment.get("identical", False):
                all_identical = False

            for diff in fragment.get("differences") or []:
                all_differences.append(LineDifference.model_validate(diff))

        if all_identical and not all_differences:
            return ComparisonResult(identical=True, differences=[])

        if len(fragments) < total_chunks:
            all_identical = False

        return ComparisonResult(identical=False, differences=all_differences)
