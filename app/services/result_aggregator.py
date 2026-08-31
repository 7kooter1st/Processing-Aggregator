import asyncio
import logging

from app.models.schemas import (
    ComparisonResult,
    LineDifference,
    ProcessedResultMessage,
    ResultResponse,
    StatusUpdateMessage,
)
from app.services.ocr_store import OcrStore
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

    def __init__(
        self,
        ws_hub: WebSocketHub,
        publish_status,
        store: OcrStore,
    ) -> None:
        self._ws_hub = ws_hub
        self._publish_status = publish_status
        self._store = store
        self._jobs: dict[str, _JobAggregation] = {}
        self._final_results: dict[str, ResultResponse] = {}
        self._lock = asyncio.Lock()

    async def get_result(self, job_id: str) -> ResultResponse | None:
        async with self._lock:
            cached = self._final_results.get(job_id)
        if cached is not None:
            return cached

        persisted = await self._store.get_comparison_result(job_id)
        if persisted is None:
            return None
        result = ResultResponse(
            comparison=ComparisonResult.model_validate(persisted)
        )
        async with self._lock:
            self._final_results.setdefault(job_id, result)
            return self._final_results[job_id]

    async def handle_processed_result(self, message: ProcessedResultMessage) -> None:
        cached: ResultResponse | None
        async with self._lock:
            cached = self._final_results.get(message.job_id)
        if cached is not None:
            logger.info(
                "[AGGREGATOR] replay persisted/cached result job=%s",
                message.job_id,
            )
            await self._ws_hub.send_result(
                message.job_id,
                cached.comparison,
            )
            return

        async with self._lock:
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

            if message.comparison_fragment is None:
                raise ValueError(
                    "processed_results has no comparison_fragment; "
                    "refusing to mark the job identical"
                )

            job.received_chunks.add(message.chunk_index)
            job.fragments[message.chunk_index] = message.comparison_fragment

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
        verdicts: set[str] = set()

        for index in sorted(fragments.keys()):
            fragment = fragments[index]
            if not fragment.get("identical", False):
                all_identical = False
            verdicts.add(fragment.get("verdict", "different"))

            for diff in fragment.get("differences") or []:
                all_differences.append(LineDifference.model_validate(diff))

        if all_identical and not all_differences:
            return ComparisonResult(
                identical=True,
                verdict="identical",
                differences=[],
            )

        if len(fragments) < total_chunks:
            all_identical = False

        if "different" in verdicts:
            verdict = "different"
        elif "content_equal" in verdicts:
            verdict = "content_equal"
        else:
            verdict = "different"
        return ComparisonResult(
            identical=False,
            verdict=verdict,
            differences=all_differences,
        )
