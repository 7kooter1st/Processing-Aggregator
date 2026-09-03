import logging

from app.config import settings
from app.models.schemas import ProcessedResultMessage, StatusUpdateMessage
from app.services.difference_classifier import DifferenceClassifier
from app.services.hierarchical_diff import HierarchicalDiffEngine, SourcePage
from app.services.ocr_store import OcrStore
from app.services.ollama_client import OllamaClient
from app.services.prompt_builder import CLASSIFICATION_PROMPT_VERSION
from app.services.state_manager import StateManager

logger = logging.getLogger(__name__)

ALGORITHM_VERSION = "hierarchical-diff-v2-page-aware"


class DocumentComparator:
    """Runs one persisted, document-level hierarchical comparison per job."""

    def __init__(
        self,
        *,
        ollama: OllamaClient,
        store: OcrStore,
        state: StateManager,
        publish_result,
        publish_status,
    ) -> None:
        self._ollama = ollama
        self._store = store
        self._state = state
        self._publish_result = publish_result
        self._publish_status = publish_status
        self._diff = HierarchicalDiffEngine()
        self._classifier = DifferenceClassifier(ollama=ollama, store=store)

    async def compare_if_ready(self, job_id: str) -> bool:
        """Claim and compare a completed OCR job. Returns whether it ran."""
        if not await self._store.try_claim_comparison(job_id):
            return False

        documents = None
        run_id: str | None = None
        result_saved = False
        try:
            documents = await self._store.load_documents(job_id)
            run_id = await self._store.create_comparison_run(
                job_id,
                algorithm_version=ALGORITHM_VERSION,
                prompt_version=CLASSIFICATION_PROMPT_VERSION,
                ollama_model=settings.ollama_model,
                settings_data={
                    "num_ctx": settings.ollama_num_ctx,
                    "temperature": settings.ollama_temperature,
                },
            )
            await self._state.ensure_job(
                job_id=job_id,
                document_id=documents.document_id,
                total_chunks=documents.total_chunks,
                status="processing",
                message="Сравнение документов…",
            )
            await self._state.set_status(
                job_id,
                status="comparing",
                message="Сравнение документов…",
            )

            await self._publish_status(
                StatusUpdateMessage(
                    job_id=job_id,
                    document_id=documents.document_id,
                    status="processing",
                    processed_chunks=documents.total_chunks,
                    total_chunks=documents.total_chunks,
                    message="Сравнение документов…",
                )
            )

            pages1 = [
                SourcePage(
                    page_number=page.chunk_index,
                    text=page.text,
                    filename=page.filename,
                    source_content_type=page.source_content_type,
                    was_ocr=page.was_ocr,
                )
                for page in documents.file1.pages
            ]
            pages2 = [
                SourcePage(
                    page_number=page.chunk_index,
                    text=page.text,
                    filename=page.filename,
                    source_content_type=page.source_content_type,
                    was_ocr=page.was_ocr,
                )
                for page in documents.file2.pages
            ]
            candidates = self._diff.compare_pages(
                pages1,
                pages2,
                candidate_prefix=job_id,
            )
            await self._store.save_diff_candidates(
                run_id,
                [
                    {
                        **candidate.classifier_dict(),
                        "alignment_id": candidate.alignment_id,
                        "result": candidate.result_dict(),
                    }
                    for candidate in candidates
                ],
            )
            await self._state.set_status(
                job_id,
                status="classifying",
                message="Проверка найденных различий…",
            )
            await self._publish_status(
                StatusUpdateMessage(
                    job_id=job_id,
                    document_id=documents.document_id,
                    status="processing",
                    processed_chunks=documents.total_chunks,
                    total_chunks=documents.total_chunks,
                    message="Проверка найденных различий…",
                )
            )
            classified, classification_summary = await self._classifier.classify(
                candidates,
                run_id=run_id,
                file1_name=documents.file1.filename,
                file2_name=documents.file2.filename,
            )
            differences = [item.result_dict() for item in classified]
            categories = {
                item.decision.category for item in classified
            }
            if not differences:
                verdict = "identical"
            elif categories <= {"technical"}:
                verdict = "content_equal"
            else:
                verdict = "different"
            comparison = {
                "identical": verdict == "identical",
                "verdict": verdict,
                "differences": differences,
            }
            await self._store.finalize_comparison(
                run_id,
                job_id=job_id,
                comparison=comparison,
            )
            result_saved = True

            logger.info(
                "[DIFF] job=%s candidates=%s substantive=%s uncertain=%s "
                "technical=%s verdict=%s",
                job_id,
                len(candidates),
                sum(
                    item.decision.category == "substantive"
                    for item in classified
                ),
                sum(
                    item.decision.category == "ocr_uncertain"
                    for item in classified
                ),
                sum(
                    item.decision.category == "technical"
                    for item in classified
                ),
                verdict,
            )

            await self._state.set_status(
                job_id,
                status="completed",
                message="Сравнение завершено",
            )
            try:
                await self._publish_result(
                    ProcessedResultMessage(
                        job_id=job_id,
                        document_id=documents.document_id,
                        chunk_index=1,
                        total_chunks=1,
                        ollama={
                            "stage": "hybrid_difference_classification",
                            "run_id": run_id,
                            **classification_summary,
                        },
                        comparison_fragment=comparison,
                    )
                )
            except Exception:
                logger.exception(
                    "[DIFF] Kafka publish failed after result save job=%s; "
                    "job stays completed for REST/outbox",
                    job_id,
                )
            return True
        except Exception as exc:
            logger.exception("[DIFF] comparison failed job=%s", job_id)
            if result_saved:
                logger.error(
                    "[DIFF] keeping completed result job=%s despite later error",
                    job_id,
                )
                return True
            if run_id is not None:
                await self._store.mark_comparison_run_failed(run_id, str(exc))
            error = "Не удалось сравнить документы. Попробуйте ещё раз."
            await self._store.mark_failed(job_id, error)
            await self._state.mark_failed(job_id, error)
            db_job = await self._store.get_job(job_id)
            document_id = (
                documents.document_id
                if documents is not None
                else (db_job or {}).get("document_id", job_id)
            )
            total_chunks = (
                documents.total_chunks
                if documents is not None
                else (db_job or {}).get("total_chunks", 0)
            )
            try:
                await self._publish_status(
                    StatusUpdateMessage(
                        job_id=job_id,
                        document_id=document_id,
                        status="failed",
                        processed_chunks=total_chunks,
                        total_chunks=total_chunks,
                        message=error,
                    )
                )
            except Exception:
                logger.exception("[DIFF] failed to publish failure status")
            raise
