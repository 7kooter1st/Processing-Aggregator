import unittest
from unittest.mock import AsyncMock, MagicMock

from app.models.schemas import RawChunkMessage, ChunkContent, ContentType
from app.services.chunk_processor import ChunkProcessor, TERMINAL_JOB_STATUSES


class ChunkProcessorSkipTests(unittest.IsolatedAsyncioTestCase):
    async def test_skips_terminal_jobs(self) -> None:
        store = MagicMock()
        store.get_job = AsyncMock(return_value={"status": "completed"})
        store.chunk_is_stored = AsyncMock()
        processor = ChunkProcessor(
            ollama=MagicMock(),
            state=MagicMock(register_chunk=AsyncMock()),
            store=store,
            comparator=MagicMock(),
            publish_status=AsyncMock(),
        )
        ready = await processor.process(
            RawChunkMessage(
                job_id="j1",
                document_id="j1",
                chunk_index=1,
                total_chunks=1,
                file1=ChunkContent(
                    filename="a.txt",
                    format="docx",
                    content_type=ContentType.TEXT,
                    content="a",
                ),
                file2=ChunkContent(
                    filename="b.txt",
                    format="docx",
                    content_type=ContentType.TEXT,
                    content="b",
                ),
            )
        )
        self.assertFalse(ready)
        store.chunk_is_stored.assert_not_called()
        self.assertIn("completed", TERMINAL_JOB_STATUSES)


class FinalizeDoesNotFailCompletedTests(unittest.IsolatedAsyncioTestCase):
    async def test_kafka_failure_after_save_keeps_completed(self) -> None:
        from app.services.document_comparator import DocumentComparator

        store = MagicMock()
        store.try_claim_comparison = AsyncMock(return_value=True)
        store.load_documents = AsyncMock(side_effect=RuntimeError("boom"))
        store.mark_comparison_run_failed = AsyncMock()
        store.mark_failed = AsyncMock()
        store.get_job = AsyncMock(return_value={"document_id": "j", "total_chunks": 1})
        comparator = DocumentComparator(
            ollama=MagicMock(),
            store=store,
            state=MagicMock(
                ensure_job=AsyncMock(),
                set_status=AsyncMock(),
                mark_failed=AsyncMock(),
            ),
            publish_result=AsyncMock(side_effect=RuntimeError("kafka down")),
            publish_status=AsyncMock(),
        )
        with self.assertRaises(RuntimeError):
            await comparator.compare_if_ready("job-1")
        store.mark_failed.assert_awaited()


if __name__ == "__main__":
    unittest.main()
