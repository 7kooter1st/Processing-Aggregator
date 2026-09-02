import json
import logging
import uuid
from dataclasses import dataclass
from typing import Any

import asyncpg

from app.config import settings
from app.models.schemas import ChunkContent, ContentType, RawChunkMessage

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class StoredPage:
    chunk_index: int
    side: int
    filename: str
    text: str
    source_content_type: str
    was_ocr: bool
    is_missing: bool


@dataclass(frozen=True)
class StoredDocument:
    side: int
    filename: str
    text: str
    chunks: list[dict[str, Any]]
    pages: list[StoredPage]


@dataclass(frozen=True)
class StoredDocumentPair:
    job_id: str
    document_id: str
    total_chunks: int
    file1: StoredDocument
    file2: StoredDocument


class OcrStore:
    """PostgreSQL source of truth for jobs and persisted OCR text."""

    def __init__(self) -> None:
        self._pool: asyncpg.Pool | None = None

    async def start(self) -> None:
        self._pool = await asyncpg.create_pool(
            dsn=settings.database_url,
            min_size=settings.database_pool_min_size,
            max_size=settings.database_pool_max_size,
            command_timeout=30,
        )
        await self._create_schema()
        # A process may have stopped after claiming a finished OCR job. Make it
        # eligible for comparison again; no OCR has to be repeated.
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE comparison_runs
                SET status = 'failed',
                    error_message = COALESCE(
                        error_message,
                        'Processing restarted during comparison'
                    ),
                    finished_at = COALESCE(finished_at, NOW())
                WHERE status = 'running'
                """
            )
            await conn.execute(
                """
                UPDATE comparison_jobs
                SET comparison_claimed = FALSE,
                    status = 'ocr_ready',
                    last_message = 'Сканирование завершено, сравнение будет продолжено',
                    updated_at = NOW()
                WHERE status = 'comparing'
                """
            )
        logger.info("[POSTGRES] OCR store connected and schema ready")

    async def stop(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def is_available(self) -> bool:
        if self._pool is None:
            return False
        try:
            async with self._pool.acquire() as conn:
                return await conn.fetchval("SELECT 1") == 1
        except Exception:
            return False

    def _require_pool(self) -> asyncpg.Pool:
        if self._pool is None:
            raise RuntimeError("PostgreSQL OCR store is not started")
        return self._pool

    async def _create_schema(self) -> None:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id UUID PRIMARY KEY,
                    username TEXT NOT NULL UNIQUE,
                    password_hash TEXT NOT NULL,
                    role TEXT NOT NULL CHECK (role IN ('admin', 'user')),
                    is_active BOOLEAN NOT NULL DEFAULT TRUE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    created_by UUID NULL
                );

                CREATE TABLE IF NOT EXISTS sessions (
                    id UUID PRIMARY KEY,
                    token_hash TEXT NOT NULL UNIQUE,
                    user_id UUID NOT NULL
                        REFERENCES users(id) ON DELETE CASCADE,
                    expires_at TIMESTAMPTZ NOT NULL,
                    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    revoked_at TIMESTAMPTZ NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );

                CREATE INDEX IF NOT EXISTS idx_sessions_user
                    ON sessions(user_id);

                CREATE TABLE IF NOT EXISTS comparison_jobs (
                    job_id TEXT PRIMARY KEY,
                    document_id TEXT NOT NULL,
                    user_id UUID NOT NULL REFERENCES users(id),
                    file1_name TEXT NOT NULL DEFAULT '',
                    file2_name TEXT NOT NULL DEFAULT '',
                    total_chunks INTEGER NOT NULL DEFAULT 0,
                    processed_chunks INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'queued',
                    last_message TEXT NOT NULL DEFAULT '',
                    comparison_claimed BOOLEAN NOT NULL DEFAULT FALSE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );

                CREATE INDEX IF NOT EXISTS idx_comparison_jobs_user
                    ON comparison_jobs(user_id, created_at DESC);

                CREATE TABLE IF NOT EXISTS ocr_chunks (
                    job_id TEXT NOT NULL
                        REFERENCES comparison_jobs(job_id) ON DELETE CASCADE,
                    chunk_index INTEGER NOT NULL CHECK (chunk_index >= 1),
                    side SMALLINT NOT NULL CHECK (side IN (1, 2)),
                    filename TEXT NOT NULL DEFAULT '',
                    format TEXT NOT NULL DEFAULT '',
                    source_content_type TEXT NOT NULL,
                    was_ocr BOOLEAN NOT NULL DEFAULT FALSE,
                    is_missing BOOLEAN NOT NULL DEFAULT FALSE,
                    text_content TEXT NOT NULL DEFAULT '',
                    ocr_model TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (job_id, chunk_index, side)
                );

                CREATE INDEX IF NOT EXISTS idx_ocr_chunks_job_side
                    ON ocr_chunks(job_id, side, chunk_index);

                CREATE TABLE IF NOT EXISTS job_files (
                    id UUID PRIMARY KEY,
                    job_id TEXT NOT NULL
                        REFERENCES comparison_jobs(job_id) ON DELETE CASCADE,
                    side SMALLINT NOT NULL CHECK (side IN (1, 2)),
                    original_filename TEXT NOT NULL,
                    content_type TEXT NOT NULL DEFAULT 'application/octet-stream',
                    size_bytes BIGINT NOT NULL,
                    sha256 TEXT NOT NULL,
                    storage_path TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (job_id, side)
                );

                CREATE TABLE IF NOT EXISTS comparison_runs (
                    run_id UUID PRIMARY KEY,
                    job_id TEXT NOT NULL
                        REFERENCES comparison_jobs(job_id) ON DELETE CASCADE,
                    run_number INTEGER NOT NULL CHECK (run_number >= 1),
                    status TEXT NOT NULL DEFAULT 'running'
                        CHECK (status IN ('running', 'completed', 'failed')),
                    algorithm_version TEXT NOT NULL,
                    ollama_model TEXT NOT NULL,
                    prompt_version TEXT NOT NULL,
                    settings_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    error_message TEXT,
                    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    finished_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (job_id, run_number)
                );

                CREATE INDEX IF NOT EXISTS idx_comparison_runs_job
                    ON comparison_runs(job_id, run_number DESC);

                CREATE TABLE IF NOT EXISTS diff_candidates (
                    run_id UUID NOT NULL
                        REFERENCES comparison_runs(run_id) ON DELETE CASCADE,
                    candidate_id TEXT NOT NULL,
                    sort_order INTEGER NOT NULL CHECK (sort_order >= 1),
                    candidate_json JSONB NOT NULL,
                    category TEXT
                        CHECK (
                            category IS NULL OR category IN (
                                'substantive',
                                'technical',
                                'alignment_error',
                                'ocr_uncertain'
                            )
                        ),
                    technical_type TEXT,
                    reason TEXT,
                    confidence DOUBLE PRECISION,
                    protection_tags TEXT[] NOT NULL DEFAULT '{}',
                    classified_by TEXT,
                    included_in_result BOOLEAN NOT NULL DEFAULT TRUE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (run_id, candidate_id),
                    UNIQUE (run_id, sort_order)
                );

                CREATE INDEX IF NOT EXISTS idx_diff_candidates_run_category
                    ON diff_candidates(run_id, category, sort_order);

                ALTER TABLE diff_candidates
                    ADD COLUMN IF NOT EXISTS included_in_result
                    BOOLEAN NOT NULL DEFAULT TRUE;

                CREATE TABLE IF NOT EXISTS classification_batches (
                    run_id UUID NOT NULL
                        REFERENCES comparison_runs(run_id) ON DELETE CASCADE,
                    batch_index INTEGER NOT NULL CHECK (batch_index >= 1),
                    candidate_ids TEXT[] NOT NULL,
                    request_json JSONB NOT NULL,
                    response_json JSONB,
                    parse_ok BOOLEAN NOT NULL DEFAULT FALSE,
                    failure_reason TEXT,
                    latency_ms INTEGER,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (run_id, batch_index)
                );

                CREATE TABLE IF NOT EXISTS comparison_results (
                    run_id UUID PRIMARY KEY
                        REFERENCES comparison_runs(run_id) ON DELETE CASCADE,
                    job_id TEXT NOT NULL
                        REFERENCES comparison_jobs(job_id) ON DELETE CASCADE,
                    verdict TEXT NOT NULL
                        CHECK (
                            verdict IN (
                                'identical', 'content_equal', 'different'
                            )
                        ),
                    comparison_json JSONB NOT NULL,
                    difference_count INTEGER NOT NULL DEFAULT 0,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );

                CREATE INDEX IF NOT EXISTS idx_comparison_results_job
                    ON comparison_results(job_id, created_at DESC);
                """
            )

    async def ensure_job(
        self,
        *,
        job_id: str,
        document_id: str,
        user_id: uuid.UUID,
        total_chunks: int = 0,
        status: str = "queued",
        message: str = "",
        file1_name: str = "",
        file2_name: str = "",
    ) -> dict[str, Any]:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            row = await self._upsert_job(
                conn,
                job_id=job_id,
                document_id=document_id,
                total_chunks=total_chunks,
                status=status,
                message=message,
                user_id=user_id,
                file1_name=file1_name,
                file2_name=file2_name,
            )
        return dict(row)

    @staticmethod
    async def _upsert_job(
        conn: asyncpg.Connection,
        *,
        job_id: str,
        document_id: str,
        total_chunks: int,
        status: str,
        message: str,
        user_id: uuid.UUID | None = None,
        file1_name: str = "",
        file2_name: str = "",
    ) -> asyncpg.Record:
        if user_id is None:
            row = await conn.fetchrow(
                """
                UPDATE comparison_jobs
                SET document_id = $2,
                    total_chunks = CASE
                        WHEN $3 > 0
                        THEN GREATEST(comparison_jobs.total_chunks, $3)
                        ELSE comparison_jobs.total_chunks
                    END,
                    status = CASE
                        WHEN $4 = 'failed' THEN 'failed'
                        WHEN comparison_jobs.status IN (
                            'ocr_ready', 'comparing', 'completed', 'failed'
                        ) AND $4 IN (
                            'queued', 'preparing', 'processing'
                        )
                        THEN comparison_jobs.status
                        ELSE $4
                    END,
                    last_message = CASE
                        WHEN $5 <> '' THEN $5
                        ELSE comparison_jobs.last_message
                    END,
                    file1_name = CASE
                        WHEN $6 <> '' THEN $6
                        ELSE comparison_jobs.file1_name
                    END,
                    file2_name = CASE
                        WHEN $7 <> '' THEN $7
                        ELSE comparison_jobs.file2_name
                    END,
                    updated_at = NOW()
                WHERE job_id = $1
                RETURNING *
                """,
                job_id,
                document_id,
                total_chunks,
                status,
                message,
                file1_name,
                file2_name,
            )
            if row is None:
                raise KeyError(
                    f"Job {job_id} is not registered; user_id is required"
                )
            return row

        return await conn.fetchrow(
            """
            INSERT INTO comparison_jobs (
                job_id,
                document_id,
                user_id,
                file1_name,
                file2_name,
                total_chunks,
                status,
                last_message
            )
            VALUES ($1, $2, $3, $4, $5, GREATEST($6, 0), $7, $8)
            ON CONFLICT (job_id) DO UPDATE SET
                document_id = EXCLUDED.document_id,
                total_chunks = CASE
                    WHEN EXCLUDED.total_chunks > 0
                    THEN GREATEST(
                        comparison_jobs.total_chunks,
                        EXCLUDED.total_chunks
                    )
                    ELSE comparison_jobs.total_chunks
                END,
                status = CASE
                    WHEN EXCLUDED.status = 'failed' THEN 'failed'
                    WHEN comparison_jobs.status IN (
                        'ocr_ready', 'comparing', 'completed', 'failed'
                    ) AND EXCLUDED.status IN (
                        'queued', 'preparing', 'processing'
                    )
                    THEN comparison_jobs.status
                    ELSE EXCLUDED.status
                END,
                last_message = CASE
                    WHEN EXCLUDED.last_message <> ''
                    THEN EXCLUDED.last_message
                    ELSE comparison_jobs.last_message
                END,
                file1_name = CASE
                    WHEN EXCLUDED.file1_name <> ''
                    THEN EXCLUDED.file1_name
                    ELSE comparison_jobs.file1_name
                END,
                file2_name = CASE
                    WHEN EXCLUDED.file2_name <> ''
                    THEN EXCLUDED.file2_name
                    ELSE comparison_jobs.file2_name
                END,
                updated_at = NOW()
            RETURNING *
            """,
            job_id,
            document_id,
            user_id,
            file1_name,
            file2_name,
            total_chunks,
            status,
            message,
        )

    async def chunk_is_stored(self, job_id: str, chunk_index: int) -> bool:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            count = await conn.fetchval(
                """
                SELECT COUNT(*)
                FROM ocr_chunks
                WHERE job_id = $1 AND chunk_index = $2
                """,
                job_id,
                chunk_index,
            )
        return count == 2

    async def save_ocr_pair(
        self,
        message: RawChunkMessage,
        file1: ChunkContent,
        file2: ChunkContent,
    ) -> tuple[int, bool]:
        """Persist both sides atomically and return (stored_chunks, job_ready)."""
        pool = self._require_pool()
        async with pool.acquire() as conn:
            async with conn.transaction():
                await self._upsert_job(
                    conn,
                    job_id=message.job_id,
                    document_id=message.document_id,
                    total_chunks=message.total_chunks,
                    status="processing",
                    message=(
                        f"Выполняется сканирование файлов: "
                        f"{message.chunk_index} из {message.total_chunks}"
                    ),
                )
                await self._save_side(
                    conn,
                    message=message,
                    side=1,
                    original=message.file1,
                    text_chunk=file1,
                )
                await self._save_side(
                    conn,
                    message=message,
                    side=2,
                    original=message.file2,
                    text_chunk=file2,
                )

                stored_chunks = await conn.fetchval(
                    """
                    SELECT COUNT(*)
                    FROM (
                        SELECT chunk_index
                        FROM ocr_chunks
                        WHERE job_id = $1
                        GROUP BY chunk_index
                        HAVING COUNT(DISTINCT side) = 2
                    ) AS completed
                    """,
                    message.job_id,
                )
                ready = (
                    message.total_chunks > 0
                    and stored_chunks >= message.total_chunks
                )
                status = "ocr_ready" if ready else "processing"
                status_message = (
                    "Сканирование завершено, начинается сравнение"
                    if ready
                    else (
                        f"Выполняется сканирование файлов: "
                        f"{stored_chunks} из {message.total_chunks}"
                    )
                )
                await conn.execute(
                    """
                    UPDATE comparison_jobs
                    SET processed_chunks = GREATEST(processed_chunks, $2),
                        status = CASE
                            WHEN status IN (
                                'failed', 'completed', 'comparing', 'ocr_ready'
                            )
                            THEN status
                            ELSE $3
                        END,
                        last_message = $4,
                        updated_at = NOW()
                    WHERE job_id = $1
                    """,
                    message.job_id,
                    stored_chunks,
                    status,
                    status_message,
                )
        return stored_chunks, ready

    @staticmethod
    async def _save_side(
        conn: asyncpg.Connection,
        *,
        message: RawChunkMessage,
        side: int,
        original: ChunkContent | None,
        text_chunk: ChunkContent,
    ) -> None:
        was_ocr = (
            original is not None
            and original.content_type == ContentType.IMAGE
        )
        await conn.execute(
            """
            INSERT INTO ocr_chunks (
                job_id,
                chunk_index,
                side,
                filename,
                format,
                source_content_type,
                was_ocr,
                is_missing,
                text_content,
                ocr_model
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (job_id, chunk_index, side) DO UPDATE SET
                filename = EXCLUDED.filename,
                format = EXCLUDED.format,
                source_content_type = EXCLUDED.source_content_type,
                was_ocr = EXCLUDED.was_ocr,
                is_missing = EXCLUDED.is_missing,
                text_content = EXCLUDED.text_content,
                ocr_model = EXCLUDED.ocr_model,
                updated_at = NOW()
            """,
            message.job_id,
            message.chunk_index,
            side,
            "" if original is None else original.filename,
            "" if original is None else original.format,
            "missing" if original is None else original.content_type.value,
            was_ocr,
            original is None,
            text_chunk.content,
            settings.ollama_model if was_ocr else None,
        )

    async def get_stored_count(self, job_id: str) -> int:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            return await conn.fetchval(
                """
                SELECT COUNT(*)
                FROM (
                    SELECT chunk_index
                    FROM ocr_chunks
                    WHERE job_id = $1
                    GROUP BY chunk_index
                    HAVING COUNT(DISTINCT side) = 2
                ) AS completed
                """,
                job_id,
            )

    async def try_claim_comparison(self, job_id: str) -> bool:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                UPDATE comparison_jobs
                SET comparison_claimed = TRUE,
                    status = 'comparing',
                    last_message = 'Сравнение документов…',
                    updated_at = NOW()
                WHERE job_id = $1
                  AND comparison_claimed = FALSE
                  AND status NOT IN ('failed', 'completed')
                  AND total_chunks > 0
                  AND processed_chunks >= total_chunks
                RETURNING job_id
                """,
                job_id,
            )
        return row is not None

    async def mark_comparison_completed(self, job_id: str) -> None:
        await self._set_status(
            job_id,
            status="completed",
            message="Сравнение завершено",
            release_claim=False,
        )

    async def mark_failed(self, job_id: str, error: str) -> None:
        await self._set_status(
            job_id,
            status="failed",
            message=error,
            release_claim=True,
        )

    async def _set_status(
        self,
        job_id: str,
        *,
        status: str,
        message: str,
        release_claim: bool,
    ) -> None:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE comparison_jobs
                SET status = $2,
                    last_message = $3,
                    comparison_claimed = CASE
                        WHEN $4 THEN FALSE
                        ELSE comparison_claimed
                    END,
                    updated_at = NOW()
                WHERE job_id = $1
                """,
                job_id,
                status,
                message,
                release_claim,
            )

    async def get_job(self, job_id: str) -> dict[str, Any] | None:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM comparison_jobs WHERE job_id = $1",
                job_id,
            )
        return None if row is None else dict(row)

    async def list_jobs(
        self,
        user_id: uuid.UUID | None = None,
    ) -> list[dict[str, Any]]:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            if user_id is None:
                rows = await conn.fetch(
                    """
                    SELECT *
                    FROM comparison_jobs
                    ORDER BY created_at DESC
                    """
                )
            else:
                rows = await conn.fetch(
                    """
                    SELECT *
                    FROM comparison_jobs
                    WHERE user_id = $1
                    ORDER BY created_at DESC
                    """,
                    user_id,
                )
        return [dict(row) for row in rows]

    async def delete_job(self, job_id: str) -> bool:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            result = await conn.execute(
                "DELETE FROM comparison_jobs WHERE job_id = $1",
                job_id,
            )
        return result == "DELETE 1"

    async def list_ready_jobs(self) -> list[str]:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT job_id
                FROM comparison_jobs
                WHERE status = 'ocr_ready'
                  AND comparison_claimed = FALSE
                  AND total_chunks > 0
                  AND processed_chunks >= total_chunks
                ORDER BY created_at
                """
            )
        return [row["job_id"] for row in rows]

    async def create_comparison_run(
        self,
        job_id: str,
        *,
        algorithm_version: str,
        prompt_version: str,
        ollama_model: str,
        settings_data: dict[str, Any] | None = None,
    ) -> str:
        pool = self._require_pool()
        run_id = uuid.uuid4()
        async with pool.acquire() as conn:
            async with conn.transaction():
                run_number = await conn.fetchval(
                    """
                    SELECT COALESCE(MAX(run_number), 0) + 1
                    FROM comparison_runs
                    WHERE job_id = $1
                    """,
                    job_id,
                )
                await conn.execute(
                    """
                    INSERT INTO comparison_runs (
                        run_id,
                        job_id,
                        run_number,
                        algorithm_version,
                        ollama_model,
                        prompt_version,
                        settings_json
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
                    """,
                    run_id,
                    job_id,
                    run_number,
                    algorithm_version,
                    ollama_model,
                    prompt_version,
                    json.dumps(settings_data or {}, ensure_ascii=False),
                )
        return str(run_id)

    async def save_diff_candidates(
        self,
        run_id: str,
        candidates: list[dict[str, Any]],
    ) -> None:
        if not candidates:
            return
        pool = self._require_pool()
        values = [
            (
                uuid.UUID(run_id),
                str(candidate["candidate_id"]),
                index,
                json.dumps(candidate, ensure_ascii=False),
            )
            for index, candidate in enumerate(candidates, start=1)
        ]
        async with pool.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO diff_candidates (
                    run_id, candidate_id, sort_order, candidate_json
                )
                VALUES ($1, $2, $3, $4::jsonb)
                ON CONFLICT (run_id, candidate_id) DO UPDATE SET
                    sort_order = EXCLUDED.sort_order,
                    candidate_json = EXCLUDED.candidate_json,
                    updated_at = NOW()
                """,
                values,
            )

    async def update_candidate_classifications(
        self,
        run_id: str,
        classifications: list[dict[str, Any]],
    ) -> None:
        if not classifications:
            return
        values = [
            (
                uuid.UUID(run_id),
                str(item["candidate_id"]),
                item["category"],
                item.get("technical_type"),
                item.get("reason"),
                item.get("confidence"),
                list(item.get("protection_tags") or []),
                item.get("classified_by", "deterministic"),
                bool(item.get("include_in_result", True)),
            )
            for item in classifications
        ]
        pool = self._require_pool()
        async with pool.acquire() as conn:
            await conn.executemany(
                """
                UPDATE diff_candidates
                SET category = $3,
                    technical_type = $4,
                    reason = $5,
                    confidence = $6,
                    protection_tags = $7,
                    classified_by = $8,
                    included_in_result = $9,
                    updated_at = NOW()
                WHERE run_id = $1 AND candidate_id = $2
                """,
                values,
            )

    async def record_classification_batch(
        self,
        run_id: str,
        *,
        batch_index: int,
        candidate_ids: list[str],
        request_data: Any,
    ) -> None:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO classification_batches (
                    run_id, batch_index, candidate_ids, request_json
                )
                VALUES ($1, $2, $3, $4::jsonb)
                ON CONFLICT (run_id, batch_index) DO UPDATE SET
                    candidate_ids = EXCLUDED.candidate_ids,
                    request_json = EXCLUDED.request_json,
                    response_json = NULL,
                    parse_ok = FALSE,
                    failure_reason = NULL,
                    latency_ms = NULL,
                    updated_at = NOW()
                """,
                uuid.UUID(run_id),
                batch_index,
                candidate_ids,
                json.dumps(request_data, ensure_ascii=False),
            )

    async def complete_classification_batch(
        self,
        run_id: str,
        *,
        batch_index: int,
        response_data: Any | None,
        parse_ok: bool,
        failure_reason: str | None,
        latency_ms: int,
    ) -> None:
        pool = self._require_pool()
        encoded_response = (
            None
            if response_data is None
            else json.dumps(response_data, ensure_ascii=False)
        )
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE classification_batches
                SET response_json = $3::jsonb,
                    parse_ok = $4,
                    failure_reason = $5,
                    latency_ms = $6,
                    updated_at = NOW()
                WHERE run_id = $1 AND batch_index = $2
                """,
                uuid.UUID(run_id),
                batch_index,
                encoded_response,
                parse_ok,
                failure_reason,
                latency_ms,
            )

    async def save_comparison_result(
        self,
        run_id: str,
        *,
        job_id: str,
        comparison: dict[str, Any],
    ) -> None:
        pool = self._require_pool()
        verdict = str(comparison["verdict"])
        async with pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    """
                    INSERT INTO comparison_results (
                        run_id,
                        job_id,
                        verdict,
                        comparison_json,
                        difference_count
                    )
                    VALUES ($1, $2, $3, $4::jsonb, $5)
                    ON CONFLICT (run_id) DO UPDATE SET
                        verdict = EXCLUDED.verdict,
                        comparison_json = EXCLUDED.comparison_json,
                        difference_count = EXCLUDED.difference_count
                    """,
                    uuid.UUID(run_id),
                    job_id,
                    verdict,
                    json.dumps(comparison, ensure_ascii=False),
                    len(comparison.get("differences") or []),
                )
                await conn.execute(
                    """
                    UPDATE comparison_runs
                    SET status = 'completed',
                        finished_at = NOW(),
                        error_message = NULL
                    WHERE run_id = $1
                    """,
                    uuid.UUID(run_id),
                )

    async def mark_comparison_run_failed(
        self,
        run_id: str,
        error: str,
    ) -> None:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE comparison_runs
                SET status = 'failed',
                    error_message = $2,
                    finished_at = NOW()
                WHERE run_id = $1
                """,
                uuid.UUID(run_id),
                error,
            )

    async def get_comparison_result(
        self,
        job_id: str,
    ) -> dict[str, Any] | None:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            value = await conn.fetchval(
                """
                SELECT comparison_json
                FROM comparison_results
                WHERE job_id = $1
                ORDER BY created_at DESC
                LIMIT 1
                """,
                job_id,
            )
        if value is None:
            return None
        return json.loads(value) if isinstance(value, str) else value

    async def get_ocr_chunks(
        self,
        job_id: str,
        side: int | None = None,
    ) -> list[dict[str, Any]]:
        pool = self._require_pool()
        async with pool.acquire() as conn:
            if side is None:
                rows = await conn.fetch(
                    """
                    SELECT *
                    FROM ocr_chunks
                    WHERE job_id = $1
                    ORDER BY chunk_index, side
                    """,
                    job_id,
                )
            else:
                rows = await conn.fetch(
                    """
                    SELECT *
                    FROM ocr_chunks
                    WHERE job_id = $1 AND side = $2
                    ORDER BY chunk_index
                    """,
                    job_id,
                    side,
                )
        return [dict(row) for row in rows]

    async def load_documents(self, job_id: str) -> StoredDocumentPair:
        job = await self.get_job(job_id)
        if job is None:
            raise KeyError(f"Job {job_id} is not registered in PostgreSQL")

        chunks = await self.get_ocr_chunks(job_id)
        by_side: dict[int, list[dict[str, Any]]] = {1: [], 2: []}
        for chunk in chunks:
            by_side[chunk["side"]].append(chunk)

        def assemble(side: int) -> StoredDocument:
            side_chunks = by_side[side]
            filename = next(
                (
                    row["filename"]
                    for row in side_chunks
                    if row["filename"]
                ),
                f"file{side}",
            )
            pages = [
                StoredPage(
                    chunk_index=row["chunk_index"],
                    side=side,
                    filename=row["filename"] or filename,
                    text=row["text_content"],
                    source_content_type=row["source_content_type"],
                    was_ocr=row["was_ocr"],
                    is_missing=row["is_missing"],
                )
                for row in side_chunks
                if not row["is_missing"]
            ]
            text = "\n\n".join(page.text for page in pages if page.text)
            return StoredDocument(
                side=side,
                filename=filename,
                text=text,
                chunks=side_chunks,
                pages=pages,
            )

        return StoredDocumentPair(
            job_id=job_id,
            document_id=job["document_id"],
            total_chunks=job["total_chunks"],
            file1=assemble(1),
            file2=assemble(2),
        )
