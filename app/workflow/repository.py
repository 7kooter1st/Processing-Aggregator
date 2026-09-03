from __future__ import annotations

import json
import logging
import uuid
from typing import Any

import asyncpg

from app.workflow.states import TERMINAL_STATUSES, can_transition

logger = logging.getLogger(__name__)

LEASE_SECONDS = 15 * 60


class WorkflowRepository:
    """CAS job transitions, work-item leases, inbox and outbox."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def transition_job(
        self,
        job_id: str,
        *,
        from_status: str | None,
        to_status: str,
        message: str = "",
        failure_code: str | None = None,
        expected_version: int | None = None,
        extra_set: str = "",
        extra_args: tuple[Any, ...] = (),
    ) -> dict[str, Any] | None:
        if from_status is not None and not can_transition(from_status, to_status):
            logger.warning(
                "[WORKFLOW] illegal transition job=%s %s -> %s",
                job_id,
                from_status,
                to_status,
            )
            return None
        args: list[Any] = [job_id, to_status, message, failure_code]
        version_clause = ""
        if expected_version is not None:
            version_clause = "AND state_version = $5"
            args.append(expected_version)
        from_clause = ""
        if from_status is not None:
            args.append(from_status)
            idx = len(args)
            from_clause = f"AND status = ${idx}"
        terminal_guard = ""
        if to_status not in TERMINAL_STATUSES:
            terminal_guard = (
                "AND status NOT IN ('completed', 'failed', 'cancelled', 'deleted')"
            )
        ts_column = {
            "queued": "queued_at",
            "preparing": "preparing_at",
            "processing": "processing_at",
            "comparing": "comparing_at",
            "completed": "completed_at",
            "failed": "failed_at",
            "cancelled": "cancelled_at",
        }.get(to_status)
        ts_sql = f", {ts_column} = COALESCE({ts_column}, NOW())" if ts_column else ""
        sql = f"""
            UPDATE comparison_jobs
            SET status = $2,
                last_message = CASE WHEN $3 <> '' THEN $3 ELSE last_message END,
                failure_code = COALESCE($4, failure_code),
                state_version = state_version + 1,
                updated_at = NOW()
                {ts_sql}
                {extra_set}
            WHERE job_id = $1
              {version_clause}
              {from_clause}
              {terminal_guard}
            RETURNING *
        """
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(sql, *args, *extra_args)
        return None if row is None else dict(row)

    async def request_cancel(self, job_id: str) -> dict[str, Any] | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                UPDATE comparison_jobs
                SET cancel_requested_at = COALESCE(cancel_requested_at, NOW()),
                    status = CASE
                        WHEN status IN ('completed', 'failed', 'cancelled', 'deleted', 'deleting')
                        THEN status
                        ELSE 'cancel_requested'
                    END,
                    last_message = CASE
                        WHEN status IN ('completed', 'failed', 'cancelled', 'deleted', 'deleting')
                        THEN last_message
                        ELSE 'Отмена запрошена'
                    END,
                    state_version = state_version + 1,
                    updated_at = NOW()
                WHERE job_id = $1
                RETURNING *
                """,
                job_id,
            )
        return None if row is None else dict(row)

    async def complete_cancel(self, job_id: str) -> dict[str, Any] | None:
        return await self.transition_job(
            job_id,
            from_status="cancel_requested",
            to_status="cancelled",
            message="Сравнение отменено",
        )

    async def begin_delete(self, job_id: str) -> dict[str, Any] | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                UPDATE comparison_jobs
                SET status = 'deleting',
                    last_message = 'Удаление…',
                    state_version = state_version + 1,
                    updated_at = NOW()
                WHERE job_id = $1
                  AND status <> 'deleted'
                RETURNING *
                """,
                job_id,
            )
        return None if row is None else dict(row)

    async def finish_delete(self, job_id: str) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE comparison_jobs
                SET status = 'deleted',
                    last_message = 'Удалено',
                    state_version = state_version + 1,
                    updated_at = NOW()
                WHERE job_id = $1
                """,
                job_id,
            )

    async def enqueue_outbox(
        self,
        conn: asyncpg.Connection,
        *,
        job_id: str,
        topic: str,
        key: str,
        payload: dict[str, Any],
        headers: dict[str, Any] | None = None,
    ) -> str:
        outbox_id = uuid.uuid4()
        await conn.execute(
            """
            INSERT INTO outbox (
                id, job_id, topic, message_key, payload_json, headers_json
            )
            VALUES ($1, $2, $3, $4, $5::jsonb, $6::jsonb)
            """,
            outbox_id,
            job_id,
            topic,
            key,
            json.dumps(payload, ensure_ascii=False),
            json.dumps(headers or {}, ensure_ascii=False),
        )
        return str(outbox_id)

    async def append_job_event(
        self,
        conn: asyncpg.Connection,
        *,
        job_id: str,
        event_type: str,
        payload: dict[str, Any],
    ) -> int:
        seq = await conn.fetchval(
            """
            UPDATE comparison_jobs
            SET last_event_seq = last_event_seq + 1,
                updated_at = NOW()
            WHERE job_id = $1
            RETURNING last_event_seq
            """,
            job_id,
        )
        await conn.execute(
            """
            INSERT INTO job_events (job_id, seq, event_type, payload_json)
            VALUES ($1, $2, $3, $4::jsonb)
            """,
            job_id,
            seq,
            event_type,
            json.dumps(payload, ensure_ascii=False),
        )
        return int(seq)

    async def claim_inbox(
        self,
        *,
        consumer_group: str,
        event_id: str,
        job_id: str,
        topic: str,
    ) -> bool:
        async with self._pool.acquire() as conn:
            try:
                await conn.execute(
                    """
                    INSERT INTO consumer_inbox (
                        consumer_group, event_id, job_id, topic
                    )
                    VALUES ($1, $2::uuid, $3, $4)
                    """,
                    consumer_group,
                    event_id,
                    job_id,
                    topic,
                )
                return True
            except asyncpg.UniqueViolationError:
                return False

    async def create_work_item(
        self,
        conn: asyncpg.Connection,
        *,
        job_id: str,
        stage: str,
        task_id: str,
        payload: dict[str, Any],
        side: int | None = None,
        chunk_index: int | None = None,
        max_attempts: int = 8,
    ) -> str:
        item_id = uuid.uuid4()
        await conn.execute(
            """
            INSERT INTO work_items (
                id, job_id, stage, task_id, side, chunk_index,
                payload_json, max_attempts
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8)
            ON CONFLICT (task_id) DO NOTHING
            """,
            item_id,
            job_id,
            stage,
            task_id,
            side,
            chunk_index,
            json.dumps(payload, ensure_ascii=False),
            max_attempts,
        )
        return str(item_id)

    async def lease_work_item(
        self,
        *,
        stage: str,
        owner: str,
        lease_seconds: int = LEASE_SECONDS,
    ) -> dict[str, Any] | None:
        token = uuid.uuid4()
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                UPDATE work_items
                SET status = 'leased',
                    attempt = attempt + 1,
                    lease_owner = $2,
                    lease_token = $3,
                    lease_epoch = lease_epoch + 1,
                    lease_expires_at = NOW() + ($4 || ' seconds')::interval,
                    updated_at = NOW()
                WHERE id = (
                    SELECT id FROM work_items
                    WHERE stage = $1
                      AND status IN ('pending', 'leased')
                      AND available_at <= NOW()
                      AND (
                        status = 'pending'
                        OR lease_expires_at IS NULL
                        OR lease_expires_at < NOW()
                      )
                    ORDER BY created_at
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                RETURNING *
                """,
                stage,
                owner,
                token,
                str(lease_seconds),
            )
        return None if row is None else dict(row)

    async def complete_work_item(
        self,
        *,
        work_item_id: str,
        lease_token: str,
        lease_epoch: int,
        outcome: str,
        error: str | None = None,
    ) -> bool:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                UPDATE work_items
                SET status = $4,
                    lease_owner = NULL,
                    lease_expires_at = NULL,
                    updated_at = NOW()
                WHERE id = $1::uuid
                  AND lease_token = $2::uuid
                  AND lease_epoch = $3
                RETURNING id
                """,
                work_item_id,
                lease_token,
                lease_epoch,
                outcome,
            )
            await conn.execute(
                """
                INSERT INTO work_attempts (
                    id, work_item_id, attempt, worker_id, lease_token,
                    finished_at, error_message, outcome
                )
                SELECT $1, $2::uuid, attempt, lease_owner, lease_token,
                       NOW(), $3, $4
                FROM work_items WHERE id = $2::uuid
                """,
                uuid.uuid4(),
                work_item_id,
                error,
                outcome,
            )
        return row is not None

    async def retry_work_item(
        self,
        *,
        work_item_id: str,
        lease_token: str,
        lease_epoch: int,
        delay_seconds: float,
        error: str,
    ) -> bool:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                UPDATE work_items
                SET status = CASE
                        WHEN attempt >= max_attempts THEN 'failed'
                        ELSE 'pending'
                    END,
                    available_at = NOW() + ($4 || ' seconds')::interval,
                    lease_owner = NULL,
                    lease_expires_at = NULL,
                    updated_at = NOW()
                WHERE id = $1::uuid
                  AND lease_token = $2::uuid
                  AND lease_epoch = $3
                RETURNING status, attempt, max_attempts
                """,
                work_item_id,
                lease_token,
                lease_epoch,
                str(delay_seconds),
            )
        if row is None:
            return False
        logger.warning(
            "[WORKFLOW] retry work_item=%s status=%s attempt=%s/%s error=%s",
            work_item_id,
            row["status"],
            row["attempt"],
            row["max_attempts"],
            error,
        )
        return row["status"] == "pending"

    async def claim_unpublished_outbox(self, limit: int = 20) -> list[dict[str, Any]]:
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                rows = await conn.fetch(
                    """
                    UPDATE outbox
                    SET claimed_at = NOW(),
                        publish_attempts = publish_attempts + 1
                    WHERE id IN (
                        SELECT id
                        FROM outbox
                        WHERE published_at IS NULL
                          AND (
                            claimed_at IS NULL
                            OR claimed_at < NOW() - INTERVAL '30 seconds'
                          )
                        ORDER BY created_at
                        FOR UPDATE SKIP LOCKED
                        LIMIT $1
                    )
                    RETURNING *
                    """,
                    limit,
                )
        return [dict(row) for row in rows]

    async def mark_outbox_published(self, outbox_id: str) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE outbox
                SET published_at = NOW(),
                    last_error = NULL
                WHERE id = $1::uuid
                """,
                outbox_id,
            )

    async def mark_outbox_error(self, outbox_id: str, error: str) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE outbox
                SET publish_attempts = publish_attempts + 1,
                    last_error = $2
                WHERE id = $1::uuid
                """,
                outbox_id,
                error[:1000],
            )

    async def oldest_outbox_age_seconds(self) -> float:
        async with self._pool.acquire() as conn:
            value = await conn.fetchval(
                """
                SELECT EXTRACT(EPOCH FROM (NOW() - MIN(created_at)))
                FROM outbox
                WHERE published_at IS NULL
                """
            )
        return float(value or 0)

    async def events_after(self, job_id: str, last_seq: int) -> list[dict[str, Any]]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT seq, event_type, payload_json, created_at
                FROM job_events
                WHERE job_id = $1 AND seq > $2
                ORDER BY seq
                """,
                job_id,
                last_seq,
            )
        result = []
        for row in rows:
            item = dict(row)
            payload = item.get("payload_json")
            if isinstance(payload, str):
                item["payload_json"] = json.loads(payload)
            result.append(item)
        return result
