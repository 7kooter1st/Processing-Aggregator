from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def new_event_id() -> str:
    return str(uuid.uuid4())


def command_envelope(
    *,
    job_id: str,
    task_id: str,
    stage: str,
    payload: dict[str, Any],
    attempt: int = 1,
    state_version: int = 1,
    pipeline_version: str = "v2",
    correlation_id: str | None = None,
    causation_id: str | None = None,
    traceparent: str | None = None,
) -> dict[str, Any]:
    event_id = new_event_id()
    body = {
        "event_id": event_id,
        "task_id": task_id,
        "job_id": job_id,
        "stage": stage,
        "attempt": attempt,
        "state_version": state_version,
        "pipeline_version": pipeline_version,
        "correlation_id": correlation_id or job_id,
        "causation_id": causation_id or event_id,
        "traceparent": traceparent,
        "created_at": utc_now(),
        "payload": payload,
    }
    return body
