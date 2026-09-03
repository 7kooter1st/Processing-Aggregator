"""HA workflow schema: state machine, inbox/outbox, leases, objects.

Revision ID: 0001_ha_workflow
Revises:
Create Date: 2026-09-02
"""

from typing import Sequence, Union

from alembic import op

revision: str = "0001_ha_workflow"
down_revision: Union[str, Sequence[str], None] = None
branch_labels = None
depends_on = None


UPGRADE_SQL = r"""
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
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    expires_at TIMESTAMPTZ NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    revoked_at TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions(user_id);

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

ALTER TABLE comparison_jobs ADD COLUMN IF NOT EXISTS state_version INTEGER NOT NULL DEFAULT 1;
ALTER TABLE comparison_jobs ADD COLUMN IF NOT EXISTS pipeline_version TEXT NOT NULL DEFAULT 'v1';
ALTER TABLE comparison_jobs ADD COLUMN IF NOT EXISTS queued_at TIMESTAMPTZ;
ALTER TABLE comparison_jobs ADD COLUMN IF NOT EXISTS preparing_at TIMESTAMPTZ;
ALTER TABLE comparison_jobs ADD COLUMN IF NOT EXISTS processing_at TIMESTAMPTZ;
ALTER TABLE comparison_jobs ADD COLUMN IF NOT EXISTS comparing_at TIMESTAMPTZ;
ALTER TABLE comparison_jobs ADD COLUMN IF NOT EXISTS completed_at TIMESTAMPTZ;
ALTER TABLE comparison_jobs ADD COLUMN IF NOT EXISTS failed_at TIMESTAMPTZ;
ALTER TABLE comparison_jobs ADD COLUMN IF NOT EXISTS cancelled_at TIMESTAMPTZ;
ALTER TABLE comparison_jobs ADD COLUMN IF NOT EXISTS failure_code TEXT;
ALTER TABLE comparison_jobs ADD COLUMN IF NOT EXISTS cancel_requested_at TIMESTAMPTZ;
ALTER TABLE comparison_jobs ADD COLUMN IF NOT EXISTS lease_owner TEXT;
ALTER TABLE comparison_jobs ADD COLUMN IF NOT EXISTS lease_token UUID;
ALTER TABLE comparison_jobs ADD COLUMN IF NOT EXISTS lease_epoch INTEGER NOT NULL DEFAULT 0;
ALTER TABLE comparison_jobs ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ;
ALTER TABLE comparison_jobs ADD COLUMN IF NOT EXISTS last_event_seq BIGINT NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS ocr_chunks (
    job_id TEXT NOT NULL REFERENCES comparison_jobs(job_id) ON DELETE CASCADE,
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
    job_id TEXT NOT NULL REFERENCES comparison_jobs(job_id) ON DELETE CASCADE,
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
    job_id TEXT NOT NULL REFERENCES comparison_jobs(job_id) ON DELETE CASCADE,
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
    run_id UUID NOT NULL REFERENCES comparison_runs(run_id) ON DELETE CASCADE,
    candidate_id TEXT NOT NULL,
    sort_order INTEGER NOT NULL CHECK (sort_order >= 1),
    candidate_json JSONB NOT NULL,
    category TEXT,
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
ALTER TABLE diff_candidates ADD COLUMN IF NOT EXISTS included_in_result BOOLEAN NOT NULL DEFAULT TRUE;

CREATE TABLE IF NOT EXISTS classification_batches (
    run_id UUID NOT NULL REFERENCES comparison_runs(run_id) ON DELETE CASCADE,
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
    run_id UUID PRIMARY KEY REFERENCES comparison_runs(run_id) ON DELETE CASCADE,
    job_id TEXT NOT NULL REFERENCES comparison_jobs(job_id) ON DELETE CASCADE,
    verdict TEXT NOT NULL,
    comparison_json JSONB NOT NULL,
    difference_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_comparison_results_job
    ON comparison_results(job_id, created_at DESC);

CREATE TABLE IF NOT EXISTS object_assets (
    id UUID PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES comparison_jobs(job_id) ON DELETE CASCADE,
    kind TEXT NOT NULL,
    side SMALLINT,
    page_number INTEGER,
    object_key TEXT NOT NULL UNIQUE,
    sha256 TEXT NOT NULL,
    size_bytes BIGINT NOT NULL DEFAULT 0,
    content_type TEXT NOT NULL DEFAULT 'application/octet-stream',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_object_assets_job ON object_assets(job_id, kind);

CREATE TABLE IF NOT EXISTS document_pages (
    job_id TEXT NOT NULL REFERENCES comparison_jobs(job_id) ON DELETE CASCADE,
    side SMALLINT NOT NULL CHECK (side IN (1, 2)),
    page_number INTEGER NOT NULL CHECK (page_number >= 1),
    object_key TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    content_type TEXT NOT NULL,
    filename TEXT NOT NULL DEFAULT '',
    format TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (job_id, side, page_number)
);

CREATE TABLE IF NOT EXISTS work_items (
    id UUID PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES comparison_jobs(job_id) ON DELETE CASCADE,
    stage TEXT NOT NULL,
    task_id TEXT NOT NULL UNIQUE,
    side SMALLINT,
    chunk_index INTEGER,
    status TEXT NOT NULL DEFAULT 'pending',
    attempt INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 8,
    payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    lease_owner TEXT,
    lease_token UUID,
    lease_epoch INTEGER NOT NULL DEFAULT 0,
    lease_expires_at TIMESTAMPTZ,
    available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_work_items_stage_status
    ON work_items(stage, status, available_at);

CREATE TABLE IF NOT EXISTS work_attempts (
    id UUID PRIMARY KEY,
    work_item_id UUID NOT NULL REFERENCES work_items(id) ON DELETE CASCADE,
    attempt INTEGER NOT NULL,
    worker_id TEXT,
    lease_token UUID,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    error_message TEXT,
    outcome TEXT
);

CREATE TABLE IF NOT EXISTS consumer_inbox (
    consumer_group TEXT NOT NULL,
    event_id UUID NOT NULL,
    job_id TEXT,
    topic TEXT NOT NULL DEFAULT '',
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (consumer_group, event_id)
);

CREATE TABLE IF NOT EXISTS outbox (
    id UUID PRIMARY KEY,
    job_id TEXT,
    topic TEXT NOT NULL,
    message_key TEXT NOT NULL,
    payload_json JSONB NOT NULL,
    headers_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ,
    publish_attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    claimed_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_outbox_unpublished
    ON outbox(created_at) WHERE published_at IS NULL;

CREATE TABLE IF NOT EXISTS job_events (
    job_id TEXT NOT NULL REFERENCES comparison_jobs(job_id) ON DELETE CASCADE,
    seq BIGINT NOT NULL,
    event_type TEXT NOT NULL,
    payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (job_id, seq)
);

CREATE TABLE IF NOT EXISTS idempotency_requests (
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    idempotency_key TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    job_id TEXT NOT NULL REFERENCES comparison_jobs(job_id) ON DELETE CASCADE,
    response_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, idempotency_key)
);
"""


def upgrade() -> None:
    op.execute(UPGRADE_SQL)


def downgrade() -> None:
    op.execute(
        """
        DROP TABLE IF EXISTS idempotency_requests;
        DROP TABLE IF EXISTS job_events;
        DROP TABLE IF EXISTS outbox;
        DROP TABLE IF EXISTS consumer_inbox;
        DROP TABLE IF EXISTS work_attempts;
        DROP TABLE IF EXISTS work_items;
        DROP TABLE IF EXISTS document_pages;
        DROP TABLE IF EXISTS object_assets;
        """
    )
