from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

_PROCESSING_ROOT = Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_name: str = "Processing Service"
    app_host: str = "0.0.0.0"
    app_port: int = 5001
    worker_id: str = ""

    database_url: str = (
        "postgresql://comparator:comparator@127.0.0.1:5432/comparator"
    )
    database_pool_min_size: int = 1
    database_pool_max_size: int = 5
    internal_api_token: str = ""

    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_consumer_group: str = "processing-service"
    kafka_topic_raw_chunks: str = "raw_chunks"
    kafka_topic_processed_results: str = "processed_results"
    kafka_topic_status_updates: str = "status_updates"
    kafka_topic_dlt: str = "raw_chunks_dlt"
    kafka_topic_ocr_cmd: str = "cmp.ocr.cmd.v1"
    kafka_topic_diff_cmd: str = "cmp.diff.cmd.v1"
    kafka_topic_classify_cmd: str = "cmp.classify.cmd.v1"
    kafka_topic_finalize_cmd: str = "cmp.finalize.cmd.v1"
    kafka_topic_stage_event: str = "cmp.stage.event.v1"
    kafka_topic_job_event: str = "cmp.job.event.v1"
    kafka_topic_ocr_retry: str = "cmp.ocr.retry.v1"
    kafka_topic_ocr_dlt: str = "cmp.ocr.dlt.v1"
    kafka_replication_factor: int = 3
    kafka_min_insync_replicas: int = 2

    # Prefer 127.0.0.1 over localhost to avoid IPv6 (::1) connect failures on Windows.
    ollama_base_url: str = "http://127.0.0.1:11434"
    ollama_model: str = "gemma4"
    ollama_num_ctx: int = 8192
    ollama_timeout_seconds: float = 300.0
    ollama_temperature: float = 0.1
    ollama_think: bool = False
    ollama_retries: int = 3
    ollama_retry_delay_seconds: float = 2.0
    ollama_startup_timeout_seconds: float = 120.0
    ocr_image_max_width: int = 1280
    ocr_image_jpeg_quality: int = 75

    consumer_max_concurrent: int = 1
    compare_max_concurrent: int = 1
    ollama_max_concurrent: int = 1
    kafka_consumer_group_aggregator: str = "processing-service-aggregator"
    kafka_max_request_size_bytes: int = 10 * 1024 * 1024
    kafka_max_poll_records: int = 1
    kafka_max_poll_interval_ms: int = 1_800_000
    kafka_session_timeout_ms: int = 45_000
    pipeline_version: str = "v2"
    allow_multiple_replicas: bool = False

    object_store_backend: str = "s3"
    object_store_root: Path = _PROCESSING_ROOT.parent / "data" / "objects"
    s3_endpoint_url: str = "http://127.0.0.1:9000"
    s3_access_key: str = "comparator"
    s3_secret_key: str = "comparator-secret"
    s3_bucket: str = "comparator"
    s3_region: str = "us-east-1"

    lease_seconds: int = 900
    outbox_poll_interval_sec: float = 1.0
    work_item_poll_interval_sec: float = 1.0


settings = Settings()
