from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_name: str = "Processing Service"
    app_host: str = "0.0.0.0"
    app_port: int = 5001

    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_consumer_group: str = "processing-service"
    kafka_topic_raw_chunks: str = "raw_chunks"
    kafka_topic_processed_results: str = "processed_results"
    kafka_topic_status_updates: str = "status_updates"
    kafka_topic_dlt: str = "raw_chunks_dlt"

    # Prefer 127.0.0.1 over localhost to avoid IPv6 (::1) connect failures on Windows.
    ollama_base_url: str = "http://127.0.0.1:11434"
    ollama_model: str = "gemma4"
    ollama_timeout_seconds: float = 300.0
    ollama_temperature: float = 0.1
    ollama_think: bool = False
    ollama_retries: int = 3
    ollama_retry_delay_seconds: float = 2.0
    ollama_startup_timeout_seconds: float = 120.0
    # Downscale page images before OCR to avoid Ollama OOM / HTTP 500.
    ocr_image_max_width: int = 1280
    ocr_image_jpeg_quality: int = 75

    # One chunk at a time: gemma4:31b + OCR images is heavy; concurrency often kills Ollama.
    consumer_max_concurrent: int = 1
    kafka_consumer_group_aggregator: str = "processing-service-aggregator"
    kafka_max_request_size_bytes: int = 10 * 1024 * 1024


settings = Settings()
