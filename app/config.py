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

    ollama_base_url: str = "http://localhost:11434"
    ollama_model: str = "gemma4"
    ollama_timeout_seconds: float = 300.0
    ollama_temperature: float = 0.1
    ollama_think: bool = False

    consumer_max_concurrent: int = 3
    kafka_consumer_group_aggregator: str = "processing-service-aggregator"


settings = Settings()
