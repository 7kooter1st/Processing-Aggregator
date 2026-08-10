from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class ContentType(str, Enum):
    TEXT = "text"
    IMAGE = "image"


class ChunkContent(BaseModel):
    filename: str
    format: str
    content_type: ContentType
    content: str


class RawChunkMessage(BaseModel):
    job_id: str
    document_id: str
    chunk_index: int
    total_chunks: int
    file1: ChunkContent | None = None
    file2: ChunkContent | None = None


class ProcessedResultMessage(BaseModel):
    job_id: str
    document_id: str
    chunk_index: int
    total_chunks: int
    ollama: dict[str, Any]
    comparison_fragment: dict[str, Any] | None = None
    processed_at: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )


class StatusUpdateMessage(BaseModel):
    job_id: str
    document_id: str
    status: str
    processed_chunks: int
    total_chunks: int
    message: str
    updated_at: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )


class JobProgress(BaseModel):
    job_id: str
    document_id: str
    total_chunks: int
    processed_chunks: set[int] = Field(default_factory=set)
    status: str = "processing"
    last_message: str = ""

    @property
    def is_complete(self) -> bool:
        return len(self.processed_chunks) >= self.total_chunks

    def progress_text(self) -> str:
        return f"{len(self.processed_chunks)}/{self.total_chunks}"


class HealthResponse(BaseModel):
    status: str
    kafka_connected: bool
    ollama_reachable: bool


class JobStatusResponse(BaseModel):
    job_id: str
    document_id: str
    status: str
    processed_chunks: int
    total_chunks: int
    message: str


class JobRegisterRequest(BaseModel):
    """Early registration from Chunking before the first Kafka chunk arrives."""

    job_id: str
    document_id: str | None = None
    total_chunks: int = 0
    status: str = "queued"
    message: str = "Ожидание чанков из Kafka..."


class LineDifference(BaseModel):
    line_number: int | None = None
    file1_line: str | None = None
    file2_line: str | None = None
    file1_span: list[int] | None = None
    file2_span: list[int] | None = None


class ComparisonResult(BaseModel):
    identical: bool
    differences: list[LineDifference] = Field(default_factory=list)


class ResultResponse(BaseModel):
    comparison: ComparisonResult


class WebSocketEvent(BaseModel):
    type: str
    job_id: str
    data: dict[str, Any]
