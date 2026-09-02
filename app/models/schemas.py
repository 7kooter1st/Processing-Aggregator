from datetime import datetime, timezone
from enum import Enum
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field, model_validator


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
    postgres_connected: bool


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
    user_id: UUID
    document_id: str | None = None
    total_chunks: int = 0
    status: str = "queued"
    message: str = "Процесс в очереди",
    file1_name: str = ""
    file2_name: str = ""


class OcrChunkResponse(BaseModel):
    chunk_index: int
    side: int = Field(ge=1, le=2)
    filename: str
    format: str
    source_content_type: str
    was_ocr: bool
    is_missing: bool
    text_content: str
    ocr_model: str | None = None
    created_at: datetime
    updated_at: datetime


class OcrJobResponse(BaseModel):
    job_id: str
    document_id: str
    status: str
    total_chunks: int
    processed_chunks: int
    chunks: list[OcrChunkResponse] = Field(default_factory=list)


class DifferenceCategory(str, Enum):
    SUBSTANTIVE = "substantive"
    TECHNICAL = "technical"
    ALIGNMENT_ERROR = "alignment_error"
    OCR_UNCERTAIN = "ocr_uncertain"


class ComparisonVerdict(str, Enum):
    IDENTICAL = "identical"
    CONTENT_EQUAL = "content_equal"
    DIFFERENT = "different"


class LineDifference(BaseModel):
    candidate_id: str | None = None
    line_number: int | None = None
    file1_line: str | None = None
    file2_line: str | None = None
    file1_span: list[int] | None = None
    file2_span: list[int] | None = None
    category: DifferenceCategory = DifferenceCategory.SUBSTANTIVE
    technical_type: str | None = None
    reason: str | None = None
    confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    protection_tags: list[str] = Field(default_factory=list)
    file1_page: int | None = Field(default=None, ge=1)
    file2_page: int | None = Field(default=None, ge=1)
    file1_block: int | None = Field(default=None, ge=1)
    file2_block: int | None = Field(default=None, ge=1)
    file1_source_type: str | None = None
    file2_source_type: str | None = None


class ComparisonResult(BaseModel):
    identical: bool
    verdict: ComparisonVerdict
    differences: list[LineDifference] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def default_legacy_verdict(cls, value: Any) -> Any:
        if isinstance(value, dict) and "verdict" not in value:
            value = dict(value)
            value["verdict"] = (
                ComparisonVerdict.IDENTICAL
                if value.get("identical") and not value.get("differences")
                else ComparisonVerdict.DIFFERENT
            )
        return value


class ResultResponse(BaseModel):
    comparison: ComparisonResult


class WebSocketEvent(BaseModel):
    type: str
    job_id: str
    data: dict[str, Any]
