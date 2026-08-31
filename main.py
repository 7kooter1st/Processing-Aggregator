import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query

from app.api.websocket import create_websocket_router
from app.config import settings
from app.kafka.consumer import KafkaConsumerWorker
from app.kafka.producer import KafkaPublisher
from app.kafka.relay_consumer import AggregatorConsumers
from app.models.schemas import (
    HealthResponse,
    JobRegisterRequest,
    JobStatusResponse,
    OcrChunkResponse,
    OcrJobResponse,
    ResultResponse,
)
from app.services.chunk_processor import ChunkProcessor
from app.services.document_comparator import DocumentComparator
from app.services.ocr_store import OcrStore
from app.services.ollama_client import OllamaClient
from app.services.result_aggregator import ResultAggregator
from app.services.state_manager import StateManager
from app.services.websocket_hub import WebSocketHub

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

publisher = KafkaPublisher()
ollama_client = OllamaClient()
ocr_store = OcrStore()
state_manager = StateManager()
ws_hub = WebSocketHub()
aggregator = ResultAggregator(
    ws_hub=ws_hub,
    publish_status=publisher.publish_status_update,
    store=ocr_store,
)
document_comparator = DocumentComparator(
    ollama=ollama_client,
    store=ocr_store,
    state=state_manager,
    publish_result=publisher.publish_processed_result,
    publish_status=publisher.publish_status_update,
)
processor = ChunkProcessor(
    ollama=ollama_client,
    state=state_manager,
    store=ocr_store,
    comparator=document_comparator,
    publish_status=publisher.publish_status_update,
)
consumer_worker = KafkaConsumerWorker(processor=processor, publisher=publisher)
relay_consumers = AggregatorConsumers(aggregator=aggregator, ws_hub=ws_hub)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("=" * 60)
    logger.info("Starting %s", settings.app_name)
    logger.info("Kafka: %s", settings.kafka_bootstrap_servers)
    logger.info("Topics: in=%s out=%s status=%s",
                settings.kafka_topic_raw_chunks,
                settings.kafka_topic_processed_results,
                settings.kafka_topic_status_updates)
    logger.info("Ollama: %s model=%s", settings.ollama_base_url, settings.ollama_model)
    logger.info("PostgreSQL OCR store configured")
    logger.info("WebSocket: ws://localhost:%s/ws/jobs/{job_id}", settings.app_port)
    logger.info("=" * 60)

    resume_tasks: list[asyncio.Task] = []
    await ocr_store.start()
    try:
        ollama_ready = await ollama_client.wait_until_available()
        if not ollama_ready:
            raise RuntimeError(
                f"Ollama is not reachable at {settings.ollama_base_url}. "
                "Start Ollama (start-all.bat or ollama serve) and retry."
            )

        await publisher.start()
        await consumer_worker.start()
        await relay_consumers.start()

        for job_id in await ocr_store.list_ready_jobs():
            logger.info(
                "[RECOVERY] resume persisted OCR job=%s — "
                "сравнение может занять несколько минут, не перезапускайте Processing",
                job_id,
            )
            resume_tasks.append(
                asyncio.create_task(
                    document_comparator.compare_if_ready(job_id)
                )
            )

        logger.info("%s ready on port %s", settings.app_name, settings.app_port)
        yield
    finally:
        for task in resume_tasks:
            if not task.done():
                task.cancel()
        if resume_tasks:
            await asyncio.gather(*resume_tasks, return_exceptions=True)
        await relay_consumers.stop()
        await consumer_worker.stop()
        await publisher.stop()
        await ocr_store.stop()
        logger.info("%s stopped", settings.app_name)


app = FastAPI(
    title=settings.app_name,
    description=(
        "Consumer → Ollama Orchestrator + Aggregator: читает raw_chunks, "
        "сохраняет OCR в PostgreSQL, выполняет page-aware diff, "
        "гибридно классифицирует различия и "
        "отправляет статус и результат на фронтенд через WebSocket."
    ),
    version="2.0.0",
    lifespan=lifespan,
)

app.include_router(create_websocket_router(ws_hub, aggregator))


@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health() -> HealthResponse:
    ollama_ok = await ollama_client.is_available()
    kafka_ok = (
        publisher.is_connected
        and consumer_worker.is_connected
        and relay_consumers.is_connected
    )
    postgres_ok = await ocr_store.is_available()

    status = "ok" if ollama_ok and kafka_ok and postgres_ok else "degraded"
    return HealthResponse(
        status=status,
        kafka_connected=kafka_ok,
        ollama_reachable=ollama_ok,
        postgres_connected=postgres_ok,
    )


@app.get("/api/jobs", response_model=list[JobStatusResponse], tags=["Jobs"])
async def list_jobs() -> list[JobStatusResponse]:
    jobs = await ocr_store.list_jobs()
    return [
        JobStatusResponse(
            job_id=job["job_id"],
            document_id=job["document_id"],
            status=job["status"],
            processed_chunks=job["processed_chunks"],
            total_chunks=job["total_chunks"],
            message=job["last_message"],
        )
        for job in jobs
    ]


@app.post("/api/jobs", response_model=JobStatusResponse, tags=["Jobs"])
async def register_job(body: JobRegisterRequest) -> JobStatusResponse:
    """Register a job early so Chunking polling does not see 404 before Kafka."""
    db_job = await ocr_store.ensure_job(
        job_id=body.job_id,
        document_id=body.document_id or body.job_id,
        total_chunks=body.total_chunks,
        status=body.status,
        message=body.message,
    )
    await state_manager.ensure_job(
        job_id=body.job_id,
        document_id=body.document_id or body.job_id,
        total_chunks=body.total_chunks,
        status=body.status,
        message=body.message,
    )
    return JobStatusResponse(
        job_id=db_job["job_id"],
        document_id=db_job["document_id"],
        status=db_job["status"],
        processed_chunks=db_job["processed_chunks"],
        total_chunks=db_job["total_chunks"],
        message=db_job["last_message"],
    )


@app.get("/api/jobs/{job_id}", response_model=JobStatusResponse, tags=["Jobs"])
async def get_job(job_id: str) -> JobStatusResponse:
    job = await ocr_store.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

    return JobStatusResponse(
        job_id=job["job_id"],
        document_id=job["document_id"],
        status=job["status"],
        processed_chunks=job["processed_chunks"],
        total_chunks=job["total_chunks"],
        message=job["last_message"],
    )


@app.get(
    "/api/jobs/{job_id}/ocr",
    response_model=OcrJobResponse,
    tags=["Jobs"],
)
async def get_job_ocr(
    job_id: str,
    side: int | None = Query(default=None, ge=1, le=2),
) -> OcrJobResponse:
    """Inspect the exact OCR/text persisted before comparison."""
    job = await ocr_store.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

    rows = await ocr_store.get_ocr_chunks(job_id, side=side)
    return OcrJobResponse(
        job_id=job["job_id"],
        document_id=job["document_id"],
        status=job["status"],
        total_chunks=job["total_chunks"],
        processed_chunks=job["processed_chunks"],
        chunks=[OcrChunkResponse.model_validate(row) for row in rows],
    )


@app.get("/api/jobs/{job_id}/result", response_model=ResultResponse, tags=["Jobs"])
async def get_job_result(job_id: str) -> ResultResponse:
    result = await aggregator.get_result(job_id)
    if result is None:
        raise HTTPException(
            status_code=404,
            detail=f"Result for job {job_id} is not ready yet",
        )
    return result


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host=settings.app_host,
        port=settings.app_port,
        reload=False,
    )
