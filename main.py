import logging
import os
import socket
import uuid
from contextlib import asynccontextmanager
from uuid import UUID

from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.responses import PlainTextResponse

from app.api.websocket import create_websocket_router
from app.config import settings
from app.kafka.consumer import KafkaConsumerWorker
from app.kafka.manual_consumer import ManualCommitConsumer
from app.kafka.producer import KafkaPublisher
from app.kafka.relay_consumer import AggregatorConsumers
from app.kafka.topics import ensure_topics
from app.metrics import metrics
from app.models.schemas import (
    HealthResponse,
    JobRegisterRequest,
    JobStatusResponse,
    OcrChunkResponse,
    OcrJobResponse,
    ResultResponse,
)
from app.services.chunk_processor import ChunkProcessor
from app.services.compare_scheduler import CompareScheduler
from app.services.document_comparator import DocumentComparator
from app.services.object_store import get_object_store
from app.services.ocr_store import OcrStore
from app.services.ollama_client import OllamaClient
from app.services.outbox import OutboxPublisher
from app.services.result_aggregator import ResultAggregator
from app.services.state_manager import StateManager
from app.services.websocket_hub import WebSocketHub
from app.workers.ocr_work_items import OcrWorkItemWorker
from app.workers.ocr_worker import OcrStageHandler
from app.workers.stage_handlers import (
    ClassifyStageHandler,
    DiffStageHandler,
    FinalizeStageHandler,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

if not settings.worker_id:
    settings.worker_id = (
        f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
    )

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
compare_scheduler = CompareScheduler(document_comparator)
consumer_worker = KafkaConsumerWorker(
    processor=processor,
    publisher=publisher,
    compare_scheduler=compare_scheduler,
)
relay_consumers = AggregatorConsumers(aggregator=aggregator, ws_hub=ws_hub)
outbox_publisher: OutboxPublisher | None = None
ocr_work_items: OcrWorkItemWorker | None = None
stage_consumers: list[ManualCommitConsumer] = []
_ready = False


@asynccontextmanager
async def lifespan(app: FastAPI):
    global outbox_publisher, ocr_work_items, _ready
    logger.info("=" * 60)
    logger.info("Starting %s worker=%s", settings.app_name, settings.worker_id)
    logger.info("Kafka: %s pipeline=%s", settings.kafka_bootstrap_servers, settings.pipeline_version)
    logger.info("Ollama: %s model=%s", settings.ollama_base_url, settings.ollama_model)
    logger.info("=" * 60)

    await ocr_store.start()
    try:
        if ocr_store._pool is not None and not settings.allow_multiple_replicas:
            async with ocr_store._pool.acquire() as conn:
                locked = await conn.fetchval("SELECT pg_try_advisory_lock(872016)")
                if not locked:
                    raise RuntimeError(
                        "Another Processing replica is running. "
                        "Set ALLOW_MULTIPLE_REPLICAS=true after leases are verified."
                    )

        ollama_ready = await ollama_client.wait_until_available()
        if not ollama_ready:
            raise RuntimeError(
                f"Ollama is not reachable at {settings.ollama_base_url}. "
                "Start Ollama (start-all.bat or ollama serve) and retry."
            )

        try:
            await ensure_topics()
        except Exception:
            logger.exception("Kafka topic ensure failed")

        await publisher.start()
        await compare_scheduler.start()
        if ocr_store.workflow is not None:
            outbox_publisher = OutboxPublisher(ocr_store.workflow, publisher)
            await outbox_publisher.start()
            ocr_handler = OcrStageHandler(
                processor, ocr_store.workflow, compare_scheduler, publisher
            )
            ocr_work_items = OcrWorkItemWorker(ocr_store.workflow, ocr_handler)
            await ocr_work_items.start()
            stage_consumers.extend(
                [
                    ManualCommitConsumer(
                        settings.kafka_topic_ocr_cmd,
                        f"{settings.kafka_consumer_group}-ocr-v2",
                        ocr_handler,
                        "OcrV2",
                    ),
                    ManualCommitConsumer(
                        settings.kafka_topic_diff_cmd,
                        f"{settings.kafka_consumer_group}-diff",
                        DiffStageHandler(document_comparator, ocr_store.workflow),
                        "DiffV2",
                    ),
                    ManualCommitConsumer(
                        settings.kafka_topic_classify_cmd,
                        f"{settings.kafka_consumer_group}-classify",
                        ClassifyStageHandler(document_comparator, ocr_store.workflow),
                        "ClassifyV2",
                    ),
                    ManualCommitConsumer(
                        settings.kafka_topic_finalize_cmd,
                        f"{settings.kafka_consumer_group}-finalize",
                        FinalizeStageHandler(document_comparator, ocr_store.workflow),
                        "FinalizeV2",
                    ),
                ]
            )
            for consumer in stage_consumers:
                await consumer.start()
        await consumer_worker.start()
        await relay_consumers.start()
        await compare_scheduler.recover(await ocr_store.list_ready_jobs())
        _ready = True
        logger.info("%s ready on port %s", settings.app_name, settings.app_port)
        yield
    finally:
        _ready = False
        if ocr_work_items is not None:
            await ocr_work_items.stop()
        if outbox_publisher is not None:
            await outbox_publisher.stop()
        for consumer in stage_consumers:
            await consumer.stop()
        await compare_scheduler.stop()
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


async def require_internal_token(
    x_internal_token: str | None = Header(default=None, alias="X-Internal-Token"),
) -> None:
    expected = settings.internal_api_token
    if not expected:
        raise HTTPException(
            status_code=503,
            detail="INTERNAL_API_TOKEN is not configured",
        )
    if x_internal_token != expected:
        raise HTTPException(status_code=401, detail="Invalid internal token")


@app.get("/live", tags=["Health"])
async def live() -> dict:
    return {"status": "ok"}


@app.get("/ready", tags=["Health"])
async def ready() -> dict:
    postgres_ok = await ocr_store.is_available()
    object_ok = await get_object_store().ping()
    outbox_ok = outbox_publisher is not None
    if not (_ready and postgres_ok and object_ok and outbox_ok):
        raise HTTPException(status_code=503, detail="not ready")
    return {
        "status": "ok",
        "postgres": postgres_ok,
        "object_store": object_ok,
        "outbox": outbox_ok,
    }


@app.get("/metrics", tags=["Health"])
async def prometheus_metrics() -> PlainTextResponse:
    if ocr_store.workflow is not None:
        metrics.set_gauge(
            "outbox_oldest_age_seconds",
            await ocr_store.workflow.oldest_outbox_age_seconds(),
        )
    return PlainTextResponse(metrics.render_prometheus(), media_type="text/plain")


@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health() -> HealthResponse:
    ollama_ok = await ollama_client.is_available()
    kafka_ok = (
        publisher.is_connected
        and consumer_worker.is_connected
        and relay_consumers.is_connected
    )
    postgres_ok = await ocr_store.is_available()
    object_ok = await get_object_store().ping()

    status = "ok" if ollama_ok and kafka_ok and postgres_ok and object_ok else "degraded"
    return HealthResponse(
        status=status,
        kafka_connected=kafka_ok,
        ollama_reachable=ollama_ok,
        postgres_connected=postgres_ok,
        object_store_connected=object_ok,
    )


@app.get(
    "/api/jobs",
    response_model=list[JobStatusResponse],
    tags=["Jobs"],
    dependencies=[Depends(require_internal_token)],
)
async def list_jobs(
    user_id: UUID = Query(..., description="Owner filter; required"),
) -> list[JobStatusResponse]:
    jobs = await ocr_store.list_jobs(user_id=user_id)
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


@app.post(
    "/api/jobs",
    response_model=JobStatusResponse,
    tags=["Jobs"],
    dependencies=[Depends(require_internal_token)],
)
async def register_job(body: JobRegisterRequest) -> JobStatusResponse:
    """Register a job early so Chunking polling does not see 404 before Kafka."""
    db_job = await ocr_store.ensure_job(
        job_id=body.job_id,
        document_id=body.document_id or body.job_id,
        user_id=body.user_id,
        total_chunks=body.total_chunks,
        status=body.status,
        message=body.message,
        file1_name=body.file1_name,
        file2_name=body.file2_name,
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


@app.get(
    "/api/jobs/{job_id}",
    response_model=JobStatusResponse,
    tags=["Jobs"],
    dependencies=[Depends(require_internal_token)],
)
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
    dependencies=[Depends(require_internal_token)],
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


@app.get(
    "/api/jobs/{job_id}/result",
    response_model=ResultResponse,
    tags=["Jobs"],
    dependencies=[Depends(require_internal_token)],
)
async def get_job_result(job_id: str) -> ResultResponse:
    result = await aggregator.get_result(job_id)
    if result is None:
        raise HTTPException(
            status_code=404,
            detail=f"Result for job {job_id} is not ready yet",
        )
    return result


@app.post(
    "/api/jobs/{job_id}/cancel",
    tags=["Jobs"],
    dependencies=[Depends(require_internal_token)],
)
async def cancel_job(job_id: str) -> dict:
    job = await ocr_store.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    updated = await ocr_store.request_cancel(job_id)
    return {
        "job_id": job_id,
        "status": (updated or job)["status"],
        "cancel_requested": True,
    }


if __name__ == "__main__":
    import uvicorn

    # Pass the app object, not "main:app". The string form re-imports this
    # module and would register WebSocket handlers against an OcrStore that
    # never runs lifespan (pool stays None).
    uvicorn.run(
        app,
        host=settings.app_host,
        port=settings.app_port,
        reload=False,
    )
