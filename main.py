import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException

from app.api.websocket import create_websocket_router
from app.config import settings
from app.kafka.consumer import KafkaConsumerWorker
from app.kafka.producer import KafkaPublisher
from app.kafka.relay_consumer import AggregatorConsumers
from app.models.schemas import HealthResponse, JobStatusResponse, ResultResponse
from app.services.chunk_processor import ChunkProcessor
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
state_manager = StateManager()
ws_hub = WebSocketHub()
aggregator = ResultAggregator(
    ws_hub=ws_hub,
    publish_status=publisher.publish_status_update,
)
processor = ChunkProcessor(
    ollama=ollama_client,
    state=state_manager,
    publish_result=publisher.publish_processed_result,
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
    logger.info("WebSocket: ws://localhost:%s/ws/jobs/{job_id}", settings.app_port)
    logger.info("WebSocketHub id=%s Aggregator id=%s", id(ws_hub), id(aggregator))
    logger.info("=" * 60)
    await publisher.start()
    await consumer_worker.start()
    await relay_consumers.start()
    logger.info("%s ready on port %s", settings.app_name, settings.app_port)
    yield
    await relay_consumers.stop()
    await consumer_worker.stop()
    await publisher.stop()
    logger.info("%s stopped", settings.app_name)


app = FastAPI(
    title=settings.app_name,
    description=(
        "Consumer → Ollama Orchestrator + Aggregator: читает raw_chunks, "
        "обрабатывает через Ollama, агрегирует processed_results и "
        "отправляет статус и результат на фронтенд через WebSocket."
    ),
    version="1.1.0",
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

    status = "ok" if ollama_ok and kafka_ok else "degraded"
    return HealthResponse(
        status=status,
        kafka_connected=kafka_ok,
        ollama_reachable=ollama_ok,
    )


@app.get("/api/jobs", response_model=list[JobStatusResponse], tags=["Jobs"])
async def list_jobs() -> list[JobStatusResponse]:
    jobs = await state_manager.list_jobs()
    return [
        JobStatusResponse(
            job_id=job.job_id,
            document_id=job.document_id,
            status=job.status,
            processed_chunks=len(job.processed_chunks),
            total_chunks=job.total_chunks,
            message=job.last_message,
        )
        for job in jobs
    ]


@app.get("/api/jobs/{job_id}", response_model=JobStatusResponse, tags=["Jobs"])
async def get_job(job_id: str) -> JobStatusResponse:
    job = await state_manager.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

    return JobStatusResponse(
        job_id=job.job_id,
        document_id=job.document_id,
        status=job.status,
        processed_chunks=len(job.processed_chunks),
        total_chunks=job.total_chunks,
        message=job.last_message,
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

    # Pass the app object (not "main:app") to avoid double-importing this module.
    # String import creates a second set of singletons (ws_hub / aggregator), so
    # WebSocket clients register on one hub while Kafka relays broadcast on another
    # → "[WS] no clients" while Comparator thinks upstream is connected.
    uvicorn.run(
        app,
        host=settings.app_host,
        port=settings.app_port,
        reload=False,
    )
