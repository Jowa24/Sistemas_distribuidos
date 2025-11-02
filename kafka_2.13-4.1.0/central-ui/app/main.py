import asyncio
import json
import logging
import contextlib
from typing import Set

from fastapi import FastAPI, WebSocket
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from aiokafka import AIOKafkaConsumer

from app.settings import (
    KAFKA_BOOTSTRAP,
    KAFKA_GROUP_ID,
    KAFKA_TOPICS,
    WEBSOCKET_PATH,
)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("central-ui")

app = FastAPI()

# Serve static assets under /static so /ws is not shadowed
app.mount("/static", StaticFiles(directory="app/static"), name="static")

# Return the dashboard HTML at /
@app.get("/")
def index():
    return FileResponse("app/static/index.html")

# ---- WebSocket + Kafka consumer ----

clients: Set[WebSocket] = set()
_consumer_task: asyncio.Task | None = None


@app.on_event("startup")
async def startup():
    global _consumer_task
    _consumer_task = asyncio.create_task(_run_consumer())
    log.info("Startup complete; consumer task scheduled.")


@app.on_event("shutdown")
async def shutdown():
    global _consumer_task
    if _consumer_task:
        _consumer_task.cancel()
        with contextlib.suppress(Exception):
            await _consumer_task
    log.info("Shutdown complete.")


@app.websocket(WEBSOCKET_PATH)
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    clients.add(ws)
    log.info("WS connected (total=%d)", len(clients))
    try:
        # Keep the socket open; we don't expect client->server messages
        while True:
            await asyncio.sleep(60)
    except Exception:
        pass
    finally:
        clients.discard(ws)
        log.info("WS disconnected (total=%d)", len(clients))


async def _run_consumer():
    consumer = AIOKafkaConsumer(
        *KAFKA_TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset="latest",
        enable_auto_commit=True,
    )
    await consumer.start()
    log.info("Kafka consumer started; bootstrap=%s topics=%s", KAFKA_BOOTSTRAP, KAFKA_TOPICS)
    try:
        async for msg in consumer:
            payload = {
                "topic": msg.topic,
                "partition": msg.partition,
                "offset": msg.offset,
                "timestamp": msg.timestamp,
                "key": _safe_decode(msg.key),
                "value": _safe_decode(msg.value),
            }
            # Broadcast to all connected WS clients
            dead = []
            for ws in list(clients):
                try:
                    await ws.send_text(json.dumps(payload))
                except Exception:
                    dead.append(ws)
            for d in dead:
                clients.discard(d)
                log.info("Dropped dead WS; total=%d", len(clients))
    finally:
        await consumer.stop()
        log.info("Kafka consumer stopped.")


def _safe_decode(b: bytes | None) -> str | None:
    if b is None:
        return None
    try:
        return b.decode("utf-8")
    except Exception:
        return str(b)
