# main.py
import base64
import json
import logging

from fastapi import FastAPI, Request, Response
from pydantic import BaseModel
from worker.dto import MEDIA_HANDLERS
from worker.media_utils import classify_media_type, normalize_file_event

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("file-worker")

app = FastAPI()


# ------------------------
# Existing Pub/Sub handler
# ------------------------


class PubSubEnvelope(BaseModel):
    message: dict
    subscription: str


@app.get("/")
async def root():
    return {"status": "ok", "service": "ed-file-worker"}


@app.post("/pubsub")
async def handle_pubsub(envelope: PubSubEnvelope):
    """
    For classic Pub/Sub push (NOT Eventarc storage events).
    You can keep this if you ever wire a Pub/Sub topic directly.
    """
    try:
        data_b64 = envelope.message.get("data", "")
        payload = base64.b64decode(data_b64).decode("utf-8")
        event = json.loads(payload)

        bucket = event.get("bucket")
        name = event.get("name")
        generation = event.get("generation")
        size = event.get("size")
        content_type = event.get("contentType")

        logger.info(
            "Pub/Sub GCS event: bucket=%s name=%s gen=%s size=%s ctype=%s",
            bucket,
            name,
            generation,
            size,
            content_type,
        )

        # TODO: call your processing pipeline here (if you use Pub/Sub path)
        return Response(status_code=204)
    except Exception as e:
        logger.exception("Error handling Pub/Sub message: %s", e)
        return Response(status_code=500)


# ------------------------
# NEW: Eventarc GCS handler
# ------------------------


@app.post("/gcs-events")
async def handle_gcs_events(request: Request):
    raw = await request.json()
    file_event = normalize_file_event(raw)
    file_event.media_type = classify_media_type(file_event.content_type or "")

    handler = MEDIA_HANDLERS.get(file_event.media_type, None)
    if not handler:
        logger.info(
            "No media handler for kind=%s, content_type=%s, object=%s",
            file_event.media_type,
            file_event.content_type,
            file_event.name,
        )
        return Response(status_code=204)

    try:
        await handler(file_event)  # enqueues jobs
    except Exception:
        logger.exception("Error scheduling analysis jobs")
        # Pub/Sub/Cloud Tasks failure -> retry
        return Response(status_code=500)

    return Response(status_code=204)
