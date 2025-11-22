# worker/media_utils.py
from typing import Any, Dict, List

from worker.dto import MediaType, HandlerFn, FileEvent
from worker.pubsub_client import publish_message
from worker.utils import parse_object_key_metadata


def classify_media_type(content_type: str) -> MediaType:
    if not content_type:
        return MediaType.OTHER

    if content_type.startswith("video/"):
        return MediaType.VIDEO
    if content_type.startswith("audio/"):
        return MediaType.AUDIO
    if content_type.startswith("text/") or content_type in {
        "application/pdf",
        "application/msword",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    }:
        return MediaType.TEXT
    if content_type.startswith("image/"):
        return MediaType.IMAGE

    return MediaType.OTHER


# CENTRAL REGISTRY
MEDIA_HANDLERS: dict[MediaType, HandlerFn] = {}


def media_handler(kind: MediaType):
    """
    Decorator to register a handler for a given MediaType.
    """

    def decorator(fn: HandlerFn):
        MEDIA_HANDLERS[kind] = fn
        return fn

    return decorator


async def enqueue_analysis_jobs(event: FileEvent, task_types: List[str]) -> None:
    """
    For each logical 'task_type', publish a message to the video-processing topic.
    ed-video-processing-service can fan these out into actual ffmpeg steps.
    """
    # Safety: if for some reason the FileEvent is missing IDs, try again from the path
    datastore_id = event.datastore_id
    upload_session_id = event.upload_session_id

    if not datastore_id or not upload_session_id:
        parsed_datastore_id, parsed_upload_session_id = parse_object_key_metadata(
            event.name
        )
        datastore_id = datastore_id or parsed_datastore_id
        upload_session_id = upload_session_id or parsed_upload_session_id

    for task_type in task_types:
        msg: Dict[str, Any] = {
            "file_id": event.file_id,
            "upload_session_id": upload_session_id,
            "datastore_id": datastore_id,
            "bucket": event.bucket,
            # Standardize on `name`, but keep `object_key` as alias for now
            "name": event.name,
            "object_key": event.name,
            "media_type": event.media_type.value if event.media_type else None,
            "task_type": task_type,
        }
        publish_message(msg)


@media_handler(MediaType.VIDEO)
async def handle_video(event: FileEvent) -> None:
    # e.g. enqueue "extract_audio", "generate_thumbnails", "transcribe"
    await enqueue_analysis_jobs(
        event,
        [
            "video-inspect",
            "video-thumbnail",
            "video-transcode-preview",
            "video-audio-extract",
            "speech-to-text",
            "embedding-text",
        ],
    )


@media_handler(MediaType.AUDIO)
async def handle_audio(event: FileEvent) -> None:
    await enqueue_analysis_jobs(
        event,
        [
            "speech-to-text",
            "embedding-text",
        ],
    )


@media_handler(MediaType.TEXT)
async def handle_text(event: FileEvent) -> None:
    await enqueue_analysis_jobs(
        event,
        [
            "text-clean",
            "text-chunk",
            "embedding-text",
        ],
    )


def normalize_file_event(raw_event: Dict[str, Any]) -> FileEvent:
    """
    Normalize Eventarc/CloudEvent or raw data payload into our FileEvent.
    """
    if isinstance(raw_event.get("data"), dict):
        data = raw_event["data"]
    else:
        data = raw_event

    size_val = data.get("size")
    size_int = int(size_val) if size_val is not None else None

    name = data["name"]

    # Try to get IDs from metadata first (future-friendly),
    # then fall back to parsing the object key convention.
    metadata = data.get("metadata") or {}
    datastore_id = metadata.get("datastoreId")
    upload_session_id = metadata.get("uploadSessionId")

    if not datastore_id or not upload_session_id:
        parsed_datastore_id, parsed_upload_session_id = parse_object_key_metadata(name)
        datastore_id = datastore_id or parsed_datastore_id
        upload_session_id = upload_session_id or parsed_upload_session_id

    return FileEvent(
        bucket=data["bucket"],
        name=name,
        content_type=data.get("contentType"),
        size=size_int,
        datastore_id=datastore_id,
        upload_session_id=upload_session_id,
        # file_id can be filled later once the DB row exists
    )
