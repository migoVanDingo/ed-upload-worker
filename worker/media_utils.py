from worker.media_utils import MediaType, HandlerFn, MEDIA_HANDLERS, FileEvent
import json
from .pubsub_client import get_publisher, get_topic_path


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


def media_handler(kind: MediaType):
    def decorator(fn: HandlerFn):
        MEDIA_HANDLERS[kind] = fn
        return fn

    return decorator


async def publish_to_pubsub(topic_id: str, msg: dict) -> None:
    """
    topic_id here can be ignored if you're only using one topic,
    or you can map it to different topics later.
    """
    publisher = get_publisher()
    topic_path = get_topic_path()

    data = json.dumps(msg).encode("utf-8")
    # NOTE: google-cloud-pubsub is sync; we're okay not awaiting the future
    future = publisher.publish(topic_path, data=data)
    # Optionally you can log or attach callbacks:
    # future.add_done_callback(lambda f: logger.info("Published message: %s", f.result()))


async def enqueue_analysis_jobs(event: FileEvent, task_types: list[str]) -> None:
    for task_type in task_types:
        msg = {
            "file_id": event.file_id,
            "upload_session_id": event.upload_session_id,
            "datastore_id": event.datastore_id,
            "bucket": event.bucket,
            "object_key": event.name,
            "media_type": event.media_type.value,
            "task_type": task_type,
        }
        await publish_to_pubsub("analysis-jobs", msg)


@media_handler(MediaType.VIDEO)
async def handle_video(event: "FileEvent") -> None:
    # e.g. enqueue "extract_audio", "generate_thumbnails", "transcribe"
    await enqueue_analysis_jobs(
        event,
        [
            "video-thumbnail",
            "video-transcode-preview",
            "video-audio-extract",
            "speech-to-text",
            "embedding-text",
        ],
    )


@media_handler(MediaType.AUDIO)
async def handle_audio(event: "FileEvent") -> None:
    await enqueue_analysis_jobs(
        event,
        [
            "speech-to-text",
            "embedding-text",
        ],
    )


@media_handler(MediaType.TEXT)
async def handle_text(event: "FileEvent") -> None:
    await enqueue_analysis_jobs(
        event,
        [
            "text-clean",
            "text-chunk",
            "embedding-text",
        ],
    )


def normalize_file_event(raw_event: dict) -> FileEvent:
    data = raw_event.get("data", {})
    return FileEvent(
        bucket=data.get("bucket"),
        name=data.get("name"),
        content_type=data.get("contentType"),
        size=int(data.get("size", 0)) if data.get("size") is not None else None,
    )
