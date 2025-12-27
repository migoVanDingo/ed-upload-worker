# worker/pubsub_client.py
from __future__ import annotations

import json
import os
import logging
from typing import Optional

from google.cloud import pubsub_v1

logger = logging.getLogger(__name__)

_publisher: Optional[pubsub_v1.PublisherClient] = None
_topic_path: Optional[str] = None


def _resolve_project_id() -> str:
    # 1) Try explicit env vars first (nice for local/dev)
    project_id = (
        os.getenv("GOOGLE_CLOUD_PROJECT")
        or os.getenv("GCP_PROJECT_ID")
        or os.getenv("GCLOUD_PROJECT")
        or ""
    ).strip()
    if project_id:
        return project_id

    # 2) Cloud Run-safe fallback: ask Application Default Credentials
    try:
        import google.auth  # type: ignore

        _, adc_project_id = google.auth.default()
        if adc_project_id:
            return adc_project_id
    except Exception:
        logger.exception("Failed to resolve project id from google.auth.default()")

    raise RuntimeError(
        "Project ID is not set and could not be resolved from ADC. "
        "Set GOOGLE_CLOUD_PROJECT or GCP_PROJECT_ID."
    )


def _resolve_topic_path(publisher: pubsub_v1.PublisherClient) -> str:
    """
    Resolve the Pub/Sub topic path.

    VIDEO_PROCESSING_TOPIC may be either:
      - short name: 'video-processing'
      - full path:  'projects/<project>/topics/<topic>'
    """
    topic = (os.getenv("VIDEO_PROCESSING_TOPIC") or "video-processing").strip()

    if not topic:
        raise RuntimeError(
            "VIDEO_PROCESSING_TOPIC is empty. Set it to 'video-processing' "
            "or 'projects/<project>/topics/<topic>'."
        )

    if topic.startswith("projects/") and "/topics/" in topic:
        return topic

    project_id = _resolve_project_id()
    return publisher.topic_path(project_id, topic)


def get_publisher() -> pubsub_v1.PublisherClient:
    global _publisher, _topic_path
    if _publisher is None:
        _publisher = pubsub_v1.PublisherClient()
    if _topic_path is None:
        _topic_path = _resolve_topic_path(_publisher)
        logger.info("Initialized Pub/Sub publisher for %s", _topic_path)
    return _publisher


def get_topic_path() -> str:
    """
    Always return a valid topic path or raise a helpful RuntimeError.
    """
    global _topic_path
    if _topic_path is None:
        # initialize publisher + topic_path together
        get_publisher()

    if not _topic_path:
        # Extremely defensive: if someone changed get_publisher() incorrectly
        raise RuntimeError(
            "Pub/Sub topic path was not initialized. "
            f"Env GOOGLE_CLOUD_PROJECT={os.getenv('GOOGLE_CLOUD_PROJECT')!r}, "
            f"GCP_PROJECT_ID={os.getenv('GCP_PROJECT_ID')!r}, "
            f"VIDEO_PROCESSING_TOPIC={os.getenv('VIDEO_PROCESSING_TOPIC')!r}"
        )

    return _topic_path


def publish_message(payload: dict) -> str:
    """
    Publish a JSON payload to the configured topic.
    Returns the Pub/Sub message ID.
    """
    publisher = get_publisher()
    topic_path = get_topic_path()

    if not topic_path or not topic_path.startswith("projects/"):
        # defensive guard so you never get the opaque Pub/Sub 400 again
        raise RuntimeError(f"Invalid Pub/Sub topic_path computed: {topic_path!r}")

    data = json.dumps(payload).encode("utf-8")

    logger.info("Publishing message to %s", topic_path)
    future = publisher.publish(topic_path, data=data)
    message_id = future.result(timeout=10)
    logger.info("Published message_id=%s", message_id)
    return message_id
