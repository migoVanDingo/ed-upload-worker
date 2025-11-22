# worker/pubsub_client.py
import json
import os
import logging

from google.cloud import pubsub_v1

logger = logging.getLogger(__name__)

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
TOPIC_ID = os.getenv("VIDEO_PROCESSING_TOPIC", "video-processing")

_publisher: pubsub_v1.PublisherClient | None = None
_topic_path: str | None = None


def get_publisher() -> pubsub_v1.PublisherClient:
    global _publisher, _topic_path
    if _publisher is None:
        _publisher = pubsub_v1.PublisherClient()
        if not PROJECT_ID:
            raise RuntimeError("GCP_PROJECT_ID is not set")
        _topic_path = _publisher.topic_path(PROJECT_ID, TOPIC_ID)
        logger.info("Initialized Pub/Sub publisher for %s", _topic_path)
    return _publisher


def get_topic_path() -> str:
    if _topic_path is None:
        # ensure publisher is initialized
        get_publisher()
    return _topic_path


def publish_message(payload: dict) -> str:
    """
    Publish a JSON payload to the video-processing topic.
    Returns the Pub/Sub message ID.
    """
    publisher = get_publisher()
    topic_path = get_topic_path()

    data = json.dumps(payload).encode("utf-8")

    future = publisher.publish(topic_path, data=data)
    message_id = future.result(timeout=10)
    logger.info("Published message to %s with message_id=%s", topic_path, message_id)
    return message_id
