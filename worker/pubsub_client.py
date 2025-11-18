# worker/pubsub_client.py
import json
import os

from google.cloud import pubsub_v1

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
TOPIC_ID = os.getenv("ANALYSIS_JOBS_TOPIC", "analysis-jobs")

_publisher: pubsub_v1.PublisherClient | None = None
_topic_path: str | None = None


def get_publisher() -> pubsub_v1.PublisherClient:
    global _publisher, _topic_path
    if _publisher is None:
        _publisher = pubsub_v1.PublisherClient()
        _topic_path = _publisher.topic_path(PROJECT_ID, TOPIC_ID)
    return _publisher


def get_topic_path() -> str:
    if _topic_path is None:
        # ensure publisher is initialized
        get_publisher()
    return _topic_path
