from typing import Callable, Awaitable
from pydantic import BaseModel
from enum import Enum


class MediaType(str, Enum):
    VIDEO = "video"
    AUDIO = "audio"
    TEXT = "text"
    IMAGE = "image"
    OTHER = "other"


class FileEvent(BaseModel):
    bucket: str
    name: str
    content_type: str | None = None
    size: int | None = None
    media_type: MediaType | None = None

    # DB related
    file_id: str | None = None
    upload_session_id: str | None = None
    datastore_id: str | None = None


HandlerFn = Callable[["FileEvent"], Awaitable[None]]

MEDIA_HANDLERS: dict[MediaType, HandlerFn] = {}
