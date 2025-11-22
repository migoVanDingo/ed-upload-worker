# worker/schemas.py (or similar)
from pydantic import BaseModel


class VideoAnalysisJob(BaseModel):
    bucket: str
    name: str
    content_type: str | None = None
    size: int | None = None
    media_type: str | None = None
    datastore_id: str | None = None
