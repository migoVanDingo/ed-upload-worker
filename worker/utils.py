# worker/media_utils.py
from typing import Optional, Tuple


def parse_object_key_metadata(object_key: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Parse datastore_id and upload_session_id from an object key like:

        raw/datastore/{datastore_id}/session/{upload_session_id}/{filename}

    Returns (datastore_id, upload_session_id). If a part can't be found, it
    returns None for that piece.
    """
    if not object_key:
        return None, None

    parts = object_key.strip("/").split("/")

    datastore_id: Optional[str] = None
    upload_session_id: Optional[str] = None

    for i, part in enumerate(parts):
        if part == "datastore" and i + 1 < len(parts):
            datastore_id = parts[i + 1]
        elif part == "session" and i + 1 < len(parts):
            upload_session_id = parts[i + 1]

    return datastore_id, upload_session_id
