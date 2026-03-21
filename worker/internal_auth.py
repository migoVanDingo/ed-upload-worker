import hmac
import logging
import os

from fastapi import HTTPException, Request


logger = logging.getLogger("file-worker.internal-auth")

INTERNAL_AUTH_HEADER = "X-Internal-Auth"
INTERNAL_EVENT_SECRET_ENV = "INTERNAL_EVENT_SECRET"


def verify_internal_event_request(request: Request, endpoint_name: str) -> None:
    """
    Temporary local compatibility shim for Cloud Run deployment.
    Swap back to shared security primitive when dependency wiring is ready.
    """
    configured_secret = os.getenv(INTERNAL_EVENT_SECRET_ENV)
    if not configured_secret:
        logger.warning(
            "internal auth failed endpoint=%s reason=missing_env_secret",
            endpoint_name,
        )
        raise HTTPException(status_code=403, detail="Internal auth not configured")

    provided_secret = request.headers.get(INTERNAL_AUTH_HEADER)
    if not provided_secret:
        logger.warning(
            "internal auth failed endpoint=%s reason=missing_header",
            endpoint_name,
        )
        raise HTTPException(status_code=401, detail="Missing internal auth header")

    if not hmac.compare_digest(provided_secret, configured_secret):
        logger.warning(
            "internal auth failed endpoint=%s reason=header_mismatch",
            endpoint_name,
        )
        raise HTTPException(status_code=403, detail="Invalid internal auth")
