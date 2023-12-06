import logging
from http import HTTPStatus
from typing import Final

from fastapi import Response

log = logging.getLogger(__name__)


READINESS_ENDPOINT_PATH: Final[str] = "/api/status/ready"


async def readiness_api() -> Response:
    """Readiness check endpoint."""
    return Response(content="ok", status_code=HTTPStatus.OK)
