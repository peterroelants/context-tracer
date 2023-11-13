import contextlib
import logging
from http import HTTPStatus
from typing import Iterator

from fastapi import FastAPI
from fastapi.responses import Response

from context_tracer.utils.fast_api_process_runner import FastAPIProcessRunner

log = logging.getLogger(__name__)


READINESS_ENDPOINT = "/api/status/ready"
UPDATE_TRACE_ENDPOINT = "/api/trace/update"


async def readiness() -> Response:
    return Response(content="ok", status_code=HTTPStatus.OK)


# async def update_trace(span_data: SpanData):
#     pass


def create_app() -> FastAPI:
    app = FastAPI()
    app.add_api_route(READINESS_ENDPOINT, readiness, methods=["GET"])
    # app.add_api_route(UPDATE_TRACE_ENDPOINT, update_trace, methods=["POST"])
    return app


@contextlib.contextmanager
def run_app() -> Iterator[FastAPIProcessRunner]:
    fast_api_app = create_app()
    with FastAPIProcessRunner(fast_api_app) as server:
        yield server
