from contextlib import asynccontextmanager
from http import HTTPStatus
from multiprocessing import Queue

import pytest
import requests
from context_tracer.utils.fast_api_process_runner import FastAPIProcessRunner
from fastapi import FastAPI
from fastapi.responses import Response

LIVENESS_ENDPOINT = "/live"


@pytest.fixture
def fast_api_app() -> FastAPI:
    app = FastAPI()

    async def live() -> Response:
        return Response(content="ok", status_code=HTTPStatus.OK)

    app.add_api_route(LIVENESS_ENDPOINT, live, methods=["GET"])
    return app


@pytest.fixture
def fast_api_app_with_lifespan() -> tuple[FastAPI, Queue]:
    queue: Queue = Queue()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # Load the ML model
        queue.put_nowait("on-start")
        yield
        queue.put_nowait("on-shutdown")

    app = FastAPI(lifespan=lifespan)

    async def live() -> Response:
        return Response(content="ok", status_code=HTTPStatus.OK)

    app.add_api_route(LIVENESS_ENDPOINT, live, methods=["GET"])
    return app, queue


def test_trace_server(fast_api_app: FastAPI) -> None:
    proc = None
    with FastAPIProcessRunner(fast_api_app) as server:
        proc = server._proc
        assert proc is not None
        assert proc.is_alive()
        assert server._proc is not None
        assert server._proc.is_alive()
        assert server.url is not None
        liveness_url = f"{server.url}{LIVENESS_ENDPOINT}"
        resp = requests.get(liveness_url)
        assert resp.status_code == 200
        assert resp.text == "ok"
    # Server should be stopped
    assert not proc.is_alive()
    assert server.url is None
    assert server._proc is None
    assert server._socket is None
    with pytest.raises(requests.exceptions.ConnectionError):
        requests.get(liveness_url)


def test_trace_server_lifespan(
    fast_api_app_with_lifespan: tuple[FastAPI, Queue]
) -> None:
    fast_api_app, queue = fast_api_app_with_lifespan
    with FastAPIProcessRunner(fast_api_app) as server:
        assert queue.get() == "on-start"
        resp = requests.get(f"{server.url}{LIVENESS_ENDPOINT}")
        assert resp.status_code == 200
        assert resp.text == "ok"
    assert queue.get() == "on-shutdown"
