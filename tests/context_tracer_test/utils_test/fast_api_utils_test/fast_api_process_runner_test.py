import contextlib
import functools
import logging
import multiprocessing as mp
from contextlib import asynccontextmanager
from http import HTTPStatus
from multiprocessing import Queue
from typing import Iterator

import pytest
import requests
from context_tracer.utils.fast_api_utils import FastAPIProcessRunner
from context_tracer.utils.fast_api_utils.readiness import (
    READINESS_ENDPOINT_PATH,
    readiness_api,
)
from fastapi import FastAPI
from fastapi.responses import Response

log = logging.getLogger(__name__)


@contextlib.contextmanager
def multiprocess_start_method(start_method: str) -> Iterator[None]:
    log.info(f"multiprocess_start_method(start_method={start_method})")
    prev_start_method = mp.get_start_method()
    mp.set_start_method(start_method, force=True)
    try:
        yield
    finally:
        mp.set_start_method(prev_start_method, force=True)


def create_simple_app(queue: Queue, host: str, port: int) -> FastAPI:
    app = FastAPI()

    async def live() -> Response:
        nonlocal queue
        queue.put_nowait((host, port))
        return Response(content="ok", status_code=HTTPStatus.OK)

    app.add_api_route(READINESS_ENDPOINT_PATH, live, methods=["GET"])
    return app


def create_lifespan_app(queue: Queue, **kwargs) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        nonlocal queue
        queue.put_nowait("on-start")
        yield
        queue.put_nowait("on-shutdown")

    app = FastAPI(lifespan=lifespan)
    app.add_api_route(READINESS_ENDPOINT_PATH, readiness_api, methods=["GET"])
    return app


@pytest.mark.parametrize(
    "mp_start_method",
    ["fork", "spawn"],
)
def test_trace_server(mp_start_method: str) -> None:
    with multiprocess_start_method(mp_start_method):
        proc = None
        ctx = mp.get_context()
        assert ctx.get_start_method() == mp_start_method
        queue = ctx.Manager().Queue()
        create_app = functools.partial(create_simple_app, queue=queue)
        with FastAPIProcessRunner(create_app=create_app) as server:
            proc = server._proc
            assert proc is not None
            assert proc.is_alive()
            assert server._proc is not None
            assert server._proc.is_alive()
            assert server.url is not None
            liveness_url = f"{server.url}{READINESS_ENDPOINT_PATH}"
            resp = requests.get(liveness_url)
            assert resp.status_code == 200
            assert resp.text == "ok"
            assert queue.get() == (server.host, server.port)
        # Server should be stopped
        assert not proc.is_alive()
        assert server._proc is None
        assert server._socket is None
        with pytest.raises(requests.exceptions.ConnectionError):
            requests.get(liveness_url)


@pytest.mark.parametrize(
    "mp_start_method",
    ["fork", "spawn"],
)
def test_trace_server_lifespan(
    mp_start_method: str,
) -> None:
    with multiprocess_start_method(mp_start_method):
        ctx = mp.get_context()
        assert ctx.get_start_method() == mp_start_method
        queue = ctx.Manager().Queue()
        create_app = functools.partial(create_lifespan_app, queue=queue)
        with FastAPIProcessRunner(create_app=create_app) as server:
            assert queue.get() == "on-start"
            resp = requests.get(f"{server.url}{READINESS_ENDPOINT_PATH}")
            assert resp.status_code == 200
            assert resp.text == "ok"
        assert queue.get() == "on-shutdown"
