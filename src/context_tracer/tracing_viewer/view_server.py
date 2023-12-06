import asyncio
import contextlib
import functools
import html
import json
import logging
import os
import signal
from http import HTTPStatus
from pathlib import Path
from typing import Any, AsyncIterator

from fastapi import APIRouter, FastAPI
from fastapi.responses import HTMLResponse
from starlette.websockets import WebSocket, WebSocketState
from websockets.exceptions import ConnectionClosedError

from context_tracer.trace_implementations.trace_sqlite import TraceTreeSqlite
from context_tracer.trace_implementations.trace_sqlite.span_db import (
    SpanDataBase,
)
from context_tracer.trace_types import TraceTree
from context_tracer.utils.fast_api_utils import (
    FastAPIProcessRunner,
    StaticNoCache,
)
from context_tracer.utils.fast_api_utils.readiness import (
    READINESS_ENDPOINT_PATH,
    readiness_api,
)
from context_tracer.utils.json_encoder import AnyEncoder
from context_tracer.utils.logging_utils import setup_logging

from .load_templates import get_flamechart_view

log = logging.getLogger(__name__)


# Resources
THIS_DIR = Path(__file__).parent.resolve()
# Static files
STATIC_FILES_DIR = THIS_DIR / "server_static"
assert STATIC_FILES_DIR.exists()
STATIC_PATH = "/static"
WEBSOCKET_PATH = "/ws"


# Server ###########################################################
class ViewServerAPI:
    """
    API for Span server.
    Provides POST endpoints to update the span database.
    """

    span_db: SpanDataBase
    host: str
    port: int
    websocket_path: str
    export_html_path: Path | None
    _websocket_active: bool = True

    def __init__(
        self,
        span_db: SpanDataBase,
        host: str,
        port: int,
        websocket_path: str,
        export_html_path: Path | None = None,
    ) -> None:
        self.span_db = span_db
        self.host = host
        self.port = port
        self.websocket_path = websocket_path
        self.export_html_path = export_html_path

    async def view(self):
        """Main page to render flamechart."""
        websocket_url = (
            f"ws://{self.host}:{self.port}/{self.websocket_path.lstrip('/')}"
        )
        flamechart_view_html = get_flamechart_view(
            css_js_static_path=STATIC_PATH,
            data_dict={},
            websocket_url=websocket_url,
        )
        return HTMLResponse(content=flamechart_view_html, status_code=HTTPStatus.OK)

    async def websocket_endpoint(self, websocket: WebSocket):
        """
        Websocket endpoint to send data to web client for viewing.
        Listen for data on queue, send to web client.
        """
        await websocket.accept()
        timestamp_last_update: float = 0
        count = 0
        while self._websocket_active:
            count += 1
            log.debug(
                f"Check for last updated data (iteration={count}) on PID={os.getpid()}"
            )
            _, timestamp = self.span_db.get_last_updated_span_uid()
            if timestamp and timestamp > timestamp_last_update:
                timestamp_last_update = timestamp
                tree_json = await self.get_full_span_tree_json()
                try:
                    await websocket.send_text(tree_json)
                except ConnectionClosedError:
                    break
            await asyncio.sleep(1)
        log.debug(f"{self._websocket_active=!r}")
        # Try sending last data
        try:
            tree_json = await self.get_full_span_tree_json()
            if websocket.client_state == WebSocketState.CONNECTED:
                await websocket.send_text(tree_json)
        except Exception as exc:
            log.warning(f"Client disconnected: {exc}")
        await websocket.close()

    async def get_full_span_tree_json(self) -> str:
        """Get full tree as JSON string."""
        dict_tree = await self.get_full_span_tree()
        tree_json = json.dumps(
            dict_tree,
            cls=AnyEncoder,
            separators=(",", ":"),
        )
        tree_json = html.escape(tree_json, quote=False)
        return tree_json

    async def get_full_span_tree(self) -> dict[str, Any]:
        root_uids = self.span_db.get_root_uids()
        if len(root_uids) == 0:
            return {}
        root = TraceTreeSqlite(span_db=self.span_db, span_uid=root_uids[-1])
        return trace_tree_to_dict(root)

    async def _export_html(self) -> None:
        """Export the trace tree as stand-alone HTML file."""
        if self.export_html_path is not None:
            dict_tree = await self.get_full_span_tree()
            self.export_html_path.write_text(get_flamechart_view(data_dict=dict_tree))

    @contextlib.asynccontextmanager
    async def lifespan(self, app: FastAPI) -> AsyncIterator[None]:
        """
        ASGI lifespan event handler.

        - https://fastapi.tiangolo.com/advanced/events/
        - https://asgi.readthedocs.io/en/latest/specs/lifespan.html
        """
        log.debug(f"{self.__class__.__name__}.lifespan()")
        # Install signal handlers to interrupt websocket loop
        signal.signal(signal.SIGTERM, self.stop_server)
        signal.signal(signal.SIGINT, self.stop_server)
        log.debug(f"Installed signal handlers on PID={os.getpid()}")
        yield
        log.debug(f"Shutdown server on PID={os.getpid()}")
        # On shutdown is only run after all connections are closed
        self._websocket_active = False
        self.span_db.wal_checkpoint()
        log.debug(f"Checkpointed WAL on PID={os.getpid()}")
        await self._export_html()

    def stop_server(self, sig_num: int, *args, **kwargs) -> None:
        self._websocket_active = False

    def get_router(self) -> APIRouter:
        """
        Returns a router with the span server HTTP API endpoints.
        Database backed server.
        To be included in a FastAPI app.
        """
        router = APIRouter()
        router.add_api_route("/", self.view, methods=["GET"])
        router.add_websocket_route(self.websocket_path, self.websocket_endpoint)
        return router

    @classmethod
    def create_app(
        cls,
        span_db: SpanDataBase,
        host: str,
        port: int,
        websocket_path: str = WEBSOCKET_PATH,
        readiness_path: str = READINESS_ENDPOINT_PATH,
        export_html_path: Path | None = None,
        log_path: Path | None = None,
        log_level: int = logging.INFO,
    ) -> FastAPI:
        """
        Returns a FastAPI app with the span server HTTP API endpoints.
        Database backed server.
        """
        setup_logging(log_path=log_path, log_level=log_level)
        api = cls(
            span_db=span_db,
            host=host,
            port=port,
            websocket_path=websocket_path,
            export_html_path=export_html_path,
        )
        app = FastAPI(lifespan=api.lifespan)
        app.mount(STATIC_PATH, StaticNoCache(directory=STATIC_FILES_DIR), name="static")
        app.add_api_route(readiness_path, readiness_api, methods=["GET"])
        app.include_router(api.get_router())
        log.debug(f"FastAPI app created: {app=!r} on PID={os.getpid()}")
        return app


def create_view_server(
    db_path: Path,
    export_html_path: Path | None,
    log_path: Path | None = None,
    **server_kwargs,
) -> FastAPIProcessRunner:
    log.info(f"Create view server with db_path={db_path}")
    log_level = server_kwargs.pop("log_level", logging.INFO)
    span_db = SpanDataBase(db_path=db_path)
    create_app = functools.partial(
        ViewServerAPI.create_app,
        span_db=span_db,
        websocket_path=WEBSOCKET_PATH,
        readiness_path=READINESS_ENDPOINT_PATH,
        export_html_path=export_html_path,
        log_level=log_level,
        log_path=log_path,
    )
    log.debug(f"Return FastAPIProcessRunner with {create_app=!r}")
    return FastAPIProcessRunner(create_app=create_app, **server_kwargs)


def trace_tree_to_dict(trace_tree: TraceTree) -> dict[str, Any]:
    return {
        "name": trace_tree.name,
        "data": trace_tree.data,
        "children": [trace_tree_to_dict(child) for child in trace_tree.children],
    }
