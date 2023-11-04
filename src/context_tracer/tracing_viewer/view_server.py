import logging
import multiprocessing as mp
import signal
import socket
from http import HTTPStatus
from pathlib import Path

import uvicorn
from fastapi import APIRouter, FastAPI, Response, WebSocket
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from websockets.exceptions import ConnectionClosedError

from context_tracer.utils.async_mp_queue import AsyncMultiProcessQueue

from .load_templates import get_flamechart_view

log = logging.getLogger(__name__)

# Resources
THIS_DIR = Path(__file__).parent.resolve()
# Static files
STATIC_FILES_DIR = THIS_DIR / "server_static"
assert STATIC_FILES_DIR.exists()
STATIC_PATH = "/static"
WEBSOCKET_PATH = "/ws"


class ViewServer:
    """
    Webserver to serve flamechart view.

    Usage:
    ```python
    server = ViewServer()
    with server as url:
        server.queue.put(data)
    ```

    - Self-contained class, no need to setup anything else.
    - Use port 0 (ephemeral port) to get a random port.
    - The webserver is started by entering the context manager.
    - The webserver is started in a separate process and runs until the context manager exits.
    - Use the queue to send data to the webserver.
    - The webserver will send the data to the client via websocket.
    - The client will render the flamechart.
    """

    host: str
    port: int
    _sock: socket.socket | None = None
    _proc: mp.Process | None = None

    def __init__(self, host: str = "localhost", port: int = 0):
        self.host = host
        self.port = port
        # Setup queue
        self.queue: AsyncMultiProcessQueue = AsyncMultiProcessQueue()
        # Router
        self.router = APIRouter()
        self.router.add_api_route("/", self.view, methods=["GET"])
        # App
        self.app = FastAPI()
        self.app.mount(
            STATIC_PATH, StaticNoCache(directory=STATIC_FILES_DIR), name="static"
        )
        self.app.include_router(self.router)
        self.app.add_websocket_route(WEBSOCKET_PATH, self.websocket_endpoint)
        # Server
        self.server = ServerNoSignalHandler(
            config=uvicorn.Config(self.app, log_level="info")
        )

    def __enter__(self) -> str:
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.bind((self.host, self.port))
        self._sock.listen(1)
        self.port = self._sock.getsockname()[1]
        log.info(f"Webserver listening on {self.host}:{self.port}")
        self._proc = mp.Process(
            target=self.server.run,
            args=(
                [
                    self._sock,
                ],
            ),
            daemon=True,
        )
        self._proc.start()
        log.debug(f"Webserver started on pid={self._proc.pid}")
        url = f"http://{self.host}:{self.port}"
        return url

    def __exit__(self, exc_type, exc_value, traceback):
        log.debug("Waiting for webserver to finish")
        try:
            self.server.should_exit = True
            self.server.handle_exit(signal.SIGINT, None)
            if self._proc and self._proc.is_alive():
                log.debug("Webserver still running, terminate")
                self._proc.terminate()
                self._proc.join()
        finally:
            if self._sock:
                log.debug("Closing socket")
                self._sock.close()
        log.info("Webserver finished")

    async def view(self):
        """Main page to render flamechart."""
        flamechart_view_html = get_flamechart_view(
            css_js_static_path=STATIC_PATH,
            data_dict={},
            websocket_url=f"ws://{self.host}:{self.port}{WEBSOCKET_PATH}",
        )
        return HTMLResponse(content=flamechart_view_html, status_code=HTTPStatus.OK)

    async def websocket_endpoint(self, websocket: WebSocket):
        """
        Websocket endpoint to send data to client.
        Listen for data on queue, send to client.
        """
        log.debug("Setup websocket")
        await websocket.accept()
        log.debug("Websocket accepted")
        count = 0
        while True:
            count += 1
            log.debug(f"Waiting for data (iteration={count})")
            data = await self.queue.get_async()
            if data is None:
                log.debug("Queue received None, exiting")
                break
            log.debug(f"Got data to send {len(data)=}")
            try:
                await websocket.send_text(data)
                log.debug("Sent data, wait")
            except ConnectionClosedError:
                log.error("Client disconnected.")
                break
        log.info("Done sending data to websocket!")


class StaticNoCache(StaticFiles):
    def is_not_modified(self, *args, **kwargs) -> bool:
        return False

    def file_response(self, *args, **kwargs) -> Response:
        resp = super().file_response(*args, **kwargs)
        resp.headers["Cache-Control"] = "no-cache"
        return resp


class ServerNoSignalHandler(uvicorn.Server):
    def install_signal_handlers(self):
        pass
