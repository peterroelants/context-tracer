import logging
import multiprocessing as mp
import socket
from abc import abstractmethod
from types import TracebackType
from typing import Protocol, Self, runtime_checkable

import uvicorn
from fastapi import FastAPI

log = logging.getLogger(__name__)


@runtime_checkable
class ServerContextProtocol(Protocol):
    """
    A Server is a process that runs a webserver during the context manager's lifetime.
    """

    @property
    def url(self) -> str | None:
        """Return the URL of the server if it's running, else return None."""
        ...

    def __enter__(self: Self) -> Self:
        ...

    @abstractmethod
    def __exit__(
        self,
        __exc_type: type[BaseException] | None,
        __exc_value: BaseException | None,
        __traceback: TracebackType | None,
    ) -> bool | None:
        ...


class FastAPIProcessRunner(ServerContextProtocol):
    """
    Server to run a FastAPI application in a separate process.

    - Use as context manager to start and stop the server.
    - Set `port=0` to let the OS assign a free port (Ephemeral port).
    """

    host: str
    port: int
    _app: FastAPI
    _server: uvicorn.Server
    _socket: socket.socket | None = None
    _proc: mp.Process | None = None
    _url: str | None = None

    def __init__(
        self,
        app: FastAPI,
        host: str = "localhost",
        port: int = 0,
        # Extra kwargs are passed to `uvicorn.Config` used to create the uvicorn Server.
        **server_kwargs,
    ):
        self.host = host
        self.port = port
        self._app = app
        self._server = uvicorn.Server(config=uvicorn.Config(self._app, **server_kwargs))

    @property
    def url(self) -> str | None:
        return self._url

    @property
    def server(self) -> uvicorn.Server:
        if self._proc is not None and self._proc.is_alive():
            raise RuntimeError(
                "Server cannot be accessed because it is running in a separate process!"
            )
        return self._server

    def start(self) -> None:
        """
        Start the Trace server in a new process and return the URL.
        """
        if self._proc is not None and self._proc.is_alive():
            raise RuntimeError("Server already started!")
        # Setup socket seperatly to get the actual port assigned (in case port=0 was used)
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.bind((self.host, self.port))
        self._socket.listen(1)
        # Overwrite self.port with actual port assigned (in case port=0 was used)
        self.port = self._socket.getsockname()[1]
        # Start server in new process
        self._proc = mp.Process(
            target=self.server.run,
            kwargs=dict(sockets=[self._socket]),
            daemon=True,
        )
        self._proc.start()
        self._url = f"http://{self.host}:{self.port}"
        log.debug(f"Trace Server started on url={self.url!r}.")

    def stop(self, timeout_sec: float = 60) -> None:
        """
        Stop the process running the webserver.
        """
        self._url = None
        try:
            if self._proc and self._proc.is_alive():
                log.debug("Webserver running, terminate")
                # Terminate server process, assume signal gets propagated
                self._proc.terminate()
                self._proc.join(timeout_sec)
                if self._proc.is_alive():
                    log.debug("Webserver still running, kill")
                    self._proc.kill()
            self._proc = None  # Terminated process cannot be reused
        finally:
            try:
                if self._socket:
                    log.debug("Closing socket")
                    self._socket.close()
            finally:
                self._socket = None  # Delete socket to avoid reusing it
        log.info("Webserver finished")

    def __enter__(self: Self) -> Self:
        self.start()
        return self

    def __exit__(self, *args, **kwargs) -> None:
        self.stop()
