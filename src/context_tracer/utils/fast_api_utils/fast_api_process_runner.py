import logging
import multiprocessing as mp
import os
import socket
from abc import abstractmethod
from types import TracebackType
from typing import Any, Protocol, Self, runtime_checkable

import uvicorn
import uvicorn.config
from fastapi import FastAPI

log = logging.getLogger(__name__)


class CreateAppType(Protocol):
    def __call__(self, host: str, port: int) -> FastAPI:
        ...


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
    _socket: socket.socket | None = None
    _proc: mp.Process | None = None
    _url: str | None = None
    # Function to create the FastAPI application (needed to avoid FastAPI pickling issues)
    _create_app: CreateAppType
    _uvicorn_server_kwargs: dict[str, Any]

    def __init__(
        self,
        create_app: CreateAppType,
        host: str = "localhost",
        port: int = 0,
        # Extra kwargs are passed to `uvicorn.Config` used to create the uvicorn Server.
        **server_kwargs: dict[str, Any],
    ):
        log.debug(f"{self.__class__.__name__}({host=}, {port=})")
        self.host = host
        self.port = port
        self._create_app = create_app
        self._uvicorn_server_kwargs = server_kwargs

    @property
    def url(self) -> str:
        assert self._url is not None, "Server not started!"
        return self._url

    def _run(self, sockets: list[socket.socket]) -> None:
        """
        Run the server in the current process.
        """
        log.debug(f"{self.__class__.__name__}._run({sockets=}) on PID={os.getpid()}")
        app = self._create_app(host=self.host, port=self.port)
        # Clear all loggers (uvicorn adds loggers that don't propagate to root logger)
        log_config = uvicorn.config.LOGGING_CONFIG
        log_config["loggers"] = {}
        self._uvicorn_server_kwargs["log_config"] = log_config
        server = ServerNoSignalHandler(
            config=uvicorn.Config(app, **self._uvicorn_server_kwargs)
        )
        log.debug(f"Starting server on {self.host}:{self.port} on PID={os.getpid()}")
        server.run(sockets=sockets)

    def start(self) -> None:
        """
        Start the Trace server in a new process and return the URL.
        """
        log.debug(f"{self.__class__.__name__}.start()")
        if self._proc is not None and self._proc.is_alive():
            raise RuntimeError("Server already started!")
        # Setup socket separately to get the actual port assigned (in case port=0 was used)
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.bind((self.host, self.port))
        self._socket.listen(1)
        # Overwrite self.port with actual port assigned (in case port=0 was used)
        self.port = self._socket.getsockname()[1]
        # Start server in new process
        self._proc = mp.Process(
            target=self._run,
            kwargs=dict(sockets=[self._socket]),
            daemon=True,
        )
        self._proc.start()
        log.info(f"Started server with PID={self._proc.pid} on {self.host}:{self.port}")
        self._url = f"http://{self.host}:{self.port}"
        log.debug(f"Trace Server started on url={self.url!r}.")

    def stop(self, timeout_sec: float = 60) -> None:
        """
        Stop the process running the webserver.
        """
        log.debug(f"{self.__class__.__name__}.stop({timeout_sec=})")
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
        log.debug(f"{self.__class__.__name__}.__enter__()")
        self.start()
        return self

    def __exit__(self, *args, **kwargs) -> None:
        log.debug(f"{self.__class__.__name__}.__exit__({args=}, {kwargs=})")
        self.stop()


class ServerNoSignalHandler(uvicorn.Server):
    """
    A uvicorn Server that does not install signal handlers.

    Do this to allow custom signal handling in the parent process.

    Related:
    - https://github.com/encode/uvicorn/issues/1579
    """

    def install_signal_handlers(self):
        pass
        super().install_signal_handlers()
