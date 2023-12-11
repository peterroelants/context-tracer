import logging
import time
import webbrowser
from pathlib import Path
from typing import (
    Any,
    Self,
)

from context_tracer.trace_implementations.trace_server.trace_server import (
    SpanDataBase,
)
from context_tracer.trace_implementations.trace_server.tracer_remote import (
    TraceSpanRemote,
    TracingRemote,
)
from context_tracer.trace_implementations.trace_sqlite.tracer_sqlite import (
    TraceTreeSqlite,
)
from context_tracer.trace_types import Tracing
from context_tracer.utils.fast_api_utils import (
    FastAPIProcessRunner,
)

from .view_server import create_view_server

log = logging.getLogger(__name__)


class TracingWithViewer(Tracing[TraceSpanRemote, TraceTreeSqlite]):
    _view_server: FastAPIProcessRunner | None = None
    _tracing_remote: TracingRemote
    _export_html_path: Path | None
    _log_dir: Path | None
    _server_kwargs: dict[str, Any]
    _open_browser: bool

    def __init__(
        self,
        db_path: Path,
        export_html_path: Path | None = None,
        log_dir: Path | None = None,
        open_browser: bool = False,
        name: str = "root",
        root_uid: bytes | None = None,
        **server_kwargs,
    ) -> None:
        log.debug(f"TracingRemote(db_path={db_path}, name={name})")
        remote_tracing_log_path = None
        if log_dir is not None:
            remote_tracing_log_path = log_dir / "remote_tracing.log"
        self._tracing_remote = TracingRemote(
            db_path=db_path,
            name=name,
            root_uid=root_uid,
            log_path=remote_tracing_log_path,
            **server_kwargs,
        )
        self._server_kwargs = server_kwargs
        self._export_html_path = export_html_path
        self._open_browser = open_browser
        self._log_dir = log_dir

    @property
    def url(self) -> str:
        """Return the URL of the server if it's running, else return None."""
        assert self._view_server is not None, "View server not running!"
        return self._view_server.url

    @property
    def span_db(self) -> SpanDataBase:
        return self._tracing_remote.span_db

    @property
    def root_span(self) -> TraceSpanRemote:
        """Root context that is the parent of all other contexts."""
        return self._tracing_remote.root_span

    @property
    def tree(self) -> TraceTreeSqlite:
        return self._tracing_remote.tree

    def __enter__(self: Self) -> Self:
        """Start a new tracing."""
        viewer_log_path = None
        if self._log_dir is not None:
            viewer_log_path = self._log_dir / "viewer.log"
        self._view_server: FastAPIProcessRunner = create_view_server(
            db_path=self._tracing_remote.span_db_path,
            export_html_path=self._export_html_path,
            log_path=viewer_log_path,
            **self._server_kwargs,
        )
        self._view_server.__enter__()
        if self._open_browser:
            webbrowser.open(self._view_server.url)
        self._tracing_remote.__enter__()
        return super().__enter__()

    def __exit__(self, *args, **kwargs) -> None:
        """Reset the tracing."""
        try:
            super().__exit__(*args, **kwargs)
        finally:
            try:
                self._tracing_remote.__exit__(*args, **kwargs)
                time.sleep(0.1)
            finally:
                # Close view server last
                if self._view_server is not None:
                    self._view_server.__exit__(*args, **kwargs)
                self._server = None
        return
