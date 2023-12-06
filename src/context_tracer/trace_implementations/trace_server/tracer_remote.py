import logging
from contextlib import AbstractContextManager
from pathlib import Path
from typing import Any, Final, Self

from context_tracer.constants import DATA_KEY, NAME_KEY
from context_tracer.trace_context import TraceSpan, Tracing
from context_tracer.trace_implementations.trace_server.trace_server import (
    FastAPIProcessRunner,
    SpanClientAPI,
    SpanDataBase,
    create_span_server,
)
from context_tracer.trace_implementations.trace_sqlite.tracer_sqlite import (
    TraceTreeSqlite,
)
from context_tracer.utils.id_utils import new_uid

log = logging.getLogger(__name__)

DEFAULT_SPAN_NAME: Final[str] = "no-name"


# Span #####################################################
class TraceSpanRemote(TraceSpan, AbstractContextManager):
    """
    TODO: Documentation
    Node in the trace tree.

    """

    _span_uid: bytes
    client: SpanClientAPI

    def __init__(self, client: SpanClientAPI, span_uid: bytes) -> None:
        self.client = client
        self._span_uid = span_uid
        super().__init__()

    @property
    def uid(self) -> bytes:
        return self._span_uid

    @property
    def name(self) -> str:
        return self.client.get_span(uid=self._span_uid)[NAME_KEY]

    @property
    def data(self) -> dict[str, Any]:
        return self.client.get_span(uid=self._span_uid)[DATA_KEY]

    @property
    def children(self: Self) -> list[Self]:
        child_ids: list[bytes] = self.client.get_children_uids(uid=self._span_uid)
        return [self.__class__(client=self.client, span_uid=id) for id in child_ids]

    @classmethod
    def new(
        cls,
        client: SpanClientAPI,
        name: str,
        data: dict[str, Any],
        parent_uid: bytes | None,
    ) -> Self:
        span_uid = new_uid()
        client.put_new_span(
            uid=span_uid,
            name=name,
            data=data,
            parent_uid=parent_uid,
        )
        return cls(client=client, span_uid=span_uid)

    def new_child(self: Self, **data) -> Self:
        name = data.pop(NAME_KEY, DEFAULT_SPAN_NAME)
        return self.new(
            client=self.client, name=name, data=data, parent_uid=self._span_uid
        )

    def update_data(self, **new_data) -> None:
        self.client.patch_update_span(
            uid=self._span_uid,
            data=new_data,
        )


class TracingRemote(Tracing[TraceSpanRemote, TraceTreeSqlite]):
    span_db: SpanDataBase
    _root_name: str
    _server: FastAPIProcessRunner | None = None
    _api_client: SpanClientAPI | None = None
    _root_uid: bytes | None
    _server_kwargs: dict[str, Any]

    def __init__(
        self,
        db_path: Path,
        name: str = "root",
        root_uid: bytes | None = None,
        **server_kwargs,
    ) -> None:
        log.debug(f"TracingRemote(db_path={db_path}, name={name})")
        self.span_db = SpanDataBase(db_path=db_path)
        self._root_name = name
        self._root_uid = root_uid
        self._server_kwargs = server_kwargs

    @property
    def span_db_path(self) -> Path:
        return self.span_db.db_path

    @property
    def root_span(self) -> TraceSpanRemote:
        """Root context that is the parent of all other contexts."""
        assert self._api_client is not None, "No API Client found, Tracing no running!"
        assert self._root_uid is not None, "No Root UID found, Tracing not started!"
        return TraceSpanRemote(client=self._api_client, span_uid=self._root_uid)

    @property
    def tree(self) -> TraceTreeSqlite:
        assert self._root_uid is not None, "No Root UID found, Tracing not started!"
        return TraceTreeSqlite(span_db=self.span_db, span_uid=self._root_uid)

    def __enter__(self: Self) -> Self:
        """Start a new tracing."""
        self._server: FastAPIProcessRunner = create_span_server(
            db_path=self.span_db.db_path, **self._server_kwargs
        )
        self._server.__enter__()
        self._api_client = SpanClientAPI(url=self._server.url)
        self._api_client.wait_for_ready()
        assert (
            self._api_client.is_ready()
        ), f"Server at {self._api_client.url!r} is not ready."
        if self._root_uid is None:
            self._root_uid = TraceSpanRemote.new(
                client=self._api_client,
                name=self._root_name,
                data={},
                parent_uid=None,
            ).uid
        return super().__enter__()

    def __exit__(self, *args, **kwargs) -> None:
        """Reset the tracing."""
        try:
            super().__exit__(*args, **kwargs)
        finally:
            if self._server is not None:
                self._server.__exit__(*args, **kwargs)
            self._server = None
        return
