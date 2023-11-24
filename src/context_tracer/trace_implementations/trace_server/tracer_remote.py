import logging
import uuid
from contextlib import AbstractContextManager
from typing import Any, Final, Self

from context_tracer.constants import DATA_KEY, NAME_KEY
from context_tracer.trace_context import TraceSpan, TraceTree, Tracing

from .trace_server import SpanClientAPI

log = logging.getLogger(__name__)

DEFAULT_SPAN_NAME: Final[str] = "no-name"


# Span #####################################################
class TraceSpanRemote(TraceSpan, AbstractContextManager):
    """
    TODO: Documentation
    Node in the trace tree.

    """

    _span_id: bytes
    client: SpanClientAPI

    def __init__(self, client: SpanClientAPI, span_id: bytes) -> None:
        self.client = client
        self._span_id = span_id
        super().__init__()

    @property
    def id(self) -> bytes:
        return self._span_id

    @property
    def name(self) -> str:
        return self.client.get_span(id=self._span_id)[NAME_KEY]

    @property
    def data(self) -> dict[str, Any]:
        return self.client.get_span(id=self._span_id)[DATA_KEY]

    @property
    def children(self: Self) -> list[Self]:
        child_ids: list[bytes] = self.client.get_children_ids(id=self._span_id)
        return [self.__class__(client=self.client, span_id=id) for id in child_ids]

    @classmethod
    def new(
        cls,
        client: SpanClientAPI,
        name: str,
        data: dict[str, Any],
        parent_id: bytes | None,
    ) -> Self:
        span_id = uuid.uuid1().bytes
        client.put_new_span(
            id=span_id,
            name=name,
            data=data,
            parent_id=parent_id,
        )
        return cls(client=client, span_id=span_id)

    def new_child(self: Self, **data) -> Self:
        name = data.pop(NAME_KEY, DEFAULT_SPAN_NAME)
        return self.new(
            client=self.client, name=name, data=data, parent_id=self._span_id
        )

    def update_data(self, **new_data) -> None:
        self.client.patch_update_span(
            id=self._span_id,
            data=new_data,
        )


class SpanTreeRemote(TraceTree):
    """ """

    _id: bytes
    _name: str
    _parent_id: bytes | None
    _data: dict[str, Any]
    client: SpanClientAPI

    def __init__(
        self,
        client: SpanClientAPI,
        id: bytes,
        name: str,
        data: dict[str, Any],
        parent_id: bytes | None,
    ) -> None:
        self.client = client
        self._id = id
        self._name = name
        self._data = data
        self._parent_id = parent_id
        super().__init__()

    @property
    def id(self) -> bytes:
        return self._id

    @property
    def name(self) -> str:
        return self._name

    @property
    def data(self) -> dict[str, Any]:
        return self._data

    @property
    def children(self: Self) -> list[Self]:
        child_ids: list[bytes] = self.client.get_children_ids(id=self._id)
        return [self.from_remote(client=self.client, id=id) for id in child_ids]

    @classmethod
    def from_remote(
        cls,
        client: SpanClientAPI,
        id: bytes,
    ) -> Self:
        span_dict = client.get_span(id=id)
        return cls(client=client, **span_dict)


class TracingRemote(Tracing[TraceSpanRemote, SpanTreeRemote]):
    _api_client: SpanClientAPI
    _root_name: str

    def __init__(self, api_client: SpanClientAPI, name: str = "root") -> None:
        self._api_client = api_client
        assert (
            self._api_client.is_ready()
        ), f"Server at {self._api_client.url!r} is not ready."
        self._root_name = name

    @property
    def _root_id(self) -> bytes:
        return self._api_client.get_root_span_ids()[0]

    @property
    def root_span(self) -> TraceSpanRemote:
        """Root context that is the parent of all other contexts."""
        return TraceSpanRemote(client=self._api_client, span_id=self._root_id)

    @property
    def tree(self) -> SpanTreeRemote:
        """Tree representation of the root of the trace tree."""
        return SpanTreeRemote.from_remote(client=self._api_client, id=self._root_id)

    def __enter__(self: Self) -> Self:
        TraceSpanRemote.new(
            client=self._api_client,
            name=self._root_name,
            data={},
            parent_id=None,
        )
        return super().__enter__()

    def __exit__(self, *args, **kwargs) -> None:
        """Reset the trace."""
        super().__exit__(*args, **kwargs)
        return
