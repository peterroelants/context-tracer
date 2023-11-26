import logging
from contextlib import AbstractContextManager
from typing import Any, Final, Self

from context_tracer.constants import DATA_KEY, NAME_KEY
from context_tracer.trace_context import TraceSpan, TraceTree, Tracing
from context_tracer.utils.id_utils import new_uid

from .trace_server import SpanClientAPI

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


class SpanTreeRemote(TraceTree):
    """ """

    _uid: bytes
    _name: str
    _parent_uid: bytes | None
    _data: dict[str, Any]
    client: SpanClientAPI

    def __init__(
        self,
        client: SpanClientAPI,
        uid: bytes,
        name: str,
        data: dict[str, Any],
        parent_uid: bytes | None,
    ) -> None:
        self.client = client
        self._uid = uid
        self._name = name
        self._data = data
        self._parent_uid = parent_uid
        super().__init__()

    @property
    def uid(self) -> bytes:
        return self._uid

    @property
    def name(self) -> str:
        return self._name

    @property
    def data(self) -> dict[str, Any]:
        return self._data

    @property
    def children(self: Self) -> list[Self]:
        child_uids: list[bytes] = self.client.get_children_uids(uid=self._uid)
        return [self.from_remote(client=self.client, uid=uid) for uid in child_uids]

    @classmethod
    def from_remote(
        cls,
        client: SpanClientAPI,
        uid: bytes,
    ) -> Self:
        span_dict = client.get_span(uid=uid)
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
    def _root_uid(self) -> bytes:
        return self._api_client.get_root_span_ids()[0]

    @property
    def root_span(self) -> TraceSpanRemote:
        """Root context that is the parent of all other contexts."""
        return TraceSpanRemote(client=self._api_client, span_uid=self._root_uid)

    @property
    def tree(self) -> SpanTreeRemote:
        """Tree representation of the root of the trace tree."""
        return SpanTreeRemote.from_remote(client=self._api_client, uid=self._root_uid)

    def __enter__(self: Self) -> Self:
        TraceSpanRemote.new(
            client=self._api_client,
            name=self._root_name,
            data={},
            parent_uid=None,
        )
        return super().__enter__()

    def __exit__(self, *args, **kwargs) -> None:
        """Reset the trace."""
        super().__exit__(*args, **kwargs)
        return
