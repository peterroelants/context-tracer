import logging
from typing import Any, Optional

from context_tracer.constants import END_TIME_KEY, NAME_KEY, START_TIME_KEY
from context_tracer.trace_context import TraceSpan, TraceTree, Tracing
from context_tracer.utils.time_utils import get_local_timestamp

logger = logging.getLogger(__name__)


class TraceSpanInMemory(TraceSpan, TraceTree):
    """
    In-memory stored implementation of Span.

    Implements both Span (parent relation) and TraceTree (children relation) interfaces.
    Mainly for illustrative purposes.
    """

    _uid: bytes
    _name: str
    _data: dict[str, Any]
    _parent: Optional["TraceSpanInMemory"]
    _children: list["TraceSpanInMemory"]

    def __init__(
        self,
        name: str,
        parent: Optional["TraceSpanInMemory"],
        data: Optional[dict] = None,
    ) -> None:
        # TODO: Name in data?
        self._uid = str(id(self)).encode()
        self._name = name
        self._data = data or dict()
        self._parent = parent
        self._children = []
        super().__init__()

    def new_child(self, **data) -> "TraceSpanInMemory":
        name = data.pop(NAME_KEY, "no-name")
        child = TraceSpanInMemory(
            parent=self,
            name=name,
            data=data,
        )
        self._children.append(child)
        return child

    def update_data(self, **new_data) -> None:
        # TODO: Recursive update?
        self._data.update(new_data)

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
    def parent(self) -> Optional["TraceSpanInMemory"]:
        return self._parent

    @property
    def children(self) -> list["TraceSpanInMemory"]:
        return self._children

    # TODO: Move timing to `trace` implementation
    def __enter__(self) -> "TraceSpanInMemory":
        start_time = get_local_timestamp().isoformat(sep=" ", timespec="seconds")
        self.update_data(**{START_TIME_KEY: start_time})
        return self

    def __exit__(self, *exc) -> None:
        end_time = get_local_timestamp().isoformat(sep=" ", timespec="seconds")
        self.update_data(**{END_TIME_KEY: end_time})
        return None


class TracingInMemory(Tracing[TraceSpanInMemory, TraceSpanInMemory]):
    """Simple in-memory implementation of Trace."""

    _root_context: TraceSpanInMemory

    def __init__(self):
        self._root_context = TraceSpanInMemory(parent=None, name="root", data={})

    @property
    def root_span(self) -> TraceSpanInMemory:
        """Root context that is the parent of all other contexts."""
        return self._root_context

    @property
    def tree(self) -> TraceSpanInMemory:
        """Tree representation of the root of the trace tree."""
        return self._root_context
