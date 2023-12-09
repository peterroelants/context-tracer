import contextlib
import logging
from abc import abstractmethod
from contextvars import ContextVar
from types import TracebackType
from typing import (
    Any,
    Iterator,
    Optional,
    Protocol,
    Self,
    TypeVar,
    runtime_checkable,
)

from .utils.types import ContextManagerProtocol

logger = logging.getLogger(__name__)


TraceSpanType = TypeVar("TraceSpanType", bound="TraceSpan")
TraceSpanType_return = TypeVar(
    "TraceSpanType_return", bound="TraceSpan", covariant=True
)
TraceTreeType_return = TypeVar(
    "TraceTreeType_return", bound="TraceTree", covariant=True
)


# Context ##########################################################
# Trace context varaible keeps track of the current execution context
_TRACE_SPAN_IN_CONTEXT = ContextVar[Optional["TraceSpan"]](
    "TRACE_SPAN_IN_CONTEXT", default=None
)


# Trace Types ######################################################
class TraceError(Exception):
    """Error related to trace context."""

    pass


# TODO: SpanData Type?


# TODO: Rename to Span?
@runtime_checkable
class TraceSpan(Protocol):
    """
    A TraceSpan is a span of execution where events occur. A TraceSpan keeps track of these events, such as other TraceSpans that are created in this span.
    TraceSpans form a hierarchy starting from the first (root) TraceSpan. This hierarchy is called a Tracing.

    TraceSpans form a tree, where each TraceSpan has a parent TraceSpan. The tree is build "backwards" from the leaves to the root. TraceSpans have references to their parent, but not to their children. The child is the latest TraceSpan, while the parent is the previous TraceSpan.

    NOTE: This is a protocol, not an abstract class. This means that it can be implemented by any class, not just subclasses of TraceSpan. We want to avoid any implementations here.
    """

    # TODO: Parent relationship?

    @property
    def uid(self) -> bytes:
        """Unique identifier of the node."""
        ...

    # TODO: Name needed now that we have id?
    # TODO: Name could be something ony for the tree, not the span?
    @property
    def name(self) -> str:
        """Name of the node. Human readable identifier."""
        ...

    # TODO: Does data needs to be accessible? Tree is used to access eventually
    @property
    def data(self) -> dict[str, Any]:
        """Data of the node."""
        ...

    @abstractmethod
    def new_child(self, **kwargs) -> Self:
        """Create a new child node with self as parent."""
        ...

    @abstractmethod
    def update_data(self, **kwargs) -> None:
        """Add data to the current node."""
        ...

    def __enter__(self: Self) -> Self:
        """Enter the context of the span."""
        return self

    def __exit__(self, *exc) -> None:
        """
        Exit the context of the span.

        Implement/Override this method if you want to do something when the span is exited,
         e.g. persist the span's data.
        """
        return None


@runtime_checkable
class TraceTree(Protocol):
    """
    Node in the trace tree, which has the inverse relationship to TraceSpan.

    TraceTree's are build from a single root node.
    """

    @property
    def name(self) -> str:
        """Name of the node."""
        ...

    @property
    def data(self) -> dict[str, Any]:
        """Data of the node."""
        ...

    @property
    def children(self: Self) -> list[Self]:
        ...


# TODO: Move Tracing and TraceTree to a separate module? Maybe even have a subtype of TraceSpan to have data and name?
# TODO: Rename to Tracer to be consistent with OpenTelemetry? https://opentelemetry.io/docs/concepts/signals/traces/#tracer-provider
# TODO: Documentation: prefer to inherit from this to keep functionality, but can also implement this protocol
@runtime_checkable
class Tracing(Protocol[TraceSpanType_return, TraceTreeType_return]):
    """
    Capture a hierarchy of TraceSpans.

    Creates the root context to build the hierarchy from. If not used no trace will be captured.
    """

    # TODO: Add Generic type to `AbstractContextManager`?
    _span_ctx_mngr: ContextManagerProtocol | None = None

    # TODO: Does root span need to be accessible? It only needs to be to enter the context...
    # Can this be given as an argument to __enter__?
    @property
    @abstractmethod
    def root_span(self) -> TraceSpanType_return:
        """Root node of the trace tree."""
        ...

    @property
    @abstractmethod
    def tree(self) -> TraceTreeType_return:
        """Return a representable version of the root of the trace tree."""
        ...

    def __enter__(self: Self) -> Self:
        """Set the root span as the current span to start the trace."""
        self._span_ctx_mngr = trace_span_context(self.root_span)
        self._span_ctx_mngr.__enter__()  # Enter Span
        return self

    def __exit__(
        self,
        __exc_type: type[BaseException] | None = None,
        __exc_value: BaseException | None = None,
        __traceback: TracebackType | None = None,
    ) -> None:
        """Reset the trace."""
        if self._span_ctx_mngr is not None:
            self._span_ctx_mngr.__exit__(
                __exc_type, __exc_value, __traceback
            )  # Exit Span
            self._span_ctx_mngr = None


# Manage Trace Context #############################################
@contextlib.contextmanager
def trace_span_context(span: TraceSpanType) -> Iterator[TraceSpanType]:
    """
    Run in the context of the given Span.
    """
    reset_token = _TRACE_SPAN_IN_CONTEXT.set(span)
    try:
        with span:  # Enter Span Context
            yield span
    finally:
        _TRACE_SPAN_IN_CONTEXT.reset(reset_token)


def get_current_span() -> TraceSpan | None:
    return _TRACE_SPAN_IN_CONTEXT.get()


def get_current_span_safe() -> TraceSpan:
    """
    Return the current trace context.

    Raises:
        Exception: If no trace is running.
    """
    current_trace: TraceSpan | None = get_current_span()
    if current_trace is None:
        raise TraceError(
            f"No Span is running. Run this only in the context of a `{Tracing.__name__}`!"
        )
    return current_trace


def get_current_span_safe_typed(T: type[TraceSpanType]) -> TraceSpanType:
    """
    Return the current trace context.

    Raises:
        Exception: If no trace is running.
    """
    current_span: TraceSpan = get_current_span_safe()
    if not isinstance(current_span, T):
        raise TraceError(
            f"Expected type {T.__name__!r}, got {type(current_span).__name__!r}!"
        )
    return current_span
