import functools
import logging
from collections.abc import Callable
from types import TracebackType
from typing import Any, ParamSpec, Self, TypeVar, Union, overload

from context_tracer.utils.time_utils import get_local_timestamp

from .constants import (
    END_TIME_KEY,
    EXCEPTION_KEY,
    EXCEPTION_MESSAGE_KEY,
    EXCEPTION_STACKTRACE_KEY,
    EXCEPTION_TYPE_KEY,
    FUNCTION_DECORATOR_KEY,
    FUNCTION_KWARGS_KEY,
    FUNCTION_NAME_KEY,
    FUNCTION_RETURNED_KEY,
    NAME_KEY,
    START_TIME_KEY,
    TRACE_METADATA_KEY,
)
from .trace_types import (
    TraceSpan,
    get_current_span,
    get_current_span_safe,
    trace_span_context,
)
from .utils.func_utils import func2str, get_func_bound_args
from .utils.merge_patch import merge_patch
from .utils.types import ContextManagerProtocol, DecoratorMeta

logger = logging.getLogger(__name__)


P = ParamSpec("P")
R = TypeVar("R")


# trace ##########################################################
@overload
def trace(func: Callable[P, R], /, **kwargs: Any) -> Callable[P, R]:
    """Trace a function by applying it with this decorator."""
    ...


@overload
def trace(**kwargs: Any) -> "_TraceContextDecorator":
    """Create a new trace context that can be used as a decorator or context manager."""
    ...


def trace(
    func: Callable[P, R] | None = None, /, **kwargs: Any
) -> Union[Callable[P, R], "_TraceContextDecorator"]:
    """
    Decorator and context manager to trace an execution.
    """
    if func is not None and callable(func):
        return _TraceContextDecorator(**kwargs)(func)
    return _TraceContextDecorator(**kwargs)


def log_with_trace(**kwargs) -> None:
    """Log kwargs with a new trace span."""
    if NAME_KEY not in kwargs:
        kwargs[NAME_KEY] = "log_with_trace"
    with trace(**kwargs):
        pass  # kwargs are automatically logged when trace is created


# trace implementation ############################################
class _TraceContextDecorator(metaclass=DecoratorMeta):
    """
    Decorator and context manager to create a new trace context.

    Use `trace` instead of this class in your code.
    """

    _trace_ctx_mngr: ContextManagerProtocol | None = None
    data: dict[str, Any]

    # TODO: parameters to disable method input/output logging
    def __init__(self, /, **data):
        """
        Create a new trace context.

        Only keyword arguments are allowed.
        """
        self.data = data

    def __call__(self, func: Callable[P, R]) -> Callable[P, R]:
        """Called when used as a decorator."""
        assert func is not None and callable(func)
        # Add function name as trace name if no name is provided
        if NAME_KEY not in self.data:
            self.data[NAME_KEY] = func2str(func)

        @functools.wraps(func)
        def wrapped_func(*args: P.args, **kwargs: P.kwargs) -> R:
            # Call function in trace context
            with self as span:  # Enter trace context
                if span is not None:
                    # Log function info
                    function_info: dict = {
                        FUNCTION_NAME_KEY: func2str(func),
                        FUNCTION_KWARGS_KEY: get_func_bound_args(func, *args, **kwargs),
                    }
                    span.update_data(**{FUNCTION_DECORATOR_KEY: function_info})
                # Call function and get result
                result = func(*args, **kwargs)
                if span is not None:
                    # Log function result
                    function_info[FUNCTION_RETURNED_KEY] = result
                    span.update_data(**{FUNCTION_DECORATOR_KEY: function_info})
            return result

        return wrapped_func

    def __enter__(self: Self) -> TraceSpan | None:
        """
        Create a new span with the current one as parent (iff a current span exists).
        """
        metadata = {
            TRACE_METADATA_KEY: {
                START_TIME_KEY: get_local_timestamp().isoformat(sep=" ")
            }
        }
        self.data = merge_patch(self.data, metadata)
        parent_span = get_current_span()
        if parent_span is not None:
            # Create a new span from the current span
            child = parent_span.new_child(**self.data)  # New child span
            self._trace_ctx_mngr = trace_span_context(child)
            return self._trace_ctx_mngr.__enter__()  # Enter span
        # If no span is found, no child can be created, and no tracing is performed.
        # Run in `Tracing` context to capture traces.
        return None

    def __exit__(
        self,
        __exc_type: type[BaseException] | None = None,
        __exc_value: BaseException | None = None,
        __traceback: TracebackType | None = None,
    ) -> None:
        """Exit the current trace span context and reset to the parent."""
        if self._trace_ctx_mngr is not None:
            new_data = {
                TRACE_METADATA_KEY: {
                    END_TIME_KEY: get_local_timestamp().isoformat(sep=" ")
                }
            }
            if __exc_type is not None or __exc_value is not None:
                # Deal with exceptions
                # Log exception
                exc_info: dict = {}
                if __exc_type is not None:
                    exc_info[EXCEPTION_TYPE_KEY] = __exc_type.__name__
                if __exc_value is not None:
                    exc_info[EXCEPTION_MESSAGE_KEY] = str(__exc_value)
                if __traceback is not None:
                    exc_info[EXCEPTION_STACKTRACE_KEY] = __traceback
                new_data[EXCEPTION_KEY] = exc_info
            # span should be there if there is a context manager
            span = get_current_span_safe()
            span.update_data(**new_data)
            # Exit the current trace span context if it exists.
            self._trace_ctx_mngr.__exit__(__exc_type, __exc_value, __traceback)
            self._trace_ctx_mngr = None
