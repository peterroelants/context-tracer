import logging
import uuid

import pytest
from context_tracer.constants import (
    EXCEPTION_KEY,
    EXCEPTION_TRACEBACK_KEY,
    EXCEPTION_TYPE_KEY,
    EXCEPTION_VALUE_KEY,
    FUNCTION_DECORATOR_KEY,
    FUNCTION_KWARGS_KEY,
    FUNCTION_NAME_KEY,
    FUNCTION_RETURNED_KEY,
)
from context_tracer.trace import trace
from context_tracer.trace_context import (
    TraceError,
    get_current_span,
    get_current_span_safe,
    get_current_span_safe_typed,
    trace_span_context,
)
from context_tracer.trace_implementations.trace_basic import (
    TraceSpanInMemory,
    TracingInMemory,
)

logger = logging.getLogger(__name__)


# Test Trace Span & Tracing ########################################
def test_as_context() -> None:
    span = TraceSpanInMemory(
        name="test",
        parent=None,
        data=None,
    )
    assert get_current_span() is None
    with trace_span_context(span):
        assert get_current_span_safe() is span
    assert get_current_span() is None


def test_tracing() -> None:
    assert get_current_span() is None
    with TracingInMemory() as tracing:
        assert get_current_span_safe() is tracing.root_span
    assert get_current_span() is None


def test_get_current_span() -> None:
    span = TraceSpanInMemory(
        name="test",
        parent=None,
        data=None,
    )
    assert get_current_span() is None
    with trace_span_context(span):
        assert get_current_span() is span
        assert get_current_span_safe() is span
        assert get_current_span_safe_typed(TraceSpanInMemory) is span
        with pytest.raises(TraceError):
            get_current_span_safe_typed(dict)  # type: ignore
    assert get_current_span() is None
    with pytest.raises(TraceError):
        get_current_span_safe()
    with pytest.raises(TraceError):
        get_current_span_safe_typed(TraceSpanInMemory)


# Test trace #######################################################
def test_trace_context_manager() -> None:
    test_var_content = str(uuid.uuid4())
    # Run with tracing
    with TracingInMemory():
        with trace(test_var=test_var_content):
            assert (
                get_current_span_safe_typed(TraceSpanInMemory).data["test_var"]
                == test_var_content
            )


def test_trace_context_manager_no_tracing() -> None:
    test_var_content = str(uuid.uuid4())
    # Run with tracing
    with trace(test_var=test_var_content):
        assert get_current_span() is None


def test_trace_decorator_no_arguments() -> None:
    spans_from_mock = []
    return_value = str(uuid.uuid4())

    @trace
    def mock_program() -> str:
        spans_from_mock.append(get_current_span_safe_typed(TraceSpanInMemory))
        return return_value

    assert callable(mock_program)
    assert mock_program.__name__ == "mock_program"

    # Run with tracing
    with TracingInMemory():
        assert mock_program() == return_value

    # Checks
    assert len(spans_from_mock) == 1
    span = spans_from_mock[0]
    assert isinstance(span, TraceSpanInMemory)
    assert span.name == mock_program.__name__
    assert span.data[FUNCTION_DECORATOR_KEY]
    assert span.data[FUNCTION_DECORATOR_KEY][FUNCTION_NAME_KEY] == mock_program.__name__
    assert span.data[FUNCTION_DECORATOR_KEY][FUNCTION_KWARGS_KEY] == {}
    assert span.data[FUNCTION_DECORATOR_KEY][FUNCTION_RETURNED_KEY] == return_value


def test_trace_decorator_arguments() -> None:
    span_from_mock = []

    test_var_content = str(uuid.uuid4())
    test_name = str(uuid.uuid4())
    return_value = str(uuid.uuid4())

    # Don't use method name as name
    @trace(name=test_name, test_var=test_var_content)
    def mock_program() -> str:
        span_from_mock.append(get_current_span_safe_typed(TraceSpanInMemory))
        return return_value

    assert callable(mock_program)
    assert mock_program.__name__ == "mock_program"  # type: ignore

    # Run with tracing
    with TracingInMemory():
        assert mock_program() == return_value

    # Checks
    assert len(span_from_mock) == 1
    span = span_from_mock[0]
    assert isinstance(span, TraceSpanInMemory)
    assert span.name == test_name
    assert span.data["test_var"] == test_var_content
    assert span.data[FUNCTION_DECORATOR_KEY]
    assert span.data[FUNCTION_DECORATOR_KEY][FUNCTION_NAME_KEY] == mock_program.__name__
    assert span.data[FUNCTION_DECORATOR_KEY][FUNCTION_KWARGS_KEY] == {}
    assert span.data[FUNCTION_DECORATOR_KEY][FUNCTION_RETURNED_KEY] == return_value


def test_trace_decorator_function_arguments() -> None:
    span_from_mock = []

    @trace
    def abcsum(a: int, b: int, c: int) -> int:
        span_from_mock.append(get_current_span_safe_typed(TraceSpanInMemory))
        return a + b + c

    assert callable(abcsum)
    assert abcsum.__name__ == "abcsum"  # type: ignore

    kwargs = {"a": 4, "b": 3, "c": 2}
    result_sum = sum(kwargs.values())

    # Run with tracing
    with TracingInMemory():
        assert abcsum(kwargs["a"], kwargs["b"], c=kwargs["c"]) == result_sum

    # Checks
    assert len(span_from_mock) == 1
    span = span_from_mock[0]
    assert isinstance(span, TraceSpanInMemory)
    assert span.name == abcsum.__name__
    assert span.data[FUNCTION_DECORATOR_KEY]
    assert span.data[FUNCTION_DECORATOR_KEY][FUNCTION_NAME_KEY] == abcsum.__name__
    assert span.data[FUNCTION_DECORATOR_KEY][FUNCTION_KWARGS_KEY] == kwargs
    assert span.data[FUNCTION_DECORATOR_KEY][FUNCTION_RETURNED_KEY] == result_sum


def test_trace_decorator_exception() -> None:
    spans_from_mock = []
    exception_value = str(uuid.uuid4())

    class MyException(Exception):
        pass

    @trace
    def mock_program() -> str:
        spans_from_mock.append(get_current_span_safe_typed(TraceSpanInMemory))
        raise MyException(exception_value)

    assert callable(mock_program)
    assert mock_program.__name__ == "mock_program"

    with pytest.raises(MyException):
        # Run with tracing
        with TracingInMemory():
            assert mock_program()

    # Checks
    assert len(spans_from_mock) == 1
    span = spans_from_mock[0]
    assert isinstance(span, TraceSpanInMemory)
    assert span.name == mock_program.__name__
    assert span.data[FUNCTION_DECORATOR_KEY]
    assert span.data[FUNCTION_DECORATOR_KEY][FUNCTION_NAME_KEY] == mock_program.__name__
    assert span.data[FUNCTION_DECORATOR_KEY][FUNCTION_KWARGS_KEY] == {}
    assert FUNCTION_RETURNED_KEY not in span.data[FUNCTION_DECORATOR_KEY]
    assert span.data[EXCEPTION_KEY][EXCEPTION_TYPE_KEY] == MyException
    assert span.data[EXCEPTION_KEY][EXCEPTION_VALUE_KEY].args[0] == exception_value
    assert span.data[EXCEPTION_KEY][EXCEPTION_TRACEBACK_KEY]
