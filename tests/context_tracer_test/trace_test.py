import logging
import uuid

import pytest
from context_tracer.constants import (
    EXCEPTION_KEY,
    EXCEPTION_MESSAGE_KEY,
    EXCEPTION_STACKTRACE_KEY,
    EXCEPTION_TYPE_KEY,
    FUNCTION_DECORATOR_KEY,
    FUNCTION_KWARGS_KEY,
    FUNCTION_NAME_KEY,
    FUNCTION_RETURNED_KEY,
)
from context_tracer.trace import trace
from context_tracer.trace_implementations.trace_basic import (
    TraceSpanInMemory,
    TracingInMemory,
)
from context_tracer.trace_types import (
    TraceSpan,
    get_current_span,
    get_current_span_safe_typed,
)

logger = logging.getLogger(__name__)


def test_trace_context_manager() -> None:
    test_var_content = str(uuid.uuid4())
    # Run with tracing
    with TracingInMemory():
        with trace(test_var=test_var_content):
            current_span = get_current_span_safe_typed(TraceSpanInMemory)
            assert current_span is not None
            assert isinstance(current_span, TraceSpan)
            assert isinstance(current_span, TraceSpanInMemory)
            assert current_span.data["test_var"] == test_var_content
    assert get_current_span() is None


def test_trace_context_manager_no_tracing() -> None:
    test_var_content = str(uuid.uuid4())
    # Run with tracing
    with trace(test_var=test_var_content):
        assert get_current_span() is None
    assert get_current_span() is None


def test_get_current_span_no_trace() -> None:
    assert get_current_span() is None


def test_trace_function_decorator_no_arguments() -> None:
    spans_from_mock = []
    return_value = str(uuid.uuid4())

    @trace
    def mock_program() -> str:
        spans_from_mock.append(get_current_span_safe_typed(TraceSpanInMemory))
        return return_value

    # Check annotated function
    assert callable(mock_program)
    assert mock_program.__name__ == "mock_program"

    # Run with tracing
    with TracingInMemory():
        assert mock_program() == return_value

    # Get Span for testing
    assert len(spans_from_mock) == 1
    span = spans_from_mock[0]

    # Test Span
    assert isinstance(span, TraceSpan)
    assert isinstance(span, TraceSpanInMemory)
    assert span.name == mock_program.__name__
    assert span.data[FUNCTION_DECORATOR_KEY]
    assert span.data[FUNCTION_DECORATOR_KEY][FUNCTION_NAME_KEY] == mock_program.__name__
    assert span.data[FUNCTION_DECORATOR_KEY][FUNCTION_KWARGS_KEY] == {}
    assert span.data[FUNCTION_DECORATOR_KEY][FUNCTION_RETURNED_KEY] == return_value


def test_trace_function_decorator_arguments() -> None:
    span_from_mock = []
    test_var_content = str(uuid.uuid4())
    test_name = str(uuid.uuid4())
    return_value = str(uuid.uuid4())

    # Don't use method name as name
    @trace(name=test_name, test_var=test_var_content)
    def mock_program() -> str:
        span_from_mock.append(get_current_span_safe_typed(TraceSpanInMemory))
        return return_value

    # Check annotated function
    assert callable(mock_program)
    assert mock_program.__name__ == "mock_program"  # type: ignore

    # Run with tracing
    with TracingInMemory():
        assert mock_program() == return_value

    # Get Span for testing
    assert len(span_from_mock) == 1
    span = span_from_mock[0]

    # Test Span
    assert isinstance(span, TraceSpan)
    assert isinstance(span, TraceSpanInMemory)
    assert span.name == test_name
    assert span.data["test_var"] == test_var_content
    assert span.data[FUNCTION_DECORATOR_KEY]
    assert span.data[FUNCTION_DECORATOR_KEY][FUNCTION_NAME_KEY] == mock_program.__name__
    assert span.data[FUNCTION_DECORATOR_KEY][FUNCTION_KWARGS_KEY] == {}
    assert span.data[FUNCTION_DECORATOR_KEY][FUNCTION_RETURNED_KEY] == return_value


def test_trace_function_decorator_function_call_arguments() -> None:
    span_from_mock = []

    @trace
    def abcsum(a: int, b: int, c: int) -> int:
        span_from_mock.append(get_current_span_safe_typed(TraceSpanInMemory))
        return a + b + c

    # Check annotated function
    assert callable(abcsum)
    assert abcsum.__name__ == "abcsum"  # type: ignore

    # Run with tracing
    kwargs = {"a": 4, "b": 3, "c": 2}
    result_sum = sum(kwargs.values())
    with TracingInMemory():
        assert abcsum(kwargs["a"], kwargs["b"], c=kwargs["c"]) == result_sum

    # Get Span for testing
    assert len(span_from_mock) == 1
    span = span_from_mock[0]
    # Test Span
    assert isinstance(span, TraceSpan)
    assert isinstance(span, TraceSpanInMemory)
    assert span.name == abcsum.__name__
    assert span.data[FUNCTION_DECORATOR_KEY]
    assert span.data[FUNCTION_DECORATOR_KEY][FUNCTION_NAME_KEY] == abcsum.__name__
    assert span.data[FUNCTION_DECORATOR_KEY][FUNCTION_KWARGS_KEY] == kwargs
    assert span.data[FUNCTION_DECORATOR_KEY][FUNCTION_RETURNED_KEY] == result_sum


def test_trace_function_decorator_exception() -> None:
    spans_from_mock = []
    exception_value = str(uuid.uuid4())

    class MyException(Exception):
        pass

    @trace
    def mock_program() -> str:
        spans_from_mock.append(get_current_span_safe_typed(TraceSpanInMemory))
        raise MyException(exception_value)

    # Check annotated function
    assert callable(mock_program)
    assert mock_program.__name__ == "mock_program"

    # Run with tracing
    with pytest.raises(MyException):
        with TracingInMemory():
            assert mock_program()

    # Get Span for testing
    assert len(spans_from_mock) == 1
    span = spans_from_mock[0]
    # Test Span
    assert isinstance(span, TraceSpan)
    assert isinstance(span, TraceSpanInMemory)
    assert span.name == mock_program.__name__
    assert span.data[FUNCTION_DECORATOR_KEY]
    assert span.data[FUNCTION_DECORATOR_KEY][FUNCTION_NAME_KEY] == mock_program.__name__
    assert span.data[FUNCTION_DECORATOR_KEY][FUNCTION_KWARGS_KEY] == {}
    assert FUNCTION_RETURNED_KEY not in span.data[FUNCTION_DECORATOR_KEY]
    assert span.data[EXCEPTION_KEY][EXCEPTION_TYPE_KEY] == MyException.__name__
    assert span.data[EXCEPTION_KEY][EXCEPTION_MESSAGE_KEY] == exception_value
    assert span.data[EXCEPTION_KEY][EXCEPTION_STACKTRACE_KEY]
