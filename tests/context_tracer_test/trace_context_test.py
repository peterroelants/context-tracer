import logging

import pytest
from context_tracer.trace_context import (
    TraceError,
    TraceSpan,
    Tracing,
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
    assert isinstance(span, TraceSpan)
    assert get_current_span() is None
    with trace_span_context(span):
        assert get_current_span_safe() is span
    assert get_current_span() is None


def test_tracing() -> None:
    assert get_current_span() is None
    with TracingInMemory() as tracing:
        assert isinstance(tracing, Tracing)
        assert get_current_span_safe() is tracing.root_span
    assert get_current_span() is None


def test_get_current_span() -> None:
    class OtherType:
        pass

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
            get_current_span_safe_typed(OtherType)  # type: ignore
    assert get_current_span() is None
    with pytest.raises(TraceError):
        get_current_span_safe()
    with pytest.raises(TraceError):
        get_current_span_safe_typed(TraceSpanInMemory)
