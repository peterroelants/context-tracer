import contextlib
import logging
import multiprocessing as mp
import uuid
from collections.abc import Iterator
from queue import Queue

import pytest
from context_tracer.trace_context import (
    TraceSpan,
    get_current_span,
    get_current_span_safe,
    trace_span_context,
)
from context_tracer.trace_implementations.trace_basic import (
    TraceSpanInMemory,
)
from context_tracer.utils.concurrency import (
    TraceProcess,
    TraceProcessPoolExecutor,
    TraceThread,
    TraceThreadPoolExecutor,
)

logger = logging.getLogger(__name__)


# Fixtures and helpers #############################################
def put_current_span_on_queue(q) -> None:
    """
    Testing function to put current span on queue.

    Needs to be defined in global scope to be picklable for multiprocessing.
    """
    q.put(get_current_span())


def return_span_id_and_name() -> tuple[bytes, str]:
    """
    Testing function to return span id and name.

    Needs to be defined in global scope to be picklable for multiprocessing.
    """
    span = get_current_span_safe()
    return span.id, span.name


@contextlib.contextmanager
def multiprocess_start_method(start_method: str) -> Iterator[None]:
    prev_start_method = mp.get_start_method()
    mp.set_start_method(start_method, force=True)
    try:
        yield
    finally:
        mp.set_start_method(prev_start_method, force=True)


# Test Context Propagation #########################################
def test_CtxThread_context_propagation() -> None:
    queue: Queue[TraceSpan] = Queue()

    span_name = f"Span_{str(uuid.uuid4())}"
    span = TraceSpanInMemory(
        name=span_name,
        parent=None,
        data=None,
    )
    assert get_current_span() is None
    with trace_span_context(span):
        assert get_current_span() is span
        # Fetch span in thread
        thread = TraceThread(target=put_current_span_on_queue, args=(queue,))
        thread.start()
        thread.join()
        assert get_current_span() is span

    # Check span in thread
    thread_span = queue.get()
    assert isinstance(thread_span, TraceSpan)
    assert thread_span.name == span_name
    assert thread_span.id == span.id
    assert thread_span is span


@pytest.mark.parametrize(
    "mp_start_method",
    ["spawn", "fork"],
)
def test_CtxProcess_context_propagation(mp_start_method: str) -> None:
    with multiprocess_start_method(mp_start_method):
        ctx = mp.get_context()
        assert ctx.get_start_method() == mp_start_method
        queue: mp.Queue[TraceSpan] = ctx.Queue()
        span_name = f"Span_{str(uuid.uuid4())}"
        span = TraceSpanInMemory(
            name=span_name,
            parent=None,
            data=None,
        )
        assert get_current_span() is None
        with trace_span_context(span):
            assert get_current_span() is span
            # Fetch span in thread
            proc = TraceProcess(target=put_current_span_on_queue, args=(queue,))
            proc.start()
            proc.join()
            assert get_current_span() is span

        # Check span in thread
        proc_span = queue.get()
        assert isinstance(proc_span, TraceSpan)
        assert proc_span.name == span_name
        assert proc_span.id == span.id


def test_CtxThreadPoolExecutor_context_propagation() -> None:
    span_name = f"Span_{str(uuid.uuid4())}"
    span = TraceSpanInMemory(
        name=span_name,
        parent=None,
        data=None,
    )
    assert get_current_span() is None
    with trace_span_context(span):
        assert get_current_span() is span
        # Fetch span in thread
        with TraceThreadPoolExecutor(max_workers=1) as executor:
            span_id, span_name = executor.submit(return_span_id_and_name).result()
        assert get_current_span() is span

    # Checks
    assert span_name == span_name
    assert span_id == span.id


def test_CtxProcessPoolExecutor_context_propagation() -> None:
    span_name = f"Span_{str(uuid.uuid4())}"
    span = TraceSpanInMemory(
        name=span_name,
        parent=None,
        data=None,
    )
    assert get_current_span() is None
    with trace_span_context(span):
        assert get_current_span() is span
        # Fetch span in thread
        with TraceProcessPoolExecutor(max_workers=1) as executor:
            span_id, span_name = executor.submit(return_span_id_and_name).result()
        assert get_current_span() is span

    # Checks
    assert span_name == span_name
    assert span_id == span.id
