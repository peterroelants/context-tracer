import contextlib
import logging
import multiprocessing as mp
import uuid
from collections.abc import Iterator
from queue import Queue

import pytest
from context_tracer.concurrency import (
    TraceProcess,
    TraceProcessPoolExecutor,
    TraceThread,
    TraceThreadPoolExecutor,
)
from context_tracer.trace import trace
from context_tracer.trace_context import (
    TraceSpan,
    get_current_span,
    get_current_span_safe,
    trace_span_context,
)
from context_tracer.trace_implementations.trace_basic import (
    TraceSpanInMemory,
)

logger = logging.getLogger(__name__)


# Fixtures and helpers #############################################
def put_current_span_on_queue(q) -> None:
    """
    Testing function to put current span on queue.

    Needs to be defined in global scope to be picklable for multiprocessing.
    """
    span = get_current_span()
    logger.info(f"put_current_span_on_queue {span=}")
    q.put(span)


@trace
def put_span_on_queue(q) -> None:
    """
    Testing function to put current span on queue.

    Needs to be defined in global scope to be picklable for multiprocessing.
    """
    span = get_current_span()
    logger.info(f"put_span_on_queue {span=}")
    q.put(span)


def return_span_id_and_name() -> tuple[bytes, str]:
    """
    Testing function to return span id and name.

    Needs to be defined in global scope to be picklable for multiprocessing.
    """
    span = get_current_span_safe()
    return span.id, span.name


@trace
def process_in_process_func(q) -> None:
    """
    Testing function to put current span on queue.

    Needs to be defined in global scope to be picklable for multiprocessing.
    """
    q.put(get_current_span())
    proc = TraceProcess(target=put_span_on_queue, args=(q,))
    proc.start()
    proc.join()


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
    assert queue.empty()
    assert isinstance(thread_span, TraceSpan)
    assert thread_span.name == span_name
    assert thread_span.id == span.id
    assert thread_span is span


def test_CtxThread_new_trace() -> None:
    queue: Queue[TraceSpan] = Queue()
    span_name = f"Span_{str(uuid.uuid4())}"
    root_span = TraceSpanInMemory(
        name=span_name,
        parent=None,
        data=None,
    )
    assert get_current_span() is None
    with trace_span_context(root_span):
        assert get_current_span() is root_span
        thread = TraceThread(target=put_span_on_queue, args=(queue,))
        thread.start()
        thread.join()
        assert get_current_span() is root_span
    thread_span = queue.get()
    assert queue.empty()
    assert isinstance(thread_span, TraceSpan)
    assert isinstance(thread_span, TraceSpanInMemory)
    assert thread_span.name == put_span_on_queue.__name__
    assert thread_span.parent is not None
    assert thread_span.parent.id == root_span.id


@pytest.mark.parametrize(
    "mp_start_method",
    ["fork", "spawn"],
)
def test_CtxProcess_context_propagation(mp_start_method: str) -> None:
    with multiprocess_start_method(mp_start_method):
        ctx = mp.get_context()
        assert ctx.get_start_method() == mp_start_method
        queue = ctx.Manager().Queue()
        span_name = f"Span_{str(uuid.uuid4())}"
        span = TraceSpanInMemory(
            name=span_name,
            parent=None,
            data=None,
        )
        assert get_current_span() is None
        with trace_span_context(span):
            assert get_current_span() is span
            proc = TraceProcess(target=put_current_span_on_queue, args=(queue,))
            proc.start()
            proc.join()
            assert get_current_span() is span
        proc_span = queue.get()
        assert queue.empty()
        assert isinstance(proc_span, TraceSpan)
        assert proc_span.name == span_name
        assert proc_span.id == span.id


@pytest.mark.parametrize(
    "mp_start_method",
    ["fork", "spawn"],
)
def test_CtxProcess_new_trace(mp_start_method: str) -> None:
    with multiprocess_start_method(mp_start_method):
        ctx = mp.get_context()
        queue = ctx.Manager().Queue()
        span_name = f"Span_{str(uuid.uuid4())}"
        root_span = TraceSpanInMemory(
            name=span_name,
            parent=None,
            data=None,
        )
        assert get_current_span() is None
        with trace_span_context(root_span):
            assert get_current_span() is root_span
            thread = TraceProcess(target=put_span_on_queue, args=(queue,))
            thread.start()
            thread.join()
            assert get_current_span() is root_span
        thread_span = queue.get()
        assert queue.empty()
        assert isinstance(thread_span, TraceSpan)
        assert isinstance(thread_span, TraceSpanInMemory)
        assert thread_span.name == put_span_on_queue.__name__
        assert thread_span.parent is not None
        assert thread_span.parent.id == root_span.id


@pytest.mark.parametrize(
    "mp_start_method",
    ["fork", "spawn"],
)
def test_CtxProcess_process_in_process_func(mp_start_method: str) -> None:
    with multiprocess_start_method(mp_start_method):
        ctx = mp.get_context()
        queue = ctx.Manager().Queue()
        span_name = f"Span_{str(uuid.uuid4())}"
        root_span = TraceSpanInMemory(
            name=span_name,
            parent=None,
            data=None,
        )
        assert get_current_span() is None
        with trace_span_context(root_span):
            assert get_current_span() is root_span
            thread = TraceProcess(target=process_in_process_func, args=(queue,))
            thread.start()
            thread.join()
            assert get_current_span() is root_span
        thread_span_1 = queue.get()
        thread_span_2 = queue.get()
        assert queue.empty()
        assert isinstance(thread_span_1, TraceSpan)
        assert isinstance(thread_span_2, TraceSpan)
        names = {thread_span_1.name, thread_span_2.name}
        assert names == {put_span_on_queue.__name__, process_in_process_func.__name__}


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
        with TraceThreadPoolExecutor(max_workers=1) as executor:
            span_id, span_name = executor.submit(return_span_id_and_name).result()
        assert get_current_span() is span
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
        with TraceProcessPoolExecutor(max_workers=1) as executor:
            span_id, span_name = executor.submit(return_span_id_and_name).result()
        assert get_current_span() is span
    assert span_name == span_name
    assert span_id == span.id
