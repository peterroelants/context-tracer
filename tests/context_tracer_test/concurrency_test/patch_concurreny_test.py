import contextlib
import logging
import multiprocessing as mp
import uuid
from collections.abc import Iterator
from queue import Queue

import pytest
from context_tracer.concurrency.trace_propagation import patch_concurrency
from context_tracer.trace import trace
from context_tracer.trace_implementations.trace_basic import (
    TraceSpanInMemory,
)
from context_tracer.trace_types import (
    TraceSpan,
    get_current_span,
    get_current_span_safe,
    trace_span_context,
)

logger = logging.getLogger(__name__)


@pytest.fixture
def patch_concurrency_fixture() -> Iterator[None]:
    with patch_concurrency():
        yield


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
    return span.uid, span.name


@trace
def process_in_process_func(q) -> None:
    """
    Testing function to put current span on queue.

    Needs to be defined in global scope to be picklable for multiprocessing.
    """
    import multiprocessing

    q.put(get_current_span())
    proc = multiprocessing.Process(target=put_span_on_queue, args=(q,))
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
def test_thread_context_propagation(patch_concurrency_fixture) -> None:
    from threading import Thread

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
        thread = Thread(target=put_current_span_on_queue, args=(queue,))
        thread.start()
        thread.join()
        assert get_current_span() is span
    thread_span = queue.get()
    assert isinstance(thread_span, TraceSpan)
    assert thread_span.name == span_name
    assert thread_span.uid == span.uid
    assert thread_span is span


@pytest.mark.parametrize(
    "mp_start_method",
    ["fork", "spawn"],
)
def test_process_context_propagation(
    mp_start_method: str, patch_concurrency_fixture
) -> None:
    from multiprocessing import Process

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
            proc = Process(target=put_current_span_on_queue, args=(queue,))
            proc.start()
            proc.join()
            assert get_current_span() is span
        proc_span = queue.get()
        assert queue.empty()
        assert isinstance(proc_span, TraceSpan)
        assert proc_span.name == span_name
        assert proc_span.uid == span.uid


@pytest.mark.parametrize(
    "mp_start_method",
    ["fork", "spawn"],
)
def test_process_new_trace(mp_start_method: str, patch_concurrency_fixture) -> None:
    with multiprocess_start_method(mp_start_method):
        from multiprocessing import Process

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
            thread = Process(target=put_span_on_queue, args=(queue,))
            thread.start()
            thread.join()
            assert get_current_span() is root_span
        thread_span = queue.get()
        assert queue.empty()
        assert isinstance(thread_span, TraceSpan)
        assert isinstance(thread_span, TraceSpanInMemory)
        assert thread_span.name == put_span_on_queue.__name__
        assert thread_span.parent is not None
        assert thread_span.parent.uid == root_span.uid


@pytest.mark.parametrize(
    "mp_start_method",
    ["fork", "spawn"],
)
def test_process_process_in_process_func(
    mp_start_method: str, patch_concurrency_fixture
) -> None:
    with multiprocess_start_method(mp_start_method):
        from multiprocessing import Process

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
            thread = Process(target=process_in_process_func, args=(queue,))
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


def test_threadpoolexecutor_context_propagation(patch_concurrency_fixture) -> None:
    from concurrent.futures import ThreadPoolExecutor

    span_name = f"Span_{str(uuid.uuid4())}"
    span = TraceSpanInMemory(
        name=span_name,
        parent=None,
        data=None,
    )
    assert get_current_span() is None
    with trace_span_context(span):
        assert get_current_span() is span
        with ThreadPoolExecutor(max_workers=1) as executor:
            span_id, span_name = executor.submit(return_span_id_and_name).result()
        assert get_current_span() is span
    assert span_name == span_name
    assert span_id == span.uid


def test_processpoolexecutor_context_propagation(patch_concurrency_fixture) -> None:
    from concurrent.futures import ProcessPoolExecutor

    span_name = f"Span_{str(uuid.uuid4())}"
    span = TraceSpanInMemory(
        name=span_name,
        parent=None,
        data=None,
    )
    assert get_current_span() is None
    with trace_span_context(span):
        assert get_current_span() is span
        with ProcessPoolExecutor(max_workers=1) as executor:
            span_id, span_name = executor.submit(return_span_id_and_name).result()
        assert get_current_span() is span
    assert span_name == span_name
    assert span_id == span.uid
