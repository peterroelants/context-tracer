import contextlib
import functools
import multiprocessing
import threading
from collections.abc import Callable, Iterator
from concurrent.futures import Future, ProcessPoolExecutor, ThreadPoolExecutor
from multiprocessing import Process
from threading import Thread
from typing import ParamSpec, TypeVar

from context_tracer.trace_context import TraceSpan, get_current_span, trace_span_context

P = ParamSpec("P")
R = TypeVar("R")


@contextlib.contextmanager
def patch_concurrency() -> Iterator[None]:
    """
    Patch concurrency modules to use TraceThread, TraceProcess, TraceThreadPoolExecutor, and TraceProcessPoolExecutor.
    """
    with patch_threading(), patch_multiprocessing():
        yield


@contextlib.contextmanager
def patch_threading() -> Iterator[None]:
    """
    Patch threading module to use TraceThread.
    """
    orig_thread = threading.Thread
    try:
        threading.Thread = TraceThread  # type: ignore
        yield
    finally:
        threading.Thread = orig_thread  # type: ignore


@contextlib.contextmanager
def patch_multiprocessing() -> Iterator[None]:
    """
    Patch multiprocessing module to use TraceProcess.
    """
    orig_process = multiprocessing.Process
    try:
        multiprocessing.Process = TraceProcess  # type: ignore
        yield
    finally:
        multiprocessing.Process = orig_process  # type: ignore


class TraceThread(Thread):
    """
    Thread with trace context propagation.
    """

    _parent_span: TraceSpan | None = None

    def start(self) -> None:
        """
        Start thread with context propagation.
        """
        self._parent_span = get_current_span()
        super().start()

    def run(self) -> None:
        """
        Run target with context propagation.

        This is called in the child process.
        """
        if self._parent_span is None:
            return super().run()
        with patch_concurrency():
            with trace_span_context(self._parent_span):
                super().run()


class TraceProcess(Process):
    """
    Process with context propagation.
    """

    _parent_span: TraceSpan | None = None

    def start(self) -> None:
        """
        Start process with context propagation.

        This is called in the parent process.
        """
        self._parent_span = get_current_span()
        super().start()

    def run(self) -> None:
        """
        Run target with context propagation.

        This is called in the child process.
        """
        if self._parent_span is None:
            return super().run()
        with patch_concurrency():
            with trace_span_context(self._parent_span):
                super().run()


def run_in_span(
    span: TraceSpan, target: Callable[P, R], /, *args: P.args, **kwargs: P.kwargs
) -> R:
    """
    Run target in span context.

    Cannot be a function decorator because the embedded wrapper function would not be picklable for multiprocessing.
    Needs to be applied via functools.partial.

    TODO: Move somewhere else?
    """
    with patch_concurrency():
        with trace_span_context(span):
            return target(*args, **kwargs)


class TraceThreadPoolExecutor(ThreadPoolExecutor):
    """
    ThreadPoolExecutor with context propagation.
    """

    def submit(
        self, fn: Callable[P, R], /, *args: P.args, **kwargs: P.kwargs
    ) -> Future[R]:
        """
        Submit target in context.
        """
        parent_span = get_current_span()
        if parent_span is not None:
            fn = functools.partial(run_in_span, parent_span, fn)
            # fn = run_in_span(func=fn, span=parent_span)
        return super().submit(fn, *args, **kwargs)


class TraceProcessPoolExecutor(ProcessPoolExecutor):
    """
    ThreadPoolExecutor with context propagation.
    """

    def submit(
        self, fn: Callable[P, R], /, *args: P.args, **kwargs: P.kwargs
    ) -> Future[R]:
        """
        Submit target in context.
        """
        parent_span = get_current_span()
        if parent_span is not None:
            fn = functools.partial(run_in_span, parent_span, fn)
        return super().submit(fn, *args, **kwargs)
