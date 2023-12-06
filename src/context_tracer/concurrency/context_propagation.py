import contextlib
import contextvars
import functools
import multiprocessing
import threading
from collections.abc import Callable, Iterator
from concurrent.futures import Future, ProcessPoolExecutor, ThreadPoolExecutor
from multiprocessing import Process
from threading import Thread
from typing import ParamSpec, TypeVar

# TODO: Test this

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
        threading.Thread = CtxThread  # type: ignore
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
        multiprocessing.Process = CtxProcess  # type: ignore
        yield
    finally:
        multiprocessing.Process = orig_process  # type: ignore


class CtxThread(Thread):
    """
    Thread with trace context propagation.
    """

    def run(self) -> None:
        """
        Run target with context propagation.

        This is called in the child process.
        """
        ctx = contextvars.copy_context()
        with patch_concurrency():
            ctx.run(super().run)


class CtxProcess(Process):
    """
    Process with context propagation.
    """

    def run(self) -> None:
        """
        Run target with context propagation.

        This is called in the child process.
        """
        ctx = contextvars.copy_context()
        with patch_concurrency():
            ctx.run(super().run)


class CtxThreadPoolExecutor(ThreadPoolExecutor):
    """
    ThreadPoolExecutor with context propagation.
    """

    def submit(
        self, fn: Callable[P, R], /, *args: P.args, **kwargs: P.kwargs
    ) -> Future[R]:
        """
        Submit target in context.
        """
        fn = functools.partial(run_in_context, fn)
        return super().submit(fn, *args, **kwargs)


class CtxProcessPoolExecutor(ProcessPoolExecutor):
    """
    ThreadPoolExecutor with context propagation.
    """

    def submit(
        self, fn: Callable[P, R], /, *args: P.args, **kwargs: P.kwargs
    ) -> Future[R]:
        """
        Submit target in context.
        """
        fn = functools.partial(run_in_context, fn)
        return super().submit(fn, *args, **kwargs)


def run_in_context(target: Callable[P, R], /, *args: P.args, **kwargs: P.kwargs) -> R:
    """
    Run target in span context.

    Cannot be a function decorator because the embedded wrapper function would not be picklable for multiprocessing.
    Needs to be applied via functools.partial.
    """
    ctx = contextvars.copy_context()
    with patch_concurrency():
        return ctx.run(target, *args, **kwargs)
