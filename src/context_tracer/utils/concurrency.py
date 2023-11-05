import functools
from collections.abc import Callable
from concurrent.futures import Future, ProcessPoolExecutor, ThreadPoolExecutor
from multiprocessing import Process
from threading import Thread
from typing import ParamSpec, TypeVar

from context_tracer.trace_context import TraceSpan, get_current_span, trace_span_context

P = ParamSpec("P")
R = TypeVar("R")


# TODO: Rename to TraceThread, TraceProcess, ?


class CtxThread(Thread):
    """
    Thread with context propagation.
    """

    _parent_span: TraceSpan | None = None

    def start(self):
        """
        Start thread with context propagation.
        """
        self._parent_span = get_current_span()
        super().start()

    def run(self):
        """
        Run target with context propagation.

        A new span is created for the process itself.

        This is called in the child process.
        """
        with trace_span_context(self._parent_span):
            super().run()


class CtxProcess(Process):
    """
    Process with context propagation.
    """

    _parent_span: TraceSpan | None = None

    def start(self):
        """
        Start process with context propagation.

        This is called in the parent process.
        """
        self._parent_span = get_current_span()
        super().start()

    def run(self):
        """
        Run target with context propagation.

        A new span is created for the process itself.

        This is called in the child process.
        """
        # TODO
        # with trace(name="Process"):
        #     super().run()
        # child_span = None
        # if self._parent_span is not None:
        #     target_name = get_func_name(self._target)
        #     child_span = self._parent_span.new_child(
        #         name=f"Process {target_name}",
        #         target=target_name,
        #     )
        with trace_span_context(self._parent_span):
            super().run()


def run_in_span(
    span: TraceSpan, target: Callable[P, R], /, *args: P.args, **kwargs: P.kwargs
) -> R:
    """
    Run target in span context.

    TODO: Move somewhere else?
    """
    with trace_span_context(span):
        return target(*args, **kwargs)


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
        parent_span = get_current_span()
        func: Callable[P, R]
        if parent_span is not None:
            func = functools.partial(run_in_span, parent_span, fn)
        else:
            func = fn
        return super().submit(func, *args, **kwargs)


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
        parent_span = get_current_span()
        if parent_span is not None:
            fn = functools.partial(run_in_span, parent_span, fn)
        return super().submit(fn, *args, **kwargs)


# class CtxProcessPoolExecutor(ProcessPoolExecutor):
#     """
#     ProcessPoolExecutor with context propagation.
#     """

#     _ctx: Context

#     def __init__(self, *args, **kwargs):
#         self._ctx = copy_context()
#         super().__init__(*args, **kwargs)

#     def submit(
#         self, fn: Callable[P, R], /, *args: P.args, **kwargs: P.kwargs
#     ) -> Future[R]:
#         """
#         Submit target in context.
#         """
#         fn_with_ctx: Callable = functools.partial(self._ctx.run, fn)
#         return super().submit(fn_with_ctx, *args, **kwargs)
