import functools
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Optional

from .datamodel import TraceNode

TRACE_CONTEXT = ContextVar[TraceNode]("TRACE_CONTEXT", default=TraceNode(name="root"))

LOG_DIR = Path("./logs")


@contextmanager
def update_context(name: str, metadata: Optional[Any] = None) -> Iterator[TraceNode]:
    parent, child = TRACE_CONTEXT.get().update(name, metadata=metadata)
    TRACE_CONTEXT.set(parent)
    orig_token = TRACE_CONTEXT.set(child)
    try:
        yield child
    finally:
        TRACE_CONTEXT.reset(orig_token)


def log_trace(msg: str) -> None:
    with update_context(name="log", metadata=msg) as logged_trace:
        root_trace = logged_trace.get_root()
        root_time = datetime.fromtimestamp(root_trace.timestamp_init_utc).strftime(
            "%Y%m%d_%H%M%S"
        )
        root_dir_name = f"{root_trace.name}_{root_time}_{root_trace.id}"
        log_dir = LOG_DIR / root_dir_name
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / f"{logged_trace.id}.log.json"
        with log_file.open("w") as f:
            f.write(logged_trace.json(exclude_none=True))


def trace(func: Callable):
    @functools.wraps(func)
    def trace_decorator(*args, **kwargs):
        # Do something before
        with update_context(name=func.__name__):
            value = func(*args, **kwargs)
            # Do something after
            return value

    return trace_decorator
