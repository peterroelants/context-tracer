import contextlib
import hashlib
import logging
import multiprocessing as mp
from collections.abc import Iterator
from pathlib import Path

import pytest
from context_tracer.concurrency import (
    TraceProcess,
    TraceProcessPoolExecutor,
    TraceThread,
)
from context_tracer.trace import trace
from context_tracer.trace_implementations.trace_server.tracer_remote import (
    TraceSpanRemote,
    TracingRemote,
)
from context_tracer.trace_types import TraceTree, get_current_span_safe_typed

log = logging.getLogger(__name__)


@contextlib.contextmanager
def multiprocess_start_method(start_method: str) -> Iterator[None]:
    log.info(f"multiprocess_start_method(start_method={start_method})")
    prev_start_method = mp.get_start_method()
    mp.set_start_method(start_method, force=True)
    try:
        yield
    finally:
        mp.set_start_method(prev_start_method, force=True)


def id_hash(id: bytes) -> str:
    return hashlib.md5(id).hexdigest()


@trace
def remote_doubling_function(test_param: int):
    """
    This function needs to be defined in global scope to be picklable for multiprocessing.
    """
    span = get_current_span_safe_typed(TraceSpanRemote)
    span.update_data(id_md5=id_hash(span.uid))
    return test_param * 2


def test_trace_thread(tmp_db_path: Path) -> None:
    with TracingRemote(db_path=tmp_db_path) as tracing:
        proc = TraceThread(target=remote_doubling_function, args=(1,))
        proc.start()
        proc.join()

    tree_root = tracing.tree
    assert isinstance(tree_root, TraceTree)
    root_children = tree_root.children
    assert len(root_children) == 1
    child = root_children[0]
    assert child.name == remote_doubling_function.__name__


@pytest.mark.parametrize(
    "mp_start_method",
    ["fork", "spawn"],
)
def test_trace_process(tmp_db_path: Path, mp_start_method: str) -> None:
    log.info(
        f"test_trace_process(tmp_db_path={tmp_db_path}, mp_start_method={mp_start_method})"
    )
    with multiprocess_start_method(mp_start_method):
        with TracingRemote(db_path=tmp_db_path) as tracing:
            proc = TraceProcess(
                target=remote_doubling_function,
                args=(1,),
            )
            proc.start()
            proc.join()

        tree_root = tracing.tree
        assert isinstance(tree_root, TraceTree)
        root_children = tree_root.children
        assert len(root_children) == 1
        child = root_children[0]
        assert child.name == remote_doubling_function.__name__


def test_trace_process_pool(tmp_db_path: Path) -> None:
    nb_children = 3
    with TracingRemote(db_path=tmp_db_path) as tracing:
        with TraceProcessPoolExecutor(max_workers=2) as executor:
            for i in range(nb_children):
                executor.submit(remote_doubling_function, i)

    tree_root = tracing.tree
    assert isinstance(tree_root, TraceTree)
    root_children = tree_root.children
    assert len(root_children) == nb_children
    for child in root_children:
        assert child.name == remote_doubling_function.__name__
