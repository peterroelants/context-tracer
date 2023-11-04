import tempfile
from collections.abc import Iterator
from pathlib import Path

import pytest
from context_tracer.trace import log_with_trace, trace
from context_tracer.trace_context import TraceSpan, TraceTree, Tracing
from context_tracer.trace_implementations.trace_json_log import (
    TraceTreeJsonLog,
    TracingJsonLog,
)


@pytest.fixture
def tmp_log_path() -> Iterator[Path]:
    with tempfile.TemporaryDirectory() as tmp_dir:
        logging_path = Path(tmp_dir) / "trace.log"
        yield logging_path


def test_trace_json_log(tmp_log_path: Path) -> None:
    with TracingJsonLog(logging_path=tmp_log_path) as tracing:
        assert tracing.root_span is not None
        assert isinstance(tracing.root_span, TraceSpan)
    assert tmp_log_path.exists()
    assert tracing.tree is not None
    assert isinstance(tracing.tree, TraceTree)


def test_trace_json_log_program(tmp_log_path: Path) -> None:
    def program():
        @trace
        def do_a():
            log_with_trace(name="A", test_var="Hello World From A!")

        @trace
        def do_d():
            log_with_trace(name="D", test_var="Hello World From D!")

        @trace
        def do_e():
            log_with_trace(name="E", test_var="Hello World From E!")

        @trace
        def do_b():
            do_a()
            do_d()

        @trace(name="C")
        def do_c():
            do_b()
            do_b()
            do_e()

        do_c()

    with TracingJsonLog(logging_path=tmp_log_path) as tracing:
        program()
    assert isinstance(tracing, Tracing)
    assert tmp_log_path.exists()
    tree_root = tracing.tree
    assert isinstance(tree_root, TraceTreeJsonLog)
    assert isinstance(tree_root, TraceTree)
    assert tree_root.name == "root"
    assert len(tree_root.children) == 1

    def get_leafs(tree_root: TraceTreeJsonLog) -> list[TraceTreeJsonLog]:
        if len(tree_root.children) == 0:
            return [tree_root]
        leafs = []
        for child in tree_root.children:
            leafs.extend(get_leafs(child))
        return leafs

    leafs = get_leafs(tree_root)
    assert len(leafs) == 5
    for leaf in leafs:
        assert leaf.name in {"A", "D", "E"}

    def found_c(tree_root: TraceTreeJsonLog) -> bool:
        if tree_root.name == "C":
            return True
        for child in tree_root.children:
            if found_c(child):
                return True
        return False

    assert found_c(tree_root)
