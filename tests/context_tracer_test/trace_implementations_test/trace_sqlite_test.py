import tempfile
from collections.abc import Iterator
from pathlib import Path

import pytest
from context_tracer.trace import log_with_trace, trace
from context_tracer.trace_context import TraceSpan, TraceTree, Tracing
from context_tracer.trace_implementations.trace_sqlite import (
    TraceSpanSqlite,
    TracingSqlite,
)
from context_tracer.trace_implementations.trace_sqlite.trace_sqlite import get_root_id


@pytest.fixture
def tmp_db_path() -> Iterator[Path]:
    with tempfile.TemporaryDirectory() as tmp_dir:
        db_path = Path(tmp_dir) / "trace.db"
        yield db_path


def test_trace_sqlite(tmp_db_path: Path) -> None:
    with TracingSqlite(db_path=tmp_db_path) as tracing:
        pass  # Just root context
        assert isinstance(tracing, Tracing)
        assert tracing.root_span is not None
        assert isinstance(tracing.root_span, TraceSpan)
    assert tmp_db_path.exists()
    assert tracing.tree is not None
    assert isinstance(tracing.tree, TraceTree)


def test_trace_sqlite_no_span(tmp_db_path: Path) -> None:
    tracing = TracingSqlite(db_path=tmp_db_path)
    with tracing.db_conn() as db_conn:
        assert get_root_id(db_conn) is None


def test_trace_sqlite_program(tmp_db_path: Path) -> None:
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

    with TracingSqlite(db_path=tmp_db_path):
        program()
    assert tmp_db_path.exists()
    # Ignore previous and make sure reading from an existing db works
    read_tracing = TracingSqlite(db_path=tmp_db_path)
    tree_root = read_tracing.tree
    assert isinstance(tree_root, TraceSpanSqlite)
    assert isinstance(tree_root, TraceTree)
    assert tree_root.name == "root"
    assert len(tree_root.children) == 1

    def get_leafs(tree_root: TraceSpanSqlite) -> list[TraceSpanSqlite]:
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

    def found_c(tree_root: TraceSpanSqlite) -> bool:
        if tree_root.name == "C":
            return True
        for child in tree_root.children:
            if found_c(child):
                return True
        return False

    assert found_c(tree_root)
