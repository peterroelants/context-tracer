import tempfile
from collections.abc import Iterator
from pathlib import Path

import pytest
from context_tracer.trace import get_current_span_safe, log_with_trace, trace
from context_tracer.trace_implementations.trace_sqlite import (
    TraceSpanSqlite,
    TraceTreeSqlite,
    TracingSqlite,
)
from context_tracer.trace_types import TraceSpan, TraceTree, Tracing


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
        assert isinstance(tracing.root_span, TraceSpanSqlite)
    assert tmp_db_path.exists()
    assert tracing.tree is not None
    assert isinstance(tracing.tree, TraceTree)


def test_trace_sqlite_initial(tmp_db_path: Path) -> None:
    tracing = TracingSqlite(db_path=tmp_db_path)
    assert len(tracing.span_db.get_root_uids()) == 1


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

    with TracingSqlite(db_path=tmp_db_path) as tracing:
        program()
    assert tmp_db_path.exists()
    assert len(tracing.span_db.get_root_uids()) == 1
    assert tracing.span_db.get_root_uids()[0] == tracing.root_span.uid
    tree_root = tracing.tree
    assert isinstance(tree_root, TraceTree)
    assert isinstance(tree_root, TraceTreeSqlite)
    assert tree_root.name == "root"
    assert len(tree_root.children) == 1

    def get_leafs(tree_root: TraceTreeSqlite) -> list[TraceTreeSqlite]:
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

    def found_c(tree_root: TraceTreeSqlite) -> bool:
        if tree_root.name == "C":
            return True
        for child in tree_root.children:
            if found_c(child):
                return True
        return False

    assert found_c(tree_root)


def test_update_data(tmp_db_path: Path) -> None:
    @trace(name="test")
    def get_trace_update_data():
        span = get_current_span_safe()
        assert span.name == "test"
        span.update_data(test_var="data_1", a_specific=1, common=dict(a=1, b=2))
        span.update_data(test_var="data_2", b_specific=22, common=dict(b=20, c=30))

    with TracingSqlite(db_path=tmp_db_path) as tracing:
        get_trace_update_data()

    test_span_id = tracing.span_db.get_span_ids_from_name(name="test")[0]
    test_span = TraceSpanSqlite(span_db=tracing.span_db, span_uid=test_span_id)
    assert test_span.data["test_var"] == "data_2"
    assert test_span.data["a_specific"] == 1
    assert test_span.data["b_specific"] == 22
    assert test_span.data["common"] == dict(a=1, b=20, c=30)
