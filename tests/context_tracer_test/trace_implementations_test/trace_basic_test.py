from context_tracer.trace import log_with_trace, trace
from context_tracer.trace_implementations.trace_basic import (
    TraceSpanInMemory,
    TracingInMemory,
)
from context_tracer.trace_types import TraceSpan, TraceTree, Tracing


def test_trace_simple() -> None:
    with TracingInMemory() as tracing:
        pass  # Just root context
        assert isinstance(tracing, Tracing)
        assert tracing.root_span is not None
        assert isinstance(tracing.root_span, TraceSpan)
    assert tracing.tree is not None
    assert isinstance(tracing.tree, TraceTree)


def test_trace_simple_program() -> None:
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

    with TracingInMemory() as tracing:
        program()
    tree_root = tracing.tree
    assert isinstance(tree_root, TraceTree)
    assert tree_root.name == "root"
    assert len(tree_root.children) == 1

    def get_leafs(tree_root: TraceSpanInMemory) -> list[TraceSpanInMemory]:
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

    def found_c(tree_root: TraceSpanInMemory) -> bool:
        if tree_root.name == "C":
            return True
        for child in tree_root.children:
            if found_c(child):
                return True
        return False

    assert found_c(tree_root)
