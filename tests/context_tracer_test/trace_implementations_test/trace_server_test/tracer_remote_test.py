from context_tracer.trace import get_current_span_safe, log_with_trace, trace
from context_tracer.trace_context import TraceSpan, TraceTree, Tracing
from context_tracer.trace_implementations.trace_server.trace_server import (
    SpanClientAPI,
)
from context_tracer.trace_implementations.trace_server.tracer_remote import (
    SpanTreeRemote,
    TraceSpanRemote,
    TracingRemote,
)


def test_trace_remote(tmp_api_client: SpanClientAPI) -> None:
    with TracingRemote(api_client=tmp_api_client) as tracing:
        pass  # Just root context
        assert isinstance(tracing, Tracing)
        assert tracing.root_span is not None
        assert isinstance(tracing.root_span, TraceSpan)
        assert isinstance(tracing.root_span, TraceSpanRemote)
    assert tracing.tree is not None
    assert isinstance(tracing.tree, TraceTree)
    assert isinstance(tracing.tree, SpanTreeRemote)


def test_trace_remote_program(tmp_api_client: SpanClientAPI) -> None:
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

    with TracingRemote(api_client=tmp_api_client):
        program()
    # Ignore previous and make sure reading from an existing server works
    read_tracing = TracingRemote(api_client=tmp_api_client)
    tree_root = read_tracing.tree
    assert isinstance(tree_root, SpanTreeRemote)
    assert isinstance(tree_root, TraceTree)
    assert tree_root.name == "root"
    assert len(tree_root.children) == 1

    def get_leafs(tree_root: SpanTreeRemote) -> list[SpanTreeRemote]:
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

    def found_c(tree_root: SpanTreeRemote) -> bool:
        if tree_root.name == "C":
            return True
        for child in tree_root.children:
            if found_c(child):
                return True
        return False

    assert found_c(tree_root)


def test_update_data(tmp_api_client: SpanClientAPI) -> None:
    @trace(name="test")
    def get_trace_update_data():
        span = get_current_span_safe()
        assert span.name == "test"
        span.update_data(test_var="data_1", a_specific=1, common=dict(a=1, b=2))
        span.update_data(test_var="data_2", b_specific=22, common=dict(b=20, c=30))

    with TracingRemote(api_client=tmp_api_client) as tracing:
        get_trace_update_data()

    test_span = tracing.tree.children[0]
    assert test_span.name == "test"
    assert test_span.data["test_var"] == "data_2"
    assert test_span.data["a_specific"] == 1
    assert test_span.data["b_specific"] == 22
    assert test_span.data["common"] == dict(a=1, b=20, c=30)
