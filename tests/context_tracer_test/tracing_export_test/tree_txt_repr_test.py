import re
import tempfile
from pathlib import Path
from typing import Iterator

import pytest
from context_tracer.trace import log_with_trace, trace
from context_tracer.trace_context import TraceTree
from context_tracer.trace_implementations.trace_basic import (
    TracingInMemory,
)
from context_tracer.tracing_export.tree_txt_repr import write_tree


@pytest.fixture
def tmp_export_path() -> Iterator[Path]:
    with tempfile.TemporaryDirectory() as tmp_dir:
        export_path = Path(tmp_dir) / "trace.txt"
        yield export_path


def get_example_trace_tree_simple_program() -> TraceTree:
    def program():
        @trace
        def do_a():
            log_with_trace(name="A", test_var="Hello World From A!")

        @trace
        def do_d():
            log_with_trace(name="D", test_var="Hello World From D!")

        @trace
        def do_e():
            log_with_trace(name="E", test_var="Hello World From E.\nWith a new line!")

        @trace
        def do_b():
            do_a()
            do_d()

        @trace
        def do_c():
            do_b()
            do_b()
            do_e()

        do_c()

    with TracingInMemory() as tracing:
        program()
    return tracing.tree


def test_write_tree(tmp_export_path: Path) -> None:
    tree: TraceTree = get_example_trace_tree_simple_program()
    write_tree(tree, file=tmp_export_path)
    assert tmp_export_path.exists()
    text = tmp_export_path.read_text()
    assert "do_c" in text
    assert "do_b" in text
    assert "do_a" in text
    assert "do_d" in text
    assert "do_e" in text
    assert "Hello World From A!" in text
    assert "Hello World From D!" in text
    assert re.search(r"Hello World From E\..*\n.*With a new line!", text)
