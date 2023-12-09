import logging
import tempfile
import uuid
from pathlib import Path
from typing import Iterator

import pytest
import requests
from bs4 import BeautifulSoup
from context_tracer.trace import log_with_trace, trace
from context_tracer.trace_types import (
    TraceSpan,
    TraceTree,
    Tracing,
)
from context_tracer.tracing_viewer.tracer_with_view import TracingWithViewer


@pytest.fixture
def tmp_html_export_path() -> Iterator[Path]:
    with tempfile.TemporaryDirectory() as tmp_dir:
        html_path = Path(tmp_dir) / "test.html"
        yield html_path


def test_tracing_with_viewer(
    tmp_db_path: Path, tmp_log_path: Path, tmp_html_export_path: Path
) -> None:
    with TracingWithViewer(
        db_path=tmp_db_path,
        log_dir=tmp_log_path,
        log_level=logging.DEBUG,
        export_html_path=tmp_html_export_path,
    ) as tracing:
        pass  # Just root context
        assert isinstance(tracing, Tracing)
        assert tracing.root_span is not None
        assert isinstance(tracing.root_span, TraceSpan)
        # Is view server rearchable?
        assert tracing.url is not None
        response = requests.get(tracing.url)
        assert response.status_code == 200
    assert tmp_db_path.exists()
    assert tmp_log_path.exists()
    assert tmp_html_export_path.exists()
    assert tracing.tree is not None
    assert isinstance(tracing.tree, TraceTree)


def test_trace_sqlite_program(tmp_db_path: Path, tmp_html_export_path: Path) -> None:
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

    root_name = f"root-{uuid.uuid4()}"
    with TracingWithViewer(
        db_path=tmp_db_path,
        name=root_name,
        export_html_path=tmp_html_export_path,
    ) as tracing:
        program()
    # Checks
    assert tmp_db_path.exists()
    # Check actual tree
    tree_root = tracing.tree
    assert isinstance(tree_root, TraceTree)
    assert tree_root.name == root_name
    assert len(tree_root.children) == 1

    def get_leafs(tree_root: TraceTree) -> list[TraceTree]:
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

    def found_c(tree_root: TraceTree) -> bool:
        if tree_root.name == "C":
            return True
        for child in tree_root.children:
            if found_c(child):
                return True
        return False

    assert found_c(tree_root)
    # Check view
    assert tmp_html_export_path.exists()
    html_text = tmp_html_export_path.read_text()
    # Find basic text
    assert "do_c" in html_text
    assert "do_b" in html_text
    assert "do_a" in html_text
    assert "do_d" in html_text
    assert "do_e" in html_text
    assert "Hello World From A!" in html_text
    assert "Hello World From D!" in html_text
    assert "Hello World From E!" in html_text
    # Parse html
    soup = BeautifulSoup(html_text, "lxml")
    assert soup is not None
