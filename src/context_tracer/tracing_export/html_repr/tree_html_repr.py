"""
Create a HTML tree representation of the trace.
"""
import html
import json
import logging
from pathlib import Path
from typing import IO, Any, Final, Optional, TypedDict
from xml.etree import ElementTree as ET

from context_tracer.trace_context import TraceTree
from context_tracer.utils.json_encoder import AnyEncoder

logger = logging.getLogger(__name__)


STYLE_CSS_PATH: Final[Path] = Path(__file__).parent / "tree_view_style.css"
JSON_VIEW_JS_PATH: Final[Path] = Path(__file__).parent / "jsonview.js"
TREE_VIEW_JS_PATH: Final[Path] = Path(__file__).parent / "tree_view.js"


class JsonTree(TypedDict):
    """
    JSON representation of a tree node.
    """

    name: str
    data: Optional[dict[str, Any]]
    children: list["JsonTree"]


def write_tree(
    trace_tree: TraceTree,
    file: IO[str] | Path,
) -> None:
    """
    Write the tree representation of the trace to the given stream or file
    """
    if isinstance(file, Path):
        with file.open("w") as f:
            return write_tree_stream(
                trace_tree,
                f,
            )
    return write_tree_stream(
        trace_tree,
        file,
    )


def write_tree_stream(
    trace_tree: TraceTree,
    stream: IO[str],
) -> None:
    """
    Write the tree representation of the trace to the given stream.
    """
    html_tree = ET.Element("html")
    html_tree.append(create_html_head())
    body_tree = ET.SubElement(html_tree, "body")
    body_tree.append(create_html_tree(trace_tree))
    ET.indent(html_tree, space="\t", level=0)
    ET.ElementTree(html_tree).write(stream, encoding="unicode", method="html")


def create_html_head() -> ET.Element:
    """
    Create the head of the HTML document, including the CSS style.
    """
    head = ET.Element("head")
    # Style
    style = ET.SubElement(
        head,
        "style",
    )
    style.text = STYLE_CSS_PATH.read_text()
    # Json View
    script_jsonview = ET.SubElement(
        head,
        "script",
    )
    script_jsonview.text = JSON_VIEW_JS_PATH.read_text()
    # Tree View
    script_treeview = ET.SubElement(
        head,
        "script",
    )
    script_treeview.text = TREE_VIEW_JS_PATH.read_text()
    return head


# Create HTML Tree #################################################
def create_html_tree(root_node: TraceTree) -> ET.Element:
    """
    Create a HTML tree representation of the trace.
    """
    DATA_JS_VAR_NAME = "data_json"
    TREE_CLASS_NAME = "tree"
    tree = ET.Element("div", attrib={"class": TREE_CLASS_NAME})
    tree_node: JsonTree = create_json_repr(root_node)
    data_json = ET.SubElement(
        tree,
        "script",
    )
    json_str = json.dumps(
        tree_node,
        indent=2,
        cls=AnyEncoder,
    )
    json_str = html.escape(json_str, quote=False)
    data_json.text = f"const {DATA_JS_VAR_NAME} = {json_str};"
    render_script = ET.SubElement(
        tree,
        "script",
    )
    render_script.text = f'render_tree({DATA_JS_VAR_NAME}, document.querySelector(".{TREE_CLASS_NAME}"));'
    return tree


def create_json_repr(node: TraceTree) -> JsonTree:
    """
    Create a json tree representation of the trace.
    """
    json_node: JsonTree = {
        "name": node.name,
        "data": node.data,
        "children": [create_json_repr(child) for child in node.children],
    }
    return json_node
