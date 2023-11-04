"""
Create a HTML tree representation of the trace.
"""
import html
import json
import logging
from pathlib import Path
from typing import IO, Any, Final, Optional, TypedDict
from xml.etree import ElementTree as ET

import jinja2

from context_tracer.trace_context import TraceTree
from context_tracer.utils.json_encoder import AnyEncoder

logger = logging.getLogger(__name__)


STYLE_CSS_PATH: Final[Path] = Path(__file__).parent / "flame_chart_style.css"
JSON_VIEW_JS_PATH: Final[Path] = Path(__file__).parent / "jsonview.js"
FLAME_CHART_VIEW_JS_PATH: Final[Path] = Path(__file__).parent / "flame_chart_view.js"


class JsonTree(TypedDict):
    """
    JSON representation of a tree node.
    """

    name: str
    value: float  # Duration
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
    create_html(trace_tree, body_tree)
    ET.indent(html_tree, space="\t", level=0)
    ET.ElementTree(html_tree).write(stream, encoding="unicode", method="html")


def create_html_head() -> ET.Element:
    """
    Create the head of the HTML document, including the CSS style.
    """
    head = ET.Element("head")
    # D3 Style
    ET.SubElement(
        head,
        "link",
        attrib={
            "rel": "stylesheet",
            "type": "text/css",
            "href": "https://cdn.jsdelivr.net/npm/d3-flame-graph@4.1.3/dist/d3-flamegraph.css",
        },
    )
    # Flame Chart Style
    flame_chart_style = ET.SubElement(
        head,
        "style",
        attrib={
            "type": "text/css",
        },
    )
    flame_chart_style.text = STYLE_CSS_PATH.read_text()
    # D3
    ET.SubElement(
        head,
        "script",
        attrib={
            "type": "text/javascript",
            "src": "https://d3js.org/d3.v7.js",
        },
    )
    # Flame Chart
    ET.SubElement(
        head,
        "script",
        attrib={
            "type": "text/javascript",
            "src": "https://cdn.jsdelivr.net/npm/d3-flame-graph@4.1.3/dist/d3-flamegraph.min.js",
        },
    )
    # Flame Chart Tooltip
    ET.SubElement(
        head,
        "script",
        attrib={
            "type": "text/javascript",
            "src": "https://cdn.jsdelivr.net/npm/d3-flame-graph@4.1.3/dist/d3-flamegraph-tooltip.js",
        },
    )
    # Json View
    script_jsonview = ET.SubElement(
        head,
        "script",
        attrib={
            "type": "text/javascript",
        },
    )
    script_jsonview.text = JSON_VIEW_JS_PATH.read_text()
    # Tree View
    script_treeview = ET.SubElement(
        head,
        "script",
        attrib={
            "type": "text/javascript",
        },
    )
    script_treeview.text = FLAME_CHART_VIEW_JS_PATH.read_text()
    return head


# Create HTML ######################################################
RENDER_TEMPLATE = """
document.getElementById("{{ title_id }}").innerHTML = {{ data_var_name }}.name;
flame_chart = render_flame_chart(
    data_obj={{ data_var_name }},
    parent_container_id="{{ flamechart_container_id }}",
    selected_node_container_id="{{ selected_node_container_id }}",
)
""".strip()


def create_html(root_node: TraceTree, container_elem: ET.Element) -> None:
    """
    Create a HTML tree representation of the trace.
    """
    DATA_JS_VAR_NAME = "data_json"
    VIEW_CLASS_NAME = "view"
    TITLE_ID = "flamechart-title"
    FLAMECHART_CONTAINER_ID = "flamechart-container"
    SELECTED_NODE_CONTAINER_ID = "selected-node-container"
    # View Container
    view = ET.SubElement(
        container_elem,
        "div",
        attrib={
            "class": VIEW_CLASS_NAME,
        },
    )
    # Title
    title = ET.SubElement(
        view,
        "h1",
        attrib={
            "id": TITLE_ID,
        },
    )
    title.text = "Placeholder"
    # Flame Chart
    ET.SubElement(
        view,
        "div",
        attrib={
            "id": FLAMECHART_CONTAINER_ID,
        },
    )
    # Selected Node
    ET.SubElement(
        view,
        "div",
        attrib={
            "id": SELECTED_NODE_CONTAINER_ID,
        },
    )
    # Data object as JSON
    tree_node: JsonTree = create_json_repr(root_node)
    data_json = ET.SubElement(
        container_elem,
        "script",
        attrib={
            "type": "text/javascript",
        },
    )
    json_str = json.dumps(
        tree_node,
        indent=2,
        cls=AnyEncoder,
    )
    json_str = html.escape(json_str, quote=False)
    data_json.text = f"const {DATA_JS_VAR_NAME} = {json_str};"
    # Render Flame Chart
    js_render = (
        jinja2.Environment(loader=jinja2.BaseLoader())
        .from_string(RENDER_TEMPLATE)
        .render(
            data_var_name=DATA_JS_VAR_NAME,
            title_id=TITLE_ID,
            flamechart_container_id=FLAMECHART_CONTAINER_ID,
            selected_node_container_id=SELECTED_NODE_CONTAINER_ID,
        )
    )
    render_script = ET.SubElement(
        container_elem,
        "script",
        attrib={
            "type": "text/javascript",
        },
    )
    render_script.text = js_render


def create_json_repr(node: TraceTree) -> JsonTree:
    """
    Create a json tree representation of the trace.
    """
    json_node: JsonTree = {
        "name": node.name,
        "value": count_children(node),
        "data": node.data,
        "children": [create_json_repr(child) for child in node.children],
    }
    return json_node


def count_children(node: TraceTree) -> int:
    """
    Count the number of children of the given node.
    """
    return sum(count_children(child) for child in node.children) + 1
