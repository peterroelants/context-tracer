"""
Create a plain-text tree representation of the trace.
Uses the [rich](https://rich.readthedocs.io/en/stable/tree.html) library for rendering the tree.
"""
import logging
from collections.abc import Callable
from pathlib import Path
from typing import IO, Optional, TypeAlias

from rich.console import Console, Group
from rich.panel import Panel
from rich.tree import Tree as RichTree

from context_tracer.trace_context import TraceTree

from ..utils.pretty_printer import NewLineStrPrettyPrinter
from .default_utils import get_timestamp_repr

logger = logging.getLogger(__name__)


# Represent tree node head (name, ...) as a string
NodeHeadReprType: TypeAlias = Callable[[TraceTree], str]
# Represent tree node body (data, ...) as a string
NodeBodyReprType: TypeAlias = Callable[[TraceTree], str | None]


def write_tree(
    trace_tree: TraceTree,
    file: IO[str] | Path,
    tab_size: int = 2,
    max_width: int = 88,
    node_repr_fn: Optional[NodeHeadReprType] = None,
    data_repr_fn: Optional[NodeBodyReprType] = None,
) -> None:
    """
    Write the tree representation of the trace to the given stream or file
    """
    if isinstance(file, Path):
        with file.open("w") as f:
            return write_tree_stream(
                trace_tree,
                f,
                tab_size=tab_size,
                max_width=max_width,
                node_repr_fn=node_repr_fn,
                data_repr_fn=data_repr_fn,
            )
    return write_tree_stream(
        trace_tree,
        file,
        tab_size=tab_size,
        max_width=max_width,
        node_repr_fn=node_repr_fn,
        data_repr_fn=data_repr_fn,
    )


def write_tree_stream(
    trace_tree: TraceTree,
    stream: IO[str],
    tab_size: int = 2,
    max_width: int = 88,
    node_repr_fn: Optional[NodeHeadReprType] = None,
    data_repr_fn: Optional[NodeBodyReprType] = None,
) -> None:
    """
    Write the tree representation of the trace to the given stream.
    """
    if node_repr_fn is None:
        node_repr_fn = default_head_repr
    if data_repr_fn is None:
        data_repr_fn = default_body_repr
    rich_tree: RichTree = create_rich_tree(
        trace_tree,
        head_repr_fn=node_repr_fn,
        body_repr_fn=data_repr_fn,
    )
    console = Console(
        file=stream,
        force_jupyter=False,
        force_terminal=False,
        force_interactive=False,
        width=max_width,
        tab_size=tab_size,
    )
    console.print(rich_tree, markup=False)
    stream.flush()


# Create Rich Tree #################################################
def create_rich_tree(
    node: TraceTree,
    head_repr_fn: NodeHeadReprType,
    body_repr_fn: NodeBodyReprType,
) -> RichTree:
    """
    Create a RichTree from the given TraceTreeNode.

    Rich is a library for rich text and beautiful formatting in the terminal.
    """
    tree_node_repr = head_repr_fn(node)
    tree = RichTree(tree_node_repr)
    _update_rich_tree_recursive(
        node=node,
        tree=tree,
        head_repr_fn=head_repr_fn,
        body_repr_fn=body_repr_fn,
    )
    return tree


def _update_rich_tree_recursive(
    node: TraceTree,
    tree: RichTree,
    head_repr_fn: NodeHeadReprType,
    body_repr_fn: NodeBodyReprType,
) -> None:
    for child in node.children:
        tree_head_repr = head_repr_fn(child)
        tree_body_repr = body_repr_fn(child)
        if tree_body_repr is None:
            tree_node = tree.add(tree_head_repr)
        else:
            tree_leaf_repr = Group(tree_head_repr, Panel(tree_body_repr))
            tree_node = tree.add(tree_leaf_repr)
        _update_rich_tree_recursive(
            child,
            tree_node,
            head_repr_fn=head_repr_fn,
            body_repr_fn=body_repr_fn,
        )


# Representation functions #########################################
# Default Methods to Generate representations of the tree nodes and data
def default_head_repr(node: TraceTree) -> str:
    """
    Default head representation of a tree node.

    Head is minimalistic: typically name and timestamp.
    """
    timestamp = get_timestamp_repr(node)
    if timestamp is None:
        tree_node_repr = f"{node.name} - ..."
    else:
        tree_node_repr = f"{node.name} - {timestamp}"
    return tree_node_repr


def default_body_repr(node: TraceTree) -> Optional[str]:
    """
    Default body representation of a tree node.

    Body is the data of the node (if any), and is complementary to the head.
    """
    pprinter = NewLineStrPrettyPrinter(indent=2, sort_dicts=False, compact=False)
    try:
        node_dct: dict = node.data  # type: ignore
        if not node_dct:
            return None
        return pprinter.pformat(node_dct)
    except Exception:
        return repr(node)
