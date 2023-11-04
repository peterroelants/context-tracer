import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from context_tracer.utils.json_encoder import JSONDictType


@dataclass
class TraceTreeJsonLog:  # TraceTree
    id: str
    name: str
    data: JSONDictType
    parent_id: Optional[str] = None
    children: list["TraceTreeJsonLog"] = field(default_factory=list)

    @classmethod
    def from_str(cls, json_str: str) -> "TraceTreeJsonLog":
        json_dict = json.loads(json_str)
        return TraceTreeJsonLog(**json_dict)


def parse_logged_tree(file: Path) -> TraceTreeJsonLog:
    with file.open("r") as f:
        id2node_map = {
            node.id: node
            for node in (TraceTreeJsonLog.from_str(line) for line in f.readlines())
        }
    assert len(id2node_map) > 0, f"No trace nodes found in {file}!"
    return create_tree_from_idmap(id2node_map)


def create_tree_from_idmap(
    id2node_map: dict[str, TraceTreeJsonLog]
) -> TraceTreeJsonLog:
    """
    Create tree from dict of TraceTreeJsonLog objects.
    TraceTreeJsonLog objects relations are defined by parent_id and id.
    The root node is the node with parent_id == None.
    """
    root_nodes = [node for node in id2node_map.values() if node.parent_id is None]
    if len(root_nodes) > 1:
        raise ValueError(f"More than one root node found! {root_nodes=}.")
    elif len(root_nodes) == 0:
        raise ValueError("No root node found!")
    root_node = root_nodes[0]
    for node in id2node_map.values():
        if node.parent_id is not None:
            id2node_map[node.parent_id].children.append(node)
    return root_node
