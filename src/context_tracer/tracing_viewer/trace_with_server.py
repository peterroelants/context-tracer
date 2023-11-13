import html
import json
import logging
import webbrowser
from pathlib import Path
from typing import (
    Any,
    Optional,
    Self,
)

from context_tracer.constants import END_TIME_KEY, NAME_KEY, START_TIME_KEY
from context_tracer.trace_context import TraceSpan, TraceTree, Tracing
from context_tracer.tracing_viewer.load_templates import get_flamechart_view
from context_tracer.utils.json_encoder import AnyEncoder
from context_tracer.utils.time_utils import get_local_timestamp

from .view_server import ViewServer

logger = logging.getLogger(__name__)


class TraceWithServer(TraceSpan, TraceTree):
    """
    In-memory stored implementation of Span that signals updates to a ViewServer.

    Implements both Span (parent relation) and TraceTree (children relation) interfaces.
    """

    _tracing: "TracingWithServer"
    _name: str
    _data: dict[str, Any]
    _parent: Optional["TraceWithServer"]
    _children: list["TraceWithServer"]

    def __init__(
        self,
        name: str,
        parent: Optional["TraceWithServer"],
        tracing: "TracingWithServer",
        data: Optional[dict] = None,
    ) -> None:
        self._tracing = tracing
        self._name = name
        self._data = data or dict()
        self._parent = parent
        self._children = []
        super().__init__()

    # TODO: Use a better id
    @property
    def id(self) -> bytes:
        return id(self).to_bytes(8, "big")

    def new_child(self, **data) -> "TraceWithServer":
        name = data.pop(NAME_KEY, "no-name")
        child = TraceWithServer(
            tracing=self._tracing,
            parent=self,
            name=name,
            data=data,
        )
        self._children.append(child)
        return child

    def update_data(self, **new_data) -> None:
        self.data.update(new_data)
        self._tracing.signal_update()

    @property
    def name(self) -> str:
        return self._name

    @property
    def data(self) -> dict[str, Any]:
        return self._data

    @property
    def parent(self) -> Optional["TraceWithServer"]:
        return self._parent

    @property
    def children(self) -> list["TraceWithServer"]:
        return self._children

    def dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "data": self.data,
            "children": [child.dict() for child in self.children],
        }

    def json(self) -> str:
        json_str = json.dumps(
            self.dict(),
            cls=AnyEncoder,
            indent=2,
        )
        return html.escape(json_str, quote=False)

    def __enter__(self) -> "TraceWithServer":
        start_time = get_local_timestamp()
        self.update_data(**{START_TIME_KEY: start_time})
        return self

    def __exit__(self, *exc) -> None:
        end_time = get_local_timestamp()
        self.update_data(**{END_TIME_KEY: end_time})
        return None


class TracingWithServer(Tracing[TraceWithServer, TraceWithServer]):
    """Simple in-memory implementation of Tracing that signals updates to a ViewServer."""

    _root_context: TraceWithServer
    _server: ViewServer

    def __init__(self, name: str = "root", final_export_dir: Optional[Path] = None):
        self._root_context = TraceWithServer(
            tracing=self,
            parent=None,
            name=name,
            data={},
        )
        self.final_export_dir = final_export_dir or Path("./")
        self._server = ViewServer(host="localhost", port=0)

    @property
    def root_span(self) -> TraceWithServer:
        """Root context that is the parent of all other contexts."""
        return self._root_context

    @property
    def tree(self) -> TraceWithServer:
        """Tree representation of the root of the trace tree."""
        return self._root_context

    def signal_update(self) -> None:
        """Signal that the trace tree has been updated."""
        self._server.queue.put_nowait(self.tree.json())

    def _export_html(self) -> None:
        """Export the trace tree as stand-alone HTML file."""
        name = self.root_span.name
        trace_path = self.final_export_dir / f"{name}_trace.html"
        logger.info(f"Exporting trace tree to '{trace_path}'")
        trace_path.write_text(get_flamechart_view(data_dict=self.tree.dict()))

    def __enter__(self: Self) -> Self:
        url = self._server.__enter__()
        webbrowser.open(url)
        return super().__enter__()

    def __exit__(self, *args, **kwargs) -> None:
        """Reset the trace."""
        # Exit super first (will exit root span)
        super().__exit__(*args, **kwargs)
        # Then exit server and delete queue
        self._server.__exit__(*args, **kwargs)
        self._export_html()
        return
