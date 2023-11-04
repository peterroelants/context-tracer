import json
import logging
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from context_tracer.constants import END_TIME_KEY, NAME_KEY, START_TIME_KEY
from context_tracer.trace_context import Tracing
from context_tracer.utils.json_encoder import AnyEncoder
from context_tracer.utils.time_utils import get_local_timestamp

from .parse_trace_json_log import TraceTreeJsonLog, parse_logged_tree
from .utils import get_uuid

logger = logging.getLogger(__name__)


@dataclass
class TraceSpanJsonLog:  # TraceSpan
    trace: "TracingJsonLog"
    name: str
    parent: Optional["TraceSpanJsonLog"] = None
    data: dict = field(default_factory=dict)
    # ID is needed to reconstruct the tree
    id: str = field(default_factory=get_uuid)

    def new_child(self, **data) -> "TraceSpanJsonLog":
        name = data.pop(NAME_KEY, "no-name")
        return TraceSpanJsonLog(
            trace=self.trace,
            name=name,
            parent=self,
            data=data,
        )

    def update_data(self, **new_data) -> None:
        """Add data to the current node."""
        self.data.update({k: v for k, v in new_data.items()})

    def json(self) -> str:
        return json.dumps(
            dict(
                id=self.id,
                name=self.name,
                parent_id=self.parent.id if self.parent else None,
                data=self.data,
            ),
            cls=AnyEncoder,
        )

    def __enter__(self) -> "TraceSpanJsonLog":
        if START_TIME_KEY not in self.data:
            self.data[START_TIME_KEY] = get_local_timestamp().isoformat(
                sep=" ", timespec="seconds"
            )
        return self

    def __exit__(self, *exc) -> None:
        if END_TIME_KEY not in self.data:
            self.data[END_TIME_KEY] = get_local_timestamp().isoformat(
                sep=" ", timespec="seconds"
            )
        self.trace.logger.info(self.json())
        return None


class TracingJsonLog(Tracing[TraceSpanJsonLog, TraceTreeJsonLog]):
    root_id: str
    logger: logging.Logger
    logging_path: Path
    _root_context: TraceSpanJsonLog

    def __init__(
        self,
        logging_path: Path,
        propagate_log=False,  # Set to True to propagate log messages to the root logger.
        extra_log_handlers: Optional[list[logging.Handler]] = None,
    ) -> None:
        self.root_id = get_uuid()
        logging_path = prepare_logging_path(logging_path)
        self.logging_path = logging_path
        self.logger = logging.getLogger(self.root_id)
        self.logger.propagate = propagate_log
        log_handlers: list[logging.Handler] = (
            [logging.FileHandler(logging_path)] if logging_path else []
        )
        log_handlers += extra_log_handlers or []
        for handler in log_handlers:
            handler.setLevel(logging.INFO)
            self.logger.addHandler(handler)
        self._root_context = TraceSpanJsonLog(
            trace=self,
            name="root",
            parent=None,
            id=self.root_id,
            data={},
        )
        super().__init__()

    @property
    def root_span(self) -> TraceSpanJsonLog:
        return self._root_context

    @property
    def tree(self) -> TraceTreeJsonLog:
        """Return a representable version of the trace tree."""
        return parse_logged_tree(self.logging_path)

    def __enter__(self) -> "TracingJsonLog":
        logger.info(f"Trace started with root_id={self.root_id}.")
        return super().__enter__()


def prepare_logging_path(logging_path: Path) -> Path:
    """Create the logging path if it does not exist."""
    if logging_path.exists() and logging_path.is_dir():
        logging_path = logging_path / f"{uuid.uuid1().hex}.log"
    logging_path.parent.mkdir(parents=True, exist_ok=True)
    return logging_path
