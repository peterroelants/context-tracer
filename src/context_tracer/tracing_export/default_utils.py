import logging
from datetime import datetime
from typing import Optional

from context_tracer.constants import NAME_KEY, START_TIME_KEY
from context_tracer.trace_context import TraceTree

logger = logging.getLogger(__name__)

DEFAULT_NAME = "NO_NAME"


# TODO: Still needed?
def get_name_robust(node: TraceTree) -> str:
    """
    Try to get a name for the node.
    """
    try:
        name = node.name  # type: ignore
        assert isinstance(name, str)
        return name
    except (AttributeError, AssertionError):
        pass
    try:
        node_dct: dict = node.dict()  # type: ignore
        name = node_dct[NAME_KEY]
        assert isinstance(name, str)
        return name
    except (AttributeError, KeyError, AssertionError):
        pass
    try:
        name = node[NAME_KEY]  # type: ignore
        assert isinstance(name, str)
        return name
    except (TypeError, KeyError, AssertionError):
        pass
    logger.warning(
        f"No name found for {node=!r}! Use a custom node representation function!"
    )
    return DEFAULT_NAME


def get_timestamp_repr(node: TraceTree) -> Optional[str]:
    """
    Try to get a timestamp for the node.
    """
    try:
        timestamp = node.timestamp  # type: ignore
        if isinstance(timestamp, datetime):
            return timestamp.isoformat(sep=" ", timespec="seconds")
        assert isinstance(timestamp, str)
        return timestamp
    except (AttributeError, AssertionError):
        pass
    try:
        node_dct: dict = node.dict()  # type: ignore
        timestamp = node_dct[START_TIME_KEY]
        if isinstance(timestamp, datetime):
            return timestamp.isoformat(sep=" ", timespec="seconds")
        assert isinstance(timestamp, str)
        return timestamp
    except (AttributeError, KeyError, AssertionError):
        pass
    try:
        timestamp = node[START_TIME_KEY]  # type: ignore
        if isinstance(timestamp, datetime):
            return timestamp.isoformat(sep=" ", timespec="seconds")
        assert isinstance(timestamp, str)
        return timestamp
    except (TypeError, KeyError, AssertionError):
        pass
    logger.warning(
        f"No timestamp found for {node=!r}!  Use a custom node representation function!"
    )
    return None
