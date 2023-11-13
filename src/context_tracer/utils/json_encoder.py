import dataclasses
import json
import logging
from datetime import datetime, timedelta
from typing import (
    Any,
    NamedTuple,
    TypeAlias,
    overload,
)

from context_tracer.utils.time_utils import format_timedelta

logger = logging.getLogger(__name__)


# Serializable dict ################################################
JSONType: TypeAlias = (
    None | str | int | float | bool | list["JSONType"] | dict[str, "JSONType"]
)

JSONDictType = dict[str, JSONType]


# JSON Encoder #####################################################
class AnyEncoder(json.JSONEncoder):
    def default(self, obj: Any):
        """Returns a serializable object for `obj` that can be serialized to a json string."""
        return make_serializable_base(obj)


# Make serializable ################################################
@overload
def make_serializable(obj: dict) -> JSONDictType:
    ...


@overload
def make_serializable(obj: list) -> list["JSONType"]:
    ...


@overload
def make_serializable(obj: Any) -> JSONType:
    ...


def make_serializable(obj: Any) -> JSONType:
    """Returns a serializable object for `obj`."""
    if isinstance(obj, list) or isinstance(obj, tuple):
        return [make_serializable(item) for item in obj]
    if isinstance(obj, dict):
        return {serialize_key(k): make_serializable(v) for k, v in obj.items()}
    return make_serializable_base(obj)


def make_serializable_base(obj: Any) -> JSONType:
    """Returns a serializable object for `obj`."""
    if isinstance(obj, datetime):
        return obj.isoformat(sep=" ", timespec="seconds")
    if isinstance(obj, timedelta):
        return format_timedelta(obj)
    if isnamedtuple(obj):
        return serialize_namedtuple(obj)
    if dataclasses.is_dataclass(obj):
        return dataclasses.asdict(obj)
    # TODO: Pydantic v2
    # TODO: dict(obj)?
    for attr in ["dict", "as_dict", "to_dict", "tolist", "to_list", "as_list"]:
        if hasattr(obj, attr):
            result = getattr(obj, attr)
            if callable(result):
                return result()
            return result
    for attr in ["json", "to_json", "as_json"]:
        if hasattr(obj, attr):
            result = getattr(obj, attr)
            if callable(result):
                result = result()
            if isinstance(result, str):
                return json.loads(result)
            return result
    return repr(obj)


def serialize_key(key: Any) -> str:
    if isinstance(key, str):
        return key
    return repr(key)


def isnamedtuple(obj: Any) -> bool:
    return (
        isinstance(obj, tuple) and hasattr(obj, "_fields") and hasattr(obj, "_asdict")
    )


def serialize_namedtuple(val: NamedTuple) -> dict:
    return {k: v for k, v in val._asdict().items()}
