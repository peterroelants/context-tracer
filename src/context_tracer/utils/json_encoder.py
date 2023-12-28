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

from pydantic import BaseModel

from context_tracer.utils.time_utils import format_timedelta

logger = logging.getLogger(__name__)


# Serializable dict ################################################
JSONType: TypeAlias = (
    None | str | int | float | bool | list["JSONType"] | dict[str, "JSONType"]
)

JSONDictType = dict[str, JSONType]


# JSON Encoder #####################################################
class CustomEncoder(json.JSONEncoder):
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
    if obj is None:
        return None
    if isinstance(obj, bytes):
        return shorted_repr(obj, max_characters=100)
    if isinstance(obj, str):
        return obj
    if isinstance(obj, int):
        return obj
    if isinstance(obj, float):
        return obj
    if isinstance(obj, complex):
        return repr(obj)
    if isinstance(obj, bool):
        return obj
    if isinstance(obj, datetime):
        return obj.isoformat(sep=" ")
    if isinstance(obj, timedelta):
        return format_timedelta(obj)
    if isnamedtuple(obj):
        return serialize_namedtuple(obj)
    if dataclasses.is_dataclass(obj):
        return dataclasses.asdict(obj)
    if isinstance(obj, BaseModel):
        return obj.model_dump()
    return shorted_repr(obj, max_characters=1000)


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


def shorted_repr(val: Any, max_characters: int = 100) -> str:
    val_str = repr(val)
    if len(val_str) > max_characters:
        half_size = min(len(val_str) // 2, max_characters // 2)
        val_str = val_str[:half_size] + "..." + val_str[-half_size:]
    return val_str
