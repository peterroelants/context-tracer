import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)


def maybe_add_to_dict(
    dct: Optional[dict],
    key: str,
    value: Any,
) -> dict:
    """Set the value in the dict only if they does not exists yet."""
    if dct is not None and key in dct:
        logger.debug(f"{key=!r} already in {dct=}.")
        return dct
    if dct is None:
        dct = {}
    if key not in dct:
        dct[key] = value
    return dct


def add_dict_unique_key(
    dct: Optional[dict],
    key: str,
    value: Any,
) -> dict:
    """
    Set the value in the dict.
    If they key already exists, add a suffix to make the key unique.
    """
    if dct is not None and key in dct:
        logger.debug(f"{key=!r} already in {dct=}.")
        while key in dct:
            key += "_"  # Add suffix to make the key unique
    if dct is None:
        dct = {}
    dct[key] = value
    return dct


def update_dict_unique_key(
    dct: Optional[dict],
    dict_to_add: dict,
) -> dict:
    """
    Set the values in the dict.
    If they key already exists, add a suffix to make the key unique.
    """
    if dct is None:
        dct = {}
    for key, value in dict_to_add.items():
        dct = add_dict_unique_key(dct, key, value)
    return dct
