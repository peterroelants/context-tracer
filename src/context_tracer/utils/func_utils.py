import inspect
from collections.abc import Callable
from typing import Any


def func2str(func: Callable) -> str:
    """Get a name representing the function."""
    return getattr(func, "__name__", repr(func))


def get_func_bound_args(func: Callable, *args, **kwargs) -> dict[str, Any]:
    """Get the kwargs dict of all arguments bounded to the function signature."""
    sig = inspect.signature(func)
    bound = sig.bind(*args, **kwargs)
    bound.apply_defaults()
    return bound.arguments
