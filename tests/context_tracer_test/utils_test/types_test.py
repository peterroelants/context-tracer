import functools
from collections.abc import Callable
from typing import Any, ParamSpec, TypeVar, overload

from context_tracer.utils.types import DecoratorMeta

P = ParamSpec("P")
R = TypeVar("R")


class DecoratorTester(metaclass=DecoratorMeta):
    def __init__(self, *args: Any, **kwargs: Any):
        self.args = args
        self.kwargs = kwargs

    def __call__(self, func: Callable[P, R]) -> Callable[P, R]:
        @functools.wraps(func)
        def wrapped_func(*args: P.args, **kwargs: P.kwargs) -> R:
            return func(*args, **kwargs)

        return wrapped_func


@overload
def decorator_tester(func: Callable[P, R], *args: Any, **kwargs: Any) -> Callable[P, R]:
    """Type when first argument is a function."""
    ...


@overload
def decorator_tester(*args: Any, **kwargs: Any) -> DecoratorTester:  # type: ignore
    """
    Type when first argument is not a function.

    Needs the `type: ignore` comment to avoid the following error:
        Overloaded function signature 2 will never be matched: signature 1's parameter type(s) are the same or broader
    The error message about overlapping signatures is more like a lint warning than a real type error.
    See also: https://stackoverflow.com/a/74567241/919431
    """
    ...


def decorator_tester(
    func: Callable[P, R] | None = None,
    *args: Any,
    **kwargs: Any,
) -> Callable[P, R] | DecoratorTester:
    """
    Create a decorator instance that can be used with and without arguments.
    """
    if func is not None and callable(func):
        return DecoratorTester(*args, **kwargs)(func)
    return DecoratorTester(*args, **kwargs)


# Tests ############################################################
def test_decorator_meta() -> None:
    # Decorate without arguments
    @decorator_tester
    def func1():
        return 42

    assert callable(func1)
    assert func1() == 42

    # Decorate with args
    @decorator_tester(1, 2, 3)
    def func2():
        return 21

    assert callable(func2)
    assert func2() == 21

    # Decorate with kwargs
    @decorator_tester(a=1, b=2, c=3)
    def func3():
        return 84

    assert callable(func3)
    assert func3() == 84

    # Decorate with args and kwargs
    @decorator_tester(1, b=2, c=3)
    def func4():
        return 11

    assert callable(func4)
    assert func4() == 11

    # Decorate directly
    func5 = decorator_tester(1, 2, 3)(lambda: 12)
    assert callable(func5)
    assert func5() == 12

    # Decorate with arguments and add function directly
    func6 = decorator_tester(lambda: 13, 1, 2, 3)
    assert callable(func6)
    assert func6() == 13

    # Decorate with kwargs and add function directly
    func6 = decorator_tester(func=lambda: 14, a=1, b=2, c=3)
    assert callable(func6)
    assert func6() == 14
