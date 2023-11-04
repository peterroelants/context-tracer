import logging
from abc import abstractmethod
from collections.abc import (
    Callable,
)
from types import TracebackType
from typing import (
    ParamSpec,
    Protocol,
    TypeVar,
    overload,
    runtime_checkable,
)

logger = logging.getLogger(__name__)


# ContextManager ###################################################
_T_co = TypeVar("_T_co", covariant=True)


@runtime_checkable
class AbstractContextManager(Protocol[_T_co]):
    """
    From https://github.com/python/typeshed/blob/1459adc/stdlib/contextlib.pyi#L40-L46
    """

    def __enter__(self) -> _T_co:
        ...

    @abstractmethod
    def __exit__(
        self,
        __exc_type: type[BaseException] | None,
        __exc_value: BaseException | None,
        __traceback: TracebackType | None,
    ) -> bool | None:
        ...


# DecoratorMeta ####################################################
P = ParamSpec("P")
R = TypeVar("R")
CD = TypeVar("CD", bound="ContextDecorator")


class ContextDecorator(Protocol):
    def __call__(self, func: Callable[P, R]) -> Callable[P, R]:
        ...


class DecoratorMeta(type):
    """
    Metaclass to use decorator classes with and without arguments.

    E.g. both
    ```
    @decorator
    def func():
        ...
    ```
    and
    ```
    @decorator(arg1, arg2=...)
    def func():
        ...
    ```
    should work and are typed.
    """

    @overload
    def __call__(cls, func: Callable[P, R], *args, **kwargs) -> Callable[P, R]:
        ...

    @overload
    def __call__(cls: type[CD], *args, **kwargs) -> CD:
        ...

    def __call__(
        cls: type[CD], func: Callable[P, R] | None = None, *args, **kwargs
    ) -> CD | Callable[P, R]:
        """
        Create a decorator instance and call it with the function to decorate.

        Type.__call__ is called at the very beginning when an object of the type is instantiated.

        Only func is allowed as positional argument.
        """
        if func is not None and callable(func):
            # Called with func if
            # - class is used as a decorator without arguments
            # - class is used as a decorator with arguments, and func is also provided as an argument
            assert callable(func)
            # Create an instance of the decorator class and call it with the function to decorate.
            return type.__call__(cls, *args, **kwargs)(func)
        # Called without function if class is used as a decorator with arguments
        return type.__call__(cls, *args, **kwargs)
