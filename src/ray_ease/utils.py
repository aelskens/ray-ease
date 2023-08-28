from functools import wraps
from typing import Any, Callable


def overload(decorator: Callable[..., Any]) -> Callable[..., Any]:
    """A decorator to overload another decorator to allow using it with or without parentheses:
    @decorator(with, arguments, and=kwargs) or @decorator.

    :param decorator: The decorator to overload.
    :type decorator: Callable[..., Any]
    :return: The overloaded decorator.
    :rtype: Callable[..., Any]
    """

    @wraps(decorator)
    def overloaded_decorator(*args: Any, **kwargs: Any) -> Callable[..., Any]:
        if len(args) == 1 and len(kwargs) == 0 and callable(args[0]):
            return decorator(args[0])
        else:
            return lambda callable_obj: decorator(callable_obj, *args, **kwargs)

    return overloaded_decorator


def memoize(callable_obj: Callable[..., Any]) -> Callable[..., Any]:
    """Decorator use to memoize the dynamic inheritence mechanism.

    :param callable_obj: The closure that provides the child class with a parent class to
    inherite from.
    :type callable_obj: Callable[..., Any]
    :return: The wrapper that memoizes the dynamic inheritence mechanisms.
    :rtype: Callable[..., Any]
    """

    class Wrapper:
        def __init__(self, callable_obj: Callable[..., Any]) -> None:
            self.callable_obj = callable_obj
            self.memoization = {}

        def __call__(self, *args: Any):
            return self.memoization.setdefault(args, self.callable_obj(*args))

    return Wrapper(callable_obj)
