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
    """Decorator use to memoize a callable.

    :param callable_obj: The callable to memoize.
    :type callable_obj: Callable[..., Any]
    :return: The wrapper that memoizes the given callable with its arguments.
    :rtype: Callable[..., Any]
    """

    class _Wrapper:
        def __init__(self, callable_obj: Callable[..., Any]) -> None:
            self.callable_obj = callable_obj
            self.memoization = {}

        def __call__(self, *args: Any, **kwargs: Any):
            key = args + tuple(sorted(kwargs.items()))
            if key in self.memoization:
                return self.memoization[key]

            self.memoization[key] = self.callable_obj(*args, **kwargs)
            return self.memoization[key]

    return _Wrapper(callable_obj)
