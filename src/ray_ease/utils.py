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
