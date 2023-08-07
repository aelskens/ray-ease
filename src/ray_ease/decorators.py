import inspect
from functools import wraps
from typing import Any, Callable

import ray

from .remote_as_local import RemoteActorAsLocal


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


@overload
def parallelize(callable_obj: Callable[..., Any], *ray_args: Any, **ray_kwargs: Any) -> Callable[..., Any]:
    """A decorator designed to wrap the ray.remote decorator. Its purpose is to enable the seamless use of
    the Ray framework without introducing syntax overhead. When applied to functions and classes, the
    decorated elements behave as if they were local functions and classes, effectively eliminating the need
    to deal with Ray's explicit syntax.

    Furthermore, in situations where Ray is not initialized, the program will automatically execute
    serially, following python default behavior. This means that the decorated functions and classes will
    be local elements, effectively avoid Ray's overhead which is unnecessary for serial application.

    In the case of decorated class, each member's signature except the dunder ones are modified to contain
    additionally an optional boolean argument `block` defaulting to False. This argument is used to determine
    whether ray.get should be used when calling the method or not (for more information and illustration refer
    to ray_ease.remote_as_local.RemoteActorAsLocal implementation).

    Additionally, the ray.remote args and kwargs can be provided to this decorator and are used when calling
    ray.remote.

    :param callable_obj: Either a function or a class to parallelize with the Ray framework.
    :type callable_obj: Callable[..., Any]
    :raises TypeError: Raised if the decorated object is not a callable.
    :return: The decorated callable (remote function or actor).
    :rtype: Callable[..., Any]
    """

    if not callable(callable_obj):
        raise TypeError(f"The decorated object should be a callable, not of type: {type(callable_obj)}.")

    # Avoid overhead if Ray is not initialized
    if not ray.is_initialized():
        return callable_obj

    ray_remote = ray.remote
    if ray_args or ray_kwargs:
        ray_remote = ray.remote(*ray_args, **ray_kwargs)

    if inspect.isfunction(callable_obj):
        callable_obj = ray_remote(callable_obj)

        class _Wrapper:
            def __init__(self, callable_obj: Callable[..., Any]) -> None:
                self.callable_obj = callable_obj

            def __call__(self, *args: Any, **kwargs: Any) -> Any:
                return self.callable_obj.remote(*args, **kwargs)

    elif inspect.isclass(callable_obj):
        callable_obj = ray_remote(callable_obj)

        class _Wrapper:
            def __init__(self, callable_obj: Callable[..., Any]) -> None:
                self.callable_obj = callable_obj

            def __call__(self, *args: Any, **kwargs: Any) -> Any:
                return RemoteActorAsLocal(self.callable_obj.remote(*args, **kwargs))

    return _Wrapper(callable_obj)
