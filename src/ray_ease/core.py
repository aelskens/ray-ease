import inspect
import os
from typing import Any, Callable, Dict, Iterable, Optional

import ray
import tqdm
from ray._private.worker import BaseContext

from .remote_as_local import remote_actor_as_local
from .utils import overload


def init(config: str = "ray", *args: Any, **kwargs: Any) -> Optional[BaseContext]:
    """Wrapper around the `ray.init()` function to specify whether the program should run in a serial or
    in a parallel manner.

    :param config: The configuration in which the code will be executed. This can either be `ray` to
    achieve parallelization or `serial` to execute the program as traditional python program, defaults
    to "ray".
    :type config: str, optional
    :return: For `config="ray"`, the wrapping function returns the Ray context similarly to that of
    `ray.init()`. Otherwise, including the case where `config="serial"`, it returns None.
    :rtype: Optional[BaseContext]
    """

    if config in ["serial"]:
        os.environ["RAY_EASE"] = config

        return None

    elif config in ["ray"]:
        os.environ["RAY_EASE"] = config

        return ray.init(*args, **kwargs)

    else:
        return None


def _parallelize(callable_obj: Callable[..., Any], *ray_args: Any, **ray_kwargs: Any) -> Callable[..., Any]:
    """This represents the current operational core encasing the ray.remote decorator. To ensure seamless
    utilization, given its dependence on whether ray initialization has occurred, this core function is
    enveloped by another layer. This added layer serves to defer the instantiation of the decorated
    callable_obj until the moment of its invocation.

    :raises TypeError: Raised if the decorated object is not a callable.
    """

    if not callable(callable_obj):
        raise TypeError(f"The decorated object should be a callable, not of type: {type(callable_obj)}.")

    # Avoid overhead if Ray is not initialized
    if os.getenv("RAY_EASE") in [None, "serial"]:
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
        initial_cls = callable_obj
        callable_obj = ray_remote(callable_obj)

        class _Wrapper:
            def __init__(self, callable_obj: Callable[..., Any]) -> None:
                self.callable_obj = callable_obj

            def __call__(self, *args: Any, **kwargs: Any) -> Any:
                remoteHandler = remote_actor_as_local(initial_cls)
                return remoteHandler(self.callable_obj.remote(*args, **kwargs), *args, **kwargs)

    return _Wrapper(callable_obj)


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
    :return: The decorated callable (remote function or actor).
    :rtype: Callable[..., Any]
    """

    class _Wrapper:
        def __init__(self, callable_obj: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
            self.callable_obj = callable_obj
            self.args = args
            self.kwargs = kwargs

            self.memoization = {}

        def __call__(self, *usage_args: Any, **usage_kwargs: Any) -> Any:
            key = self.args + tuple(sorted(self.kwargs.items()))

            if key in self.memoization:
                usable_callable_obj = self.memoization[key]

                return usable_callable_obj(*usage_args, **usage_kwargs)

            self.memoization[key] = _parallelize(self.callable_obj, *self.args, **self.kwargs)
            usable_callable_obj = self.memoization[key]

            return usable_callable_obj(*usage_args, **usage_kwargs)

    return _Wrapper(callable_obj, *ray_args, **ray_kwargs)


def retrieve(
    loop: Iterable[Any], parallel_progress: bool = False, parallel_progress_kwargs: Dict[str, Any] = {}
) -> Iterable[Any]:
    """Retrieve the results from a pseudo-parallelized loop. It is a pseudo-parallelized rather than a
    parallelized loop because if Ray is not initialized, then the loop is serial instead.

    :param loop: The pseudo-parallelized loop.
    :type loop: Iterable[Any]
    :param parallel_progress: Whether to display the progression bar with the `tqdm` package or not. This
    argument is exclusively useful when parallelizing as the computations are performed when `ray.get()` is
    called. In serial computation, everything is already finished at this stage. Defaults to False.
    :type parallel_progress: bool, optional
    :param parallel_progress_kwargs: A dictionary of the traditional arguments allowed in `tqdm.tqdm()`,
    defaults to {}.
    :type parallel_progress_kwargs: Dict[str, Any], optional
    :return: The resulting iterable.
    :rtype: Iterable[Any]
    """

    if os.getenv("RAY_EASE") in ["ray"]:
        if parallel_progress:
            # Remove eventual total key-value because automatically computed hereunder
            parallel_progress_kwargs.pop("total", None)
            return [ray.get(obj) for obj in tqdm.tqdm(loop, total=len(loop), **parallel_progress_kwargs)]

        return ray.get(loop)

    return loop
