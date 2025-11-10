import inspect
import os
from typing import Any, Callable, Dict, Generator, Iterable, Optional, TypeVar, overload

import ray
import tqdm
from ray._private.worker import BaseContext

from .remote_as_local import remote_actor_as_local

F = TypeVar("F", bound=Callable[..., Any])
O = TypeVar("O")


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

    os.environ["RAY_EASE"] = config

    if config == "serial":
        return None

    elif config == "ray":
        # Prevent initializing ray if it has already been initialized
        if ray.is_initialized():
            return None

        return ray.init(*args, **kwargs)

    else:
        raise ValueError(f"{config} is not one of the allowed configurations (serial and ray).")


def _parallelize(callable_obj: F, *ray_args: Any, **ray_kwargs: Any) -> F:
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
def parallelize(callable_obj: F) -> F: ...


@overload
def parallelize(*ray_args: Any, **ray_kwargs: Any) -> F: ...


def parallelize(callable_obj: Optional[F] = None, *ray_args: Any, **ray_kwargs: Any) -> F:
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

    :param callable_obj: Either a function or a class to parallelize with the Ray framework, defaults to None.
    :type callable_obj: Optional[F], optional
    :return: The decorated callable (remote function or actor).
    :rtype: F
    """

    class _Wrapper:
        def __init__(self, callable_obj: F, *args: Any, **kwargs: Any) -> None:
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

    if callable_obj is not None:
        return _Wrapper(callable_obj, *ray_args, **ray_kwargs)
    else:
        return lambda callable_obj: _Wrapper(callable_obj, *ray_args, **ray_kwargs)


# In both serial and ray, for Iterable[func() | Object.method() | Task() | Actor.method()]
@overload
def retrieve(
    object_refs: Iterable[Any],
    ordered: bool = False,
    parallel_progress: bool = False,
    parallel_progress_kwargs: Dict[str, Any] = {},
) -> Iterable[Any]: ...


# In both serial and ray, for Actor.method()
@overload
def retrieve(
    object_refs: Any,
    ordered: bool = False,
    parallel_progress: bool = False,
    parallel_progress_kwargs: Dict[str, Any] = {},
) -> Any: ...


def retrieve(
    object_refs: Iterable[Any] | Any,
    ordered: bool = False,
    parallel_progress: bool = False,
    parallel_progress_kwargs: Dict[str, Any] = {},
) -> Iterable[Any] | Any:
    """Retrieve the results from a pseudo-parallelized iterable or object. It is a pseudo-parallelized rather
    than a parallelized iterable or object because if Ray is not initialized, then the iterabel or object is
    serial instead.

    :param object_refs: The pseudo-parallelized iterable or object.
    :type loop: Iterable[Any] | Any
    :param ordered: Whether the order should be kept or not, cf. Ray anti-pattern using `.wait()` rather
    than `.get()` (https://docs.ray.io/en/latest/ray-core/patterns/ray-get-submission-order.html). Ignored if
    object_refs is not an iterable, defaults to False.
    :type ordered: Iterable[Any]
    :param parallel_progress: Whether to display the progression bar with the `tqdm` package or not. This
    argument is exclusively useful when parallelizing as the computations are performed when `ray.get()` is
    called. In serial computation, everything is already finished at this stage. Ignored if object_refs is not
    an iterable, defaults to False.
    :type parallel_progress: bool, optional
    :param parallel_progress_kwargs: A dictionary of the traditional arguments allowed in `tqdm.tqdm()`. Ignored
    if object_refs is not an iterable, defaults to {}.
    :type parallel_progress_kwargs: Dict[str, Any], optional
    :return: The results of each element call from the iterable or of the object call.
    :rtype: Iterable[Any] | Any
    """

    if os.getenv("RAY_EASE") in ["ray"]:
        if not isinstance(object_refs, Iterable):
            return ray.get(object_refs)

        if isinstance(object_refs, Generator):
            object_refs = list(object_refs)

        # Remove eventual total key-value because automatically computed hereunder
        parallel_progress_kwargs.pop("total", None)

        # Disable the progress bar if not desired
        if not parallel_progress:
            parallel_progress_kwargs["disable"] = True

        progress = tqdm.tqdm(range(len(object_refs)), **parallel_progress_kwargs)

        results = []
        if ordered:
            for obj in object_refs:
                results.append(ray.get(obj))
                progress.update()
        else:
            unfinished = object_refs
            while unfinished:
                # Returns the first ObjectRef that is ready.
                finished, unfinished = ray.wait(unfinished, num_returns=1)
                results.append(ray.get(finished[0]))
                progress.update()

        return results

    return object_refs
