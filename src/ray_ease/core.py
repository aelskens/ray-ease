"""Core public API for ray-ease.

This module exposes three symbols that form the core ray-ease interface:

* :func:`init`: choose serial or parallel execution mode.
* :func:`parallelize`: decorator that wraps functions and classes with
  ``ray.remote`` while preserving ordinary Python call syntax.
* :func:`retrieve`: resolve one or more Ray ``ObjectRef`` futures into
  concrete values.
"""

import inspect
import os
from typing import Any, Callable, Generator, Iterable, Optional, TypeVar, overload

import ray
import tqdm
from ray._private.worker import BaseContext

from .registry import Registry, _RegistryProxy
from .remote_as_local import remote_actor_as_local

F = TypeVar("F", bound=Callable[..., Any])
O = TypeVar("O")


def init(config: str = "ray", *args: Any, **kwargs: Any) -> Optional[BaseContext]:
    """Initialise ray-ease and choose the execution mode for the current process.

    Sets the ``RAY_EASE`` environment variable to *config* and, when ``config="ray"``,
    forwards all remaining positional and keyword arguments to :func:`ray.init`.  If Ray
    has already been initialised this call is a no-op (the environment variable is still
    updated).

    :param config: Execution mode.  Use ``"ray"`` (default) for parallel execution
        backed by Ray, or ``"serial"`` to run everything in the calling process without
        any Ray overhead.
    :type config: str, optional
    :param args: Positional arguments forwarded verbatim to :func:`ray.init` when
        ``config="ray"``.
    :param kwargs: Keyword arguments forwarded verbatim to :func:`ray.init` when
        ``config="ray"``.
    :raises ValueError: If *config* is not ``"ray"`` or ``"serial"``.
    :return: The :class:`~ray._private.worker.BaseContext` returned by :func:`ray.init`
        when ``config="ray"`` and Ray was not yet initialised; ``None`` in every other
        case.
    :rtype: Optional[BaseContext]

    Example::

        import ray_ease as rez

        # Parallel mode (default) - spins up a local Ray cluster.
        rez.init()

        # Serial mode - zero Ray overhead, useful for debugging.
        rez.init("serial")

        # Forward Ray options (e.g. limit resources for testing).
        rez.init(num_cpus=4)
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
    """Wrap *callable_obj* with the appropriate Ray remote decorator.

    This is the operational core of :func:`parallelize`.  It is intentionally kept
    private; end-users should always go through :func:`parallelize`, which defers
    this call until the first invocation so that Ray has been fully initialised.

    The function distinguishes three cases:

    * **Serial mode** (``RAY_EASE`` is ``None`` or ``"serial"``): returns
      *callable_obj* unchanged, no Ray dependency is introduced.
    * **Parallel function**: wraps with ``ray.remote`` and exposes a
      ``__call__`` that transparently invokes ``.remote()``.
    * **Parallel class**: wraps with ``ray.remote``.  If the class inherits
      from :class:`Registry`, a :class:`_RegistryProxy` is returned in place of
      the standard :func:`~ray_ease.remote_as_local.remote_actor_as_local` wrapper
      to provide claim/wait race-condition protection.

    :param callable_obj: The function or class to wrap.
    :type callable_obj: F
    :param ray_args: Positional arguments forwarded to ``ray.remote``.
    :param ray_kwargs: Keyword arguments forwarded to ``ray.remote``
        (e.g. ``max_restarts``, ``num_cpus``).
    :raises TypeError: If *callable_obj* is neither a function nor a class.
    :return: The wrapped callable, or *callable_obj* unchanged in serial mode.
    :rtype: F
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
        is_registry = issubclass(callable_obj, Registry)
        callable_obj = ray_remote(callable_obj)

        if is_registry:
            # Registry subclasses get the claim/wait proxy instead of the
            # generic remote_actor_as_local wrapper.
            class _Wrapper:
                def __init__(self, callable_obj: Callable[..., Any]) -> None:
                    self.callable_obj = callable_obj

                def __call__(self, *args: Any, **kwargs: Any) -> Any:
                    return _RegistryProxy(self.callable_obj.remote(*args, **kwargs))

        else:

            class _Wrapper:
                def __init__(self, callable_obj: Callable[..., Any]) -> None:
                    self.callable_obj = callable_obj

                def __call__(self, *args: Any, **kwargs: Any) -> Any:
                    remoteHandler = remote_actor_as_local(initial_cls)
                    return remoteHandler(self.callable_obj.remote(*args, **kwargs))

    return _Wrapper(callable_obj)


@overload
def parallelize(callable_obj: F) -> F: ...


@overload
def parallelize(*ray_args: Any, **ray_kwargs: Any) -> F: ...


def parallelize(callable_obj: Optional[F] = None, *ray_args: Any, **ray_kwargs: Any) -> F:
    """Decorator that makes functions and classes transparently usable with Ray.

    Wraps :func:`ray.remote` so that decorated callables behave like ordinary
    Python functions and classes regardless of whether Ray is running.  The same
    codebase executes serially (for debugging) or in parallel (for production) with
    a single :func:`init` call and no further code changes.

    **Functions**: the decorated function is called like any regular function and
    returns a Ray ``ObjectRef`` in parallel mode, or the plain return value in serial
    mode.  Use :func:`retrieve` to resolve ``ObjectRef`` values.

    **Classes**: instances are created with the usual constructor syntax.  Public
    method calls return ``ObjectRef`` values in parallel mode; dunder methods are
    forwarded directly to the underlying Ray actor so that third-party code (e.g.
    pickle) continues to work.  Classes that additionally inherit from
    :class:`Registry` receive a :class:`_RegistryProxy` in parallel mode, which adds
    claim/wait semantics to eliminate race conditions between concurrent jobs sharing
    a cache key.

    ``ray.remote`` arguments (e.g. ``num_cpus``, ``max_restarts``,
    ``max_task_retries``) can be passed directly to this decorator::

        @rez.parallelize(num_cpus=2, max_restarts=-1)
        def heavy_task(x: int) -> int:
            ...

    Memoisation ensures that the internal ``ray.remote`` wrapping is performed at most
    once per unique set of Ray arguments, keeping overhead minimal across repeated
    instantiations.

    .. Warning::
        Direct attribute access on parallelized class instances (e.g. ``o.attribute``) is
        not transparent between serial and parallel modes.  In serial mode ``o.attribute``
        returns the current value because ``o`` is a plain Python object.  In
        parallel mode it returns the value frozen at construction time, because the
        local mirror is never updated to reflect mutations that happen on the remote
        actor.  All reads of mutable state must go through a method call resolved
        with :func:`retrieve`::

            # Correct in both modes.
            value = rez.retrieve(o.attribute_getter())

            # Broken in parallel mode, returns the construction-time snapshot.
            value = o.attribute

    :param callable_obj: The function or class to parallelize.  When the decorator is
        used *without* parentheses this is the decorated object; when used *with*
        parentheses it is ``None`` and the Ray arguments are captured in *ray_args* /
        *ray_kwargs*.
    :type callable_obj: Optional[F], optional
    :param ray_args: Positional arguments forwarded to ``ray.remote``.
    :param ray_kwargs: Keyword arguments forwarded to ``ray.remote``.
    :return: The wrapped callable (behaves identically to the original in both serial
        and parallel mode).
    :rtype: F

    Examples::

        import ray_ease as rez

        rez.init()

        # Decorate a function - no parentheses needed when no Ray args.
        @rez.parallelize
        def square(x: int) -> int:
            return x * x

        futures = [square(i) for i in range(8)]
        results = rez.retrieve(futures)

        # Decorate a class with Ray actor options.
        @rez.parallelize(max_restarts=-1)
        class Counter:
            def __init__(self) -> None:
                self.count = 0

            def increment(self) -> None:
                self.count += 1

            def value(self) -> int:
                return self.count

        c = Counter()
        c.increment()
        print(rez.retrieve(c.value()))  # 1

        # Registry subclass - claim/wait proxy injected automatically.
        @rez.parallelize(max_restarts=-1, max_task_retries=-1)
        class UIDRegistry(rez.Registry):
            def __init__(self) -> None:
                self.finished_uids: dict[str, str] = {}

            def get(self, uid: str, default: Any = None) -> Any:
                return self.finished_uids.get(uid, default)

            def add_uid(self, uid: str, value: str) -> None:
                self.finished_uids[uid] = value

            def contains(self, uid: str) -> bool:
                return uid in self.finished_uids
    """

    class _Wrapper:
        def __init__(self, callable_obj: F, *args: Any, **kwargs: Any) -> None:
            self.callable_obj = callable_obj
            self.args = args
            self.kwargs = kwargs

            self.memoization: dict[Any, Any] = {}

        def __call__(self, *usage_args: Any, **usage_kwargs: Any) -> Any:
            key = self.args + tuple(sorted(self.kwargs.items()))

            if key not in self.memoization:
                self.memoization[key] = _parallelize(self.callable_obj, *self.args, **self.kwargs)

            return self.memoization[key](*usage_args, **usage_kwargs)

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
    parallel_progress_kwargs: dict[str, Any] = {},
) -> Iterable[Any]: ...


# In both serial and ray, for Actor.method()
@overload
def retrieve(
    object_refs: Any,
    ordered: bool = False,
    parallel_progress: bool = False,
    parallel_progress_kwargs: dict[str, Any] = {},
) -> Any: ...


def retrieve(
    object_refs: Iterable[Any] | Any,
    ordered: bool = False,
    parallel_progress: bool = False,
    parallel_progress_kwargs: dict[str, Any] = {},
) -> Iterable[Any] | Any:
    """Resolve one or more Ray ``ObjectRef`` values returned by parallelized callables.

    In **serial mode** (``RAY_EASE="serial"``) the iterable or object is already fully
    evaluated, so this function returns it unchanged.

    In **parallel mode** (``RAY_EASE="ray"``) this function waits (cf. Ray anti-pattern
    https://docs.ray.io/en/latest/ray-core/patterns/ray-get-submission-order.html) for each
    future and collects its result. Two ordering modes are available:

    * ``ordered=False`` (default): uses :func:`ray.wait` internally so that the
      progress bar advances as *any* task completes.  The returned list is in
      **completion order** (fastest task first).
    * ``ordered=True``: also uses :func:`ray.wait` internally for live progress, but
      stores each result at its **original submission index** before returning.  This
      preserves the ordering expected by downstream code.

    :param object_refs: A single ``ObjectRef`` or an iterable of ``ObjectRef`` values
        as returned by decorated functions and class methods.
    :type object_refs: Iterable[Any] | Any
    :param ordered: When ``True``, the returned list preserves submission order.
        Required when downstream code depends on result ordering. Defaults to ``False``.
    :type ordered: bool, optional
    :param parallel_progress: Display a :mod:`tqdm` progress bar while waiting for
        futures to complete.  Has no visible effect in serial mode because all work is
        already done by the time :func:`retrieve` is called.  Defaults to ``False``.
    :type parallel_progress: bool, optional
    :param parallel_progress_kwargs: Keyword arguments forwarded verbatim to
        :class:`tqdm.tqdm` (e.g. ``desc``, ``unit``, ``colour``).  The ``total`` key is
        always overridden with the number of futures.  Defaults to ``{}``.
    :type parallel_progress_kwargs: dict[str, Any], optional
    :return: Resolved results in completion order (``ordered=False``) or submission
        order (``ordered=True``), or the unchanged *object_refs* in serial mode.
    :rtype: Iterable[Any] | Any

    Examples::

        import ray_ease as rez

        rez.init()

        @rez.parallelize
        def work(x: int) -> int:
            return x * 2

        futures = [work(i) for i in range(10)]

        # Unordered - fastest result first, live progress bar.
        results = rez.retrieve(futures, parallel_progress=True, parallel_progress_kwargs={"desc": "work"})

        # Ordered - preserves submission order, live progress bar.
        results = rez.retrieve(futures, ordered=True, parallel_progress=True)
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

        progress = tqdm.tqdm(total=len(object_refs), **parallel_progress_kwargs)

        results: list[Any]
        if ordered:
            # Use ray.wait() so the bar advances as *any* task finishes, even when an
            # earlier submission is still running.  Results are stored at their original
            # index so the returned list preserves submission order.
            results = [None] * len(object_refs)
            unfinished: dict[Any, int] = {ref: i for i, ref in enumerate(object_refs)}
            while unfinished:
                finished_refs, _ = ray.wait(list(unfinished), num_returns=1)
                finished_ref = finished_refs[0]
                results[unfinished.pop(finished_ref)] = ray.get(finished_ref)
                progress.update()
        else:
            results = []
            unfinished_refs = list(object_refs)
            while unfinished_refs:
                # Returns the first ObjectRef that is ready.
                finished, unfinished_refs = ray.wait(unfinished_refs, num_returns=1)
                results.append(ray.get(finished[0]))
                progress.update()

        progress.close()
        return results

    return object_refs
