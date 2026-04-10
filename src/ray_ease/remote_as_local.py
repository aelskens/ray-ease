"""Utility for calling Ray actor methods with ordinary Python syntax.

The single public symbol exported from this module is
:func:`remote_actor_as_local`, a memoised factory that, given an unwrapped class,
returns a wrapper class whose instances forward every public method call to the
corresponding ``.remote()`` invocation on a live Ray actor handle.  Callers can
opt into blocking behaviour on a per-call basis via the ``block`` keyword argument
(default ``False``).

This module is an internal implementation detail of ray-ease; end-users interact
with it indirectly through :func:`ray_ease.parallelize`.
"""

import inspect
from functools import cache
from typing import Any, Callable

import ray
from ray.actor import ActorClass


@cache
def remote_actor_as_local(base_cls: Callable[..., Any]) -> Callable[..., Any]:
    """Return a wrapper class that makes a Ray actor look like a local object.

    The returned class inherits from *base_cls* and re-binds every public method
    so it can be called without ``.remote()`` syntax.  Dunder methods (e.g.
    ``__reduce__``) are forwarded directly to the underlying actor handle so that
    third-party code such as pickle continues to work transparently.

    Results are decorated by default with ``block=False`` (i.e. they return a Ray
    ``ObjectRef``).  Pass ``block=True`` to any method call to block until the
    remote call returns its value directly.

    The function is memoised via :func:`functools.cache` so that the dynamic class
    is created only once per unique *base_cls*, regardless of how many actor
    instances are wrapped.

    Adapted from
    https://github.com/JaneliaSciComp/ray-janelia/blob/main/remote_as_local_wrapper.py.

    :param base_cls: The original (unwrapped) class whose methods will be
        re-bound for remote-as-local calls.
    :type base_cls: Callable[..., Any]
    :return: A ``RemoteActorAsLocal`` class whose constructor accepts the remote
        actor handle as its first argument followed by the usual *base_cls*
        constructor arguments.
    :rtype: Callable[..., Any]

    Example::

        @ray.remote
        class Counter:
            def __init__(self, base_count: int) -> None:
                self._count = base_count

            def increment(self, inc: int = 1) -> None:
                self._count += inc

            def get_count(self) -> int:
                return self._count

        actor = Counter.remote(base_count=0)

        WrapperCls = remote_actor_as_local(Counter)
        counter = WrapperCls(actor, base_count=0)

        # Call methods without .remote():
        counter.increment(inc=2)

        # Block until result is ready:
        value = counter.get_count(block=True)   # returns 2

        # Or get a future and resolve later:
        future = counter.get_count(block=False)
        value = ray.get(future)                 # also 2
    """

    class RemoteActorAsLocal(base_cls):
        """Wrapper that allows calling Ray actor methods as if they were local.

        Instances of this class hold a reference to a remote actor and expose its
        public methods via ordinary Python call syntax.  Each method accepts an
        additional ``block`` keyword argument (default ``False``):

        * ``block=False``: returns a Ray ``ObjectRef`` (future) immediately.
        * ``block=True``: calls :func:`ray.get` and returns the resolved value.

        This class is generated dynamically by :func:`remote_actor_as_local` and
        inherits from the original (unwrapped) class so that ``isinstance`` checks
        continue to work as expected.

        .. Warning::
            This wrapper does **not** call ``super().__init__()`` and therefore holds no
            local copy of the actor's attributes.  Direct attribute access (e.g.
            ``o.attribute``) will raise :class:`AttributeError` in parallel mode, while
            it works as expected in serial mode because ``o`` is then a plain Python
            object.  This asymmetry is an inherent limitation of the remote-actor model,
            the actor owns its state exclusively and mutations inside its methods are
            not observable from outside.  All reads of mutable state must go through a
            method call resolved with :func:`ray_ease.retrieve`.
        """

        def __init__(self, remote_handle: ActorClass, *args: Any, **kwargs: Any) -> None:
            """Wrap a remote actor handle.

            :param remote_handle: The Ray actor handle to wrap (obtained from
                ``SomeClass.remote(...)``).
            :type remote_handle: ray.actor.ActorClass
            :param args: Positional arguments forwarded to *base_cls.__init__* so
                that the local mirror has the same attribute state as the actor.
            :param kwargs: Keyword arguments forwarded to *base_cls.__init__*.
            """

            object.__setattr__(self, "_remote_handle", remote_handle)

            def _remote_caller(method_name: str) -> Callable[..., Any]:
                """Create a local-call shim for *method_name* on the remote actor."""

                def _wrapper(*args: Any, block: bool = False, **kwargs: Any) -> Any:
                    obj_ref = getattr(self._remote_handle, method_name).remote(*args, **kwargs)

                    # Block until called method returns.
                    if block:
                        return ray.get(obj_ref)

                    # Don't block and return a future.
                    return obj_ref

                return _wrapper

            for name, _ in inspect.getmembers(self._remote_handle):
                if not name.startswith("__"):
                    # Re-bind public methods as local-call shims.
                    setattr(self, name, _remote_caller(name))
                else:
                    # Forward dunder members directly to the actor handle so that
                    # third-party code (e.g. pickle) continues to work.
                    # e.g.: self.__reduce__ = self._remote_handle.__reduce_
                    setattr(self, name, getattr(self._remote_handle, name))

        def __dir__(self) -> list[str]:
            return dir(self._remote_handle)

    return RemoteActorAsLocal
