"""Registry abstraction for shared result caches in ray-ease.

This module defines two components:

* :class:`Registry`: an abstract base class that formalises the contract for
  result caches shared across parallel jobs.  Subclasses decorated with
  :func:`ray_ease.parallelize` automatically receive claim/wait semantics in
  parallel mode via :func:`registry_as_proxy`.

* :func:`registry_as_proxy`: a memoised factory that, given a :class:`Registry`
  subclass, returns a ``RegistryProxy`` class whose instances wrap a live Ray actor
  handle and add in-progress tracking.  When a job calls
  :meth:`~RegistryProxy.get` for a UID that is currently being computed by another
  job, the proxy blocks on a :class:`ray.util.queue.Queue` until
  :meth:`~RegistryProxy.add_uid` fires the result, preventing concurrent jobs from
  duplicating work.

Both are consumed by :mod:`ray_ease.core`; end-users interact with
:class:`Registry` directly (as a base class) but never instantiate a
``RegistryProxy`` themselves.
"""

import inspect
from abc import ABC, abstractmethod
from functools import cache
from typing import Any, Callable

import ray


class Registry(ABC):
    """Abstract base class for result caches shared across parallel jobs.

    Inherit from this class together with ``@rez.parallelize`` to receive automatic
    claim/wait semantics in parallel mode with **zero changes** to the calling code.
    In serial mode the instance is used as-is with no proxy overhead.

    When ``@rez.parallelize`` wraps a class that inherits from :class:`Registry` it
    calls :func:`registry_as_proxy` to generate a ``RegistryProxy`` in parallel mode.
    The proxy tracks which UIDs are currently being computed and makes concurrent jobs
    that request the same UID block on :meth:`get` rather than proceeding to duplicate
    work.

    Usage example::

        import ray_ease as rez

        @rez.parallelize(max_restarts=-1, max_task_retries=-1)
        class UIDRegistry(rez.Registry):
            def __init__(self, checkpoint_file: str) -> None:
                self.finished_uids: dict[str, str] = {}
                # ... load from checkpoint ...

            def get(self, uid: str, default: Any = None) -> Any:
                return self.finished_uids.get(uid, default)

            def add_uid(self, uid: str, value: Any) -> None:
                self.finished_uids[uid] = value
                # ... persist to disk ...

            def contains(self, uid: str) -> bool:
                return uid in self.finished_uids
    """

    @abstractmethod
    def get(self, uid: str, default: Any = None) -> Any:
        """Return the cached value for *uid*, or *default* if not yet stored.

        :param uid: Unique identifier for the cached entry.
        :type uid: str
        :param default: Fallback value when *uid* is absent, defaults to ``None``.
        :return: The cached value, or *default*.
        """
        ...

    @abstractmethod
    def add_uid(self, uid: str, value: Any) -> None:
        """Store *value* under *uid* and mark it as completed.

        Implementations should persist the entry so it survives actor restarts.

        :param uid: Unique identifier for the cached entry.
        :type uid: str
        :param value: The value to store.
        :type value: Any
        """
        ...

    @abstractmethod
    def contains(self, uid: str) -> bool:
        """Return ``True`` if *uid* has been fully computed and stored.

        :param uid: Unique identifier to check.
        :type uid: str
        :rtype: bool
        """
        ...


@cache
def registry_as_proxy(base_cls: Callable[..., Any]) -> Callable[..., Any]:
    """Return a proxy class that wraps a :class:`Registry` Ray actor with claim/wait semantics.

    The returned ``RegistryProxy`` class inherits from :class:`Registry` (not from
    *base_cls*) to avoid inheriting concrete attributes and methods that reference
    actor-side state which does not exist locally.  It exposes the full
    :class:`Registry` interface (:meth:`~RegistryProxy.get`,
    :meth:`~RegistryProxy.add_uid`, :meth:`~RegistryProxy.contains`,
    :meth:`~RegistryProxy.claim`) as blocking calls that resolve immediately without
    :func:`ray_ease.retrieve`.  Unknown public methods are forwarded transparently via
    :meth:`~RegistryProxy.__getattr__`.  Dunder methods defined on *base_cls* (e.g.
    ``__len__``, ``__repr__``, ``__str__``) are set on the generated class so that
    Python's type-level dunder lookup finds them correctly.

    The function is memoised via :func:`functools.cache` so that the dynamic class is
    created only once per unique *base_cls*, regardless of how many actor instances are
    wrapped.

    :param base_cls: The original (unwrapped) :class:`Registry` subclass.
    :type base_cls: Callable[..., Any]
    :return: A ``RegistryProxy`` class whose constructor accepts the remote actor handle
        as its sole argument.
    :rtype: Callable[..., Any]

    Example::

        @rez.parallelize(max_restarts=-1, max_task_retries=-1)
        class UIDRegistry(rez.Registry):
            def __init__(self) -> None:
                self.finished_uids: dict[str, str] = {}

            def __len__(self) -> int:
                return len(self.finished_uids)

            def get(self, uid: str, default: Any = None) -> Any:
                return self.finished_uids.get(uid, default)

            def add_uid(self, uid: str, value: Any) -> None:
                self.finished_uids[uid] = value

            def contains(self, uid: str) -> bool:
                return uid in self.finished_uids

        registry = UIDRegistry()
        registry.add_uid("uid_a", "path_a")

        # Registry methods resolve immediately without rez.retrieve():
        assert registry.contains("uid_a")
        assert registry.get("uid_a") == "path_a"

        # Forwarded dunders work transparently:
        assert len(registry) == 1
    """

    class RegistryProxy(Registry):
        """Proxy that wraps a :class:`Registry` Ray actor with claim/wait semantics.

        Instances hold a reference to the remote actor and expose the full
        :class:`Registry` interface as blocking calls that return concrete Python
        values directly.  Unknown public methods are forwarded transparently via
        :meth:`__getattr__`.  Dunder methods defined on the actor class are bound
        on this generated class so that Python's type-level dunder lookup finds them.

        .. warning::
            Direct attribute access on the underlying actor (e.g.
            ``registry.finished_uids``) is not supported.  Ray actors own their state
            exclusively inside a remote worker process and plain attributes are not
            accessible from outside.  All reads of mutable state must go through an
            explicit getter method, which can be called directly without
            :func:`ray_ease.retrieve` since all methods on this proxy resolve
            immediately::

                # Correct
                value = registry.get_finished_uids()

                # Not supported: the attribute does not exist on the actor handle
                value = registry.finished_uids

        .. note::
            Unlike the generic :func:`~ray_ease.remote_as_local.remote_actor_as_local`
            wrapper whose methods return raw ``ObjectRef`` futures, all methods of this
            proxy call :func:`ray.get` internally and return concrete Python values
            directly.  Callers must therefore **not** wrap proxy method calls in
            :func:`ray_ease.retrieve`, passing an already-resolved value to
            :func:`ray_ease.retrieve` will raise a :class:`ValueError` at runtime::

                # Correct
                if not registry.contains(uid):
                    registry.add_uid(uid, path)
                return registry.get(uid)

                # Incorrect: raises ValueError
                if not rez.retrieve(registry.contains(uid)):
                    ...
        """

        def __init__(self, remote_handle: Any) -> None:
            """Wrap a remote :class:`Registry` actor handle.

            :param remote_handle: The Ray actor handle to wrap (obtained from
                ``SomeClass.remote(...)``).
            :type remote_handle: ray.actor.ActorClass
            """

            object.__setattr__(self, "_remote_handle", remote_handle)
            # Maps uid -> list of ray.util.queue.Queue waiting for that uid to complete.
            object.__setattr__(self, "_in_progress", {})

        def get(self, uid: str, default: Any = None) -> Any:
            """Return the cached value for *uid*, blocking if a claim is in progress.

            If another job has claimed *uid* (via :meth:`claim`) but has not yet called
            :meth:`add_uid`, this call blocks until the result is available and then
            returns it directly, ensuring at-most-one computation per UID.

            :param uid: Cache key to look up.
            :type uid: str
            :param default: Value to return if *uid* is absent and not in progress.
            :return: The stored result, or *default*.
            """

            if uid in self._in_progress:
                # Another job is computing this uid - wait for it to finish.
                from ray.util.queue import Queue

                q: Queue = Queue(maxsize=1)
                self._in_progress[uid].append(q)
                return q.get(block=True)

            return ray.get(self._remote_handle.get.remote(uid, default))

        def claim(self, uid: str) -> bool:
            """Atomically claim *uid* so that only one job computes it.

            Returns ``True`` if the claim succeeded (no other job has claimed or
            completed this UID).  Returns ``False`` if *uid* is already done or
            currently in progress; in that case the caller should skip computation
            and call :meth:`get` to obtain the result once it is ready.

            :param uid: Cache key to claim.
            :type uid: str
            :return: ``True`` if the claim was granted, ``False`` otherwise.
            :rtype: bool
            """

            already_done: bool = ray.get(self._remote_handle.contains.remote(uid))
            if already_done or uid in self._in_progress:
                return False

            self._in_progress[uid] = []

            return True

        def add_uid(self, uid: str, value: Any) -> None:
            """Store *value* under *uid* on the actor and unblock all waiting jobs.

            :param uid: Cache key to store.
            :type uid: str
            :param value: The computed result.
            :type value: Any
            """

            ray.get(self._remote_handle.add_uid.remote(uid, value))
            # Wake up every waiter that blocked in get().
            for q in self._in_progress.pop(uid, []):
                q.put(value)

        def contains(self, uid: str) -> bool:
            """Return ``True`` if *uid* has been fully stored on the actor.

            :param uid: Cache key to check.
            :type uid: str
            :rtype: bool
            """

            return ray.get(self._remote_handle.contains.remote(uid))

        def __getattr__(self, name: str) -> Any:
            if name in ("_remote_handle", "_in_progress"):
                raise AttributeError(name)

            attr = getattr(self._remote_handle, name)

            # Wrap remote methods so they can be called without .remote() and
            # resolve immediately, consistent with the explicit methods above.
            if callable(attr):

                def _resolved(*args: Any, **kwargs: Any) -> Any:
                    return ray.get(attr.remote(*args, **kwargs))

                return _resolved

            return attr

        def __dir__(self) -> list[str]:
            return dir(self._remote_handle)

    # Forward dunder methods from base_cls onto RegistryProxy at the class
    # level. Instance-level setattr is silently ignored for dunders
    # (e.g. len(obj) looks up type(obj).__len__, not obj.__len__), so this must
    # be done on the class.
    _EXCLUDED_DUNDERS = {"__init__", "__class__", "__dict__", "__weakref__"}

    def _make_dunder(method_name: str) -> Callable[..., Any]:
        """Create a dunder shim that forwards the call to the remote actor."""

        def _dunder(self: Any, *args: Any, **kwargs: Any) -> Any:
            return ray.get(getattr(self._remote_handle, method_name).remote(*args, **kwargs))

        return _dunder

    for name, _ in inspect.getmembers(base_cls, predicate=inspect.isfunction):
        if name.startswith("__") and name.endswith("__") and name not in _EXCLUDED_DUNDERS:
            setattr(RegistryProxy, name, _make_dunder(name))

    return RegistryProxy
