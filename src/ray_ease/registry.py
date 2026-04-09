"""Registry abstraction for shared result caches in ray-ease.

This module defines two components:

* :class:`Registry`: an abstract base class that formalises the contract for
  result caches shared across parallel jobs.  Subclasses decorated with
  :func:`ray_ease.parallelize` automatically receive claim/wait semantics in
  parallel mode via :class:`_RegistryProxy`.

* :class:`_RegistryProxy`: an internal transparent proxy that wraps a
  :class:`Registry` Ray actor handle and adds in-progress tracking.  When a
  job calls :meth:`~_RegistryProxy.get` for a UID that is currently being
  computed by another job, the proxy blocks on a
  :class:`ray.util.queue.Queue` until :meth:`~_RegistryProxy.add_uid` fires
  the result, preventing concurrent jobs from duplicating work.

Both classes are consumed by :mod:`ray_ease.core`; end-users interact with
:class:`Registry` directly (as a base class) but never instantiate
:class:`_RegistryProxy` themselves.
"""

from abc import ABC, abstractmethod
from typing import Any

import ray


class Registry(ABC):
    """Abstract base class for result caches shared across parallel jobs.

    Inherit from this class (together with ``@rez.parallelize``) to receive automatic
    claim/wait semantics in parallel mode with **zero changes** to the calling code.
    In serial mode the instance is used as-is with no proxy overhead.

    The three abstract methods, :meth:`get`, :meth:`add_uid`, and :meth:`contains`.

    When ``@rez.parallelize`` wraps a class that inherits from :class:`Registry` it
    injects a :class:`_RegistryProxy` in parallel mode.  The proxy tracks which UIDs
    are currently being computed (in-progress) and makes concurrent jobs that request
    the same UID block on :meth:`get` rather than proceeding to duplicate work.

    Usage example::

        import ray_ease as rez

        @rez.parallelize(max_restarts=-1, max_task_retries=-1)
        class UIDRegistry(rez.Registry):
            def __init__(self, checkpoint_file: str) -> None:
                self.finished_uids: dict[str, str] = {}
                # ... load from checkpoint ...

            def get(self, uid: str, default: Any = None) -> Any:
                return self.finished_uids.get(uid, default)

            def add_uid(self, uid: str, value: str) -> None:
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
        """Store the result *value* under *uid* and mark it as completed.

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


class _RegistryProxy:
    """Transparent proxy for a :class:`Registry` Ray actor that adds claim/wait semantics.

    When a job calls :meth:`get` for a UID that is currently being computed by another
    job, this proxy blocks (using a :class:`ray.util.queue.Queue`) until the computing
    job calls :meth:`add_uid`.  This prevents the classic TOCTOU race where two jobs
    both observe a missing UID, both compute the same stage, and collide on the output
    value.

    The proxy is fully transparent: callers use :meth:`get`, :meth:`add_uid`,
    :meth:`contains`, and :meth:`claim` exactly as they would on the plain actor.

    .. Note::
        Unlike the generic :func:`~ray_ease.remote_as_local.remote_actor_as_local`
        wrapper whose methods return raw ``ObjectRef`` futures, all methods of this
        proxy call :func:`ray.get` internally and return concrete Python values
        directly.  Callers must therefore **not** wrap proxy method calls in
        :func:`ray_ease.retrieve`, passing an already-resolved value to
        :func:`ray_ease.retrieve` will raise a :class:`ValueError` at runtime.

        Correct::

            if not registry.contains(uid):
                registry.add_uid(uid, path)
            return registry.get(uid)

        Incorrect::

            if not rez.retrieve(registry.contains(uid)):  # ValueError

    :param actor: The underlying ``ray.remote`` actor handle for the
        :class:`Registry` subclass.
    """

    def __init__(self, actor: Any) -> None:
        self._actor = actor
        # Maps uid -> list of ray.util.queue.Queue waiting for that uid to complete.
        self._in_progress: dict[str, list] = {}

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

        return ray.get(self._actor.get.remote(uid, default))

    def claim(self, uid: str) -> bool:
        """Atomically claim *uid* so that only one job computes it.

        Returns ``True`` if the claim succeeded (no other job has claimed or
        completed this UID).  Returns ``False`` if *uid* is already done or currently
        in progress; in that case the caller should skip computation and call
        :meth:`get` to obtain the result once it is ready.

        :param uid: Cache key to claim.
        :type uid: str
        :return: ``True`` if the claim was granted, ``False`` otherwise.
        :rtype: bool
        """

        already_done: bool = ray.get(self._actor.contains.remote(uid))
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

        ray.get(self._actor.add_uid.remote(uid, value))
        # Wake up every waiter that blocked in get().
        for q in self._in_progress.pop(uid, []):
            q.put(value)

    def contains(self, uid: str) -> bool:
        """Return ``True`` if *uid* has been fully stored on the actor.

        :param uid: Cache key to check.
        :type uid: str
        :rtype: bool
        """

        return ray.get(self._actor.contains.remote(uid))

    def __getattr__(self, name: str) -> Any:
        # Delegate unknown attribute access to the underlying actor handle so
        # that the proxy remains transparent for methods not listed above.
        if name in ("_actor", "_in_progress"):
            raise AttributeError(name)
        return getattr(self._actor, name)
