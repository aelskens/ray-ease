import inspect
from functools import cache
from typing import Any, Callable

import ray
from ray.actor import ActorClass


@cache
def remote_actor_as_local(base_cls: Callable[..., Any]) -> Callable[..., Any]:
    """Closure to give a base class from which the RemoteActorAsLocal wrapper will inherite.

    :param base_cls: The base class which is transformed into a remote actor and then wrapped by
    the RemoteActorAsLocal class.
    :type base_cls: Callable[..., Any]
    :return: The RemoteActorAsLocal wrapping class that has inherited from the base class.
    :rtype: Callable[..., Any]
    """

    class RemoteActorAsLocal(base_cls):
        """This wrapper allows calling methods of remote Ray actors (e.g. classes decorated with
        @ray.remote) as if they were local. It can be used to wrap classes from external libraries
        to simplify their integration with Ray.

        Adapted from:
        https://github.com/JaneliaSciComp/ray-janelia/blob/main/remote_as_local_wrapper.py.

        Example:
        @ray.remote
        class Counter():
            def __init__(self, base_count):
                self._counts = base_count
            def increment(self, inc=1):
                self._counts += inc
            def get_counts(self):
                return self._counts

        # Normal Ray usage (without this wrapper):
        kwargs = {"base_count": 0}
        counter = Counter.remote(**kwargs)  # Instantiate as remote.
        counter.increment.remote(inc=2)  # Call as remote.
        obj_ref = counter.get_counts.remote()  # Call as remote; returns a future.
        ray.get(obj_ref)  # Blocks and returns 2.

        # Using Ray with this wrapper:
        kwargs = {"base_count": 0}
        counter = Counter.remote(**kwargs)  # Instantiate as remote.
        wrapper = remote_actor_as_local(Counter)
        counter = wrapper(counter, **kwargs)  # Wrap.
        counter.increment(inc=2)  # Call as local.

        # Can be called to either return a future or block until call returns (the
        # latter is the default behavior):
        obj_ref = counter.get_counts(block=False)  # Call as local; returns a future.
        counter.get_counts(block=True)  # Call as local; blocks and returns 2.
        """

        def __init__(self, remote_handle: ActorClass, *args: Any, **kwargs: Any) -> None:
            """Constructor of the Wrapper class.

            :param remote_handle: The remote actor to wrap.
            :type remote_handle: ray.actor.ActorClass
            """

            # This is used to have the same attributes as the base class.
            super().__init__(*args, **kwargs)

            self._remote_handle = remote_handle

            def _remote_caller(method_name: str):
                """Wrap the remote class's method to mimic local calling."""

                def _wrapper(*args: Any, block: bool = False, **kwargs: Any):
                    obj_ref = getattr(self._remote_handle, method_name).remote(*args, **kwargs)

                    # Block until called method returns.
                    if block:
                        return ray.get(obj_ref)
                    # Don't block and return a future.
                    else:
                        return obj_ref

                return _wrapper

            for name, _ in inspect.getmembers(self._remote_handle):
                # Wrap public methods for remote-as-local calls.
                if not name.startswith("__"):
                    setattr(self, name, _remote_caller(name))
                # Reassign dunder members for API-unaware callers (e.g. pickle).
                # For example, it is doing the following reassignment:
                # self.__reduce__ = self._remote_handle.__reduce__
                else:
                    setattr(self, name, getattr(self._remote_handle, name))

        def __dir__(self):
            return dir(self._remote_handle)

    return RemoteActorAsLocal
