import inspect
from typing import Any, Iterable

import ray
from ray.actor import ActorClass


class RemoteActorAsLocal:
    """This wrappers allows calling methods of remote Ray actors (e.g. classes decorated with
    @ray.remote) as if they were local. It can be used to wrap classes from external libraries
    to simplify their integration with Ray.

    Adapted from:
    https://github.com/JaneliaSciComp/ray-janelia/blob/main/remote_as_local_wrapper.py.

    Example:
    @ray.remote
    class Counter():
        def __init__(self):
            self._counts = 0
        def increment(self, inc=1):
            self._counts += inc
        def get_counts(self):
            return self._counts

    # Normal Ray usage (without this wrapper):
    counter = Counter.remote()  # Instantiate as remote.
    counter.increment.remote(inc=2)  # Call as remote.
    obj_ref = counter.get_counts.remote()  # Call as remote; returns a future.
    ray.get(obj_ref)  # Blocks and returns 2.

    # Using Ray with this wrapper:
    counter = Counter.remote()  # Instantiate as remote.
    counter = RemoteActorAsLocal(counter)  # Wrap.
    counter.increment(inc=2)  # Call as local.

    # Can be called to either return a future or block until call returns (the
    # latter is the default behavior):
    obj_ref = counter.get_counts(block=False)  # Call as local; returns a future.
    counter.get_counts(block=True)  # Call as local; blocks and returns 2.
    """

    def __init__(self, remote_handle: ActorClass) -> None:
        """Constructor of the Wrapper class.

        :param remote_handle: The remote actor to wrap.
        :type remote_handle: ray.actor.ActorClass
        """

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


def retrieve_parallel_loop(loop: Iterable[Any]) -> Iterable[Any]:
    """Retrieve the results from a pseudo-parallelized loop. It is a pseudo-parallelized rather than a
    parallelized loop because if Ray is not initialized, then the loop is serial instead.

    :param loop: The pseudo-parallelized loop.
    :type loop: Iterable[Any]
    :return: The resulting iterable.
    :rtype: Iterable[Any]
    """

    if ray.is_initialized():
        return ray.get(loop)

    return loop
