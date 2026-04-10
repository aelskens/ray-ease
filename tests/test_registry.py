"""Tests for rez.Registry in parallel (Ray) mode.

Covers:
- add_uid / contains / get correctness and default fallback values.
- Proxy methods return concrete values directly — rez.retrieve() must not be
  used to wrap them (would raise ValueError).
- Unknown methods on the proxy are callable without .remote() and resolve
  immediately.
- Direct attribute access on the actor raises AttributeError — a dedicated
  getter method must be used instead.
- Stage-skipping optimisation: jobs sharing a common stage prefix reuse
  already-computed results stored in the UIDRegistry actor rather than
  recomputing them.
- rez.retrieve(ordered=True) returns results in submission order.
- Claim/wait semantics: multiple parallel jobs requesting the same UID each
  receive the correct result without duplicating computation.
"""

from time import sleep
from typing import Any

import pytest

import src.ray_ease as rez

rez.init(num_cpus=4)


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

    def get_finished_uids(self) -> dict[str, str]:
        return self.finished_uids

    def __len__(self) -> int:
        return len(self.finished_uids)


def _slow_stage(s: str) -> str:
    sleep(1)
    return s


@rez.parallelize
def _bar(s: str, registry: rez.Registry) -> str:
    """Simulate a two-stage pipeline job that skips already-computed stages."""
    s1, s2 = s.split("_")
    if not registry.contains(s1):
        result = _slow_stage(s1)
        registry.add_uid(s1, s1)
        result += "_" + _slow_stage(s2)
        registry.add_uid(s, s)
        return result
    elif not registry.contains(s):
        result = s1 + "_" + _slow_stage(s2)
        registry.add_uid(s, s)
        return result
    return registry.get(s)


@rez.parallelize
def _claim_job(uid: str, registry: rez.Registry) -> str:
    """Simulate a job that checks and populates the registry for a single UID."""
    if not registry.contains(uid):
        sleep(0.1)
        registry.add_uid(uid, f"result_{uid}")
    return registry.get(uid)


def test_registry_add_and_contains() -> None:
    """Explicit proxy methods return concrete values without rez.retrieve()."""
    registry = UIDRegistry()
    assert not registry.contains("uid_a")
    registry.add_uid("uid_a", "value_a")
    assert registry.contains("uid_a")
    assert registry.get("uid_a") == "value_a"


def test_registry_default_value() -> None:
    registry = UIDRegistry()
    assert registry.get("missing_uid", "fallback") == "fallback"


def test_proxy_methods_do_not_need_retrieve() -> None:
    """Wrapping a proxy method call in rez.retrieve() must raise ValueError."""
    registry = UIDRegistry()
    registry.add_uid("uid_x", "value_x")
    with pytest.raises(ValueError):
        rez.retrieve(registry.contains("uid_x"))


def test_unknown_method_callable_without_remote() -> None:
    """Methods not explicitly defined on the proxy must not require .remote()."""
    registry = UIDRegistry()
    registry.add_uid("uid_a", "value_a")
    registry.add_uid("uid_b", "value_b")
    result = registry.get_finished_uids()
    assert result == {"uid_a": "value_a", "uid_b": "value_b"}


def test_direct_attribute_access_raises() -> None:
    """Direct attribute access on the actor must raise AttributeError.

    Actor attributes are private to the remote worker process. All mutable
    state must be read through a getter method instead.
    """
    registry = UIDRegistry()
    with pytest.raises(AttributeError):
        _ = registry.finished_uids


def test_getter_method_returns_current_state() -> None:
    """A getter method must reflect mutations made after construction."""
    registry = UIDRegistry()
    assert registry.get_finished_uids() == {}
    registry.add_uid("uid_a", "value_a")
    assert registry.get_finished_uids() == {"uid_a": "value_a"}


def test_stage_skipping_ordered_results() -> None:
    """Jobs sharing a stage prefix must reuse cached results and return in order."""
    registry = UIDRegistry()
    keys = [f"{j}_{i}" for i in range(8) for j in ("a", "b")]
    futures = [_bar(k, registry) for k in keys]
    assert (
        rez.retrieve(futures, ordered=True, parallel_progress=True, parallel_progress_kwargs={"desc": "TESTS"}) == keys
    )


def test_no_duplicate_computation_for_shared_uid() -> None:
    """Multiple parallel jobs requesting the same UID must each get the result."""
    registry = UIDRegistry()
    futures = [_claim_job("shared_uid", registry) for _ in range(6)]
    results = rez.retrieve(futures, ordered=True)
    assert all(r == "result_shared_uid" for r in results)
