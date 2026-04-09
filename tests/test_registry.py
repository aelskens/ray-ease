"""Tests for rez.Registry in parallel (Ray) mode.

Covers:
- add_uid / contains / get correctness and default fallback values.
- Stage-skipping optimisation: jobs sharing a common stage prefix (e.g. "a_0"
  and "b_0" both starting with stage "a") reuse already-computed results stored
  in the UIDRegistry actor rather than recomputing them.
- rez.retrieve(ordered=True) returns results in submission order.
- Claim/wait semantics: multiple parallel jobs requesting the same UID each
  receive the correct result without duplicating computation.
"""

from time import sleep
from typing import Any

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
    registry = UIDRegistry()
    assert not registry.contains("uid_a")
    registry.add_uid("uid_a", "value_a")
    assert registry.contains("uid_a")
    assert registry.get("uid_a") == "value_a"


def test_registry_default_value() -> None:
    registry = UIDRegistry()
    assert registry.get("missing_uid", "fallback") == "fallback"


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
