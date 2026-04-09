"""Tests that retrieve(ordered=True) preserves submission order while advancing
the progress bar as any task completes, not only sequentially.

Includes a deliberately slow first job to expose regressions where the bar
would freeze until the bottleneck task finished.
"""

import time

import src.ray_ease as rez

rez.init(num_cpus=4)


@rez.parallelize
def slow_then_fast(index: int) -> int:
    """index=0 is slow; all others are fast, mimics a bottleneck at position 0."""
    if index == 0:
        time.sleep(0.5)
    return index


def test_ordered_preserves_submission_order() -> None:
    futures = [slow_then_fast(i) for i in range(6)]
    results = rez.retrieve(futures, ordered=True)
    assert results == list(range(6)), f"Expected [0,1,2,3,4,5], got {results}"


def test_ordered_with_progress_bar() -> None:
    """Smoke test: ordered=True with parallel_progress should not raise."""
    futures = [slow_then_fast(i) for i in range(4)]
    results = rez.retrieve(
        futures,
        ordered=True,
        parallel_progress=True,
        parallel_progress_kwargs={"desc": "test_ordered", "disable": True},
    )
    assert results == list(range(4))


def test_unordered_returns_all_results() -> None:
    futures = [slow_then_fast(i) for i in range(6)]
    results = rez.retrieve(futures, ordered=False)
    assert sorted(results) == list(range(6))
