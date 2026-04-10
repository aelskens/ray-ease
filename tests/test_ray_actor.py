"""Tests for @rez.parallelize applied to classes (Ray actors) in parallel mode.

Covers:
- Basic actor instantiation, method calls, and result retrieval via
  rez.retrieve().
- Direct attribute access raises AttributeError — the actor owns its state
  exclusively inside a remote worker process.
- A getter method reflects the current actor state, including mutations made
  after construction.
- Method calls do not require .remote() — the RemoteActorAsLocal wrapper
  handles this transparently.
"""

import pytest

import src.ray_ease as rez

rez.init()


@rez.parallelize
class Counter:
    def __init__(self, start: int = 0) -> None:
        self.i = start

    def get(self) -> int:
        return self.i

    def incr(self, value: int) -> None:
        self.i += value

    def double(self) -> None:
        self.i *= 2


def test_counter_increments_correctly() -> None:
    c = Counter()
    for _ in range(10):
        c.incr(1)
    assert rez.retrieve(c.get()) == 10


def test_counter_respects_initial_value() -> None:
    c = Counter(start=5)
    c.incr(3)
    assert rez.retrieve(c.get()) == 8


def test_counter_multiple_operations() -> None:
    c = Counter(start=2)
    c.incr(3)
    c.double()
    assert rez.retrieve(c.get()) == 10


def test_direct_attribute_access_raises() -> None:
    """Direct attribute access on the actor must raise AttributeError.

    The actor owns its state exclusively inside a remote worker process.
    All mutable state must be read through a method call resolved with
    rez.retrieve().
    """
    c = Counter(start=0)
    c.incr(1)
    with pytest.raises(AttributeError):
        _ = c.i


def test_getter_method_reflects_current_state() -> None:
    """A getter method must reflect mutations made after construction."""
    c = Counter(start=0)
    assert rez.retrieve(c.get()) == 0
    c.incr(5)
    assert rez.retrieve(c.get()) == 5
    c.double()
    assert rez.retrieve(c.get()) == 10


def test_method_callable_without_remote() -> None:
    """Actor methods must not require .remote() — the wrapper handles it."""
    c = Counter(start=0)
    # If .remote() were required, calling c.incr(1) directly would either
    # raise AttributeError or return a coroutine rather than executing.
    c.incr(1)
    assert rez.retrieve(c.get()) == 1
