"""Tests for @rez.parallelize applied to classes (Ray actors) in parallel mode.

Covers basic actor instantiation, method calls, and result retrieval via
rez.retrieve() for a stateful counter actor.
"""

import src.ray_ease as rez

rez.init()


@rez.parallelize
class Counter:
    def __init__(self):
        self.i = 0

    def get(self):
        return self.i

    def incr(self, value):
        self.i += value


def test_Counter():
    c = Counter()
    for _ in range(10):
        c.incr(1)

    assert rez.retrieve(c.get()) == 10
