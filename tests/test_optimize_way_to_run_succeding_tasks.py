from time import sleep

import src.ray_ease as rez

rez.init(num_cpus=4)


@rez.parallelize
class UIDRegistry:
    def __init__(self):
        self.finished_uids = set()

    def add_uid(self, uid):
        if uid not in self.finished_uids:
            self.finished_uids.add(uid)

    def contains(self, uid):
        return uid in self.finished_uids


def foo(s):
    sleep(1)
    return s


@rez.parallelize
def bar(s, registry):
    s1, s2 = s.split("_")
    if not rez.retrieve(registry.contains(s1)):
        result = foo(s1)
        registry.add_uid(s1)
        result += "_" + foo(s2)
        registry.add_uid(s)
        return result
    elif not rez.retrieve(registry.contains(s)):
        result = s1 + "_" + foo(s2)
        registry.add_uid(s)
        return result


def test_optimize_way_to_run_repeated_succeeding_tasks():
    futures = []
    registry = UIDRegistry()
    for i in [f"{j}_{i}" for i in range(8) for j in ("a", "b", "c", "d", "e", "f")]:
        futures.append(bar(i, registry))

    assert rez.retrieve(futures, ordered=True, parallel_progress=True, parallel_progress_kwargs={"desc": "TESTS"}) == [
        f"{j}_{i}" for i in range(8) for j in ("a", "b", "c", "d", "e", "f")
    ]
