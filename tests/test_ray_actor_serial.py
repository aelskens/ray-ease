import src.ray_ease as rez

rez.init("serial")


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
