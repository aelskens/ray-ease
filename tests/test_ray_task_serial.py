import src.ray_ease as rez

rez.init("serial")


@rez.parallelize
def square(x):
    return x * x


def test_ordered():
    futures = [square(i) for i in range(4)]

    assert rez.retrieve(futures, ordered=True) == [0, 1, 4, 9]


def test_unordered():
    futures = [square(i) for i in range(4)]

    assert sorted(rez.retrieve(futures)) == [0, 1, 4, 9]
