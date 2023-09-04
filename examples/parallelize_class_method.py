"""
Here is an example to parallelize the method of a class rather than parallelizing the
entire class itself.

Note that this still works when executed in a serial manner by providing `-c serial` as argument.
"""

import argparse
import time
from typing import Dict

from psutil import Process

import ray_ease as rez


def create_parser() -> argparse.ArgumentParser:
    """Create the arg parser.
    For addition information: https://docs.python.org/3/library/argparse.html

    :return parser: The arg parser
    :rtype: argparse.ArgumentParser
    """

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-c",
        "--config",
        type=str,
        required=True,
        help="The configuration in which the script will run. It can either be `ray` to run the "
        "application in parallel or `serial`.",
    )
    parser.add_argument(
        "-r",
        "--range",
        type=int,
        required=False,
        default=100,
        help="The size of the ranging dictionary to use.",
    )

    return parser


class SomeStuff:
    def __init__(self) -> None:
        self.outputs = {}

    @rez.parallelize
    def create_subdict(self, key: int) -> Dict[int, int]:
        time.sleep(1)
        cpu_id = Process().cpu_num()
        return {key: cpu_id}

    def populate(self, d: Dict[int, int]) -> None:
        tmp = rez.retrieve([__class__.create_subdict(self, k) for k in d.keys()])
        for sd in tmp:
            self.outputs.update(sd)


if __name__ == "__main__":
    parser = create_parser()
    args = parser.parse_args()

    rez.init(config=args.config)

    print(f"Running in configuration: {args.config}.")

    o = SomeStuff()
    d = {i: i for i in range(args.range)}

    start = time.time()

    o.populate(d)

    end = time.time()
    duration = end - start
    print(f"Execution time of the class' method: {duration:.2f} seconds.")

    print(f"Object's output (i: cpu_id): {o.outputs}")
