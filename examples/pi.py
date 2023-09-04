"""
Here is an example to compute pi in a serial manner.

The number of the batches can be changed with the `-b` argument flag. For instance, here is the
command to compute pi over 100 batches:
`python examples/pi.py -b 100`
"""

import argparse
import math
import random
import time
from fractions import Fraction


def create_parser() -> argparse.ArgumentParser:
    """Create the arg parser.
    For addition information: https://docs.python.org/3/library/argparse.html

    :return parser: The arg parser
    :rtype: argparse.ArgumentParser
    """

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-b",
        "--batches",
        type=int,
        required=False,
        default=1000,
        help="The number of batches to do.",
    )

    return parser


def pi4_sample(sample_count):
    """pi4_sample runs sample_count experiments, and returns the
    fraction of time it was inside the circle.
    """

    in_count = 0

    for _ in range(sample_count):
        x = random.random()
        y = random.random()
        if x * x + y * y <= 1:
            in_count += 1

    return Fraction(in_count, sample_count)


if __name__ == "__main__":
    parser = create_parser()
    args = parser.parse_args()

    SAMPLE = 1000 * 1000
    BATCHES = args.batches

    print(f"Doing {BATCHES} batches in a serial manner.")
    output = []

    start = time.time()

    for _ in range(BATCHES):
        output.append(pi4_sample(sample_count=SAMPLE))

    end = time.time()
    duration = end - start
    print(f"Running {SAMPLE*BATCHES} tests took {duration:.2f} seconds.")

    pi = float(sum(output) * 4 / len(output))

    print("Approximation off by:", abs(pi - math.pi) / pi)
