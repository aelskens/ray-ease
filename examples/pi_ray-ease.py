"""
Here is an example to compute pi using the ray-ease package. Therefore, the computations can
be performed either in serial or in parallel without changing the code itself.

To achieve a serial execution of the code, run the script as:
`python examples/pi_ray-ease.py -c serial`

To achieve a parallel execution of the code, run the script as:
`python examples/pi_ray-ease.py -c ray`

Additionally, one can change the number of batches used to compute pi by providing an argument
with the flag `-b`. For instance, to run the script in parallel with 100 batches:
`python examples/pi_ray-ease.py -c ray -b 100` 
"""

import argparse
import math
import random
import time
from fractions import Fraction

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
        "-b",
        "--batches",
        type=int,
        required=False,
        default=1000,
        help="The number of batches to do.",
    )

    return parser


@rez.parallelize
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

    rez.init(config=args.config)

    SAMPLE = 1000 * 1000
    BATCHES = args.batches

    print(f"Doing {BATCHES} batches in configuration: {args.config}.")
    results = []

    start = time.time()

    for _ in range(BATCHES):
        results.append(pi4_sample(sample_count=SAMPLE))
    output = rez.retrieve(results)

    end = time.time()
    duration = end - start
    print(f"Running {SAMPLE*BATCHES} tests took {duration:.2f} seconds.")

    pi = float(sum(output) * 4 / len(output))

    print("Approximation off by:", abs(pi - math.pi) / pi)
