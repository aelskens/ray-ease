"""
Here is an example to illustrate the use of rez.Registry for shared result caching.

Consider a multi-stage image registration pipeline where each job applies a
sequence of stages to an image pair.  Several jobs may share an identical
first stage (same algorithm, same image pair) and should therefore reuse the
already-computed result rather than repeat the work.

The registry maps a stage UID (derived from the inputs and all preceding stage
outputs) to the output path produced by that stage.  When a job queries a UID
that is currently being computed by another job, the claim/wait proxy injected
by @rez.parallelize blocks the caller until the result is available, preventing
duplicate computation.

Note that this still works when executed in a serial manner by providing `-c serial` as argument.
"""

import argparse
import hashlib
import time
from typing import Any

import ray_ease as rez


def create_parser() -> argparse.ArgumentParser:
    """Create the argument parser.

    :return: The argument parser.
    :rtype: argparse.ArgumentParser
    """

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-c",
        "--config",
        type=str,
        required=True,
        help="Execution mode: `ray` for parallel or `serial` for sequential.",
    )

    return parser


@rez.parallelize(max_restarts=-1, max_task_retries=-1)
class UIDRegistry(rez.Registry):
    """Persistent cache mapping stage UIDs to their output paths.

    In parallel mode @rez.parallelize wraps this actor in a _RegistryProxy
    that adds claim/wait semantics: a job that queries a UID currently being
    computed by another job blocks until the result is ready, rather than
    proceeding to duplicate the work.
    """

    def __init__(self) -> None:
        self.finished_uids: dict[str, str] = {}

    def get(self, uid: str, default: Any = None) -> Any:
        return self.finished_uids.get(uid, default)

    def add_uid(self, uid: str, path: str) -> None:
        self.finished_uids[uid] = path

    def contains(self, uid: str) -> bool:
        return uid in self.finished_uids


def _compute_uid(image: str, algorithm: str, previous_uids: list[str]) -> str:
    """Derive a deterministic UID from the stage inputs and all prior stage UIDs.

    :param image: Identifier of the image being processed.
    :type image: str
    :param algorithm: Name of the algorithm applied at this stage.
    :type algorithm: str
    :param previous_uids: UIDs of all stages that preceded this one in the
        pipeline, used to make the UID sensitive to the full history.
    :type previous_uids: list[str]
    :return: A short hexadecimal UID string.
    :rtype: str
    """

    payload = "|".join([image, algorithm] + previous_uids)
    return hashlib.md5(payload.encode()).hexdigest()[:12]


def _run_stage(image: str, algorithm: str, stage_idx: int) -> str:
    """Simulate running a single registration stage and return its output path.

    :param image: Identifier of the image being processed.
    :type image: str
    :param algorithm: Name of the algorithm applied at this stage.
    :type algorithm: str
    :param stage_idx: Zero-based index of this stage in the pipeline.
    :type stage_idx: int
    :return: The (simulated) output path produced by the stage.
    :rtype: str
    """

    time.sleep(0.5)  # simulate computation
    return f"outputs/{image}/stage{stage_idx}/{algorithm}.result"


@rez.parallelize
def run_pipeline(image: str, pipeline: tuple[str, ...], registry: Any) -> list[str]:
    """Run a full multi-stage pipeline on *image*, skipping cached stages.

    For each stage the UID is computed from the image identifier, the stage
    algorithm, and all preceding stage UIDs.  If the registry already holds a
    result for that UID the stage is skipped and the cached output path is
    reused.  Otherwise the stage is computed and its result is stored in the
    registry for future jobs to reuse.

    .. note::
        Because *registry* is a :class:`_RegistryProxy` in parallel mode, its
        methods (:meth:`~UIDRegistry.contains`, :meth:`~UIDRegistry.get`,
        :meth:`~UIDRegistry.add_uid`) return concrete Python values directly.
        Do **not** wrap them in :func:`rez.retrieve`.

    :param image: Identifier of the image to process.
    :type image: str
    :param pipeline: Sequence of algorithm names defining the pipeline stages.
    :type pipeline: tuple[str, ...]
    :param registry: The shared result cache (plain instance in serial mode,
        :class:`_RegistryProxy` in parallel mode).
    :return: Output paths produced by each stage, in order.
    :rtype: list[str]
    """

    output_paths: list[str] = []
    previous_uids: list[str] = []

    for stage_idx, algorithm in enumerate(pipeline):
        uid = _compute_uid(image, algorithm, previous_uids)

        if registry.contains(uid):
            output_path = registry.get(uid)
            print(f"  [SKIP]    image={image!r}  stage={stage_idx}  uid={uid}  (cached)")
        else:
            output_path = _run_stage(image, algorithm, stage_idx)
            registry.add_uid(uid, output_path)
            print(f"  [COMPUTE] image={image!r}  stage={stage_idx}  uid={uid}")

        output_paths.append(output_path)
        previous_uids.append(uid)

    return output_paths


if __name__ == "__main__":
    parser = create_parser()
    args = parser.parse_args()

    rez.init(config=args.config)

    # Two pipeline variants that share an identical first stage (same algorithm
    # applied to the same images).  In parallel mode the registry ensures that
    # the shared stage is computed only once across all jobs.
    images = ["img_A", "img_B", "img_C"]
    pipeline_variants: list[tuple[str, ...]] = [
        ("algo_0", "algo_1"),  # variant 0: two stages
        ("algo_0", "algo_2"),  # variant 1: shares stage 0 with variant 0
    ]

    registry = UIDRegistry()

    print(f"\nRunning in {args.config!r} mode.\n")
    start = time.time()

    futures = [run_pipeline(image, pipeline, registry) for image in images for pipeline in pipeline_variants]
    all_results = rez.retrieve(
        futures,
        ordered=True,
        parallel_progress=True,
        parallel_progress_kwargs={"desc": "Pipelines"},
    )

    duration = time.time() - start
    print(f"\nCompleted {len(futures)} pipeline runs in {duration:.2f}s.\n")

    for (image, pipeline), output_paths in zip(
        [(img, pipe) for img in images for pipe in pipeline_variants],
        all_results,
    ):
        print(f"image={image!r}  pipeline={pipeline}")
        for path in output_paths:
            print(f"    {path}")
        print()
