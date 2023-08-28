# read version from installed package
from importlib.metadata import version

__version__ = version("ray_ease")

from .core import init, parallelize, retrieve_parallel_loop
