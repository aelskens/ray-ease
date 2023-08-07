# read version from installed package
from importlib.metadata import version

__version__ = version("ray_ease")

from .decorators import parallelize
from .remote_as_local import retrieve_parallel_loop
