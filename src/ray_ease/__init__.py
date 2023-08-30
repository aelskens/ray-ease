# read version from installed package
from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("ray_ease")
except PackageNotFoundError:
    __version__ = "dev"

from .core import init, parallelize, retrieve
