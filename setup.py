#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Adapted from: https://github.com/navdeep-G/setup.py/blob/master/setup.py

# Note: To use the 'upload' functionality of this file, you must:
#   $ pipenv install twine --dev

import io
import os
import sys
from shutil import rmtree

from setuptools import Command, find_packages, setup

# Package meta-data.
NAME = "ray_ease"
DESCRIPTION = "Switch from serial to parallel computing without requiring any code modifications."
URL = "https://github.com/aelskens/ray-ease"
EMAIL = "arthurelsk@gmail.com"
AUTHOR = "Arthur Elskens"
REQUIRES_PYTHON = ">=3.8"
VERSION = "0.0.0"

# What packages are optional?
EXTRAS = {
    # 'fancy feature': ['django'],
}

here = os.path.abspath(os.path.dirname(__file__))

# Import the README and use it as the long-description.
# Note: this will only work if 'README.md' is present in your MANIFEST.in file!
try:
    with io.open(os.path.join(here, "README.md"), encoding="utf-8") as f:
        long_description = "\n" + f.read()
except FileNotFoundError:
    long_description = DESCRIPTION

# Load the package's __version__.py module as a dictionary.
about = {}
if not VERSION or VERSION == "0.0.0":
    with open(os.path.join(here, "src", NAME, "__version__.py")) as f:
        exec(f.read(), about)
else:
    about["__version__"] = VERSION


def _load_from_requirements():
    requirements_fp = os.path.join(here, "requirements.txt")
    print(requirements_fp)
    if not os.path.exists(requirements_fp):
        return []

    requirements = []
    with io.open(requirements_fp, encoding="utf-8") as f:
        requirements = f.read().splitlines()

    return requirements


required = _load_from_requirements()


class UploadCommand(Command):
    """Support setup.py upload."""

    description = "Build and publish the package."
    user_options = []

    @staticmethod
    def status(s):
        """Prints things in bold."""
        print("\033[1m{0}\033[0m".format(s))

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        try:
            self.status("Removing previous builds…")
            rmtree(os.path.join(here, "dist"))
        except OSError:
            pass

        self.status("Building Source and Wheel (universal) distribution…")
        os.system("{0} setup.py sdist bdist_wheel --universal".format(sys.executable))

        self.status("Uploading the package to PyPI via Twine…")
        os.system("twine upload dist/*")

        self.status("Pushing git tags…")
        os.system("git tag v{0}".format(about["__version__"]))
        os.system("git push --tags")

        sys.exit()


# Where the magic happens:
setup(
    name=NAME,
    version=about["__version__"],
    description=DESCRIPTION,
    long_description=long_description,
    long_description_content_type="text/markdown",
    author=AUTHOR,
    author_email=EMAIL,
    # If your package require specific python version
    python_requires=REQUIRES_PYTHON,
    # The url of the repository where this package lives
    url=URL,
    packages=find_packages("src", exclude=["tests", "*.tests", "*.tests.*", "tests.*"]),
    package_dir={"": "src"},
    # If your package is a single module, use this instead of 'packages':
    # py_modules=['mypackage'],
    # entry_points={
    #     'console_scripts': ['mycli=mymodule:cli'],
    # },
    install_requires=required,
    extras_require=EXTRAS,
    include_package_data=True,
    license="MIT",
    # If you want to define classifiers
    classifiers=[
        # Trove classifiers
        # Full list: https://pypi.python.org/pypi?%3Aaction=list_classifiers
        "License :: OSI Approved :: MIT",
        "Programming Language :: Python",
        f"Programming Language :: Python :: {list(filter(lambda x: x.isnumeric() , REQUIRES_PYTHON))[0]}",
        f"Programming Language :: Python :: {''.join(filter(lambda x: x in '.1234567890', REQUIRES_PYTHON))}",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy",
    ],
    # $ setup.py publish support.
    cmdclass={
        "upload": UploadCommand,
    },
    project_urls={
        "GitHub": URL,
        "Tracker": os.path.join(URL, "issues"),
    },
)
