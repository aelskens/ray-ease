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

EXTRAS = {}

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


class BumpVersion(Command):
    """Support setup.py bump --level=<given level> [--value=<to given value>]."""

    description = "Bump the version of the package."
    user_options = [
        (
            "level=",
            "l",
            "The level of the version bump, e.g. `minor` will increment the middle number of the version.",
        ),
        ("value=", "v", "The value to which the number should be bumped."),
    ]

    @staticmethod
    def status(s: str) -> None:
        """Prints things in bold.

        :param s: The message to print in the status.
        :type s: str
        """

        print(f"\033[1m{s}\033[0m")

    def initialize_options(self) -> None:
        self.level = None
        self.value = None

    def finalize_options(self) -> None:
        def __get_current_level_value() -> int:
            return int(about["__version__"].split(".")[self.level])

        if self.level not in ("major", "minor", "micro"):
            raise ValueError(
                f"The given value, {self.level}, for parameter --level is invalid. Available choices are: "
                "`major`, `minor` and `micro` each bumps respectively x, y, and z in a x.y.z version "
                "convention."
            )
        else:
            self.level = ["major", "minor", "micro"].index(self.level)

        if self.value is None:
            self.value = __get_current_level_value() + 1

        if not isinstance(self.value, int):
            try:
                self.value = eval(self.value)
                if not isinstance(self.value, int):
                    raise TypeError

            except:
                raise TypeError(
                    f"The given value, {self.value}, for parameter --value is of type={type(self.value)} while "
                    "only integers are allowed."
                )

            current_value = __get_current_level_value()
            if self.value < current_value:
                raise ValueError(
                    f"The given value, {self.value}, for parameter --value is lower than the current value, "
                    f"{current_value}, for target level. This is prohibited."
                )

    def run(self) -> None:
        self.status("Bumping version...")
        print(f"With level={['major', 'minor', 'micro'][self.level]} and value={self.value}.")

        with open(os.path.join(here, "src", NAME, "__version__.py"), "r", encoding="utf-8") as f:
            file = f.readlines()

        with open(os.path.join(here, "src", NAME, "__version__.py"), "w", encoding="utf-8") as f:
            for i, line in enumerate(file):
                if "VERSION" in line:
                    tmp = about["__version__"].split(".")
                    tmp[self.level] = str(self.value)

                    new_line = f"VERSION = ({', '.join(tmp)})\n"
                    break

            file[i] = new_line
            f.write("".join(file))

        print(f"From {about['__version__']} -> {'.'.join(tmp)}.")
        sys.exit()


class CustomBuildCommand(Command):
    """Support setup.py custom_build."""

    description = "Build the package: bdist, sdist and egg-info."
    user_options = []

    @staticmethod
    def status(s: str) -> None:
        """Prints things in bold.

        :param s: The message to print in the status.
        :type s: str
        """

        print(f"\033[1m{s}\033[0m")

    def initialize_options(self) -> None:
        pass

    def finalize_options(self) -> None:
        pass

    def run(self) -> None:
        try:
            self.status("Removing previous builds...")
            rmtree(os.path.join(here, "dist"))
        except OSError:
            pass

        self.status("Building Source and Wheel (universal) distribution...")
        os.system(f"{sys.executable} setup.py sdist bdist_wheel --universal")

        sys.exit()


class UploadCommand(Command):
    """Support setup.py upload [--repository=<repository type>]."""

    description = "Upload the package."
    user_options = [
        ("repository=", "r", "The repository where it will be uploaded."),
    ]

    @staticmethod
    def status(s: str) -> None:
        """Prints things in bold.

        :param s: The message to print in the status.
        :type s: str
        """

        print(f"\033[1m{s}\033[0m")

    def initialize_options(self) -> None:
        self.repository = "pypi"

    def finalize_options(self) -> None:
        if self.repository not in ("pypi", "testpypi"):
            raise ValueError(
                f"The given value, {self.repository}, for parameter --repository is invalid. Available "
                "choices are: `pypi` and `testpypi`."
            )

    def run(self) -> None:
        self.status("Check dist/* via Twine...")
        os.system("twine check dist/*")

        from twine.utils import get_config

        secrets = get_config("./.pypirc")
        username = secrets[self.repository].get("username")
        api_token = secrets[self.repository].get("password")

        if self.repository == "pypi":
            self.status("Uploading the package to PyPI via Twine...")
            os.system(f"twine upload dist/* -u {username} -p {api_token}")

            self.status("Pushing git tags...")
            os.system(f"git tag v{about['__version__']}")
            os.system("git push --tags")
        else:
            self.status(f"Uploading the package to {self.repository} via Twine...")
            os.system(f"twine upload -r {self.repository} dist/* -u {username} -p {api_token}")

        sys.exit()


setup(
    name=NAME,
    version=about["__version__"],
    description=DESCRIPTION,
    long_description=long_description,
    long_description_content_type="text/markdown",
    author=AUTHOR,
    author_email=EMAIL,
    python_requires=REQUIRES_PYTHON,
    url=URL,
    packages=find_packages("src", exclude=["tests", "*.tests", "*.tests.*", "tests.*"]),
    package_dir={"": "src"},
    install_requires=required,
    extras_require=EXTRAS,
    include_package_data=True,
    license="MIT",
    classifiers=[
        # Trove classifiers
        # Full list: https://pypi.python.org/pypi?%3Aaction=list_classifiers
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python",
        f"Programming Language :: Python :: {list(filter(lambda x: x.isnumeric() , REQUIRES_PYTHON))[0]}",
        f"Programming Language :: Python :: {''.join(filter(lambda x: x in '.1234567890', REQUIRES_PYTHON))}",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy",
    ],
    cmdclass={
        "bump": BumpVersion,
        "custom_build": CustomBuildCommand,
        "upload": UploadCommand,
    },
    project_urls={
        "GitHub": URL,
        "Tracker": os.path.join(URL, "issues"),
    },
)
