import codecs
import os
import re
from pathlib import Path

from setuptools import find_packages, setup

here = os.path.abspath(os.path.dirname(__file__))


def read(*parts):
    with codecs.open(os.path.join(here, *parts), "r") as fp:
        return fp.read()


def find_version(*file_paths):
    version_file = read(*file_paths)
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]", version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")


with open("requirements.txt", "r") as f:
    required = f.read().splitlines()

# read the contents of README file
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

setup(
    version=find_version("caveclient", "__init__.py"),
    name="caveclient",
    description="A client for interacting with the Connectome Annotation Versioning Engine",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Forrest Collman, Casey Schneider-Mizell, Sven Dorkenwald",
    author_email="forrestc@alleninstute.org,caseys@alleninstitute.org,svenmd@princeton.edu,",
    url="https://github.com/CAVEconnectome/CAVEclient",
    packages=find_packages(where="."),
    include_package_data=True,
    install_requires=required,
    setup_requires=["pytest-runner"],
    python_requires=">=3.7",
)
