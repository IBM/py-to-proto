"""A setuptools setup module for py_to_proto"""

# Standard
import os

# Third Party
from setuptools import setup

# Read the README to provide the long description
python_base = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(python_base, "README.md"), "r") as handle:
    long_description = handle.read()

long_description = (
    """
# DEPRECATED

This project has been renamed to `py-to-proto` to reflect its expansion to include other input schema formats. Please see https://pypi.org/project/py-to-proto/

"""
    + long_description
)

# Read version from the env
version = os.environ.get("RELEASE_VERSION")
assert version is not None, "Must set RELEASE_VERSION"

# Read in the requirements
with open(os.path.join(python_base, "requirements.txt"), "r") as handle:
    requirements = handle.read()

setup(
    name="jtd_to_proto",
    version=version,
    description="DEPRECATED: Please see py-to-proto: https://pypi.org/project/py-to-proto/",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/IBM/py-to-proto",
    author="Gabe Goodhart",
    author_email="gabe.l.hart@gmail.com",
    license="MIT",
    classifiers=[
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    keywords=["json", "json typedef", "jtd", "protobuf", "proto", "dataclass"],
    packages=["py_to_proto"],
    install_requires=requirements,
)
