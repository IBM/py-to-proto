"""A setuptools setup module for jtd_to_proto"""

# Standard
import os

# Third Party
from setuptools import setup

# Read the README to provide the long description
python_base = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(python_base, "README.md"), "r") as handle:
    long_description = handle.read()

# Read version from the env
version = os.environ.get("RELEASE_VERSION")
assert version is not None, "Must set RELEASE_VERSION"

# Read in the requirements
with open(os.path.join(python_base, "requirements.txt"), "r") as handle:
    requirements = handle.read().split("\n")


# Split out jtd as the optional requirement for validation
jtd_req = [req for req in requirements if req.startswith("jtd")]
requirements = list(set(requirements) - set(jtd_req))
extras_require = {"validation": jtd_req}

setup(
    name="jtd_to_proto",
    version=version,
    description="A tool to dynamically create protobuf message classes from JSON Typedef",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/IBM/jtd-to-proto",
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
    ],
    keywords=["json", "json typedef", "jtd", "protobuf", "proto"],
    packages=["jtd_to_proto"],
    install_requires=requirements,
    extras_require=extras_require,
)
