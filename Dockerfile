## Base ########################################################################
#
# This phase sets up dependencies for the other phases
##
ARG PYTHON_VERSION=3.8
ARG BASE_IMAGE=python:${PYTHON_VERSION}-slim
FROM ${BASE_IMAGE} as base
ARG PROTOBUF_VERSION=""

# This image is only for building, so we run as root
WORKDIR /src

# Install build, test, and publish dependencies
COPY requirements.txt requirements_test.txt /src/
RUN true && \
    apt-get update -y && \
    apt-get install make git -y && \
    apt-get clean autoclean && \
    apt-get autoremove --yes && \
    pip install pip --upgrade && \
    pip install twine pre-commit && \
    pip install -r /src/requirements_test.txt && \
    pip install -r /src/requirements.txt && \
    if [ ! "${PROTOBUF_VERSION}" ]; then \
        pip uninstall -y protobuf grpcio-tools && \
        pip install "protobuf${PROTOBUF_VERSION}" grpcio-tools; \
    fi && \
    true

## Test ########################################################################
#
# This phase runs the unit tests for the library
##
FROM base as test
COPY . /src
ARG RUN_FMT="true"
RUN true && \
    ./scripts/run_tests.sh && \
    RELEASE_DRY_RUN=true RELEASE_VERSION=0.0.0 \
        ./scripts/publish.sh && \
    ./scripts/fmt.sh && \
    true

## Release #####################################################################
#
# This phase builds the release and publishes it to pypi
##
FROM test as release
ARG PYPI_TOKEN
ARG RELEASE_VERSION
ARG RELEASE_DRY_RUN
RUN ./scripts/publish.sh
# Create a temp file that the release_test stage uses to ensure
# correct order of build stages
RUN touch RELEASED.txt

## Release Test ################################################################
#
# This phase installs the indicated version from PyPi and runs the unit tests
# against the installed version.
##
FROM base as release_test
# Copy a random file from the release phase just
# to ensure release_test runs _after_ release
COPY --from=release /src/RELEASED.txt .
ARG RELEASE_VERSION
ARG RELEASE_DRY_RUN
COPY ./tests /src/tests
COPY ./scripts/install_release.sh /src/scripts/install_release.sh
RUN true && \
    ./scripts/install_release.sh && \
    python3 -m pytest -W error && \
    true
