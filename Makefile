##@ General

all:	help

# NOTE: Help stolen from operator-sdk auto-generated makfile!
.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-\\.]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

.PHONY: test
test: ## Run the unit tests
	PARALLEL=1 ./scripts/run_tests.sh

.PHONY: fmt
fmt: ## Run code formatting
	./scripts/fmt.sh

.PHONY: wheel
wheel: ## Build release wheels
	./scripts/build_wheel.sh

##@ Develop

PYTHON_VERSION ?= 3.8
PROTOBUF_VERSION ?=

.PHONY: develop.build
develop.build: ## Build the development environment container
	docker build . --target=base \
		-t jtd-to-proto-develop \
		--build-arg PYTHON_VERSION=${PYTHON_VERSION} \
		--build-arg PROTOBUF_VERSION="${PROTOBUF_VERSION}"

.PHONY: develop
develop:	develop.build ## Run the develop shell with the local codebase mounted
	touch .bash_history
	docker run --rm -it \
		--entrypoint bash \
		-w /src \
		-v ${PWD}:/src \
		-v ${PWD}/.bash_history:/root/.bash_history \
		jtd-to-proto-develop
