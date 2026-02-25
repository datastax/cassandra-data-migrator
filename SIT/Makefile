
# Container runtime configuration
CONTAINER_RUNTIME ?= auto
RUNTIME_FLAG = $(if $(filter-out auto,$(CONTAINER_RUNTIME)),-r $(CONTAINER_RUNTIME),)

all: setup test_smoke_cmd test_regression_cmd test_features_cmd env_teardown

setup: build env_setup

reset: build env_reset
reset_hard: build env_teardown env_setup

build:
	cd .. && mvn clean package

test_smoke: reset test_smoke_cmd
test_smoke_cmd:
	./test.sh -p smoke

test_regression: reset test_regression_cmd
test_regression_cmd:
	./test.sh -p regression

test_features: reset test_features_cmd
test_features_cmd:
	./test.sh -p features

test: test_smoke test_regression_cmd test_features_cmd

# Local tests are not included in automated tests, but provide a means
# to use the test harness to validate project-specific work
test_local: reset test_local_cmd
test_local_cmd:
	./test.sh -p local

env_setup:
	chmod -R 777 ./*.sh
	./environment.sh -m setup -j ../target/cassandra-data-migrator*.jar $(RUNTIME_FLAG)
env_reset:
	./environment.sh -m reset -j ../target/cassandra-data-migrator*.jar $(RUNTIME_FLAG)
env_validate:
	./environment.sh -m validate $(RUNTIME_FLAG)
env_teardown:
	./environment.sh -m teardown $(RUNTIME_FLAG)

# Convenience targets for explicit runtime selection
setup_podman: CONTAINER_RUNTIME=podman
setup_podman: setup

setup_docker: CONTAINER_RUNTIME=docker
setup_docker: setup

test_podman: CONTAINER_RUNTIME=podman
test_podman: test

test_docker: CONTAINER_RUNTIME=docker
test_docker: test

