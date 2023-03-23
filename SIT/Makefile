
all: setup test_smoke env_teardown

setup: build env_setup

reset: build env_reset
reset_hard: build env_teardown env_setup

build:
	cd .. && mvn clean package

test_smoke: reset test_smoke_cmd
test_smoke_cmd:
	./test.sh -p smoke

env_setup:
    chmod +x ./environment.sh
	./environment.sh -m setup -j ../target/cassandra-data-migrator*.jar
env_reset:
	./environment.sh -m reset -j ../target/cassandra-data-migrator*.jar
env_validate:
	./environment.sh -m validate
env_teardown:
	./environment.sh -m teardown

