export DOCKER_CASS=cdm-sit-cass
export DOCKER_CDM=cdm-sit-cdm
export CASS_USERNAME=cassandra
export CASS_PASSWORD=cassandra
export KEYSPACES="origin target"
export CDM_DIRECTORY=/local
export CDM_JARFILE=cassandra-data-migrator.jar

_info() {
  echo "INFO $*"
}

_warn() {
  echo "WARN $*"
}

_error() {
  echo "ERROR $*"
}

_fatal() {
  echo "FATAL $*"
  exit 2
}
