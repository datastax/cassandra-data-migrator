DOCKER_CASS=cdm-sit-cass
DOCKER_CDM=cdm-sit-cdm
CASS_USERNAME=cassandra
CASS_PASSWORD=cassandra
KEYSPACES="source target"
CDM_DIRECTORY=/local
CDM_JARFILE=cassandra-data-migrator.jar

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
