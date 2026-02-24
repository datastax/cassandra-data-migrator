#!/bin/bash

MODES="setup|reset|validate|teardown"
DEFAULT_CIDR=172.16.242.0/24
DEFAULT_NETWORK_NAME=cdm-sit-network

_usage() {
  cat <<EOF

usage: $0 [-c CIDR] [-n network_name] [-r runtime] -m mode -j jar

Required
  -m mode: one of {$MODES}
  -j jar: required when mode=setup or mode=reset - path to the cassandra-data-migrator.jar file to use for the test

Optional
  -c CIDR        : CIDR subnet for container network, defaults to ${DEFAULT_CIDR}
  -n network_name: name for the container network, defaults to ${DEFAULT_NETWORK_NAME}
  -r runtime     : container runtime to use (podman|docker|auto), defaults to auto

EOF
exit 1
}

#==============================================================================================================================
# Input argument processing, setting default environment
#==============================================================================================================================
CIDR=${DEFAULT_CIDR}
NETWORK_NAME=${DEFAULT_NETWORK_NAME}
argErrors=0
while getopts "c:m:n:j:r:" opt; do
  case $opt in
    c) CIDR="$OPTARG"
       ;;
    n) NETWORK_NAME="$OPTARG"
       ;;
    m) MODE="$OPTARG"
       ;;
    j) JAR="$OPTARG"
       ;;
    r) CONTAINER_RUNTIME="$OPTARG"
       export CONTAINER_RUNTIME
       ;;
    ?) echo "ERROR invalid option was specified - ${OPTARG}"
       _usage
       ;;
  esac
done

if [ -z "$MODE" ]; then
  echo "ERROR missing -m mode"
  argErrors=1
elif ! [[ "$MODE" =~ ^($MODES)$ ]]; then
  echo "ERROR invalid mode: $MODE"
  argErrors=1
fi


if [[ "$MODE" == "setup" || "$MODE" == "reset" ]]; then
  if [[ -z "$JAR" ]]; then
    echo "ERROR missing -j jar"
    argErrors=1
  elif [[ ! -f $JAR || $(ls $JAR | wc -l) -ne 1 ]]; then
    echo "ERROR $JAR file missing, or there is more than one file present"
    argErrors=1
  fi
fi
if [ $argErrors -ne 0 ]; then
  _usage
fi

###
# These variables are hard-coded for now
SUBNET=$(echo ${CIDR} | cut -d. -f1-3)
CASS_VERSION=5
CDM_VERSION=latest
#==============================================================================================================================
# Helper Functions
#==============================================================================================================================
# Common environment and functions
. common.sh

# Initialize container runtime
_initContainerRuntime

_testContainerNetwork() {
  _containerNetwork list | awk 'BEGIN{FOUND="no"} {if ($2 == "'${NETWORK_NAME}'") FOUND="yes"} END{print FOUND}'
}

_testCassandraContainer() {
  containerPs=$(_containerPs --all --filter "name=${CONTAINER_CASS}" --format "{{.Status}}" | awk '{if ($1 == "Up") {print "yes"}}')
  if [ "$containerPs" != "yes" ]; then
    echo "no"
    return
  fi
  _containerExec ${CONTAINER_CASS} cqlsh -u ${CASS_USERNAME} -p ${CASS_PASSWORD} -e 'SELECT cluster_name FROM system.local' localhost > /dev/null 2>&1
  if [ $? -eq 0 ]; then
    echo "yes"
  else
    echo "no"
  fi
}

_testKeyspace() {
  testKeyspace=$1
  statement="select count(*) from system_schema.keyspaces where keyspace_name = '"${testKeyspace}"';"
  twoKeyspace=$(_containerExec ${CONTAINER_CASS} cqlsh -u ${CASS_USERNAME} -p ${CASS_PASSWORD} -e "${statement}"  localhost 2>/dev/null | egrep -c '^ *1 *')
  if [[ $twoKeyspace -eq 1 ]]; then
    echo "yes"
  else
    echo "no"
  fi
}

_testKeyspaces() {
  ksCount=0;
  ksPass=0;
  for ks in $KEYSPACES; do
    ksCount=$((ksCount+1))
    if [ $(_testKeyspace $ks) == "yes" ]; then
      ksPass=$((ksPass+1))
    fi
  done
  if [ $ksPass -eq $ksCount ]; then
    echo "yes"
  else
    echo "no"
  fi
}

_createKeyspaces() {
  for keyspace in $KEYSPACES; do
    if [ $(_testKeyspace $keyspace) != "yes" ]; then
      _info "Creating keyspace $keyspace"
      createKS=$(_containerExec ${CONTAINER_CASS} cqlsh -u ${CASS_USERNAME} -p ${CASS_PASSWORD} -e "create keyspace "${keyspace}" with replication = { 'class':'SimpleStrategy', 'replication_factor':1};" 2>&1)
      createRtn=$?
      if [ $createRtn -ne 0 ]; then
        _error "Problem creating keyspace ${keyspace}, rtn=$createRtn, output=$createKS"
      fi
    fi
  done
  if [[ "$(_testKeyspaces)" != "yes" ]]; then
    _fatal "Problem ensuring one or more of the following keyspaces exist: ${KEYSPACES}"
  fi
}

_dropKeyspaces() {
  for keyspace in $KEYSPACES; do
    if [ $(_testKeyspace $keyspace) == "yes" ]; then
      _info "Dropping keyspace $keyspace"
      cmdOut=$(_containerExec ${CONTAINER_CASS} cqlsh -u ${CASS_USERNAME} -p ${CASS_PASSWORD} -e "drop keyspace "${keyspace}";" 2>&1)
      rtn=$?
      if [ $rtn -ne 0 ]; then
        _error "Problem dropping keyspace ${keyspace}, rtn=$rtn, output=$cmdOut"
      fi
    fi
  done
  if [[ "$(_testKeyspaces)" == "yes" ]]; then
    _warn "One or more keyspaces still exist after trying to drop them."
  fi
}

_testCDMContainer() {
  containerPs=$(_containerPs --all --filter "name=${CONTAINER_CDM}" --format "{{.Status}}" | awk '{if ($1 == "Up") {print "yes"}}')
  if [ "$containerPs" != "yes" ]; then
    echo "no"
  else
    echo "yes"
  fi
}

_testCDMDirectory() {
  _containerExec ${CONTAINER_CDM} ls ${CDM_DIRECTORY}/${CDM_JARFILE} >/dev/null 2>&1
  rtn=$?
  if [ $rtn -eq 0 ]; then
    echo "yes"
  else
    echo "no"
  fi
}

_dropCDMDirectory() {
  _containerExec ${CONTAINER_CDM} rm -rf ${CDM_DIRECTORY}
  if [[ "$(_testCDMDirectory)" == "yes" ]]; then
    _warn "Directory ${CDM_DIRECTORY} is not dropped"
  fi
}

_createCDMDirectory() {
  _containerExec ${CONTAINER_CDM} mkdir -p ${CDM_DIRECTORY}
  _containerCp ${JAR} ${CONTAINER_CDM}:${CDM_DIRECTORY}/${CDM_JARFILE}
  _containerCp cdm.sh ${CONTAINER_CDM}:${CDM_DIRECTORY}/cdm.sh
  _containerCp cdm-assert.sh ${CONTAINER_CDM}:${CDM_DIRECTORY}/cdm-assert.sh
  if [[ "$(_testCDMDirectory)" != "yes" ]]; then
    _fatal "Directory ${CDM_DIRECTORY} cannot be created, or ${JAR} cannot be copied to ${CDM_DIRECTORY}"
  fi
}

#==============================================================================================================================
# 'Real' Functions
#==============================================================================================================================
###############################################################################################################################
### Setup #####################################################################################################################
# Goal of Setup is to
#   1. Ensure there is a Docker network
#   2. Ensure there is a Cassandra container running
#   3. Ensure the KEYSPACES are created
#   4. Ensure there is a CDM container runnning, and a directory /local exists
_Setup() {
  if [ "$(_testContainerNetwork)" != "yes" ]; then
    _info "Creating container network ${NETWORK_NAME}"
    _containerNetwork create --subnet=${CIDR} ${NETWORK_NAME}
  fi

  if [ "$(_testCassandraContainer)" != "yes" ]; then
    containerVersion=cassandra:${CASS_VERSION}
    _info "Pulling latest container image for ${containerVersion}"
    _containerPull ${containerVersion}
    _info "Starting Cassandra container ${CONTAINER_CASS}"
    _containerRun --name $CONTAINER_CASS --network ${NETWORK_NAME} --ip ${SUBNET}.2 -e "CASS_USERNAME=${CASS_USERNAME}" -e "CASS_PASSWORD=${CASS_PASSWORD}" -e "CASS_CLUSTER=${CONTAINER_CASS}" -d ${containerVersion}
    attempt=1
    while [[ $attempt -le 12 && "$(_testCassandraContainer)" != "yes" ]]; do
      _info "waiting for Cassandra to start, attempt $attempt"
      sleep 10
      attempt=$((attempt+1))
    done
    if [[ "$(_testCassandraContainer)" != "yes" ]]; then
      _fatal "starting Cassandra Container"
    fi
  fi

  if [ "$(_testKeyspaces)" != "yes" ]; then
    _createKeyspaces
  fi

  if [ "$(_testCDMContainer)" != "yes" ]; then
    containerVersion=datastax/cassandra-data-migrator:${CDM_VERSION}

    # Uncomment the below '_containerBuild' lines when making container changes to ensure you test the changes
    # Also comment the '_containerPull' line when '_containerBuild' is uncommented.
    # Note this ('_containerBuild') should be done only when testing container changes locally (i.e. Do not commit)
    # If you commit the '_containerBuild' step, the build will work but it will take too long as each time it will build
    # CDM container image instead of just downloading from DockerHub.
    _info "Pulling latest container image for ${containerVersion}"
    _containerPull ${containerVersion}
    # _info "Building latest container image for ${containerVersion}"
    # _containerBuild -t ${containerVersion} ..

    _info "Starting CDM container ${CONTAINER_CDM}"
    _containerRun --name ${CONTAINER_CDM} --network ${NETWORK_NAME} --ip ${SUBNET}.3 -e "CASS_USERNAME=${CASS_USERNAME}" -e "CASS_PASSWORD=${CASS_PASSWORD}" -e "CASS_CLUSTER=${CONTAINER_CASS}" -d ${containerVersion}
    attempt=1
    while [[ $attempt -le 12 && "$(_testCDMContainer)" != "yes" ]]; do
      _info "waiting for CDM to start, attempt $attempt"
      sleep 10
      attempt=$((attempt+1))
    done
  fi

  if [ "$(_testCDMDirectory)" != "yes" ]; then
    _createCDMDirectory
  fi
}

##################################################################################################################################
### Reset #####################################################################################################################
# Reset cleans the environment without removing containers
#   1. Drop/Create Keyspaces
#   2. Drop/Create CDM Directory
_Reset() {
  _info "Dropping Keyspaces"
  _dropKeyspaces
  _info "Creating Keyspaces"
  _createKeyspaces

  _info "Removing directory ${CDM_DIRECTORY}"
  _dropCDMDirectory
  _info "Creating directory ${CDM_DIRECTORY}"
  _createCDMDirectory
}

##################################################################################################################################
### Validate #####################################################################################################################
# Validate is for purposes of showing the environment state, written to STDOUT
# Exits the script with return code:
#   0 : valid
#   1 : invalid
_Validate() {
  invalid=0
  if [ "$(_testContainerNetwork)" == "yes" ]; then
    _info "Container network is valid"
  else
    _warn "Container network is invalid"
    invalid=1
  fi

  if [ "$(_testCassandraContainer)" == "yes" ]; then
    _info "Cassandra container is valid and running"
  else
    _warn "Cassandra container is invalid"
    invalid=1
  fi

  if [ "$(_testKeyspaces)" == "yes" ]; then
    _info "Keyspaces are valid"
  else
    _warn "Keyspaces are invalid"
    invalid=1
  fi

  if [ "$(_testCDMContainer)" == "yes" ]; then
    _info "CDM container is valid and running"
  else
    _warn "CDM container is invalid"
    invalid=1
  fi

  if [ "$(_testCDMDirectory)" == "yes" ]; then
    _info "CDM directory ${CDM_DIRECTORY} is valid"
  else
    _warn "CDM directory ${CDM_DIRECTORY} is invalid (is the -j jar installed?)"
    invalid=1
  fi

  exit $invalid
}


##################################################################################################################################
### Teardown #####################################################################################################################
# Goal of teardown is to restore the Docker environment to the pre-Setup state
#  1. Stop and remove Cassandra container
#  2. Stop and remove CDM container
#  3. Remove the Docker network
_Teardown() {
  _info "Stopping and removing container ${CONTAINER_CDM}"
  output=$(_containerRm -f ${CONTAINER_CDM} 2>&1)
  rtn=$?
  if [[ $rtn -ne 0 || "$(_testCDMContainer)" == "yes" ]]; then
    _fatal "removing CDM container, rtn=$rtn, output=$output"
  fi

  _info "Stopping and removing container ${CONTAINER_CASS}"
  output=$(_containerRm -f ${CONTAINER_CASS} 2>&1)
  rtn=$?
  if [[ $rtn -ne 0 || "$(_testCassandraContainer)" == "yes" ]]; then
    _fatal "removing Cassandra container, rtn=$rtn, output=$output"
  fi

  if [ "$(_testContainerNetwork)" == "yes" ]; then
    _info "Removing container network ${NETWORK_NAME}"
    output=$(_containerNetwork rm ${NETWORK_NAME} 2>&1)
    rtn=$?
    if [[ $rtn -ne 0 || "$(_testContainerNetwork)" == "yes" ]]; then
      _fatal "removing container network, rtn=$rtn, output=$output"
    fi
  fi
}


#==============================================================================================================================
# Do something for real
#==============================================================================================================================
if [ "$MODE" == "setup" ]; then
  _Setup
elif [ "$MODE" == "reset" ]; then
  _Reset
elif [ "$MODE" == "validate" ]; then
  _Validate
elif [ "$MODE" == "teardown" ]; then
  _Teardown
fi

