#!/bin/bash

MODES="setup|reset|validate|teardown"
DEFAULT_CIDR=172.16.242.0/24
DEFAULT_NETWORK_NAME=cdm-sit-network

_usage() {
  cat <<EOF

usage: $0 [-c CIDR] [-n network_name] -m mode -j jar

Required
  -m mode: one of {$MODES}
  -j jar: required when mode=setup or mode=reset - path to the cassandra-data-migrator.jar file to use for the test

Optional
  -c CIDR        : CIDR subnet for docker network, defaults to ${DEFAULT_CIDR}
  -n network_name: name for the docker network, defaults to ${DEFAULT_NETWORK_NAME}

EOF
exit 1
}

#==============================================================================================================================
# Input argument processing, setting default environment
#==============================================================================================================================
CIDR=${DEFAULT_CIDR}
NETWORK_NAME=${DEFAULT_NETWORK_NAME}
argErrors=0
while getopts "c:m:n:j:" opt; do
  case $opt in 
    c) CIDR="$OPTARG"
       ;;
    n) NETWORK_NAME="$OPTARG"
       ;;
    m) MODE="$OPTARG"
       ;;
    j) JAR="$OPTARG"
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

_testDockerNetwork() {
  docker network list | awk 'BEGIN{FOUND="no"} {if ($2 == "'${NETWORK_NAME}'") FOUND="yes"} END{print FOUND}'  
}

_testDockerCassandra() {
  dockerPs=$(docker ps --all --filter "name=${DOCKER_CASS}" --format "{{.Status}}" | awk '{if ($1 == "Up") {print "yes"}}')
  if [ "$dockerPs" != "yes" ]; then
    echo "no"
    return
  fi
  docker exec ${DOCKER_CASS} cqlsh -u ${CASS_USERNAME} -p ${CASS_PASSWORD} -e 'SELECT cluster_name FROM system.local' localhost > /dev/null 2>&1
  if [ $? -eq 0 ]; then
    echo "yes"
  else    
    echo "no"
  fi
}

_testKeyspace() {
  testKeyspace=$1
  statement="select count(*) from system_schema.keyspaces where keyspace_name = '"${testKeyspace}"';"
  twoKeyspace=$(docker exec ${DOCKER_CASS} cqlsh -u ${CASS_USERNAME} -p ${CASS_PASSWORD} -e "${statement}"  localhost 2>/dev/null | egrep -c '^ *1 *')
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
      createKS=$(docker exec ${DOCKER_CASS} cqlsh -u ${CASS_USERNAME} -p ${CASS_PASSWORD} -e "create keyspace "${keyspace}" with replication = { 'class':'SimpleStrategy', 'replication_factor':1};" 2>&1)
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
      cmdOut=$(docker exec ${DOCKER_CASS} cqlsh -u ${CASS_USERNAME} -p ${CASS_PASSWORD} -e "drop keyspace "${keyspace}";" 2>&1)
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

_testDockerCDM() {
  dockerPs=$(docker ps --all --filter "name=${DOCKER_CDM}" --format "{{.Status}}" | awk '{if ($1 == "Up") {print "yes"}}')
  if [ "$dockerPs" != "yes" ]; then
    echo "no"
  else
    echo "yes"
  fi
}

_testDockerCDM_Directory() {
  docker exec ${DOCKER_CDM} ls ${CDM_DIRECTORY}/${CDM_JARFILE} >/dev/null 2>&1
  rtn=$?
  if [ $rtn -eq 0 ]; then
    echo "yes"
  else
    echo "no"
  fi
}

_dropDockerCDM_Directory() {
  docker exec ${DOCKER_CDM} rm -rf ${CDM_DIRECTORY}
  if [[ "$(_testDockerCDM_Directory)" == "yes" ]]; then
    _warn "Directory ${CDM_DIRECTORY} is not dropped"
  fi  
}

_createDockerCDM_Directory() {
  docker exec ${DOCKER_CDM} mkdir -p ${CDM_DIRECTORY}
  docker cp ${JAR} ${DOCKER_CDM}:${CDM_DIRECTORY}/${CDM_JARFILE}
  docker cp cdm.sh ${DOCKER_CDM}:${CDM_DIRECTORY}/cdm.sh
  docker cp cdm-assert.sh ${DOCKER_CDM}:${CDM_DIRECTORY}/cdm-assert.sh
  if [[ "$(_testDockerCDM_Directory)" != "yes" ]]; then
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
  if [ "$(_testDockerNetwork)" != "yes" ]; then
    _info "Creating Docker network ${NETWORK_NAME}"
    docker network create --driver=bridge --subnet=${CIDR} ${NETWORK_NAME}
  fi

  if [ "$(_testDockerCassandra)" != "yes" ]; then
    dockerContainerVersion=cassandra:${CASS_VERSION}
    _info "Pulling latest Docker container for ${dockerContainerVersion}"
    docker pull ${dockerContainerVersion}
    _info "Starting Docker container ${DOCKER_CASS}"
    docker run --name $DOCKER_CASS --network ${NETWORK_NAME} --ip ${SUBNET}.2 -e "CASS_USERNAME=${CASS_USERNAME}" -e "CASS_PASSWORD=${CASS_PASSWORD}" -e "CASS_CLUSTER=${DOCKER_CASS}" -d ${dockerContainerVersion}
    attempt=1
    while [[ $attempt -le 12 && "$(_testDockerCassandra)" != "yes" ]]; do
      _info "waiting for Cassandra to start, attempt $attempt"
      sleep 10
      attempt=$((attempt+1))
    done
    if [[ "$(_testDockerCassandra)" != "yes" ]]; then
      _fatal "starting Cassandra Container"
    fi
  fi

  if [ "$(_testKeyspaces)" != "yes" ]; then  
    _createKeyspaces
  fi  

  if [ "$(_testDockerCDM)" != "yes" ]; then  
    dockerContainerVersion=datastax/cassandra-data-migrator:${CDM_VERSION}

    # Uncomment the below 'docker build' lines when making docker changes to ensure you test the docker changes
    # Also comment the 'docker pull' line when 'docker build' is uncommented.
    # Note this ('docker build') should be done only when testing docker changes locally (i.e. Do not commit)
    # If you commit the 'docker build' step, the build will work but it will take too long as each time it will build
    # CDM docker image instead of just downloading from DockerHub.
    _info "Pulling latest Docker container for ${dockerContainerVersion}"
    docker pull ${dockerContainerVersion}
    # _info "Building latest Docker container for ${dockerContainerVersion}"
    # docker build -t ${dockerContainerVersion} ..

    _info "Starting Docker container ${DOCKER_CASS}"
    docker run --name ${DOCKER_CDM} --network ${NETWORK_NAME} --ip ${SUBNET}.3 -e "CASS_USERNAME=${CASS_USERNAME}" -e "CASS_PASSWORD=${CASS_PASSWORD}" -e "CASS_CLUSTER=${DOCKER_CASS}" -d ${dockerContainerVersion}
    attempt=1
    while [[ $attempt -le 12 && "$(_testDockerCDM)" != "yes" ]]; do
      _info "waiting for CDM to start, attempt $attempt"
      sleep 10
      attempt=$((attempt+1))
    done
  fi

  if [ "$(_testDockerCDM_Directory)" != "yes" ]; then
    _createDockerCDM_Directory
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
  _dropDockerCDM_Directory
  _info "Creating directory ${CDM_DIRECTORY}"
  _createDockerCDM_Directory
}

##################################################################################################################################
### Validate #####################################################################################################################
# Validate is for purposes of showing the environment state, written to STDOUT
# Exits the script with return code:
#   0 : valid
#   1 : invalid
_Validate() {
  invalid=0
  if [ "$(_testDockerNetwork)" == "yes" ]; then
    _info "Docker network is valid"
  else
    _warn "Docker network is invalid"
    invalid=1
  fi

  if [ "$(_testDockerCassandra)" == "yes" ]; then
    _info "Cassandra Docker is valid and running"
  else
    _warn "Cassandra Docker is invalid"
    invalid=1
  fi

  if [ "$(_testKeyspaces)" == "yes" ]; then
    _info "Keyspaces are valid"
  else
    _warn "Keyspaces are invalid"
    invalid=1
  fi  

  if [ "$(_testDockerCDM)" == "yes" ]; then
    _info "CDM Docker is valid and running"
  else
    _warn "CDM Docker is invalid"
    invalid=1
  fi

  if [ "$(_testDockerCDM_Directory)" == "yes" ]; then
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
  _info "Stopping and removing Docker ${DOCKER_CDM}"
  output=$(docker rm -f ${DOCKER_CDM} 2>&1)
  rtn=$?
  if [[ $rtn -ne 0 || "$(_testDockerCDM)" == "yes" ]]; then
    _fatal "removing CDM container, rtn=$rtn, output=$output"
  fi

  _info "Stopping and removing Docker ${DOCKER_CASS}"
  output=$(docker rm -f ${DOCKER_CASS} 2>&1)
  rtn=$?
  if [[ $rtn -ne 0 || "$(_testDockerCassandra)" == "yes" ]]; then
    _fatal "removing Cassandra container, rtn=$rtn, output=$output"
  fi

  if [ "$(_testDockerNetwork)" == "yes" ]; then
    _info "Removing Docker Network ${NETWORK_NAME}"
    output=$(docker network rm ${NETWORK_NAME} 2>&1)
    rtn=$?
    if [[ $rtn -ne 0 || "$(_testDockerNetwork)" == "yes" ]]; then
      _fatal "removing Docker Network, rtn=$rtn, output=$output"
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

