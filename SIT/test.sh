#!/bin/bash

_usage() {
  cat <<EOF

usage: $0 -p phase 

Required
  -p phase: one of {$PHASES}

EOF
exit 1
}

while getopts "p:" opt; do
  case $opt in 
    p) PHASE="$OPTARG"
       ;;
    ?) echo "ERROR invalid option was specified - ${OPTARG}"
       _usage
       ;;
  esac
done

if [ -z "$PHASE" ]; then
  echo "ERROR missing -p phase"
  _usage
fi

if [[ ! -d ${PHASE} || $(ls -d ${PHASE}/* | wc -l) -eq 0 ]]; then
  _fatal "Phase directory ${PHASE} does not exist, or is empty"
fi

# Common enviornment and functions
. common.sh

EXPECTED_FILES="setup.cql expected.cql expected.out execute.sh"
GENERATED_FILES='setup.out setup.err execute.out execute.err actual.out actual.err cdm.*.out cdm.*.err other.*.out other.*.err'
CDM_JAR=/local/cassandra-data-migrator.jar

echo "=========================================================="
echo " Phase ${PHASE} Starting"
echo "=========================================================="
echo " Checking for expected files:"
echo "   ${EXPECTED_FILES}"
echo " Removing generated files:"
echo "   ${GENERATED_FILES}"
errors=0
for testDir in $(ls -d ${PHASE}/*); do
  for f in ${EXPECTED_FILES}; do
    if [[ ! -f ${testDir}/$f ]]; then
      _error "${testDir} is missing ${f}"
      errors=1
    fi
  done

  # Clean up any previous results that may exist
  for f in ${GENERATED_FILES}; do
    rm -f ${testDir}/$f
  done
done

# The .jar file is expected to be present
docker exec ${DOCKER_CDM} ls -d ${CDM_JAR} >/dev/null 2>&1
if [ $? -ne 0 ]; then
  _error "Required file ${CDM_JAR} is not installed in Docker container ${DOCKER_CDM}"
  errors=1
fi

if [ $errors -ne 0 ]; then
  _fatal "One or more required files is missing. See above ERROR(s) for details."
fi

# Copy all the files for the phase to both Docker containers
echo
echo "=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-"
echo " Copying phase files into Docker containers"
echo "=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-"
for dockerContainer in ${DOCKER_CASS} ${DOCKER_CDM}; do
  docker exec ${dockerContainer} rm -rf /${PHASE}
  docker cp ${PHASE} ${dockerContainer}:/${PHASE}
done

echo
echo "=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-"
echo " Setting up tables and data (Cassandra container)"
echo "=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-"
errors=0
for testDir in $(ls -d ${PHASE}/*); do
  _info ${testDir} Setup tables and data 
  docker exec ${DOCKER_CASS} cqlsh -u $CASS_USERNAME -p $CASS_PASSWORD -f $testDir/setup.cql > $testDir/setup.out 2>$testDir/setup.err
  if [ $? -ne 0 ]; then
    _error "${testDir}/setup.cql failed, see setup.out and setup.err"
    errors=1
  fi
done
if [ $errors -ne 0 ]; then
  _fatal "One or more setup.cql failed. See above ERROR(s) for details."
fi

echo
echo "=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-"
echo " Executing test (CDM container)"
echo "=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-"
errors=0
for testDir in $(ls -d ${PHASE}/*); do
  _info ${testDir} Executing test
  docker exec ${DOCKER_CDM} bash $testDir/execute.sh /$testDir > $testDir/execute.out 2>$testDir/execute.err
  if [ $? -ne 0 ]; then
    _error "${testDir}/execute.sh failed, see $testDir/execute.out and $testDir/execute.err"
    errors=1
  fi
done
if [ $errors -ne 0 ]; then
  _fatal "One or more execute.sh failed. See above ERROR(s) for details."
fi

echo
echo "=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-"
echo " Checking for expected results (Cassandra container)"
echo "=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-"
errors=0
for testDir in $(ls -d ${PHASE}/*); do
  _info ${testDir} Check Expected Results 
  docker exec ${DOCKER_CASS} cqlsh -u $CASS_USERNAME -p $CASS_PASSWORD -f $testDir/expected.cql > $testDir/actual.out 2>$testDir/actual.err
  if [ $? -ne 0 ]; then
    _error "${testDir}/expected.cql failed, see actual.out and actual.err"
    errors=1
    continue
  fi
  diff -q $testDir/expected.out $testDir/actual.out > /dev/null 2>&1
  rtn=$?
  if [ $rtn -eq 1 ]; then
    _error "${testDir} files differ (expected vs actual):"
    sdiff ${testDir}/expected.out ${testDir}/actual.out
    errors=1
    continue
  elif [ $rtn -ne 0 ]; then
    _error "${testDir} had some other problem running diff"
    errors=1
    continue
  fi

  _info "PASS: ${testDir} returned expected results"
done
if [ $errors -ne 0 ]; then
  _fatal "One or more expected results failed. See above ERROR(s) for details."
fi

echo
echo "=========================================================="
echo " Phase ${PHASE} Complete"
echo "=========================================================="

