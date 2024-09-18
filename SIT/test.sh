#!/bin/bash

_usage() {
  cat <<EOF

usage: $0 -p phase 

Required
  -p phase: one of ${PHASES}

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

# Common environment and functions
. common.sh

_captureOutput() {
  _info "Copying ${DOCKER_CDM}:/${testDir} into ${testDir}/output/"
  docker cp ${DOCKER_CDM}:/${testDir} ${testDir}/output/
  
  _info "Moving ${testDir}/output/$(basename ${testDir})/*.out TO ${testDir}/output/"
  find ${testDir}/output/$(basename ${testDir})/ -type f -name "*.out" | xargs -I{} mv {} ${testDir}/output/

  _info "Moving ${testDir}/output/$(basename ${testDir})/*.err TO ${testDir}/output/"
  find ${testDir}/output/$(basename ${testDir})/ -type f -name "*.err" | xargs -I{} mv {} ${testDir}/output/

  _info "Moving ${testDir}/output/$(basename ${testDir})/output/*.out TO ${testDir}/output"
  find ${testDir}/output/$(basename ${testDir})/output/ -type f -name "*.out" | xargs -I{} mv {} ${testDir}/output/

  _info "Moving ${testDir}/output/$(basename ${testDir})/output/*.err TO ${testDir}/output/"
  find ${testDir}/output/$(basename ${testDir})/output/ -type f -name "*.err" | xargs -I{} mv {} ${testDir}/output/

  _info "Removing ${testDir}/output/$(basename ${testDir})"
  rm -rf ${testDir}/output/$(basename ${testDir})
}

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
  export testDir
  for f in ${EXPECTED_FILES}; do
    if [[ ! -f ${testDir}/$f ]]; then
      _error "${testDir} is missing ${f}"
      errors=1
    fi
  done

  # Clean up any previous results that may exist
  for f in ${GENERATED_FILES}; do
    rm -rf ${testDir}/$f
  done
  rm -rf ${testDir}/output/*
  mkdir -p ${testDir}/output
  chmod -R 777 ${testDir}/output
done

# The .jar file is expected to be present
docker exec ${DOCKER_CDM} bash -c "ls -d ${CDM_JAR} >/dev/null 2>&1"
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
  docker exec ${dockerContainer} chmod -R 755 ./${PHASE}/*/*.sh
done

echo
echo "=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-"
echo " Setting up tables and data (Cassandra container)"
echo "=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-"
errors=0
for testDir in $(ls -d ${PHASE}/*); do
  export testDir
  _info ${testDir} Setup tables and data 
  docker exec ${DOCKER_CASS} bash -c "cqlsh -u ${CASS_USERNAME} -p ${CASS_PASSWORD} -f ${testDir}/setup.cql" > ${testDir}/output/setup.out 2>${testDir}/output/setup.err
  if [ $? -ne 0 ]; then
    _error "${testDir}/setup.cql failed, see $testDir/output/setup.out and $testDir/output/setup.err"
    errors=1
  fi
done
if [ $errors -ne 0 ]; then
  _captureOutput
  _fatal "One or more setup.cql failed. See above ERROR(s) for details."
fi

echo
echo "=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-"
echo " Executing test (CDM container)"
echo "=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-"
errors=0
for testDir in $(ls -d ${PHASE}/*); do
  export testDir
  _info ${testDir} Executing test
  docker exec ${DOCKER_CDM} bash -e -c "${testDir}/execute.sh /${testDir}" > ${testDir}/output/execute.out 2>${testDir}/output/execute.err
  if [ $? -ne 0 ]; then
    _error "${testDir}/execute.sh failed, see $testDir/output/execute.out and $testDir/output/execute.err"
    echo "=-=-=-=-=-=-=- Container Directory Listing -=-=-=-=-=-=-=-"
    echo "$(docker exec ${DOCKER_CDM} ls -laR ${testDir})"
    echo "=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-==-=-=-=-=-=-=-=-=-=-=-=-"
    _captureOutput
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
  export testDir
  if [ -x ${testDir}/alternateCheckResults.sh ]; then
    _info ${testDir} Running Alternate Check Expected Results 
    # This provides a mechanism that allows a more nuanced means of generating actual.out
    # as not every assertion can be based on a simple "dump from cqlsh"
    ${testDir}/alternateCheckResults.sh > $testDir/output/actual.out 2>$testDir/output/actual.err
  else 
    _info ${testDir} Check Expected Results 
    docker exec ${DOCKER_CASS} bash -c "cqlsh -u ${CASS_USERNAME} -p ${CASS_PASSWORD} -f ${testDir}/expected.cql" > ${testDir}/output/actual.out 2>${testDir}/output/actual.err
  fi
  if [ $? -ne 0 ]; then
    _error "${testDir}/expected.cql failed, see $testDir/output/actual.out $testDir/output/and actual.err"
    _captureOutput
    errors=1
    continue
  fi
  diff -q $testDir/expected.out $testDir/output/actual.out > /dev/null 2>&1
  rtn=$?
  if [ $rtn -eq 1 ]; then
    _error "${testDir} files differ (expected vs actual):"
    sdiff -w 200 ${testDir}/expected.out ${testDir}/output/actual.out
    _captureOutput
    errors=1
    continue
  elif [ $rtn -ne 0 ]; then
    _error "${testDir} had some other problem running diff"
    _captureOutput
    errors=1
    continue
  fi

  _info "PASS: ${testDir} returned expected results"
done
if [ $errors -ne 0 ]; then
  _fatal "One or more expected results failed. See above ERROR(s) for details."
fi


_captureOutput

echo
echo "=========================================================="
echo " Phase ${PHASE} Complete"
echo "=========================================================="

