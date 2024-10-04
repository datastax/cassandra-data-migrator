#!/bin/bash

_usage() {
  cat <<EOF

usage: $0 -s scenario -f config_file [-d directory]

Required
  -s scenario      : scenario to run
  -f config_file   : a file with list of scenarios

Optional
  -d directory     : directory in which config_file may be found

config_file format
==================
File should be space-separated list of columns:

    scenario: any one word that forms the key
       class: a valid CDM class to invoke, e.g. datastax.astra.migrate.Migrate
  properties: path to a valid CDM .properties file, there can be different files for each scenario

Example file:
-------------------------
migrateData datastax.astra.migrate.Migrate /smoke/01_basic_kvp/migrate.properties
validateData datastax.astra.migrate.DiffData /smoke/01_basic_kvp/migrate.properties
-------------------------

Output will be put into the same directory as the .properties file named like:
  cdm.scenario.out (stdout)
  cdm.scenario.err (stderr)

So the above example would generate files:
  cdm.migrateData.out
  cdm.migrateData.err
  cdm.validateData.out
  cdm.validateData.err

EOF
exit 1
}

while getopts "s:f:d:" opt; do
  case $opt in 
    s) SCENARIO="$OPTARG"
       ;;
    f) CONFIG_FILENAME="$OPTARG"
       ;;
    d) CONFIG_DIR="$OPTARG"
       ;;
    ?) echo "ERROR invalid option was specified - ${OPTARG}"
       _usage
       ;;
  esac
done

argErrors=0
if [[ -z "$CONFIG_FILENAME" ]]; then
  echo "missing -f config_file"
  argErrors=1
else
  if [[ -z "${CONFIG_DIR}" ]]; then
    CONFIG_FILE="${CONFIG_FILENAME}"
  else
    CONFIG_FILE="${CONFIG_DIR}/${CONFIG_FILENAME}"
  fi
  if [[ ! -r ${CONFIG_FILE} ]]; then
    echo "config file ${CONFIG_FILE} not found, or is not readable"
    argErrors=1
  fi
fi

if [[ -z "$SCENARIO" ]]; then
  echo "missing -s scenario"
  argErrors=1
else
  if [[ $(egrep -c '^'$SCENARIO' ' $CONFIG_FILE) -ne 1 ]]; then
    echo "scenario not found in: ${CONFIG_FILE}"
    argErrors=1
  else
    CLASS=$(egrep '^'$SCENARIO' ' $CONFIG_FILE | awk '{print $2}')
    PROPERTIES=$(egrep '^'$SCENARIO' ' $CONFIG_FILE | awk '{print $3}')

    if [[ ! -r "${PROPERTIES}" ]]; then
      echo ".properties file not found: ${PROPERTIES}"
      argErrors=1
    fi
  fi
fi
if [ $argErrors -ne 0 ]; then
  _usage
fi

if [ ! -f /local/log4j2_docker.properties ]; then
  cd /local && jar xvf /local/cassandra-data-migrator.jar log4j2_docker.properties && cd -
fi

spark-submit --properties-file "${PROPERTIES}" \
  --master "local[*]" \
  --conf "spark.driver.extraJavaOptions=-Dlog4j.configurationFile=file:///local/log4j2_docker.properties -Dcom.datastax.cdm.log.level=DEBUG" \
  --conf "spark.executor.extraJavaOptions=-Dlog4j.configurationFile=file:///local/log4j2_docker.properties -Dcom.datastax.cdm.log.level=DEBUG" \
  --class ${CLASS} \
  /local/cassandra-data-migrator.jar

