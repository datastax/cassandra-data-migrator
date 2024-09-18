#!/bin/bash

assertCmd="egrep 'JobCounter.* Final ' \${OUTPUT_FILE} | sed 's/^.*Final //'"

_usage() {
  cat <<EOF

usage: $0 -f output_file -a assert_file [-d directory]

Required
  -f output_file   : a file with list of scenarios, same format as cdm.sh
  -a assert_file    : a file with the assertions

Optional
  -d directory     : directory in which output_file and assertFile may be found

==================
assert_file Format
==================
Expected to contain the "Final" job session summary information, generated similar to
    ${assertCmd}

File might look like:
---------------
Read Record Count: 3
Mismatch Record Count: 0
Corrected Mismatch Record Count: 0
Missing Record Count: 0
Corrected Missing Record Count: 0
Valid Record Count: 3
Skipped Record Count: 0
Error Record Count: 0
---------------

EOF
exit 1
}

while getopts "f:d:a:" opt; do
  case $opt in 
    f) OUTPUT_FILENAME="$OPTARG"
       ;;
    d) CONFIG_DIR="$OPTARG"
       ;;
    a) ASSERT_FILENAME="$OPTARG"
       ;;
    ?) echo "ERROR invalid option was specified - ${OPTARG}"
       _usage
       ;;
  esac
done

argErrors=0
if [[ -z "$OUTPUT_FILENAME" ]]; then
  echo "missing -o output_file"
  argErrors=1
else
  if [[ -z "${CONFIG_DIR}" ]]; then
    OUTPUT_FILE="${OUTPUT_FILENAME}"
  else
    OUTPUT_FILE="${CONFIG_DIR}/${OUTPUT_FILENAME}"
  fi
  if [[ ! -r ${OUTPUT_FILE} ]]; then
    echo "config file ${OUTPUT_FILE} not found, or is not readable"
    argErrors=1
  fi
fi

if [[ -z "$ASSERT_FILENAME" ]]; then
  echo "missing -a assert_file"
  argErrors=1
else
  if [[ -z "${CONFIG_DIR}" ]]; then
    ASSERT_FILE="${ASSERT_FILENAME}"
  else
    ASSERT_FILE="${CONFIG_DIR}/${ASSERT_FILENAME}"
  fi
  if [[ ! -r ${ASSERT_FILE} ]]; then
    echo "config file ${ASSERT_FILE} not found, or is not readable"
    argErrors=1
  fi
fi


if [ $argErrors -ne 0 ]; then
  _usage
fi

WORKING_FILE="${ASSERT_FILENAME}".actual.out
if [[ -n "${CONFIG_DIR}" ]]; then
  WORKING_FILE="${CONFIG_DIR}/${WORKING_FILE}"
fi

eval ${assertCmd} > ${WORKING_FILE}

diff -q ${ASSERT_FILE} ${WORKING_FILE} > /dev/null 2>&1
rtn=$?
if [ $rtn -eq 1 ]; then
  echo "ERROR: CDM output $(basename ${OUTPUT_FILE}) differs from expected (expected vs actual):"
  sdiff ${ASSERT_FILE} ${WORKING_FILE}
  exit 1
fi
