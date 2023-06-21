#!/bin/bash -e

workingDir="$1"
cd "$workingDir"

/local/cdm.sh -f cdm.txt -s migrateDataDefault -d "$workingDir"
/local/cdm.sh -f cdm.txt -s migrateData -d "$workingDir"


