#!/bin/bash -e

workingDir="$1"
cd "$workingDir"

/local/cdm.sh -f cdm.txt -s migrateData -d "$workingDir"

cqlsh -u $CASS_USERNAME -p $CASS_PASSWORD $CASS_CLUSTER -f $workingDir/breakData.cql > $workingDir/other.breakData.out 2> $workingDir/other.breakData.err

/local/cdm.sh -f cdm.txt -s validateData -d "$workingDir"

