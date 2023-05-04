#!/bin/bash -e

workingDir="$1"
cd "$workingDir"

dsbulk load -url $workingDir/data.csv -h $CASS_CLUSTER -u $CASS_USERNAME -p $CASS_PASSWORD -k origin -t regression_performance

/local/cdm.sh -f cdm.txt -s migrateData -d "$workingDir" > cdm.migrateData.out 2>cdm.migrateData.err
/local/cdm-assert.sh -f cdm.migrateData.out -a cdm.migrateData.assert -d "$workingDir"

/local/cdm.sh -f cdm.txt -s validateData -d "$workingDir" > cdm.validateData.out 2>cdm.validateData.err
/local/cdm-assert.sh -f cdm.validateData.out -a cdm.validateData.assert -d "$workingDir"

dsbulk load -url $workingDir/data_break.csv -h $CASS_CLUSTER -u $CASS_USERNAME -p $CASS_PASSWORD -k target -t regression_performance
cqlsh -u $CASS_USERNAME -p $CASS_PASSWORD $CASS_CLUSTER -f $workingDir/breakData.cql > $workingDir/breakData.out 2> $workingDir/breakData.err

/local/cdm.sh -f cdm.txt -s fixData -d "$workingDir" > cdm.fixData.out 2>cdm.fixData.err
/local/cdm-assert.sh -f cdm.fixData.out -a cdm.fixData.assert -d "$workingDir"
