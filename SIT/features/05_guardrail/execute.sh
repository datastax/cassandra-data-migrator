#!/bin/bash -e

workingDir="$1"
cd "$workingDir"

/local/cdm.sh -f cdm.txt -s guardrailCheck -d "$workingDir" > cdm.guardrailCheck.out 2>cdm.guardrailCheck.err
/local/cdm-assert.sh -f cdm.guardrailCheck.out -a cdm.guardrailCheck.assert -d "$workingDir"

/local/cdm.sh -f cdm.txt -s migrateData -d "$workingDir" > cdm.migrateData.out 2>cdm.migrateData.err
/local/cdm-assert.sh -f cdm.migrateData.out -a cdm.migrateData.assert -d "$workingDir"

/local/cdm.sh -f cdm.txt -s validateData -d "$workingDir" > cdm.validateData.out 2>cdm.validateData.err
/local/cdm-assert.sh -f cdm.validateData.out -a cdm.validateData.assert -d "$workingDir"

/local/cdm.sh -f cdm.txt -s fixData -d "$workingDir" > cdm.fixData.out 2>cdm.fixData.err
/local/cdm-assert.sh -f cdm.fixData.out -a cdm.fixData.assert -d "$workingDir"
