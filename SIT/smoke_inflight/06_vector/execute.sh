#!/bin/bash -e

workingDir="$1"
cd "$workingDir"

for scenario in $(cat cdm.txt | awk '{print $1}'); do
  /local/cdm.sh -f cdm.txt -s $scenario -d "$workingDir" > "cdm.$scenario.out" 2>cdm.$scenario.err
  /local/cdm-assert.sh -f "cdm.$scenario.out" -a "cdm.$scenario.assert" -d "$workingDir"
done
