#!/bin/bash -e

workingDir="$1"
cd "$workingDir"

for scenario in $(cat cdm.txt | awk '{print $1}'); do
  /local/cdm.sh -f cdm.txt -s $scenario -d "$workingDir"
done

