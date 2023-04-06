#!/bin/bash
# We need TTL to go down by at least 1 second, e.g. make 60000 to 59999
sleep 1
docker exec ${DOCKER_CASS} /bin/bash -c "cqlsh -u $CASS_USERNAME -p $CASS_PASSWORD -f $testDir/expected.cql | tr -d ' ' | cut -c1-46"
