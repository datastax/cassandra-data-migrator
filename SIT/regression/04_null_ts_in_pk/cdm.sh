#!/bin/bash -e

cat <<EOF
!!!!!!!!
!!!!!!!!  Testing Migrate
!!!!!!!!
EOF

/local/cdm.sh -c 
spark-submit \
  --properties-file /smoke/01_basic_kvp/migrate.properties \
  --master "local[*]" \
  --class datastax.astra.migrate.Migrate /local/cassandra-data-migrator.jar 

cat <<EOF
!!!!!!!!
!!!!!!!!  Testing DiffData
!!!!!!!!
EOF

spark-submit \
  --properties-file /smoke/01_basic_kvp/migrate.properties \
  --master "local[*]" \
  --class datastax.astra.migrate.DiffData /local/cassandra-data-migrator.jar
