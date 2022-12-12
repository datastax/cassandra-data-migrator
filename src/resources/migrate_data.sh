#! /bin/bash

###########################################################################################################################
#
# This script can be used to Migrate data between two Cassandra Clusters (including Astra) in chunks. It migrates data
# sequentially in progressive token-range slices. It also helps to restart migration from a point where the previous
# run might have stopped/failed for whatever reasons.
#
# Before running the script, update the below params
#        SPARK_SUBMIT - Path to the spark-submit command
#        PROPS_FILE - Path to the spark configuration for the table
#        S_IDX - Change this value only if you want to set a custom starting point (e.g. after a previous incomplete run)
#
# *** IMP Note: Run this script using nohup in background using a logfile and tail the logfile to monitor progress ***
# e.g.  nohup ./migrate_data.sh > logs/spark/migrate_data.out &
#
# To monitor migration progress, you could use the below command
# grep "Running Migrate for Partition Range" logs/spark/migrate_data.out
#
###########################################################################################################################

# Path to spark-submit
SPARK_SUBMIT=/home/ubuntu/spark-2.4.8-bin-hadoop2.6/bin/spark-submit

# Path to spark configuration for the table
PROPS_FILE=/home/ubuntu/sparkConf.properties

# Starting partition token (Default is Min possible value of a Cassandra token - min long value in Java).
# Change this value only if you want to start from a custom partition token (e.g. when a migrate job failed midway)
S_IDX=-9223372036854775808

# ** DO NOT CHANGE ANYTHING BELOW THIS **
SLICE=999999999999999999

echo "Starting Migration using $PROPS_FILE !!"

# Migrate initial partition tokens from min-long to -9000000000000000000
if [ $S_IDX -lt -9000000000000000000 ]
then
  E_IDX=-9000000000000000001
  echo "Running Migrate for Partition Range $S_IDX to $E_IDX .."
  $SPARK_SUBMIT --properties-file $PROPS_FILE --master "local[*]" --conf spark.origin.minPartition=$S_IDX --conf spark.origin.maxPartition=$E_IDX --class datastax.astra.migrate.Migrate cassandra-data-migrator-*.jar
  S_IDX=-9000000000000000000
fi

# Migrate partition tokens from -9000000000000000000 to 8999999999999999999 in slices of 1000000000000000000
while [ $S_IDX -lt 9000000000000000000 ]
do
  if [ $S_IDX -gt 8223372036854775807 ]
  then
    E_IDX=8999999999999999999
  else
    E_IDX=$(( $S_IDX + $SLICE ))
  fi
  echo "Running Migrate for Partition Range $S_IDX to $E_IDX .."
  $SPARK_SUBMIT --properties-file $PROPS_FILE --master "local[*]" --conf spark.origin.minPartition=$S_IDX --conf spark.origin.maxPartition=$E_IDX --class datastax.astra.migrate.Migrate cassandra-data-migrator-*.jar
  S_IDX=$(( $E_IDX + 1 ))
done

# Migrate final partition tokens from 9000000000000000000 to max-long
E_IDX=9223372036854775807
echo "Running Migrate for Partition Range $S_IDX to 9223372036854775807 .."
$SPARK_SUBMIT --properties-file $PROPS_FILE --master "local[*]" --conf spark.origin.minPartition=$S_IDX --conf spark.origin.maxPartition=$E_IDX --class datastax.astra.migrate.Migrate cassandra-data-migrator-*.jar
echo "Completed Migration using $PROPS_FILE !!"
