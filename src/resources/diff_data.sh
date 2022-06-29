#! /bin/bash

########################################################################################################################
#
# This script can be used to find differences in a certain percent of data (typically after a data migration) between
# two Cassandra Clusters (including Astra). This script will divide the Cassandra token range into 200 smaller slices
# that add up to the user-defined percent.
#   e.g. If you decide to perform a 2% diff on a table with 1 billion rows, this script will perform 200 smaller diffs
#   of 100K slice each. Total diff volume will be 100K * 200 i.e. 20 million i.e. 2% of 1 billion.
#
# Before running the script, update the below params
#        SPARK_SUBMIT - Path to the spark-submit command
#        PROPS_FILE - Path to the spark configuration for the table
#        VALIDATE_PERCENT - Int value between 1 and 20 - Percent of data to be validated
#
# Run this script using nohup in background using a logfile and tail the logfile to monitor progress
# e.g.  nohup ./diff_data.sh > logs/spark/diff_data.out &
#
# To summarise the results after migration, you could use the below command
# egrep "Running DiffData for Partition Range|Job Final Read Record Count|Job Final Read Valid Count" logs/spark/diff_data.out
#
# To validate results, run below command. Result should be 0 or close to 0, if not, find the cause of diff
# grep "ERROR DiffJobSession" logs/spark/diff_data.out | wc -l
#
########################################################################################################################

# Path to spark-submit
SPARK_SUBMIT=/home/ubuntu/spark-2.4.8-bin-hadoop2.6/bin/spark-submit

# Path to spark configuration for the table
PROPS_FILE=/home/ubuntu/sparkConf.properties

# Set the percent (between 1 to 20) of data to be validated - value over 20 is not advised
VALIDATE_PERCENT=1

# ** DO NOT CHANGE ANYTHING BELOW THIS **

# Starting partition token
MIN_TOKEN=-9223372036854775808
MAX_TOKEN=9223372036854775807
S_IDX=$MIN_TOKEN

# Slice unit - this will create around 200 units within Cassandra token range (between min-long to max-long)
SLICE_UNIT=$(( $MAX_TOKEN / 100 ))

# Slice to be validated/diff within each of the 200 slice units based on input VALIDATE_PERCENT
SLICE_DIFF=$(( ( $SLICE_UNIT / 100 ) * $VALIDATE_PERCENT ))

echo "Starting DiffData using $PROPS_FILE !!"

# Validate a percent of partition-token-ranges in progressive slices
CEIL=$(( $MAX_TOKEN - $SLICE_UNIT ))
while [ $S_IDX -lt $CEIL ]
do
  E_IDX=$(( $S_IDX + $SLICE_DIFF ))
  echo "Running DiffData for Partition Range $S_IDX to $E_IDX .."
  $SPARK_SUBMIT --properties-file $PROPS_FILE --master "local[*]" --conf spark.migrate.source.minPartition=$S_IDX --conf spark.migrate.source.maxPartition=$E_IDX --class datastax.astra.migrate.DiffData migrate-*.jar
  S_IDX=$(( $S_IDX + $SLICE_UNIT + 1 ))
done

echo "Completed DiffData using $PROPS_FILE !!"
