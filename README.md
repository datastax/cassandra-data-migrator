# astra-spark-migration-ranges

Spark jobs in this repo can be used for data migration and data validation.

> Please note: This job has been tested with spark version [2.4.8](https://downloads.apache.org/spark/spark-2.4.8/)

## Prerequisite

Install single instance of spark on a node where we want to run this job. Spark can be installed by running the following: -

```
wget https://downloads.apache.org/spark/spark-2.4.8/
tar -xvzf <spark downloaded file name>
```

# Steps:

1. sparkConf.properties file needs to be configured accordingly
   > Sample Spark conf file configuration can be found here: `astra-spark-migration-ranges/src/resources/sparkConf.properties`

```
Example of the conf file is below: -
`spark.migrate.source.username <Source cluster user_id>`
`spark.migrate.source.password <Source cluster password>`
`spark.migrate.source.host <Source cluster host ip address or contact ip address>`
`spark.migrate.astra.scb <path to scb for the target database on Astra>`
`spark.migrate.astra.username <Astra client_id from the token file>`
`spark.migrate.astra.password <Client_Secret from token file>`
`spark.migrate.keyspaceName <keyspace name>`
`spark.migrate.tableName <table name>`
`spark.migrate.readRateLimit 200000 <can be configured as needed>`
`spark.migrate.writeRateLimit 200000 <can be configured as needed>`
`spark.migrate.batchSize 2 <batch size can be configured as needed>`
`spark.migrate.source.writeTimeStampFilter 0`
```

2. Place the conf file where it can be accessed while running the job via spark-submit.
3. If the project is not already packaged, it needs to be packaged via mvn. It will be generate a fat jar called `migrate-0.x.jar`
4. Find the Min and Max partition values for the table using dsbulk as shown below:

```
dsbulk unload -h <contact_points> -query "select token(<partition_keys>) from <keyspace>.<table>;" -verbosity 0 --connector.csv.header false | sort -un | { tee >(head -1 >&2; cat >/dev/null) | tail -1; }
```

5. Once the jar file is ready, we can run the 'Data Migration' job via spark-submit command, below is spark submit for example:

```
./spark-submit --properties-file /media/bulk/sparkConf.properties /
--master "local[*]" /
--conf spark.migrate.source.minPartition=-9223372036854775808 /
--conf spark.migrate.source.maxPartition=9223372036854775807 /
--class datastax.astra.migrate.Migrate /media/bulk/migrate-0.x.jar
```

6. Additionally you could write the output to a log file like “logfile_name.txt” to avoid getting the output on the console.

```
./spark-submit --properties-file /media/bulk/sparkConf.properties /
--master "local[*]" /
--conf spark.migrate.source.minPartition=-9223372036854775808 /
--conf spark.migrate.source.maxPartition=9223372036854775807 /
--class datastax.astra.migrate.Migrate /media/bulk/migrate-0.x.jar &> logfile_name.txt
```

# Data-validation job

- For Data validation same prerequisite applies as Data migration, however you will need to use the class option `--class datastax.astra.migrate.DiffData`

```
./spark-submit --properties-file /media/bulk/sparkConf.properties /
--master "local[*]" /
--conf spark.migrate.source.minPartition=-9223372036854775808 /
--conf spark.migrate.source.maxPartition=9223372036854775807 /
--class datastax.astra.migrate.DiffData /media/bulk/migrate-0.x.jar
```

- On the output of the run, the job will report differences as “ERRORS” as shown below

```
22/02/16 12:41:15 INFO Executor: Finished task 5.0 in stage 0.0 (TID 5). 794 bytes result sent to driver
22/02/16 12:41:15 INFO Executor: Finished task 1.0 in stage 0.0 (TID 1). 751 bytes result sent to driver
22/02/16 12:41:15 INFO TaskSetManager: Finished task 2.0 in stage 0.0 (TID 2) in 2814 ms on localhost (executor driver) (3/6)
22/02/16 12:41:15 INFO TaskSetManager: Finished task 3.0 in stage 0.0 (TID 3) in 2814 ms on localhost (executor driver) (4/6)
22/02/16 12:41:15 ERROR DiffJobSession: Data is missing in Astra: e7cd5752-bc0d-4157-a80f-7523add8dbcd
22/02/16 12:41:15 ERROR DiffJobSession: Data difference found -  Key: 1 Data:  (Index: 3 Source: [val-A, val-B] Astra: [val-A, val-B, val-C] )
22/02/16 12:41:15 INFO DiffJobSession: TreadID: 57 Final Read Record Count: 1
```

- Please grep for all `ERROR` from the output log files to get the list of differences, notice that its listing differences by partition key value in this case.
