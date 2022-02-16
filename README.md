# astra-spark-migration-ranges

Spark jobs in this repo can be used for data migration and data validation.

>Please note: This job has been tested with spark version [2.4.8](https://downloads.apache.org/spark/spark-2.4.8/)

## Pre-requisite

Install single instance of spark on a node where we want to run this job. Spark can be installed by running the following: -

```
wget https://downloads.apache.org/spark/spark-2.4.8/
tar -xvzf <spark downloaded file name>
```

# Steps:

1. sparkConf.properties file needs to be configured accordingly, for example below is the conf file for data migration job.
> Sample Spark conf file configuration for `Data Migration` can be found here: `astra-spark-migration-ranges/src/resources/sparkConf.properties`

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

2. Place the conf file where it can be accessed while runninn the job via spark-submit.
3. If the project is not already packaged, it needs to packaged via mvn. It will be generate a fat jar called `migrate-0.1.jar`
4. Once the jar file is ready, we can run the 'Data Migration' job via spark-submit command, below is spark submit for example:

```
./spark-submit --properties-file /media/bulk/sparkConf.properties /
--master "local[*]" /
--conf spark.migrate.source.minPartition=-9223372036854775808 /
--conf spark.migrate.source.maxPartition=9223372036854775807 /
--class datastax.astra.migrate.Migrate /media/bulk/migrate-0.1.jar
```

5. Additionally you could write the output to a log file like “logfile_name.txt” to avoid getting the output on the console.

```
./spark-submit --properties-file /media/bulk/sparkConf.properties /
--master "local[*]" /
--conf spark.migrate.source.minPartition=-9223372036854775808 /
--conf spark.migrate.source.maxPartition=9223372036854775807 /
--class datastax.astra.migrate.Migrate /media/bulk/migrate-0.1.jar &> logfile_name.txt
```

6. On the output of the run, job will report “ERRORS” like below reporting data differences.

```
22/01/14 10:17:10 ERROR CopyJobSession: Data difference found - Key: 5b6962dd-3f90-4c93-8f61-eabfa4a803e5 Data: (Index: 1 Source: Anoop Astra: AA ) (Index: 2 Source: T Astra: TIW )
22/01/14 10:17:10 INFO CopyJobSession: TreadID: 56 Final Differences Count: 1
22/01/14 10:17:10 INFO CopyJobSession: TreadID: 56 Final Valid Count: 1
22/01/14 10:17:10 INFO Executor: Finished task 3.0 in stage 0.0 (TID 3). 751 bytes result sent to driver
22/01/14 10:17:10 INFO TaskSetManager: Finished task 3.0 in stage 0.0 (TID 3) in 4541 ms on localhost (executor driver) (3/6)
22/01/14 10:17:10 ERROR CopyJobSession: Data is missing in Astra: e7cd5752-bc0d-4157-a80f-7523add8dbcd
22/01/14 10:17:10 ERROR CopyJobSession: Data is missing in Astra: 5b6962dd-3f90-4c93-8f61-eabfa4a803e3
```

7. Please grep for all `ERROR` from the output log files to get the list of differences, notice that its listing differences by partition key value in this case.

# Data-validation job

* For Data validation same pre-requisite applies as Data migration, however you will need to use the class option `--class datastax.astra.migrate.DiffData`
> Sample Spark conf file configuration for `Data Validation` can be found here: astra-spark-migration-ranges/src/resources/sparkConf.properties`astra-spark-migration-ranges/src/resources/SparkConfDataValidation.txt`

```
./spark-submit --properties-file /media/bulk/sparkConf.properties /
--master "local[*]" /
--conf spark.migrate.source.minPartition=-9223372036854775808 /
--conf spark.migrate.source.maxPartition=9223372036854775807 /
--class datastax.astra.migrate.DiffData /media/bulk/migrate-0.1.jar
```
