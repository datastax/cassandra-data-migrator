# astra-spark-migration-ranges

Spark jobs in this repo can be used for data migration and data validation.

> Please note: This job has been tested with spark version [2.4.8](https://downloads.apache.org/spark/spark-2.4.8/)

## Prerequisite

Install Java8 as spark binaries are compiled with it.
Install single instance of spark on a node where you want to run this job. Spark can be installed by running the following: -

```
wget https://downloads.apache.org/spark/spark-2.4.8/
tar -xvzf <spark downloaded file name>
```

# Steps:

1. sparkConf.properties file needs to be configured as applicable for the environment
   > A sample Spark conf file configuration can be found here: `astra-spark-migration-ranges/src/resources/sparkConf.properties`

```
Example of the conf file is below: -
spark.migrate.source.isAstra                                    false
spark.migrate.source.host                                       <host contact point>
spark.migrate.source.username                                   <username>
spark.migrate.source.password                                   <password>
spark.migrate.source.read.consistency.level                     LOCAL_QUORUM
spark.migrate.source.keyspaceTable                              test.a1

spark.migrate.destination.isAstra                               true
spark.migrate.destination.scb                                   file:///aaa/bbb/secure-connect-enterprise.zip
spark.migrate.destination.username                              <astra-client-id>
spark.migrate.destination.password                              <astra-client-secret>
spark.migrate.destination.read.consistency.level                LOCAL_QUORUM
spark.migrate.destination.keyspaceTable                         test.a2
spark.migrate.destination.autocorrect.missing                   false
spark.migrate.destination.autocorrect.mismatch                  false
```

2. Place the conf file where it can be accessed while running the job via spark-submit.
3. Generate a fat jar (`migrate-0.x.jar`) using command `mvn clean package`
4. Run the 'Data Migration' job using `spark-submit` command as shown below:

```
./spark-submit --properties-file sparkConf.properties /
--master "local[*]" /
--conf spark.migrate.source.minPartition=-9223372036854775808 /
--conf spark.migrate.source.maxPartition=9223372036854775807 /
--class datastax.astra.migrate.Migrate migrate-0.x.jar &> logfile_name.txt
```

Note: Above command also generates a log file `logfile_name.txt` to avoid log output on the console.


# Data-validation job

- To run the job in Data validation mode, use class option `--class datastax.astra.migrate.DiffData` as shown below

```
./spark-submit --properties-file sparkConf.properties /
--master "local[*]" /
--conf spark.migrate.source.minPartition=-9223372036854775808 /
--conf spark.migrate.source.maxPartition=9223372036854775807 /
--class datastax.astra.migrate.DiffData migrate-0.x.jar &> logfile_name.txt
```

- Validation job will report differences as “ERRORS” in the log file as shown below

```
22/02/16 12:41:15 ERROR DiffJobSession: Data is missing in Astra: e7cd5752-bc0d-4157-a80f-7523add8dbcd
22/02/16 12:41:15 ERROR DiffJobSession: Data difference found -  Key: 1 Data:  (Index: 3 Source: [val-A, val-B] Astra: [val-A, val-B, val-C] )
```

- Please grep for all `ERROR` from the output log files to get the list of missing and mismatched records.
  - Note that it lists differences by partition key values.
- The Validation job can also be run in an AutoCorrect mode. This mode can
  - Add any missing records from source to target
  - Fix any inconsistencies between source and target (makes target same as source). 
- Enable/disable this feature using one or both of the below setting in the config file

```
spark.migrate.destination.autocorrect.missing                   true|false
spark.migrate.destination.autocorrect.mismatch                  true|false
```

# Additional features
- Counter tables
- Preserve writetimes and TTL
- Advanced DataTypes (Sets, Lists, Maps, UDTs)
- Filter records from source using writetime
- SSL Support (including custom cipher algorithms)
- Migrate from any Cassandra source (Cassandra/DSE/Astra) to any Cassandra target (Cassandra/DSE/Astra)
- Validate migration accuracy and performance using a smaller randomized data-set
