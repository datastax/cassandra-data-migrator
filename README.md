# cassandra-data-migrator

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
   > A sample Spark conf file configuration can be [found here](./src/resources/sparkConf.properties)
2. Place the conf file where it can be accessed while running the job via spark-submit.
3. Generate a fat jar (`cassandra-data-migrator-1.x.jar`) using command `mvn clean package`
4. Run the 'Data Migration' job using `spark-submit` command as shown below:

```
./spark-submit --properties-file sparkConf.properties /
--master "local[*]" /
--class datastax.astra.migrate.Migrate cassandra-data-migrator-1.x.jar &> logfile_name.txt
```

Note: Above command also generates a log file `logfile_name.txt` to avoid log output on the console.


# Data-validation job

- To run the job in Data validation mode, use class option `--class datastax.astra.migrate.DiffData` as shown below

```
./spark-submit --properties-file sparkConf.properties /
--master "local[*]" /
--class datastax.astra.migrate.DiffData cassandra-data-migrator-1.x.jar &> logfile_name.txt
```

- Validation job will report differences as “ERRORS” in the log file as shown below

```
22/09/27 11:21:24 ERROR DiffJobSession: Data mismatch found -  Key: ek-1 %% mn1 %% c1 %% true Data:  (Index: 4 Source: 30 Astra: 20 )
22/09/27 11:21:24 ERROR DiffJobSession: Corrected mismatch data in Astra: ek-1 %% mn1 %% c1 %% true
22/09/27 11:21:24 ERROR DiffJobSession: Data is missing in Astra: ek-2 %% mn2 %% c2 %% true
22/09/27 11:21:24 ERROR DiffJobSession: Corrected missing data in Astra: ek-2 %% mn2 %% c2 %% true
```

- Please grep for all `ERROR` from the output log files to get the list of missing and mismatched records.
  - Note that it lists differences by partition key values.
- The Validation job can also be run in an AutoCorrect mode. This mode can
  - Add any missing records from source to target
  - Fix any inconsistencies between source and target (makes target same as source). 
- Enable/disable this feature using one or both of the below setting in the config file

```
spark.destination.autocorrect.missing                   true|false
spark.destination.autocorrect.mismatch                  true|false
```

# Additional features
- Counter tables
- Preserve writetimes and TTL
- Advanced DataTypes (Sets, Lists, Maps, UDTs)
- Filter records from source using writetime
- SSL Support (including custom cipher algorithms)
- Migrate from any Cassandra source (Cassandra/DSE/Astra) to any Cassandra target (Cassandra/DSE/Astra)
- Validate migration accuracy and performance using a smaller randomized data-set
