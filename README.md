# cassandra-data-migrator

Spark jobs in this repo can be used for data migration and data validation.

> :warning: Please note this job has been tested with spark version [2.4.8](https://archive.apache.org/dist/spark/spark-2.4.8/)

## Prerequisite

Install Java8 as spark binaries are compiled with it.
Install single instance of spark on a node where you want to run this job. Spark can be installed by running the following: -

```
wget https://downloads.apache.org/spark/spark-2.4.8/
tar -xvzf <spark downloaded file name>
```

# Steps:

1. `sparkConf.properties` file needs to be configured as applicable for the environment
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
22/09/27 11:21:24 ERROR DiffJobSession: Data mismatch found -  Key: ek-1 %% mn1 %% c1 %% true Data:  (Index: 4 Origin: 30 Target: 20 )
22/09/27 11:21:24 ERROR DiffJobSession: Corrected mismatch data in Astra: ek-1 %% mn1 %% c1 %% true
22/09/27 11:21:24 ERROR DiffJobSession: Data is missing in Astra: ek-2 %% mn2 %% c2 %% true
22/09/27 11:21:24 ERROR DiffJobSession: Corrected missing data in Astra: ek-2 %% mn2 %% c2 %% true
```

- Please grep for all `ERROR` from the output log files to get the list of missing and mismatched records.
  - Note that it lists differences by partition key values.
- The Validation job can also be run in an AutoCorrect mode. This mode can
  - Add any missing records from origin to target
  - Fix any inconsistencies between origin and target (makes target same as origin). 
- Enable/disable this feature using one or both of the below setting in the config file

```
spark.target.autocorrect.mismatch                   true|false
spark.target.custom.writeTime                       true|false
```

# Migrating specific partition ranges
- You can also use the tool to migrate specific partition ranges, use class option `--class datastax.astra.migrate.MigratePartitionsFromFile` as shown below
```
./spark-submit --properties-file sparkConf.properties /
--master "local[*]" /
--class datastax.astra.migrate.MigratePartitionsFromFile cassandra-data-migrator-1.x.jar &> logfile_name.txt
```

When running in above mode the tool assumes a `partitions.csv` file to be present in the current folder in the below format, where each line (`min,max`) represents a partition-range 
```
-507900353496146534,-107285462027022883
-506781526266485690,1506166634797362039
2637884402540451982,4638499294009575633
798869613692279889,8699484505161403540
```
This mode is specifically useful to processes a subset of partition-ranges that may have generated errors as a result of a previous long-running job to migrate a large table.

# Additional features
- [Counter tables](https://docs.datastax.com/en/dse/6.8/cql/cql/cql_using/useCountersConcept.html)
- Preserve [writetimes](https://docs.datastax.com/en/dse/6.8/cql/cql/cql_reference/cql_commands/cqlSelect.html#cqlSelect__retrieving-the-datetime-a-write-occurred-p) and [TTL](https://docs.datastax.com/en/dse/6.8/cql/cql/cql_reference/cql_commands/cqlSelect.html#cqlSelect__ref-select-ttl-p)
- Advanced DataTypes ([Sets](https://docs.datastax.com/en/dse/6.8/cql/cql/cql_reference/refDataTypes.html#refDataTypes__set), [Lists](https://docs.datastax.com/en/dse/6.8/cql/cql/cql_reference/refDataTypes.html#refDataTypes__list), [Maps](https://docs.datastax.com/en/dse/6.8/cql/cql/cql_reference/refDataTypes.html#refDataTypes__map), [UDTs](https://docs.datastax.com/en/dse/6.8/cql/cql/cql_reference/refDataTypes.html#refDataTypes__udt))
- Filter records from origin using writetime
- SSL Support (including custom cipher algorithms)
- Migrate from any Cassandra origin ([Apache Cassandra](https://cassandra.apache.org)/[DataStax Enterprise (DSE)](https://www.datastax.com/products/datastax-enterprise)/[DataStax Astra DB](https://www.datastax.com/products/datastax-astra)) to any Cassandra target ([Apache Cassandra](https://cassandra.apache.org)/[DataStax Enterprise (DSE)](https://www.datastax.com/products/datastax-enterprise)/[DataStax Astra DB](https://www.datastax.com/products/datastax-astra))
- Validate migration accuracy and performance using a smaller randomized data-set
- Custom writetime
