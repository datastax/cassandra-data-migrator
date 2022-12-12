# cassandra-data-migrator

Migrate and Validate Tables between Origin and Target Cassandra Clusters.

> :warning: Please note this job has been tested with spark version [2.4.8](https://archive.apache.org/dist/spark/spark-2.4.8/)

## Container Image
- Get the latest image that includes all dependencies from [DockerHub](https://hub.docker.com/r/datastax/cassandra-data-migrator) 
  - If you use this route, all migration tools (`cassandra-data-migrator` + `dsbulk` + `cqlsh`) would be available in the `/assets/` folder of the container
- OR follow the below build steps (and Prerequisite) to build the jar locally

### Prerequisite

- Install Java8 as spark binaries are compiled with it.
- Install Maven 3.8.x
- Install single instance of spark on a node where you want to run this job. Spark can be installed by running the following: -

```
wget https://downloads.apache.org/spark/spark-2.4.8/
tar -xvzf <spark downloaded file name>
```

### Build
1. Clone this repo
2. Move to the repo folder `cd cassandra-data-migrator`
3. Run the build `mvn clean package`
4. The fat jar (`cassandra-data-migrator-2.x.x.jar`) file should now be present in the `target` folder

# Steps for Data-Migration:

1. `sparkConf.properties` file needs to be configured as applicable for the environment
   > A sample Spark conf file configuration can be [found here](./src/resources/sparkConf.properties)
2. Place the conf file where it can be accessed while running the job via spark-submit.
3. Run the below job using `spark-submit` command as shown below:

```
./spark-submit --properties-file sparkConf.properties /
--master "local[*]" /
--class datastax.astra.migrate.Migrate cassandra-data-migrator-2.x.x.jar &> logfile_name.txt
```

Note: Above command also generates a log file `logfile_name.txt` to avoid log output on the console.


# Steps for Data-Validation:

- To run the job in Data validation mode, use class option `--class datastax.astra.migrate.DiffData` as shown below

```
./spark-submit --properties-file sparkConf.properties /
--master "local[*]" /
--class datastax.astra.migrate.DiffData cassandra-data-migrator-2.x.x.jar &> logfile_name.txt
```

- Validation job will report differences as “ERRORS” in the log file as shown below

```
22/10/27 23:25:29 ERROR DiffJobSession: Missing target row found for key: Grapes %% 1 %% 2020-05-22 %% 2020-05-23T00:05:09.353Z %% skuid %% Aliquam faucibus
22/10/27 23:25:29 ERROR DiffJobSession: Inserted missing row in target: Grapes %% 1 %% 2020-05-22 %% 2020-05-23T00:05:09.353Z %% skuid %% Aliquam faucibus
22/10/27 23:25:30 ERROR DiffJobSession: Mismatch row found for key: Grapes %% 1 %% 2020-05-22 %% 2020-05-23T00:05:09.353Z %% skuid %% augue odio at quam Data:  (Index: 8 Origin: Hello 3 Target: Hello 2 )
22/10/27 23:25:30 ERROR DiffJobSession: Updated mismatch row in target: Grapes %% 1 %% 2020-05-22 %% 2020-05-23T00:05:09.353Z %% skuid %% augue odio at quam
```

- Please grep for all `ERROR` from the output log files to get the list of missing and mismatched records.
  - Note that it lists differences by partition key values.
- The Validation job can also be run in an AutoCorrect mode. This mode can
  - Add any missing records from origin to target
  - Fix any inconsistencies between origin and target (makes target same as origin). 
- Enable/disable this feature using one or both of the below setting in the config file

```
spark.target.autocorrect.missing                    true|false
spark.target.autocorrect.mismatch                   true|false
```

# Migrating specific partition ranges
- You can also use the tool to migrate specific partition ranges, use class option `--class datastax.astra.migrate.MigratePartitionsFromFile` as shown below
```
./spark-submit --properties-file sparkConf.properties /
--master "local[*]" /
--class datastax.astra.migrate.MigratePartitionsFromFile cassandra-data-migrator-2.x.x.jar &> logfile_name.txt
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
- Filter records from origin using writetimes, CQL conditions, token-ranges
- Fully containerized (Docker and K8s friendly)
- SSL Support (including custom cipher algorithms)
- Migrate from any Cassandra origin ([Apache Cassandra](https://cassandra.apache.org) / [DataStax Enterprise](https://www.datastax.com/products/datastax-enterprise) / [DataStax Astra DB](https://www.datastax.com/products/datastax-astra)) to any Cassandra target ([Apache Cassandra](https://cassandra.apache.org) / [DataStax Enterprise](https://www.datastax.com/products/datastax-enterprise) / [DataStax Astra DB](https://www.datastax.com/products/datastax-astra))
- Validate migration accuracy and performance using a smaller randomized data-set
- Custom writetime
