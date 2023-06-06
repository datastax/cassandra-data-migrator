[![License Apache2](https://img.shields.io/hexpm/l/plug.svg)](http://www.apache.org/licenses/LICENSE-2.0)
[![Star on Github](https://img.shields.io/github/stars/datastax/cassandra-data-migrator.svg?style=social)](https://github.com/datastax/cassandra-data-migrator/stargazers)

# cassandra-data-migrator

Migrate and Validate Tables between Origin and Target Cassandra Clusters.

> :warning: Please note this job has been tested with spark version [3.3.1](https://archive.apache.org/dist/spark/spark-3.3.1/)

## Install as a Container
- Get the latest image that includes all dependencies from [DockerHub](https://hub.docker.com/r/datastax/cassandra-data-migrator)
    - All migration tools (`cassandra-data-migrator` + `dsbulk` + `cqlsh`) would be available in the `/assets/` folder of the container

## Install as a JAR file
- Download the latest jar file from the GitHub [packages area here](https://github.com/orgs/datastax/packages?repo_name=cassandra-data-migrator)

### Prerequisite
- Install Java8 as spark binaries are compiled with it.
- Install Spark version [3.3.1](https://archive.apache.org/dist/spark/spark-3.3.1/) on a single VM (no cluster necessary) where you want to run this job. Spark can be installed by running the following: -
```
wget https://archive.apache.org/dist/spark/spark-3.3.1/spark-3.3.1-bin-hadoop3.tgz
tar -xvzf spark-3.3.1-bin-hadoop3.tgz
```

# Steps for Data-Migration:

> :warning: Note that Version 4 of the tool is not backward-compatible with .properties files created in previous versions, and that package names have changed.

1. `cdm.properties` file needs to be configured as applicable for the environment. Parameter descriptions and defaults are described in the file.
   > A sample properties file configuration can be [found here](./src/resources/cdm.properties)
2. Place the properties file where it can be accessed while running the job via spark-submit.
3. Run the below job using `spark-submit` command as shown below:

```
./spark-submit --properties-file cdm.properties /
--conf spark.cdm.schema.origin.keyspaceTable="<keyspace-name>.<table-name>" /
--master "local[*]" /
--class datastax.cdm.job.Migrate cassandra-data-migrator-4.x.x.jar &> logfile_name_$(date +%Y%m%d_%H_%M).txt
```

Note:
- Above command generates a log file `logfile_name.txt` to avoid log output on the console.
- Add option `--driver-memory 25G --executor-memory 25G` as shown below if the table migrated is large (over 100GB)
```
./spark-submit --properties-file cdm.properties /
--conf spark.cdm.schema.origin.keyspaceTable="<keyspace-name>.<table-name>" /
--master "local[*]" --driver-memory 25G --executor-memory 25G /
--class datastax.cdm.job.Migrate cassandra-data-migrator-4.x.x.jar &> logfile_name_$(date +%Y%m%d_%H_%M).txt
```

# Steps for Data-Validation:

- To run the job in Data validation mode, use class option `--class datastax.cdm.job.DiffData` as shown below

```
./spark-submit --properties-file cdm.properties /
--conf spark.cdm.schema.origin.keyspaceTable="<keyspace-name>.<table-name>" /
--master "local[*]" /
--class datastax.cdm.job.DiffData cassandra-data-migrator-4.x.x.jar &> logfile_name_$(date +%Y%m%d_%H_%M).txt
```

- Validation job will report differences as “ERRORS” in the log file as shown below

```
23/04/06 08:43:06 ERROR DiffJobSession: Mismatch row found for key: [key3] Mismatch: Target Index: 1 Origin: valueC Target: value999) 
23/04/06 08:43:06 ERROR DiffJobSession: Corrected mismatch row in target: [key3]
23/04/06 08:43:06 ERROR DiffJobSession: Missing target row found for key: [key2]
23/04/06 08:43:06 ERROR DiffJobSession: Inserted missing row in target: [key2]
```

- Please grep for all `ERROR` from the output log files to get the list of missing and mismatched records.
    - Note that it lists differences by primary-key values.
- The Validation job can also be run in an AutoCorrect mode. This mode can
    - Add any missing records from origin to target
    - Update any mismatched records between origin and target (makes target same as origin).
- Enable/disable this feature using one or both of the below setting in the config file
```
spark.cdm.autocorrect.missing                     false|true
spark.cdm.autocorrect.mismatch                    false|true
```
Note:
- The validation job will never delete records from target i.e. it only adds or updates data on target

# Migrating specific partition ranges
- You can also use the tool to migrate specific partition ranges using class option `--class datastax.cdm.job.MigratePartitionsFromFile` as shown below
```
./spark-submit --properties-file cdm.properties /
--conf spark.cdm.schema.origin.keyspaceTable="<keyspace-name>.<table-name>" /
--master "local[*]" /
--class datastax.cdm.job.MigratePartitionsFromFile cassandra-data-migrator-4.x.x.jar &> logfile_name_$(date +%Y%m%d_%H_%M).txt
```

When running in above mode the tool assumes a `partitions.csv` file to be present in the current folder in the below format, where each line (`min,max`) represents a partition-range
```
-507900353496146534,-107285462027022883
-506781526266485690,1506166634797362039
2637884402540451982,4638499294009575633
798869613692279889,8699484505161403540
```
This mode is specifically useful to processes a subset of partition-ranges that may have failed during a previous run.

> **Note:**
> Here is a quick tip to prepare `partitions.csv` from the log file,

```
grep "ERROR CopyJobSession: Error with PartitionRange" /path/to/logfile_name.txt | awk '{print $13","$15}' > partitions.csv
```
# Data validation for specific partition ranges
- You can also use the tool to validate data for a specific partition ranges using class option `--class datastax.cdm.job.DiffPartitionsFromFile` as shown below,
```
./spark-submit --properties-file cdm.properties /
--conf spark.origin.keyspaceTable="<keyspace-name>.<table-name>" /
--master "local[*]" /
--class datastax.cdm.job.DiffPartitionsFromFile cassandra-data-migrator-4.x.x.jar &> logfile_name_$(date +%Y%m%d_%H_%M).txt
```

When running in above mode the tool assumes a `partitions.csv` file to be present in the current folder.

# Perform large-field Guardrail violation checks
- The tool can be used to identify large fields from a table that may break you cluster guardrails (e.g. AstraDB has a 10MB limit for a single large field)  `--class datastax.astra.migrate.Guardrail` as shown below
```
./spark-submit --properties-file cdmGuardrail.properties /
--conf spark.origin.keyspaceTable="<keyspace-name>.<table-name>" /
--master "local[*]" /
--class datastax.cdm.job.GuardrailCheck cassandra-data-migrator-4.x.x.jar &> logfile_name_$(date +%Y%m%d_%H_%M).txt
```
> A sample Guardrail properties file can be [found here](./src/resources/cdmGuardrail.properties)

# Features
- Auto-detects table schema (column names, types, keys, collections, UDTs, etc.)
    - Including counter table [Counter tables](https://docs.datastax.com/en/dse/6.8/cql/cql/cql_using/useCountersConcept.html)
- Preserve [writetimes](https://docs.datastax.com/en/dse/6.8/cql/cql/cql_reference/cql_commands/cqlSelect.html#cqlSelect__retrieving-the-datetime-a-write-occurred-p) and [TTLs](https://docs.datastax.com/en/dse/6.8/cql/cql/cql_reference/cql_commands/cqlSelect.html#cqlSelect__ref-select-ttl-p)
- Supports migration/validation of advanced DataTypes ([Sets](https://docs.datastax.com/en/dse/6.8/cql/cql/cql_reference/refDataTypes.html#refDataTypes__set), [Lists](https://docs.datastax.com/en/dse/6.8/cql/cql/cql_reference/refDataTypes.html#refDataTypes__list), [Maps](https://docs.datastax.com/en/dse/6.8/cql/cql/cql_reference/refDataTypes.html#refDataTypes__map), [UDTs](https://docs.datastax.com/en/dse/6.8/cql/cql/cql_reference/refDataTypes.html#refDataTypes__udt))
- Filter records from `Origin` using `writetimes` and/or CQL conditions and/or min/max token-range
- Perform guardrail checks (identify large fields)
- Supports adding `constants` as new columns on `Target`
- Supports expanding `Map` columns on `Origin` into multiple records on `Target`
- Fully containerized (Docker and K8s friendly)
- SSL Support (including custom cipher algorithms)
- Migrate from any Cassandra `Origin` ([Apache Cassandra®](https://cassandra.apache.org) / [DataStax Enterprise&trade;](https://www.datastax.com/products/datastax-enterprise) / [DataStax Astra DB&trade;](https://www.datastax.com/products/datastax-astra)) to any Cassandra `Target` ([Apache Cassandra®](https://cassandra.apache.org) / [DataStax Enterprise&trade;](https://www.datastax.com/products/datastax-enterprise) / [DataStax Astra DB&trade;](https://www.datastax.com/products/datastax-astra))
- Supports migration/validation from and to [Azure Cosmos Cassandra](https://learn.microsoft.com/en-us/azure/cosmos-db/cassandra)
- Validate migration accuracy and performance using a smaller randomized data-set
- Supports adding custom fixed `writetime`
- Validation - Log partitions range level exceptions, use the exceptions file as input for rerun  

# Known Limitations
- This tool does not migrate `ttl` & `writetime` at the field-level (for optimization reasons). It instead finds the field with the highest `ttl` & the field with the highest `writetime` within an `origin` row and uses those values on the entire `target` row.

# Building Jar for local development
1. Clone this repo
2. Move to the repo folder `cd cassandra-data-migrator`
3. Run the build `mvn clean package` (Needs Maven 3.8.x)
4. The fat jar (`cassandra-data-migrator-4.x.x.jar`) file should now be present in the `target` folder

# Contributors
Checkout all our wonderful contributors [here](./CONTRIBUTING.md#contributors).

---
