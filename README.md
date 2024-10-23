[![License Apache2](https://img.shields.io/hexpm/l/plug.svg)](http://www.apache.org/licenses/LICENSE-2.0)
[![Star on Github](https://img.shields.io/github/stars/datastax/cassandra-data-migrator.svg?style=social)](https://github.com/datastax/cassandra-data-migrator/stargazers)
![GitHub release (with filter)](https://img.shields.io/github/v/release/datastax/cassandra-data-migrator?label=latest%20release&color=green&link=!%5BGitHub%20release%20(with%20filter)%5D(https%3A%2F%2Fimg.shields.io%2Fgithub%2Fv%2Frelease%2Fdatastax%2Fcassandra-data-migrator%3Flabel%3Dlatest%2520release%26color%3Dgreen))
![Docker Pulls](https://img.shields.io/docker/pulls/datastax/cassandra-data-migrator)

# cassandra-data-migrator (also known as CDM)

Migrate and Validate Tables between Origin and Target Cassandra Clusters.

> :warning: Please note this job has been tested with spark version [3.5.3](https://archive.apache.org/dist/spark/spark-3.5.3/)

## Install as a Container
- Get the latest image that includes all dependencies from [DockerHub](https://hub.docker.com/r/datastax/cassandra-data-migrator)
    - All migration tools (`cassandra-data-migrator` + `dsbulk` + `cqlsh`) would be available in the `/assets/` folder of the container

## Install as a JAR file
- Download the latest jar file from the GitHub [packages area here](https://github.com/datastax/cassandra-data-migrator/packages/1832128)

### Prerequisite
- Install **Java11** (minimum) as Spark binaries are compiled with it.
- Install Spark version [`3.5.3`](https://archive.apache.org/dist/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3-scala2.13.tgz) on a single VM (no cluster necessary) where you want to run this job. Spark can be installed by running the following: -
```
wget https://archive.apache.org/dist/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3-scala2.13.tgz
tar -xvzf spark-3.5.3-bin-hadoop3-scala2.13.tgz
```

> :warning: If the above Spark and Scala version is not properly installed, you'll then see a similar exception like below when running the CDM jobs,
```
Exception in thread "main" java.lang.NoSuchMethodError: scala.runtime.Statics.releaseFence()V
```

# Steps for Data-Migration:

> :warning: Note that Version 4 of the tool is not backward-compatible with .properties files created in previous versions, and that package names have changed.

1. `cdm.properties` file needs to be configured as applicable for the environment. Parameter descriptions and defaults are described in the file. The file can have any name, it does not need to be `cdm.properties`.
   > * A simplified sample properties file configuration can be found here as [cdm.properties](./src/resources/cdm.properties)
   > * A complete sample properties file configuration can be found here as [cdm-detailed.properties](./src/resources/cdm-detailed.properties)
2. Place the properties file where it can be accessed while running the job via spark-submit.
3. Run the below job using `spark-submit` command as shown below:

```
spark-submit --properties-file cdm.properties \
--conf spark.cdm.schema.origin.keyspaceTable="<keyspacename>.<tablename>" \
--master "local[*]" --driver-memory 25G --executor-memory 25G \
--class com.datastax.cdm.job.Migrate cassandra-data-migrator-4.x.x.jar &> logfile_name_$(date +%Y%m%d_%H_%M).txt
```

Note:
- Above command generates a log file `logfile_name_*.txt` to avoid log output on the console.
- Update the memory options (driver & executor memory) based on your use-case
- To track details of a run in the `target` keyspace, pass param `--conf spark.cdm.trackRun=true`
- To filter and migrate data only in a specific token range, you can pass the below two additional params to the `Migration` or `Validation` jobs 

```
--conf spark.cdm.filter.cassandra.partition.min=<token-range-min>
--conf spark.cdm.filter.cassandra.partition.max=<token-range-max>
```

# Steps for Data-Validation:

- To run the job in Data validation mode, use class option `--class com.datastax.cdm.job.DiffData` as shown below

```
spark-submit --properties-file cdm.properties \
--conf spark.cdm.schema.origin.keyspaceTable="<keyspacename>.<tablename>" \
--master "local[*]" --driver-memory 25G --executor-memory 25G \
--class com.datastax.cdm.job.DiffData cassandra-data-migrator-4.x.x.jar &> logfile_name_$(date +%Y%m%d_%H_%M).txt
```

- Validation job will report differences as “ERRORS” in the log file as shown below. 

```
23/04/06 08:43:06 ERROR DiffJobSession: Mismatch row found for key: [key3] Mismatch: Target Index: 1 Origin: valueC Target: value999) 
23/04/06 08:43:06 ERROR DiffJobSession: Corrected mismatch row in target: [key3]
23/04/06 08:43:06 ERROR DiffJobSession: Missing target row found for key: [key2]
23/04/06 08:43:06 ERROR DiffJobSession: Inserted missing row in target: [key2]
```

- Please grep for all `ERROR` from the output log files to get the list of missing and mismatched records.
    - Note that it lists differences by primary-key values.
- If you would like to redirect such logs (rows with details of `missing` and `mismatched` rows) into a separate file, you could use the `log4j2.properties` file [provided here](./src/resources/log4j2.properties) as shown below

```
spark-submit --properties-file cdm.properties \
--conf spark.cdm.schema.origin.keyspaceTable="<keyspacename>.<tablename>" \
--conf spark.executor.extraJavaOptions='-Dlog4j.configurationFile=log4j2.properties' \ 
--conf spark.driver.extraJavaOptions='-Dlog4j.configurationFile=log4j2.properties' \ 
--master "local[*]" --driver-memory 25G --executor-memory 25G \
--class com.datastax.cdm.job.DiffData cassandra-data-migrator-4.x.x.jar &> logfile_name_$(date +%Y%m%d_%H_%M).txt
```

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

# Rerun (previously incomplete) Migration or Validation 
- You can rerun/resume a Migration or Validation job to complete a previous run that could have stopped (or completed with some errors) for any reasons. This mode will skip any token-ranges from the previous run that were migrated (or validated) successfully. This is done by passing the `spark.cdm.trackRun.previousRunId` param as shown below

```
spark-submit --properties-file cdm.properties \
 --conf spark.cdm.schema.origin.keyspaceTable="<keyspacename>.<tablename>" \
 --conf spark.cdm.trackRun.previousRunId=<prev_run_id> \
 --master "local[*]" --driver-memory 25G --executor-memory 25G \
 --class com.datastax.cdm.job.<Migrate|DiffData> cassandra-data-migrator-4.x.x.jar &> logfile_name_$(date +%Y%m%d_%H_%M).txt
```

# Perform large-field Guardrail violation checks
- The tool can be used to identify large fields from a table that may break you cluster guardrails (e.g. AstraDB has a 10MB limit for a single large field), use class option `--class com.datastax.cdm.job.GuardrailCheck` as shown below

```
spark-submit --properties-file cdm.properties \
--conf spark.cdm.schema.origin.keyspaceTable="<keyspacename>.<tablename>" \
--conf spark.cdm.feature.guardrail.colSizeInKB=10000 \
--master "local[*]" --driver-memory 25G --executor-memory 25G \
--class com.datastax.cdm.job.GuardrailCheck cassandra-data-migrator-4.x.x.jar &> logfile_name_$(date +%Y%m%d_%H_%M).txt
```

# Features
- Auto-detects table schema (column names, types, keys, collections, UDTs, etc.)
    - Including counter table [Counter tables](https://docs.datastax.com/en/dse/6.8/cql/cql/cql_using/useCountersConcept.html)
- Rerun job from where the previous job had stopped for any reason (killed, had exceptions, etc.)
    - If you rerun a `validation` job, it will include any token-ranges that had differences in the previous run 
- Preserve [writetimes](https://docs.datastax.com/en/dse/6.8/cql/cql/cql_reference/cql_commands/cqlSelect.html#cqlSelect__retrieving-the-datetime-a-write-occurred-p) and [TTLs](https://docs.datastax.com/en/dse/6.8/cql/cql/cql_reference/cql_commands/cqlSelect.html#cqlSelect__ref-select-ttl-p)
- Supports migration/validation of advanced DataTypes ([Sets](https://docs.datastax.com/en/dse/6.8/cql/cql/cql_reference/refDataTypes.html#refDataTypes__set), [Lists](https://docs.datastax.com/en/dse/6.8/cql/cql/cql_reference/refDataTypes.html#refDataTypes__list), [Maps](https://docs.datastax.com/en/dse/6.8/cql/cql/cql_reference/refDataTypes.html#refDataTypes__map), [UDTs](https://docs.datastax.com/en/dse/6.8/cql/cql/cql_reference/refDataTypes.html#refDataTypes__udt))
- Filter records from `Origin` using `writetime` and/or CQL conditions and/or a list of token-ranges
- Perform guardrail checks (identify large fields)
- Supports adding `constants` as new columns on `Target`
- Supports expanding `Map` columns on `Origin` into multiple records on `Target`
- Supports extracting value from a JSON column in `Origin` and map it to a specific field on `Target`
- Fully containerized (Docker and K8s friendly)
- SSL Support (including custom cipher algorithms)
- Migrate from any Cassandra `Origin` ([Apache Cassandra®](https://cassandra.apache.org) / [DataStax Enterprise&trade;](https://www.datastax.com/products/datastax-enterprise) / [DataStax Astra DB&trade;](https://www.datastax.com/products/datastax-astra)) to any Cassandra `Target` ([Apache Cassandra®](https://cassandra.apache.org) / [DataStax Enterprise&trade;](https://www.datastax.com/products/datastax-enterprise) / [DataStax Astra DB&trade;](https://www.datastax.com/products/datastax-astra))
- Supports migration/validation from and to [Azure Cosmos Cassandra](https://learn.microsoft.com/en-us/azure/cosmos-db/cassandra)
- Validate migration accuracy and performance using a smaller randomized data-set
- Supports adding custom fixed `writetime` and/or `ttl`
- Track run information (start-time, end-time, status, etc.) in tables (`cdm_run_info` and `cdm_run_details`) on the target keyspace

# Things to know
- Each run (Migration or Validation) can be tracked (when enabled). You can find summary and details of the same in tables `cdm_run_info` and `cdm_run_details` in the target keyspace.
- CDM does not migrate `ttl` & `writetime` at the field-level (for optimization reasons). It instead finds the field with the highest `ttl` & the field with the highest `writetime` within an `origin` row and uses those values on the entire `target` row.
- CDM ignores using collection and UDT fields for `ttl` & `writetime` calculations by default for performance reasons. If you want to include such fields, set `spark.cdm.schema.ttlwritetime.calc.useCollections` param to `true`.
- If a table has only collection and/or UDT non-key columns and no table-level `ttl` configuration, the target will have no `ttl`, which can lead to inconsistencies between `origin` and `target` as rows expire on `origin` due to `ttl` expiry. If you want to avoid this, we recommend setting `spark.cdm.schema.ttlwritetime.calc.useCollections` param to `true` in such scenarios.
- If a table has only collection and/or UDT non-key columns, the `writetime` used on target will be time the job was run. If you want to avoid this, we recommend setting `spark.cdm.schema.ttlwritetime.calc.useCollections` param to `true` in such scenarios. 
- When CDM migration (or validation with autocorrect) is run multiple times on the same table (for whatever reasons), it could lead to duplicate entries in `list` type columns. Note this is [due to a Cassandra/DSE bug](https://issues.apache.org/jira/browse/CASSANDRA-11368) and not a CDM issue. This issue can be addressed by enabling and setting a positive value for `spark.cdm.transform.custom.writetime.incrementBy` param. This param was specifically added to address this issue.
- When you rerun job to resume from a previous run, the run metrics (read, write, skipped, etc.) captured in table `cdm_run_info` will be only for the current run. If the previous run was killed for some reasons, its run metrics may not have been saved. If the previous run did complete (not killed) but with errors, then you will have all run metrics from previous run as well.

# Performance recommendations 
Below recommendations may only be useful when migrating large tables where the default performance is not good enough
- Performance bottleneck are usually the result of
    - Low resource availability on `Origin` OR `Target` cluster
    - Low resource availability on CDM VMs, [see recommendations here](https://docs.datastax.com/en/data-migration/deployment-infrastructure.html#_machines)
    - Bad schema design which could be caused by out of balance `Origin` cluster, large partitions (> 100 MB), large rows (> 10MB) and/or high column count.
- Incorrect configuration of below properties may negatively impact performance
    - `numParts`: Default is 5K, but ideal value is usually around table-size/10MB. 
    - `batchSize`: Default is 5, but this should be set to 1 for tables where primary-key=partition-key OR where average row-size is > 20 KB. Similarly, this should be set to a value > 5, if row-size is small (< 1KB) and most partitions have several rows (100+).
    - `fetchSizeInRows`: Default is 1K and this usually works fine. However you can reduce this as needed if your table has many large rows (over 100KB).
    - `ratelimit`: Default is 20K, but this property should usually be updated (after updating other properties) to the highest possible value that your `origin` and `target` clusters can efficiently handle.
- Using schema manipulation features (like `constantColumns`, `explodeMap`, `extractJson`), transformation functions and/or where-filter-conditions (except partition min/max) may negatively impact performance
- We typically recommend [this infrastructure](https://docs.datastax.com/en/data-migration/deployment-infrastructure.html#_machines) for CDM VMs and [this starter conf](https://github.com/datastax/cassandra-data-migrator/blob/main/src/resources/cdm.properties). You can then optimize the job further based on CDM params info provided above and the observed load and throughput on `Origin` and `Target` clusters

Note: For additional performance tuning, refer to details mentioned in the [cdm-detailed.properties file here](https://github.com/datastax/cassandra-data-migrator/blob/main/src/resources/cdm-detailed.properties)

# Building Jar for local development
1. Clone this repo
2. Move to the repo folder `cd cassandra-data-migrator`
3. Run the build `mvn clean package` (Needs Maven 3.9.x)
4. The fat jar (`cassandra-data-migrator-4.x.x.jar`) file should now be present in the `target` folder

# Contributors
Checkout all our wonderful contributors [here](./CONTRIBUTING.md#contributors).

---
