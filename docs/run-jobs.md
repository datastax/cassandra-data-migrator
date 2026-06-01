# Run CDM jobs

> **IMPORTANT**
>
> If you deploy CDM on a Spark cluster, you must modify your `spark-submit` commands as follows:
>
> * Replace `--master "local[*]"` with the host and port for your Spark cluster, as in `--master "spark://MASTER_HOST:PORT"`.
> * Remove parameters related to single-VM installations, such as `--driver-memory` and `--executor-memory`.

## Run a CDM data migration job

A data migration job copies data from a table in your origin cluster to a table with the same schema in your target cluster.

To optimize large-scale migrations, CDM can run multiple concurrent migration jobs on the same table.

The following `spark-submit` command migrates one table from the origin to the target cluster, using the configuration in your properties file. The migration job is specified in the `--class` argument.

**Migration job using a local installation**
```bash
./spark-submit --properties-file cdm.properties \
--conf spark.cdm.schema.origin.keyspaceTable="KEYSPACE_NAME.TABLE_NAME" \
--master "local[*]" --driver-memory 25G --executor-memory 25G \
--class com.datastax.cdm.job.Migrate cassandra-data-migrator-VERSION.jar &> logfile_name_$(date +%Y%m%d_%H_%M).txt
```

**Migration job using an Apache Spark cluster**
```bash
./spark-submit --properties-file cdm.properties \
--conf spark.cdm.schema.origin.keyspaceTable="KEYSPACE_NAME.TABLE_NAME" \
--master "spark://MASTER_HOST:PORT" \
--class com.datastax.cdm.job.Migrate cassandra-data-migrator-VERSION.jar &> logfile_name_$(date +%Y%m%d_%H_%M).txt
```

Replace or modify the following:

* `--properties-file cdm.properties`: If your properties file has a different name, specify the actual name of your properties file. Depending on where your properties file is stored, you might need to specify the full or relative file path.
* `KEYSPACE_NAME.TABLE_NAME`: Specify the name of the table that you want to migrate and the keyspace that it belongs to.
You can also set `spark.cdm.schema.origin.keyspaceTable` in your properties file using the same format of `KEYSPACE_NAME.TABLE_NAME`.
* `--driver-memory` and `--executor-memory` (local installations only): Specify the appropriate memory settings for your environment.
* `--master` (Spark cluster deployments only): Provide the URL of your Spark cluster.
* `VERSION`: Specify the full CDM version that you installed, such as `5.2.1`.

This command generates a log file (`logfile_name_TIMESTAMP.txt`) instead of logging output to the console.
For additional modifications to this command, see [Configure CDM](/configure.md).

## Run a CDM data validation job

After migrating data, use CDM's data validation mode to identify any inconsistencies between the origin and target tables, such as missing or mismatched records.

Optionally, CDM can automatically correct discrepancies in the target cluster during validation.

Use the following `spark-submit` command to run a data validation job using the configuration in your properties file. The data validation job is specified in the `--class` argument.

**Validation job using a local installation**
```bash
./spark-submit --properties-file cdm.properties \
--conf spark.cdm.schema.origin.keyspaceTable="KEYSPACE_NAME.TABLE_NAME" \
--master "local[*]" --driver-memory 25G --executor-memory 25G \
--class com.datastax.cdm.job.DiffData cassandra-data-migrator-VERSION.jar &> logfile_name_$(date +%Y%m%d_%H_%M).txt
```

**Validation job using an Apache Spark cluster**
```bash
./spark-submit --properties-file cdm.properties \
--conf spark.cdm.schema.origin.keyspaceTable="KEYSPACE_NAME.TABLE_NAME" \
--master "spark://MASTER_HOST:PORT" \
--class com.datastax.cdm.job.DiffData cassandra-data-migrator-VERSION.jar &> logfile_name_$(date +%Y%m%d_%H_%M).txt
```

Replace or modify the following:

* `--properties-file cdm.properties`: If your properties file has a different name, specify the actual name of your properties file. Depending on where your properties file is stored, you might need to specify the full or relative file path.
* `KEYSPACE_NAME.TABLE_NAME`: Specify the name of the table that you want to validate and the keyspace that it belongs to. You can also set `spark.cdm.schema.origin.keyspaceTable` in your properties file using the same format of `KEYSPACE_NAME.TABLE_NAME`.
* `--driver-memory` and `--executor-memory` (local installations only): Specify the appropriate memory settings for your environment.
* `--master` (Spark cluster deployments only): Provide the URL of your Spark cluster.
* `VERSION`: Specify the full CDM version that you installed, such as `5.2.1`.

Allow the command some time to run, and then open the log file (`logfile_name_TIMESTAMP.txt`) to search for `ERROR` entries. The CDM validation job records differences as `ERROR` entries in the log file, listed by primary key values. For example:

```text
23/04/06 08:43:06 ERROR DiffJobSession: Mismatch row found for key: [key3] Mismatch: Target Index: 1 Origin: valueC Target: value999
23/04/06 08:43:06 ERROR DiffJobSession: Corrected mismatch row in target: [key3]
23/04/06 08:43:06 ERROR DiffJobSession: Missing target row found for key: [key2]
23/04/06 08:43:06 ERROR DiffJobSession: Inserted missing row in target: [key2]
```

When validating large datasets or multiple tables, you might want to extract the complete list of missing or mismatched records. There are many ways to do this. For example, you can `grep` for all `ERROR` entries in your CDM log files, or use the `log4j2` example provided in the [CDM README](https://github.com/datastax/cassandra-data-migrator?tab=readme-ov-file#steps-for-data-validation).

## Run a validation job in AutoCorrect mode

Optionally, you can run CDM validation jobs in AutoCorrect mode, which offers the following functions:

* `autocorrect.missing`: Add any missing records in the target with the value from the origin.
* `autocorrect.missing.counter`: By default, counter tables are not copied when missing, unless explicitly set.
* `autocorrect.mismatch`: Reconcile any mismatched records between the origin and target by replacing the target value with the origin value.

> **IMPORTANT**
>
> Timestamps impact the `autocorrect.mismatch` function.
>
> If the `writetime` of the origin record (determined with `.writetime.names`) is before the `writetime` of the corresponding target record, then the original write won't appear in the target cluster.
>
> This comparative state can be challenging to troubleshoot if individual columns or cells were modified in the target cluster.

In your `cdm.properties` file, use the following properties to enable (`true`) or disable (`false`) autocorrect functions:

```text
spark.cdm.autocorrect.missing                     false|true
spark.cdm.autocorrect.mismatch                    false|true
spark.cdm.autocorrect.missing.counter             false|true
```

> **TIP**
>
> The CDM validation job never deletes records from either the origin or target. Data validation only inserts or updates data on the target.
>
> For an initial data validation, consider disabling AutoCorrect so that you can generate a list of data discrepancies, investigate those discrepancies, and then decide whether you want to rerun the validation with AutoCorrect enabled.