# Release Notes

## [5.5.1] - 2025-08-01
- Fixed issue related to empty text fields not getting migrated (introduced in 5.4.0). `Null` fields will still be skipped, however not empty strings.
- Filtered rows will now be logged at LOG4J `TRACE` level to avoid filling the logs. Users can enabled `TRACE` level logging if such logs are needed.

## [5.5.0] - 2025-07-07
- Logged metrics will now report how many Partition-Ranges out of the configured [`numParts`](https://github.com/datastax/cassandra-data-migrator/blob/main/src/resources/cdm-detailed.properties#L230) passed or failed.

## [5.4.1] - 2025-06-26
- Bug fix: Fixed auto column mapping bug when `target` table has more columns than `origin`.

## [5.4.0] - 2025-06-16
- Use `UNSET` value for null fields (including empty texts) to avoid creating (or carrying forward) tombstones during row creation.

## [5.3.1] - 2025-06-03
- Upgrade Spark version to [`3.5.6`](https://spark.apache.org/releases/spark-release-3-5-6.html).

## [5.3.0] - 2025-05-05
- Auto-download Astra DB Secure Connect Bundle (SCB) when connecting to Astra DB.

## [5.2.3] - 2025-04-15
- Randomized the pending token-range list returned by the `trackRun` feature (when rerunning a previously incomplete job) for better load distribution across the cluster.

## [5.2.2] - 2025-04-02
- Replaced deprecated (not supported in Cassandra 5.0.3+) function `dateof()` with `totimestamp()`

## [5.2.1] - 2025-03-10
- Implemented [`column.skip`](https://github.com/datastax/cassandra-data-migrator/blob/main/src/resources/cdm-detailed.properties#L97) feature

## [5.2.0] - 2025-02-28
- Upgraded to use Spark `3.5.5`.
- Cassandra Docker image tag is now set to `cassandra:5`.

## [5.1.4] - 2024-12-04
- Bug fix: Any run started with a `previousRunId` that is not found in the `cdm_run_info` table (for whatever reason), will be executed as a fresh new run instead of doing nothing.

## [5.1.3] - 2024-11-27
- Bug fix: Fixed connection issue caused when using different types of origin and target clusters (e.g. Cassandra/DSE with host/port and Astra with SCB).

## [5.1.2] - 2024-11-26
- Bug fix: SCB file on some Spark worker nodes may get deleted before the connection is established, which may cause connection exception on that worker node. Added a static async SCB delete delay to address such issues.

## [5.1.1] - 2024-11-22
- Bug fix: Writetime filter does not work as expected when custom writetimestamp is also used (issue #327).
- Removed deprecated properties `printStatsAfter` and `printStatsPerPart`. Run metrics should now be tracked using the `trackRun` feature instead.

## [5.1.0] - 2024-11-15
- Improves metrics output by producing stats labels in an intuitive and consistent order
- Refactored JobCounter by removing any references to `thread` or `global` as CDM operations are now isolated within partition-ranges (`parts`). Each such `part` is then parallelly processed and aggregated by Spark.

## [5.0.0] - 2024-11-08
- CDM refactored to be fully Spark Native and more performant when deployed on a multi-node Spark Cluster
- `trackRun` feature has been expanded to record `run-info` for each part in the `CDM_RUN_DETAILS` table. Along with granular metrics, this information can be used to troubleshoot any unbalanced problematic partitions.
- This release has feature parity with 4.x release and is also backword compatible while adding the above mentioned improvements. However, we are upgrading it to 5.x as its a major rewrite of the code to make it Spark native.

## [4.7.0] - 2024-10-25
- CDM refactored to work when deployed on a Spark Cluster
- More performant for large migration efforts (multi-terabytes clusters with several billions of rows) using Spark Cluster (instead of individual VMs)
- No functional changes and fully backward compatible, just refactor to support Spark cluster deployment

Note: The Spark Cluster based deployment in this release currently has a bug. It reports '0' for all count metrics, while doing underlying tasks (Migration, Validation, etc.). We are working to address this in the upcoming releases. Also note that this issue is only with the Spark cluster deployment and not with the single VM run (i.e. no impact to current users).

## [4.6.0] - 2024-10-18
- Allow using Collections and/or UDTs for `ttl` & `writetime` calculations. This is specifically helpful in scenarios where the only non-key columns are Collections and/or UDTs.

## [4.5.1] - 2024-10-11
- Made CDM generated SCB unique & much short-lived when using the TLS option to connect to Astra more securely.

## [4.5.0] - 2024-10-03
- Upgraded to use log4j 2.x and included a template properties file that will help separate general logs from CDM class specific logs including a separate log for rows identified by `DiffData` (Validation) errors.
- Upgraded to use Spark `3.5.3`.

## [4.4.1] - 2024-09-20
- Added two new codecs `STRING_BLOB` and `ASCII_BLOB` to allow migration from `TEXT` and `ASCII` fields to `BLOB` fields. These codecs can also be used to convert `BLOB` to `TEXT` or `ASCII`, but in such cases the `BLOB` value must be TEXT based in nature & fit within the applicable limits.

## [4.4.0] - 2024-09-19
- Added property `spark.cdm.connect.origin.tls.isAstra` and `spark.cdm.connect.target.tls.isAstra` to allow connecting to Astra DB without using [SCB](https://docs.datastax.com/en/astra-db-serverless/drivers/secure-connect-bundle.html). This may be needed for enterprises that may find credentials packaged within SCB as a security risk. TLS properties can now be passed as params OR wrapper scripts (not included) could be used to pull sensitive credentials from a vault service in real-time & pass them to CDM.
- Switched to using Apache Cassandra® `5.0` docker image for testing
- Introduces smoke testing of `vector` CQL data type

## [4.3.10] - 2024-09-12
- Added property `spark.cdm.trackRun.runId` to support a custom unique identifier for the current run. This can be used by wrapper scripts to pass a known `runId` and then use it to query the `cdm_run_info` and `cdm_run_details` tables.

## [4.3.9] - 2024-09-11
- Added new `status` value of `DIFF_CORRECTED` on `cdm_run_details` table to specifically mark partitions that were corrected during the CDM validation run.
- Upgraded Validation job to skip partitions with `DIFF_CORRECTED` status on rerun with a previous `runId`. 

## [4.3.8] - 2024-09-09
- Upgraded `spark.cdm.trackRun` feature to include `status` on `cdm_run_info` table. Also improved the code to handle rerun of previous run which may have exited before being correctly initialized. 

## [4.3.7] - 2024-09-03
- Added property `spark.cdm.transform.custom.ttl` to allow a custom constant value to be set for TTL instead of using the values from `origin` rows.
- Repo wide code formating & imports organization

## [4.3.6] - 2024-08-29
- Added `overwrite` option to conditionally check or skip `Validation` when it has a non-null value in `target` for the `spark.cdm.feature.extractJson` feature.

## [4.3.5] - 2024-08-23
- Added feature `spark.cdm.feature.extractJson` which allows you to extract a json value from a column with json content in an Origin table and map it to a column in the Target table.
- Upgraded to use Spark `3.5.2`.

## [4.3.4] - 2024-07-31
- Use `spark.cdm.schema.origin.keyspaceTable` when `spark.cdm.schema.target.keyspaceTable` is missing. Fixes [bug introduced in prior version](https://github.com/datastax/cassandra-data-migrator/issues/284).

## [4.3.3] - 2024-07-22
- Removed deprecated functionality related to processing token-ranges via partition-file
- Upgraded Spark Cassandra Connector (SCC) version to 3.5.1.
- Minor big fix (Enable tracking when only `previousRunId` provided, but `trackRun` not set to `true`)

## [4.3.2] - 2024-07-19
- Removed deprecated functionality related to retry

## [4.3.1] - 2024-07-19
- Fixed a validation run [bug](https://github.com/datastax/cassandra-data-migrator/issues/266) that sometimes did not report a failed token-range
- Removed deprecated MigrateRowsFromFile job

## [4.3.0] - 2024-07-18
- Added `spark.cdm.trackRun` feature to support stop and resume function for Migration and Validation jobs
- Validation jobs ran with `auto-correct` feature disabled, can now be rerun with `auto-correct` feature enabled in a much optimal way to only correct the token-ranges with validation errors during the rerun
- Records summary and details of each run in tables (`cdm_run_info` and `cdm_run_details`) on `target` keyspace

## [4.2.0] - 2024-07-09
- Upgraded `constant-column` feature to support `replace` and `remove` of constant columns 
- Fixed `constant-column` feature to support any data-types within the PK columns
- Added `Things to know` in docs

## [4.1.16] - 2024-05-31
- Added property to manage null values in Map fields
- Allow separate input and output partition CSV files
- Updated README
  
## [4.1.15] - 2024-03-05
- Internal CI/CD release fix
  
## [4.1.14] - 2024-02-29
- Fixed OOM bug caused when using partition file with large value for num-parts

## [4.1.13] - 2024-02-27
- Upgraded to use Spark `3.5.1`.

## [4.1.12] - 2024-01-22
- Upgraded to use Spark `3.4.2`.
- Added Java `11` as the minimally required pre-requisite to run CDM jobs.

## [4.1.9 to 4.1.11] - 2023-12-11
- Code test & coverage changes

## [4.1.8] - 2023-10-13
- Upgraded to use Scala 2.13 

## [4.1.7] - 2023-09-27
- Allow support for Spark 3.4.1, SCC 3.4.1 and begin automated testing using Cassandra® latest 4 series.
- Improved unit test coverage

## [4.1.6] - 2023-09-22
- Allow support for vector CQL data type

## [4.1.5] - 2023-08-29
- Allow reserved keywords used as Target column-names

## [4.1.4] - 2023-08-16
- In rare edge situations, counter tables with existing data in Target can have null values on target. This release will handle null values in Target counter table transparently.

## [4.1.3] - 2023-08-14
- Counter table columns will usually have zeros to begin with, but in rare edge situations, they can have null values. This release will handle null values in counter table transparently.

## [4.1.2] - 2023-07-06
- Fixed docker build

## [4.1.1] - 2023-06-29
- Documentation fixes in readme & properties file
- Config namespace fixes

## [4.1.0] - 2023-06-20
- Refactored exception handling and loading of token-range filters to use the same Migrate & DiffData jobs instead of separate jobs to reduce code & maintenance overhead

## [4.0.2] - 2023-06-16
- Capture failed partitions in a file for easier reruns 
- Optimized mvn to reduce jar size
- Fixed bugs in docs 

## [4.0.1] - 2023-06-08
- Fixes broken maven link in docker build process
- Upgrades to latest stable Maven 3.x

## [4.0.0] - 2023-06-02
This release is a major code refactor of Cassandra Data Migrator, focused on internal code structure and organization. 
Automated testing (both unit and integration) was introduced and incorporated into the build process. It includes all
features of the previous version, but the properties specified within configuration (.properties) file have been 
re-organized and renamed; therefore, the configuration file from the previous version will not work with this version.

New features were also introduced with this release, on top of the 3.4.5 version.
### Added
- New features:
    - `Column renaming`: Column names can differ between Origin and Target
    - `Migrate UDTs across keyspaces`: UDTs can be migrated from Origin to Target, even when the keyspace names differ
    - `Data Type Conversion`: Some predefined Codecs support type conversion between Origin and Target; custom Codecs can be added
    - `Separate Writetime and TTL configuration`: Writetime columns can differ from TTL columns
    - `Subset of columns can be specified with Writetime and TTL`: Not all eligible columns need to be used to compute the origin value
    - `Automatic RandomPartitioner min/max`: Partition min/max values no longer need to be manually configured
    - `Populate Target columns with constant values`: New columns can be added to the Target table, and populated with constant values
    - `Explode Origin Map Column into Target rows`: A Map in Origin can be expanded into multiple rows in Target when the Map key is part of the Target primary key

## [3.x.x] 
Previous releases of the project have not been documented in this file
