# Release Notes
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
