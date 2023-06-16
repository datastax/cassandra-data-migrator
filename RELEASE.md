# Release Notes
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
