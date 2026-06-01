# Configure CDM

To configure CDM, you can use a `cdm.properties` file and pass `--conf` arguments on the command line.

## Create a properties file

1. Create a `cdm.properties` file.

   If you use a different name, make sure you specify the correct filename in your `spark-submit` commands.

2. Configure the properties for your environment.

   To help you get started, use the [sample properties file with default values](https://github.com/datastax/cassandra-data-migrator/blob/main/src/resources/cdm.properties). For all options, see the [fully annotated properties file](https://github.com/datastax/cassandra-data-migrator/blob/main/src/resources/cdm-detailed.properties).

   CDM jobs process all uncommented parameters. Any parameters that are commented out are ignored or use default values.

   If you want to reuse a properties file created for a previous CDM version, make sure it is compatible with the version you are currently using. Check the [CDM release notes](https://github.com/datastax/cassandra-data-migrator/releases) for possible breaking changes in interim releases. For example, the 4.x series of CDM isn't backwards compatible with earlier properties files.

3. Store your properties file where it can be accessed while running CDM jobs using `spark-submit`.

## Additional options for CDM jobs

You can modify your properties file or append additional `--conf` arguments to your `spark-submit` commands to customize your CDM jobs.
For example, you can do the following:

* Check for large field guardrail violations before migrating.
* Use the `partition.min` and `partition.max` parameters to migrate or validate specific token ranges.
* Use the `track-run` feature to monitor progress and rerun a failed migration or validation job from point of failure.

For more information, see the [CDM README](https://github.com/datastax/cassandra-data-migrator) and the [fully annotated properties file](https://github.com/datastax/cassandra-data-migrator/blob/main/src/resources/cdm-detailed.properties).