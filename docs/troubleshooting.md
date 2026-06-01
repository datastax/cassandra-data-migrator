# Troubleshoot CDM

* **Java NoSuchMethodError**:

   If you installed Spark as a JAR file, and your Spark and Scala versions aren't compatible with your installed version of CDM, CDM jobs can throw exceptions such a the following:

   ```
   Exception in thread "main" java.lang.NoSuchMethodError: 'void scala.runtime.Statics.releaseFence()'
   ```

   Make sure that your Spark binary is compatible with your CDM version. If you installed an earlier version of CDM, you might need to install an earlier Spark binary.

* **Rerun a failed or partially completed job**:

   You can use the `track-run` feature to track the progress of a migration or validation, and then, if necessary, use the `run-id` to rerun a failed job from the last successful migration or validation point. For more information, see the [CDM README](https://github.com/datastax/cassandra-data-migrator#things-to-know) and the [fully annotated properties file](https://github.com/datastax/cassandra-data-migrator/blob/main/src/resources/cdm-detailed.properties).