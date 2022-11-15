package datastax.astra.migrate

import com.datastax.spark.connector.cql.CassandraConnector

class AbstractJob extends BaseJob {

  abstractLogger.info("PARAM -- Min Partition: " + minPartition)
  abstractLogger.info("PARAM -- Max Partition: " + maxPartition)
  abstractLogger.info("PARAM -- Split Size: " + splitSize)
  abstractLogger.info("PARAM -- Coverage Percent: " + coveragePercent)

  // Always connect to non-Astra source first. This is a workaround due to a CassandraConnector bug which gives
  // preference to host in "spark.cassandra.connection.config.cloud.path" variable over "spark.cassandra.connection.host"
  var sourceConnectionOpt =
    if ("false".equals(sourceIsAstra))
      Some(getConnection(true, sourceIsAstra, sourceScbPath, sourceHost, sourceUsername, sourcePassword, sourceReadConsistencyLevel,
        sourceTrustStorePath, sourceTrustStorePassword, sourceTrustStoreType, sourceKeyStorePath, sourceKeyStorePassword, sourceEnabledAlgorithms))
    else Option.empty;
  var destinationConnectionOpt =
    if ("false".equals(destinationIsAstra))
      Some(getConnection(false, destinationIsAstra, destinationScbPath, destinationHost, destinationUsername, destinationPassword, destinationReadConsistencyLevel,
        destinationTrustStorePath, destinationTrustStorePassword, destinationTrustStoreType, destinationKeyStorePath, destinationKeyStorePassword, destinationEnabledAlgorithms));
    else Option.empty;

  // After connecting to non-Astra source (if present), now connect to Astra (if present)
  var sourceConnection = sourceConnectionOpt.getOrElse(getConnection(true, sourceIsAstra, sourceScbPath, sourceHost, sourceUsername, sourcePassword, sourceReadConsistencyLevel,
    sourceTrustStorePath, sourceTrustStorePassword, sourceTrustStoreType, sourceKeyStorePath, sourceKeyStorePassword, sourceEnabledAlgorithms));
  var destinationConnection = destinationConnectionOpt.getOrElse(getConnection(false, destinationIsAstra, destinationScbPath, destinationHost, destinationUsername, destinationPassword, destinationReadConsistencyLevel,
    destinationTrustStorePath, destinationTrustStorePassword, destinationTrustStoreType, destinationKeyStorePath, destinationKeyStorePassword, destinationEnabledAlgorithms));

  private def getConnection(isSource: Boolean, isAstra: String, scbPath: String, host: String, username: String, password: String, readConsistencyLevel: String,
                            trustStorePath: String, trustStorePassword: String, trustStoreType: String,
                            keyStorePath: String, keyStorePassword: String, enabledAlgorithms: String): CassandraConnector = {
    var connType: String = "Source"
    if (!isSource) {
      connType = "Destination"
    }

    if ("true".equals(isAstra)) {
      abstractLogger.info(connType + ": Connecting to Astra using SCB: " + scbPath);

      return CassandraConnector(sc
        .set("spark.cassandra.auth.username", username)
        .set("spark.cassandra.auth.password", password)
        .set("spark.cassandra.input.consistency.level", readConsistencyLevel)
        .set("spark.cassandra.connection.config.cloud.path", scbPath))
    } else if (null != trustStorePath && !trustStorePath.trim.isEmpty) {
      abstractLogger.info(connType + ": Connecting to Cassandra (or DSE) with SSL host: " + host);

      // Use defaults when not provided
      var enabledAlgorithmsVar = enabledAlgorithms
      if (enabledAlgorithms == null || enabledAlgorithms.trim.isEmpty) {
        enabledAlgorithmsVar = "TLS_RSA_WITH_AES_128_CBC_SHA, TLS_RSA_WITH_AES_256_CBC_SHA"
      }

      return CassandraConnector(sc
        .set("spark.cassandra.auth.username", username)
        .set("spark.cassandra.auth.password", password)
        .set("spark.cassandra.input.consistency.level", readConsistencyLevel)
        .set("spark.cassandra.connection.host", host)
        .set("spark.cassandra.connection.ssl.enabled", "true")
        .set("spark.cassandra.connection.ssl.enabledAlgorithms", enabledAlgorithmsVar)
        .set("spark.cassandra.connection.ssl.trustStore.password", trustStorePassword)
        .set("spark.cassandra.connection.ssl.trustStore.path", trustStorePath)
        .set("spark.cassandra.connection.ssl.keyStore.password", keyStorePassword)
        .set("spark.cassandra.connection.ssl.keyStore.path", keyStorePath)
        .set("spark.cassandra.connection.ssl.trustStore.type", trustStoreType)
        .set("spark.cassandra.connection.ssl.clientAuth.enabled", "true")
      )
    } else {
      abstractLogger.info(connType + ": Connecting to Cassandra (or DSE) host: " + host);

      return CassandraConnector(sc.set("spark.cassandra.auth.username", username)
        .set("spark.cassandra.auth.password", password)
        .set("spark.cassandra.input.consistency.level", readConsistencyLevel)
        .set("spark.cassandra.connection.host", host))
    }

  }

}
