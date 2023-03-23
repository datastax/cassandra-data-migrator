package datastax.astra.migrate

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.SparkConf

class AbstractJob extends BaseJob {

  abstractLogger.info("PARAM -- Min Partition: " + minPartition)
  abstractLogger.info("PARAM -- Max Partition: " + maxPartition)
  abstractLogger.info("PARAM -- Number of Splits : " + numSplits)
  abstractLogger.info("PARAM -- Coverage Percent: " + coveragePercent)
  abstractLogger.info("PARAM -- Origin SSL Enabled: {}", sourceSSLEnabled);
  abstractLogger.info("PARAM -- Target SSL Enabled: {}", destinationSSLEnabled);

  val destinationScbPath = Util.getSparkPropOrEmpty(sc, "spark.target.scb")
  val destinationHost = Util.getSparkPropOrEmpty(sc, "spark.target.host")
  val destinationPort = Util.getSparkPropOr(sc, "spark.target.port", "9042")
  val destinationUsername = Util.getSparkProp(sc, "spark.target.username")
  val destinationPassword = Util.getSparkProp(sc, "spark.target.password")
  val destinationSSLEnabled = Util.getSparkPropOr(sc, "spark.target.ssl.enabled", "false")
  val destinationTrustStorePath = Util.getSparkPropOrEmpty(sc, "spark.target.trustStore.path")
  val destinationTrustStorePassword = Util.getSparkPropOrEmpty(sc, "spark.target.trustStore.password")
  val destinationTrustStoreType = Util.getSparkPropOr(sc, "spark.target.trustStore.type", "JKS")
  val destinationKeyStorePath = Util.getSparkPropOrEmpty(sc, "spark.target.keyStore.path")
  val destinationKeyStorePassword = Util.getSparkPropOrEmpty(sc, "spark.target.keyStore.password")
  val destinationEnabledAlgorithms = Util.getSparkPropOrEmpty(sc, "spark.target.enabledAlgorithms")

  var sourceConnection = getConnection(true, sourceScbPath, sourceHost, sourcePort, sourceUsername, sourcePassword, sourceSSLEnabled,
    sourceTrustStorePath, sourceTrustStorePassword, sourceTrustStoreType, sourceKeyStorePath, sourceKeyStorePassword, sourceEnabledAlgorithms);

  var destinationConnection = getConnection(false, destinationScbPath, destinationHost, destinationPort, destinationUsername, destinationPassword, destinationSSLEnabled,
    destinationTrustStorePath, destinationTrustStorePassword, destinationTrustStoreType, destinationKeyStorePath, destinationKeyStorePassword, destinationEnabledAlgorithms);

  private def getConnection(isSource: Boolean, scbPath: String, host: String, port: String, username: String, password: String,
                            sslEnabled: String, trustStorePath: String, trustStorePassword: String, trustStoreType: String,
                            keyStorePath: String, keyStorePassword: String, enabledAlgorithms: String): CassandraConnector = {
    var connType: String = "Source"
    if (!isSource) {
      connType = "Destination"
    }

    var config: SparkConf = sContext.getConf
    if (scbPath.nonEmpty) {
      abstractLogger.info(connType + ": Connecting to Astra using SCB: " + scbPath);

      return CassandraConnector(config
        .set("spark.cassandra.auth.username", username)
        .set("spark.cassandra.auth.password", password)
        .set("spark.cassandra.input.consistency.level", consistencyLevel)
        .set("spark.cassandra.connection.config.cloud.path", scbPath))
    } else if (trustStorePath.nonEmpty) {
      abstractLogger.info(connType + ": Connecting (with clientAuth) to Cassandra (or DSE) host:port " + host + ":" + port);

      // Use defaults when not provided
      var enabledAlgorithmsVar = enabledAlgorithms
      if (enabledAlgorithms == null || enabledAlgorithms.trim.isEmpty) {
        enabledAlgorithmsVar = "TLS_RSA_WITH_AES_128_CBC_SHA, TLS_RSA_WITH_AES_256_CBC_SHA"
      }

      return CassandraConnector(config
        .set("spark.cassandra.auth.username", username)
        .set("spark.cassandra.auth.password", password)
        .set("spark.cassandra.input.consistency.level", consistencyLevel)
        .set("spark.cassandra.connection.host", host)
        .set("spark.cassandra.connection.port", port)
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
      abstractLogger.info(connType + ": Connecting to Cassandra (or DSE) host:port " + host + ":" + port);

      return CassandraConnector(config.set("spark.cassandra.auth.username", username)
        .set("spark.cassandra.connection.ssl.enabled", sslEnabled)
        .set("spark.cassandra.auth.password", password)
        .set("spark.cassandra.input.consistency.level", consistencyLevel)
        .set("spark.cassandra.connection.host", host)
        .set("spark.cassandra.connection.port", port))
    }

  }

}
