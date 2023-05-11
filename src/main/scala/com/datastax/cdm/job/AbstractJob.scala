package com.datastax.cdm.job

import com.datastax.cdm.properties.KnownProperties
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.SparkConf

import java.math.BigInteger

class AbstractJob extends BaseJob {

  // TODO: CDM-31 - add localDC configuration support
  var originConnection = getConnection(true, originScbPath, originHost, originPort, originUsername, originPassword, originSSLEnabled,
    originTrustStorePath, originTrustStorePassword, originTrustStoreType, originKeyStorePath, originKeyStorePassword, originEnabledAlgorithms);

  var targetConnection = getConnection(false, targetScbPath, targetHost, targetPort, targetUsername, targetPassword, targetSSLEnabled,
    targetTrustStorePath, targetTrustStorePassword, targetTrustStoreType, targetKeyStorePath, targetKeyStorePassword, targetEnabledAlgorithms);

  val hasRandomPartitioner: Boolean = {
    val partitionerName = originConnection.withSessionDo(_.getMetadata.getTokenMap.get().getPartitionerName)
    partitionerName.endsWith("RandomPartitioner")
  }

  val minPartition = getMinPartition(propertyHelper.getString(KnownProperties.PARTITION_MIN), hasRandomPartitioner)
  val maxPartition = getMaxPartition(propertyHelper.getString(KnownProperties.PARTITION_MAX), hasRandomPartitioner)

  abstractLogger.info("PARAM -- Min Partition: " + minPartition)
  abstractLogger.info("PARAM -- Max Partition: " + maxPartition)
  abstractLogger.info("PARAM -- Number of Splits : " + numSplits)
  abstractLogger.info("PARAM -- Coverage Percent: " + coveragePercent)
  abstractLogger.info("PARAM -- Origin SSL Enabled: {}", originSSLEnabled);
  abstractLogger.info("PARAM -- Target SSL Enabled: {}", targetSSLEnabled);

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

  def getMinPartition(minPartition: String, hasRandomPartitioner: Boolean): BigInteger = {
    if (minPartition != null && minPartition.nonEmpty) new BigInteger(minPartition)
    else if (hasRandomPartitioner) BigInteger.ZERO
    else BigInteger.valueOf(Long.MinValue)
  }

  def getMaxPartition(maxPartition: String, hasRandomPartitioner: Boolean): BigInteger = {
    if (maxPartition != null && maxPartition.nonEmpty) new BigInteger(maxPartition)
    else if (hasRandomPartitioner) new BigInteger("2").pow(127).subtract(BigInteger.ONE)
    else BigInteger.valueOf(Long.MaxValue)
  }
}
