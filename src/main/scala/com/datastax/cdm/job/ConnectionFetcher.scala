package com.datastax.cdm.job

import com.datastax.cdm.properties.{KnownProperties, PropertyHelper}
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

class ConnectionFetcher(sparkContext: SparkContext, propertyHelper: PropertyHelper) {
  val logger: Logger = LoggerFactory.getLogger(this.getClass.getName)

  def getConnectionDetails(side: String): ConnectionDetails = {
    if ("ORIGIN".equals(side.toUpperCase)) {
      ConnectionDetails(
        propertyHelper.getAsString(KnownProperties.ORIGIN_CONNECT_SCB),
        propertyHelper.getAsString(KnownProperties.ORIGIN_CONNECT_HOST),
        propertyHelper.getAsString(KnownProperties.ORIGIN_CONNECT_PORT),
        propertyHelper.getAsString(KnownProperties.ORIGIN_CONNECT_USERNAME),
        propertyHelper.getAsString(KnownProperties.ORIGIN_CONNECT_PASSWORD),
        propertyHelper.getAsString(KnownProperties.ORIGIN_TLS_ENABLED),
        propertyHelper.getAsString(KnownProperties.ORIGIN_TLS_TRUSTSTORE_PATH),
        propertyHelper.getAsString(KnownProperties.ORIGIN_TLS_TRUSTSTORE_PASSWORD),
        propertyHelper.getString(KnownProperties.ORIGIN_TLS_TRUSTSTORE_TYPE),
        propertyHelper.getAsString(KnownProperties.ORIGIN_TLS_KEYSTORE_PATH),
        propertyHelper.getAsString(KnownProperties.ORIGIN_TLS_KEYSTORE_PASSWORD),
        propertyHelper.getAsString(KnownProperties.ORIGIN_TLS_ALGORITHMS)
      )
    }
    else {
      ConnectionDetails(
        propertyHelper.getAsString(KnownProperties.TARGET_CONNECT_SCB),
        propertyHelper.getAsString(KnownProperties.TARGET_CONNECT_HOST),
        propertyHelper.getAsString(KnownProperties.TARGET_CONNECT_PORT),
        propertyHelper.getAsString(KnownProperties.TARGET_CONNECT_USERNAME),
        propertyHelper.getAsString(KnownProperties.TARGET_CONNECT_PASSWORD),
        propertyHelper.getAsString(KnownProperties.TARGET_TLS_ENABLED),
        propertyHelper.getAsString(KnownProperties.TARGET_TLS_TRUSTSTORE_PATH),
        propertyHelper.getAsString(KnownProperties.TARGET_TLS_TRUSTSTORE_PASSWORD),
        propertyHelper.getString(KnownProperties.TARGET_TLS_TRUSTSTORE_TYPE),
        propertyHelper.getAsString(KnownProperties.TARGET_TLS_KEYSTORE_PATH),
        propertyHelper.getAsString(KnownProperties.TARGET_TLS_KEYSTORE_PASSWORD),
        propertyHelper.getAsString(KnownProperties.TARGET_TLS_ALGORITHMS)
      )
    }
  }

  def getConnection(side: String, consistencyLevel: String): CassandraConnector = {
    val connectionDetails = getConnectionDetails(side)
    val config: SparkConf = sparkContext.getConf

    logger.info("PARAM --  SSL Enabled: "+connectionDetails.sslEnabled);

    if (connectionDetails.scbPath.nonEmpty) {
      logger.info("Connecting to "+side+" using SCB "+connectionDetails.scbPath);
      return CassandraConnector(config
        .set("spark.cassandra.auth.username", connectionDetails.username)
        .set("spark.cassandra.auth.password", connectionDetails.password)
        .set("spark.cassandra.input.consistency.level", consistencyLevel)
        .set("spark.cassandra.connection.config.cloud.path", connectionDetails.scbPath))
    } else if (connectionDetails.trustStorePath.nonEmpty) {
      logger.info("Connecting to "+side+" (with truststore) at "+connectionDetails.host+":"+connectionDetails.port);

      // Use defaults when not provided
      var enabledAlgorithmsVar = connectionDetails.enabledAlgorithms
      if (connectionDetails.enabledAlgorithms == null || connectionDetails.enabledAlgorithms.trim.isEmpty) {
        enabledAlgorithmsVar = "TLS_RSA_WITH_AES_128_CBC_SHA, TLS_RSA_WITH_AES_256_CBC_SHA"
      }

      return CassandraConnector(config
        .set("spark.cassandra.auth.username", connectionDetails.username)
        .set("spark.cassandra.auth.password", connectionDetails.password)
        .set("spark.cassandra.input.consistency.level", consistencyLevel)
        .set("spark.cassandra.connection.host", connectionDetails.host)
        .set("spark.cassandra.connection.port", connectionDetails.port)
        .set("spark.cassandra.connection.ssl.enabled", "true")
        .set("spark.cassandra.connection.ssl.enabledAlgorithms", enabledAlgorithmsVar)
        .set("spark.cassandra.connection.ssl.trustStore.password", connectionDetails.trustStorePassword)
        .set("spark.cassandra.connection.ssl.trustStore.path", connectionDetails.trustStorePath)
        .set("spark.cassandra.connection.ssl.keyStore.password", connectionDetails.keyStorePassword)
        .set("spark.cassandra.connection.ssl.keyStore.path", connectionDetails.keyStorePath)
        .set("spark.cassandra.connection.ssl.trustStore.type", connectionDetails.trustStoreType)
        .set("spark.cassandra.connection.ssl.clientAuth.enabled", "true")
      )
    } else {
      logger.info("Connecting to "+side+" at "+connectionDetails.host+":"+connectionDetails.port);

      return CassandraConnector(config.set("spark.cassandra.auth.username", connectionDetails.username)
        .set("spark.cassandra.connection.ssl.enabled", connectionDetails.sslEnabled)
        .set("spark.cassandra.auth.password", connectionDetails.password)
        .set("spark.cassandra.input.consistency.level", consistencyLevel)
        .set("spark.cassandra.connection.host", connectionDetails.host)
        .set("spark.cassandra.connection.port", connectionDetails.port))
    }
  }
}