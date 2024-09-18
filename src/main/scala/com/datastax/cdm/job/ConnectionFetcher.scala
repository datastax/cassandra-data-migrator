/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.cdm.job

import com.datastax.cdm.properties.{KnownProperties, IPropertyHelper}
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}
import com.datastax.cdm.data.DataUtility.generateSCB
import com.datastax.cdm.data.PKFactory.Side

// TODO: CDM-31 - add localDC configuration support
class ConnectionFetcher(sparkContext: SparkContext, propertyHelper: IPropertyHelper) {
  val logger: Logger = LoggerFactory.getLogger(this.getClass.getName)

  def getConnectionDetails(side: Side): ConnectionDetails = {
    if (Side.ORIGIN.equals(side)) {
      ConnectionDetails(
        propertyHelper.getAsString(KnownProperties.CONNECT_ORIGIN_SCB),
        propertyHelper.getAsString(KnownProperties.CONNECT_ORIGIN_HOST),
        propertyHelper.getAsString(KnownProperties.CONNECT_ORIGIN_PORT),
        propertyHelper.getAsString(KnownProperties.CONNECT_ORIGIN_USERNAME),
        propertyHelper.getAsString(KnownProperties.CONNECT_ORIGIN_PASSWORD),
        propertyHelper.getAsString(KnownProperties.ORIGIN_TLS_ENABLED),
        propertyHelper.getAsString(KnownProperties.ORIGIN_TLS_TRUSTSTORE_PATH),
        propertyHelper.getAsString(KnownProperties.ORIGIN_TLS_TRUSTSTORE_PASSWORD),
        propertyHelper.getAsString(KnownProperties.ORIGIN_TLS_TRUSTSTORE_TYPE),
        propertyHelper.getAsString(KnownProperties.ORIGIN_TLS_KEYSTORE_PATH),
        propertyHelper.getAsString(KnownProperties.ORIGIN_TLS_KEYSTORE_PASSWORD),
        propertyHelper.getAsString(KnownProperties.ORIGIN_TLS_ALGORITHMS),
        propertyHelper.getBoolean(KnownProperties.ORIGIN_TLS_IS_ASTRA)
      )
    }
    else {
      ConnectionDetails(
        propertyHelper.getAsString(KnownProperties.CONNECT_TARGET_SCB),
        propertyHelper.getAsString(KnownProperties.CONNECT_TARGET_HOST),
        propertyHelper.getAsString(KnownProperties.CONNECT_TARGET_PORT),
        propertyHelper.getAsString(KnownProperties.CONNECT_TARGET_USERNAME),
        propertyHelper.getAsString(KnownProperties.CONNECT_TARGET_PASSWORD),
        propertyHelper.getAsString(KnownProperties.TARGET_TLS_ENABLED),
        propertyHelper.getAsString(KnownProperties.TARGET_TLS_TRUSTSTORE_PATH),
        propertyHelper.getAsString(KnownProperties.TARGET_TLS_TRUSTSTORE_PASSWORD),
        propertyHelper.getAsString(KnownProperties.TARGET_TLS_TRUSTSTORE_TYPE),
        propertyHelper.getAsString(KnownProperties.TARGET_TLS_KEYSTORE_PATH),
        propertyHelper.getAsString(KnownProperties.TARGET_TLS_KEYSTORE_PASSWORD),
        propertyHelper.getAsString(KnownProperties.TARGET_TLS_ALGORITHMS),
        propertyHelper.getBoolean(KnownProperties.TARGET_TLS_IS_ASTRA)
      )
    }
  }

  def getConnection(side: Side, consistencyLevel: String): CassandraConnector = {
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
    } else if (connectionDetails.trustStorePath.nonEmpty && connectionDetails.isAstra) {
      logger.info("Connecting to Astra "+side+" (with truststore) using host metadata at "+connectionDetails.host+":"+connectionDetails.port);

      val scbFile = generateSCB(connectionDetails.host, connectionDetails.port, 
      	connectionDetails.trustStorePassword, connectionDetails.trustStorePath, 
      	connectionDetails.keyStorePassword, connectionDetails.keyStorePath, side)
      return CassandraConnector(config
        .set("spark.cassandra.auth.username", connectionDetails.username)
        .set("spark.cassandra.auth.password", connectionDetails.password)
        .set("spark.cassandra.input.consistency.level", consistencyLevel)
        .set("spark.cassandra.connection.config.cloud.path", "file://" + scbFile.getAbsolutePath()))
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