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

import com.datastax.cdm.properties.{KnownProperties, PropertyHelper}
import com.datastax.spark.connector.cql.CassandraConnector
import com.google.cloud.secretmanager.v1.{AccessSecretVersionRequest, SecretManagerServiceClient, SecretPayload}
import org.apache.spark.{SparkConf, SparkContext}
import org.json.JSONObject
import org.slf4j.{Logger, LoggerFactory}

// TODO: CDM-31 - add localDC configuration support
class ConnectionFetcher(sparkContext: SparkContext, propertyHelper: PropertyHelper) {
  val logger: Logger = LoggerFactory.getLogger(this.getClass.getName)

    def getConnectionDetails(side: String): ConnectionDetails = {
    val (username, password) = if ("ORIGIN".equals(side.toUpperCase)) {
      getCredentials(
        KnownProperties.CONNECT_ORIGIN_USERNAME,
        KnownProperties.CONNECT_ORIGIN_PASSWORD,
        KnownProperties.CONNECT_ORIGIN_GCP_SECRET_NAME
      )
    } else {
      getCredentials(
        KnownProperties.CONNECT_TARGET_USERNAME,
        KnownProperties.CONNECT_TARGET_PASSWORD,
        KnownProperties.CONNECT_TARGET_GCP_SECRET_NAME
      )
    }
    if ("ORIGIN".equals(side.toUpperCase)) {
      ConnectionDetails(
        propertyHelper.getAsString(KnownProperties.CONNECT_ORIGIN_SCB),
        propertyHelper.getAsString(KnownProperties.CONNECT_ORIGIN_HOST),
        propertyHelper.getAsString(KnownProperties.CONNECT_ORIGIN_PORT),
        username,
        password,
        propertyHelper.getAsString(KnownProperties.ORIGIN_TLS_ENABLED),
        propertyHelper.getAsString(KnownProperties.ORIGIN_TLS_TRUSTSTORE_PATH),
        propertyHelper.getAsString(KnownProperties.ORIGIN_TLS_TRUSTSTORE_PASSWORD),
        propertyHelper.getString(KnownProperties.ORIGIN_TLS_TRUSTSTORE_TYPE),
        propertyHelper.getAsString(KnownProperties.ORIGIN_TLS_KEYSTORE_PATH),
        propertyHelper.getAsString(KnownProperties.ORIGIN_TLS_KEYSTORE_PASSWORD),
        propertyHelper.getAsString(KnownProperties.ORIGIN_TLS_ALGORITHMS)
      )
    } else {
      ConnectionDetails(
        propertyHelper.getAsString(KnownProperties.CONNECT_TARGET_SCB),
        propertyHelper.getAsString(KnownProperties.CONNECT_TARGET_HOST),
        propertyHelper.getAsString(KnownProperties.CONNECT_TARGET_PORT),
        username,
        password,
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

  private def getSecret(projectId: String, secretId: String): (String, String) = {
    var client: SecretManagerServiceClient = null
    try {
      client = SecretManagerServiceClient.create()
      val secretName = s"projects/$projectId/secrets/$secretId/versions/latest"
      logger.info("Secret Name is: " + secretName)
      val request = AccessSecretVersionRequest.newBuilder().setName(secretName).build()
      val payload: SecretPayload = client.accessSecretVersion(request).getPayload
      val jsonObject: JSONObject = new JSONObject(payload.getData.toStringUtf8)

      val client_id = jsonObject.get("client_id").toString
      val secret =  jsonObject.get("secret").toString
      (client_id, secret)
    } catch {
      case e: Exception => throw new RuntimeException("Failed to get access secret ", e)
    } finally {
      if (client != null) client.close()
    }
  }

  private def getCredentials(usernameKey: String, passwordKey: String, secretNameKey: String): (String, String) = {
    val username = propertyHelper.getAsString(usernameKey)
    val password = propertyHelper.getAsString(passwordKey)

    if ((username != null && username.nonEmpty) && (password != null && password.nonEmpty)) {
      logger.info("Using provided username and password...")
      (username, password)
    } else {
      logger.info("Fetching credentials from GSM...")
      val projectId = propertyHelper.getAsString(KnownProperties.CONNECT_GCP_SECRET_PROJECT_ID)
      val secretName = propertyHelper.getAsString(secretNameKey)
      val (username, password)  = getSecret(projectId, secretName)
      (username, password)
    }
  }
}