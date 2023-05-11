package com.datastax.cdm.job

import com.datastax.cdm.properties.{KnownProperties, PropertyHelper}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.math.BigInteger

class BaseJob extends App {

  val abstractLogger = LoggerFactory.getLogger(this.getClass.getName)
  val spark = SparkSession.builder
    .appName("Cassandra Data Migrator")
    .getOrCreate()
  abstractLogger.info("################################################################################################")
  abstractLogger.info("############################## Cassandra Data Migrator - Starting ##############################")
  abstractLogger.info("################################################################################################")

  val sContext = spark.sparkContext
  val sc = sContext.getConf
  val propertyHelper = PropertyHelper.getInstance(sc);

  val consistencyLevel = propertyHelper.getString(KnownProperties.READ_CL)

  val originScbPath = propertyHelper.getAsString(KnownProperties.ORIGIN_CONNECT_SCB)
  val originHost = propertyHelper.getAsString(KnownProperties.ORIGIN_CONNECT_HOST)
  val originPort = propertyHelper.getAsString(KnownProperties.ORIGIN_CONNECT_PORT)
  val originUsername = propertyHelper.getAsString(KnownProperties.ORIGIN_CONNECT_USERNAME)
  val originPassword = propertyHelper.getAsString(KnownProperties.ORIGIN_CONNECT_PASSWORD)
  val originSSLEnabled = propertyHelper.getAsString(KnownProperties.ORIGIN_TLS_ENABLED)
  val originTrustStorePath = propertyHelper.getAsString(KnownProperties.ORIGIN_TLS_TRUSTSTORE_PATH)
  val originTrustStorePassword = propertyHelper.getAsString(KnownProperties.ORIGIN_TLS_TRUSTSTORE_PASSWORD)
  val originTrustStoreType = propertyHelper.getString(KnownProperties.ORIGIN_TLS_TRUSTSTORE_TYPE)
  val originKeyStorePath = propertyHelper.getAsString(KnownProperties.ORIGIN_TLS_KEYSTORE_PATH)
  val originKeyStorePassword = propertyHelper.getAsString(KnownProperties.ORIGIN_TLS_KEYSTORE_PASSWORD)
  val originEnabledAlgorithms = propertyHelper.getAsString(KnownProperties.ORIGIN_TLS_ALGORITHMS)

  val targetScbPath = propertyHelper.getAsString(KnownProperties.TARGET_CONNECT_SCB)
  val targetHost = propertyHelper.getAsString(KnownProperties.TARGET_CONNECT_HOST)
  val targetPort = propertyHelper.getAsString(KnownProperties.TARGET_CONNECT_PORT)
  val targetUsername = propertyHelper.getAsString(KnownProperties.TARGET_CONNECT_USERNAME)
  val targetPassword = propertyHelper.getAsString(KnownProperties.TARGET_CONNECT_PASSWORD)
  val targetSSLEnabled = propertyHelper.getAsString(KnownProperties.TARGET_TLS_ENABLED)
  val targetTrustStorePath = propertyHelper.getAsString(KnownProperties.TARGET_TLS_TRUSTSTORE_PATH)
  val targetTrustStorePassword = propertyHelper.getAsString(KnownProperties.TARGET_TLS_TRUSTSTORE_PASSWORD)
  val targetTrustStoreType = propertyHelper.getString(KnownProperties.TARGET_TLS_TRUSTSTORE_TYPE)
  val targetKeyStorePath = propertyHelper.getAsString(KnownProperties.TARGET_TLS_KEYSTORE_PATH)
  val targetKeyStorePassword = propertyHelper.getAsString(KnownProperties.TARGET_TLS_KEYSTORE_PASSWORD)
  val targetEnabledAlgorithms = propertyHelper.getAsString(KnownProperties.TARGET_TLS_ALGORITHMS)

  val coveragePercent = propertyHelper.getAsString(KnownProperties.TOKEN_COVERAGE_PERCENT)
  val numSplits = propertyHelper.getInteger(KnownProperties.PERF_NUM_PARTS)

  protected def exitSpark() = {
    spark.stop()
    abstractLogger.info("################################################################################################")
    abstractLogger.info("############################## Cassandra Data Migrator - Stopped ###############################")
    abstractLogger.info("################################################################################################")
    sys.exit(0)
  }

}
