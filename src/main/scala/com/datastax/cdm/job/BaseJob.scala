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

  val minPartition = new BigInteger(propertyHelper.getAsString(KnownProperties.PARTITION_MIN))
  val maxPartition = new BigInteger(propertyHelper.getAsString(KnownProperties.PARTITION_MAX))
  val coveragePercent = propertyHelper.getAsString(KnownProperties.TOKEN_COVERAGE_PERCENT)
  val numSplits = propertyHelper.getInteger(KnownProperties.PERF_NUM_PARTS)

  abstractLogger.info("PARAM -- Min Partition: " + minPartition)
  abstractLogger.info("PARAM -- Max Partition: " + maxPartition)
  abstractLogger.info("PARAM -- Number of Splits : " + numSplits)
  abstractLogger.info("PARAM -- Coverage Percent: " + coveragePercent)

  // TODO: CDM-31 - add localDC configuration support
  private val connectionFetcher = new ConnectionFetcher(sContext, propertyHelper)
  var originConnection = connectionFetcher.getConnection("ORIGIN", consistencyLevel)
  var targetConnection = connectionFetcher.getConnection("TARGET", consistencyLevel)

  protected def exitSpark() = {
    spark.stop()
    abstractLogger.info("################################################################################################")
    abstractLogger.info("############################## Cassandra Data Migrator - Stopped ###############################")
    abstractLogger.info("################################################################################################")
    sys.exit(0)
  }

}
