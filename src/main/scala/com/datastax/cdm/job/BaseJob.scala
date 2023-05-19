package com.datastax.cdm.job

import com.datastax.cdm.properties.{KnownProperties, PropertyHelper}
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.math.BigInteger
import java.util
import scala.reflect.ClassTag
import scala.collection.JavaConverters._

// http://www.russellspitzer.com/2016/02/16/Multiple-Clusters-SparkSql-Cassandra/

abstract class BaseJob[T: ClassTag] extends App {

  val abstractLogger = LoggerFactory.getLogger(this.getClass.getName)

  var jobName: String = _
  var jobFactory: IJobSessionFactory[T] = _
  var inputFilename: String = ""

  var spark: SparkSession = _
  var sContext: SparkContext = _
  var sc: SparkConf = _
  var propertyHelper: PropertyHelper = _

  var consistencyLevel: String = _
  var minPartition: BigInteger = _
  var maxPartition: BigInteger = _
  var coveragePercent: Int = _
  var numSplits: Int = _

  var parts: util.Collection[T] = _
  var slices: RDD[T] = _

  var originConnection: CassandraConnector = _
  var targetConnection: CassandraConnector = _

  def setup(jobName: String, jobFactory: IJobSessionFactory[T], inputFilename: String): Unit = {
    this.inputFilename = inputFilename
    setup(jobName, jobFactory)
  }

  def setup(jobName: String, jobFactory: IJobSessionFactory[T]): Unit = {
    logBanner(jobName + " - Starting")
    this.jobName = jobName
    this.jobFactory = jobFactory

    spark = SparkSession.builder
      .appName(jobName)
      .getOrCreate()
    sContext = spark.sparkContext
    sc = sContext.getConf
    propertyHelper = PropertyHelper.getInstance(sc);

    consistencyLevel = propertyHelper.getString(KnownProperties.READ_CL)
    minPartition = new BigInteger(propertyHelper.getAsString(KnownProperties.PARTITION_MIN))
    maxPartition = new BigInteger(propertyHelper.getAsString(KnownProperties.PARTITION_MAX))
    coveragePercent = propertyHelper.getInteger(KnownProperties.TOKEN_COVERAGE_PERCENT)
    numSplits = propertyHelper.getInteger(KnownProperties.PERF_NUM_PARTS)
    abstractLogger.info("PARAM -- Min Partition: " + minPartition)
    abstractLogger.info("PARAM -- Max Partition: " + maxPartition)
    abstractLogger.info("PARAM -- Number of Splits : " + numSplits)
    abstractLogger.info("PARAM -- Coverage Percent: " + coveragePercent)

    this.parts = getParts(numSplits)
    this.slices = sContext.parallelize(parts.asScala.toSeq, parts.size);
    abstractLogger.info("PARAM Calculated -- Total Partitions: " + parts.size())
    abstractLogger.info("Spark parallelize created : " + slices.count() + " parts!");

    val connectionFetcher = new ConnectionFetcher(sContext, propertyHelper)
    originConnection = connectionFetcher.getConnection("ORIGIN", consistencyLevel)
    targetConnection = connectionFetcher.getConnection("TARGET", consistencyLevel)
  }

  def getParts(pieces: Int): util.Collection[T]
  def printSummary(): Unit = {
    jobFactory.getInstance(null, null, sc).printCounts(true);
  }

  def execute(): Unit = {
    slices.foreach(slice => {
      originConnection.withSessionDo(sourceSession =>
        targetConnection.withSessionDo(destinationSession =>
          jobFactory.getInstance(sourceSession, destinationSession, sc)
            .processSlice(slice)))
    })
  }

  def execute(jobName: String, jobFactory: IJobSessionFactory[T]): Unit = {
      setup(jobName, jobFactory)
      slices.foreach(slice => {
        originConnection.withSessionDo(sourceSession =>
          targetConnection.withSessionDo(destinationSession =>
            jobFactory.getInstance(sourceSession, destinationSession, sc)
              .processSlice(slice)))
      })
      printSummary()
      finish()
  }

  protected def finish() = {
    printSummary()
    spark.stop()
    logBanner(jobName + " - Stopped")
  }

  protected def logBanner(message: String): Unit = {
    val bannerFill = "################################################################################################"
    val maxLength = bannerFill.length
    val prefix = "###"
    val suffix = "###"

    val trimmedMessage = message.substring(0, Math.min(message.length, maxLength - prefix.length - suffix.length))
    val remainingSpace = maxLength - prefix.length - suffix.length - trimmedMessage.length
    val leftPadding = remainingSpace / 2
    val rightPadding = remainingSpace - leftPadding
    val formattedMessage = s"$prefix${" " * leftPadding}$trimmedMessage${" " * rightPadding}$suffix"

    abstractLogger.info(bannerFill)
    abstractLogger.info(formattedMessage)
    abstractLogger.info(bannerFill)
  }

}
