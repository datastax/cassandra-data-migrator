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

import com.datastax.cdm.job.SplitPartitions.getPartitionFileInput
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

  private val abstractLogger = LoggerFactory.getLogger(this.getClass.getName)

  private var jobName: String = _
  var jobFactory: IJobSessionFactory[T] = _

  var spark: SparkSession = _
  var sContext: SparkContext = _
  var sc: SparkConf = _
  var propertyHelper: PropertyHelper = _

  var consistencyLevel: String = _
  var minPartition: BigInteger = _
  var maxPartition: BigInteger = _
  var coveragePercent: Int = _
  var numSplits: Int = _
  var trackRun: Boolean = _
  var prevRunId: Int = _

  var parts: util.Collection[T] = _
  var slices: RDD[T] = _

  var originConnection: CassandraConnector = _
  var targetConnection: CassandraConnector = _
  var partitionFileNameInput: String = ""

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
    this.partitionFileNameInput = getPartitionFileInput(propertyHelper);

    consistencyLevel = propertyHelper.getString(KnownProperties.READ_CL)
    val connectionFetcher = new ConnectionFetcher(sContext, propertyHelper)
    originConnection = connectionFetcher.getConnection("ORIGIN", consistencyLevel)
    targetConnection = connectionFetcher.getConnection("TARGET", consistencyLevel)

    val hasRandomPartitioner: Boolean = {
      val partitionerName = originConnection.withSessionDo(_.getMetadata.getTokenMap.get().getPartitionerName)
      partitionerName.endsWith("RandomPartitioner")
    }
    minPartition = getMinPartition(propertyHelper.getString(KnownProperties.PARTITION_MIN), hasRandomPartitioner)
    maxPartition = getMaxPartition(propertyHelper.getString(KnownProperties.PARTITION_MAX), hasRandomPartitioner)
    coveragePercent = propertyHelper.getInteger(KnownProperties.TOKEN_COVERAGE_PERCENT)
    numSplits = propertyHelper.getInteger(KnownProperties.PERF_NUM_PARTS)
    trackRun = propertyHelper.getBoolean(KnownProperties.TRACK_RUN)
    prevRunId = propertyHelper.getInteger(KnownProperties.PREV_RUN_ID)

    abstractLogger.info("PARAM -- Min Partition: " + minPartition)
    abstractLogger.info("PARAM -- Max Partition: " + maxPartition)
    abstractLogger.info("PARAM -- Number of Splits : " + numSplits)
    abstractLogger.info("PARAM -- Track Run : " + trackRun)
    abstractLogger.info("PARAM -- Previous RunId : " + prevRunId)
    abstractLogger.info("PARAM -- Coverage Percent: " + coveragePercent)
    this.parts = getParts(numSplits)
    this.slices = sContext.parallelize(parts.asScala.toSeq, parts.size);
    abstractLogger.info("PARAM Calculated -- Total Partitions: " + parts.size())
    abstractLogger.info("Spark parallelize created : " + slices.getNumPartitions + " slices!");
  }

  def getParts(pieces: Int): util.Collection[T]
  def printSummary(): Unit = {
    jobFactory.getInstance(null, null, sc).printCounts(true);
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
