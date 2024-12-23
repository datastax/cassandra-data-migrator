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
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import com.datastax.cdm.data.PKFactory.Side
import com.datastax.cdm.job.IJobSessionFactory.JobType

import java.math.BigInteger
import java.util
import scala.reflect.ClassTag
import scala.collection.JavaConverters._

// http://www.russellspitzer.com/2016/02/16/Multiple-Clusters-SparkSql-Cassandra/

abstract class BaseJob[T: ClassTag] extends App {

  protected val abstractLogger = LoggerFactory.getLogger(this.getClass.getName)

  private var jobName: String = _
  var jobFactory: IJobSessionFactory[T] = _

  var spark: SparkSession = _
  var sContext: SparkContext = _
  var propertyHelper: PropertyHelper = _

  var consistencyLevel: String = _
  var connectionFetcher: ConnectionFetcher = _
  var minPartition: BigInteger = _
  var maxPartition: BigInteger = _
  var coveragePercent: Int = _
  var numSplits: Int = _
  var trackRun: Boolean = _
  var runId: Long = _
  var prevRunId: Long = _
  var jobType: JobType = _

  var parts: util.Collection[T] = _
  var slices: RDD[T] = _

  var originConnection: CassandraConnector = _
  var targetConnection: CassandraConnector = _

  def setup(jobName: String, jobFactory: IJobSessionFactory[T]): Unit = {
    logBanner(jobName + " - Starting")
    this.jobName = jobName
    this.jobFactory = jobFactory

    spark = SparkSession.builder
      .appName(jobName)
      .getOrCreate()
    sContext = spark.sparkContext
    propertyHelper = PropertyHelper.getInstance(sContext.getConf);

    runId = propertyHelper.getLong(KnownProperties.RUN_ID)
    prevRunId = propertyHelper.getLong(KnownProperties.PREV_RUN_ID)
    trackRun = if (0 != prevRunId || 0 != runId) true else propertyHelper.getBoolean(KnownProperties.TRACK_RUN)
    if (trackRun == true && runId == 0) {
      runId = System.nanoTime();
    }
    consistencyLevel = propertyHelper.getString(KnownProperties.READ_CL)
    connectionFetcher = new ConnectionFetcher(propertyHelper)
    originConnection = connectionFetcher.getConnection(sContext.getConf, Side.ORIGIN, consistencyLevel, runId)
    targetConnection = connectionFetcher.getConnection(sContext.getConf, Side.TARGET, consistencyLevel, runId)

    val hasRandomPartitioner: Boolean = {
      val partitionerName = originConnection.withSessionDo(_.getMetadata.getTokenMap.get().getPartitionerName)
      partitionerName.endsWith("RandomPartitioner")
    }
    minPartition = getMinPartition(propertyHelper.getString(KnownProperties.PARTITION_MIN), hasRandomPartitioner)
    maxPartition = getMaxPartition(propertyHelper.getString(KnownProperties.PARTITION_MAX), hasRandomPartitioner)
    coveragePercent = propertyHelper.getInteger(KnownProperties.TOKEN_COVERAGE_PERCENT)
    numSplits = propertyHelper.getInteger(KnownProperties.PERF_NUM_PARTS)

    abstractLogger.info("PARAM -- Min Partition: " + minPartition)
    abstractLogger.info("PARAM -- Max Partition: " + maxPartition)
    abstractLogger.info("PARAM -- Number of Splits : " + numSplits)
    abstractLogger.info("PARAM -- Track Run : " + trackRun)
    if (trackRun == true) {
      abstractLogger.info("PARAM -- RunId : " + runId)
      abstractLogger.info("PARAM -- Previous RunId : " + prevRunId)
    }
    abstractLogger.info("PARAM -- Coverage Percent: " + coveragePercent)
    this.parts = getParts(numSplits)
    abstractLogger.info("PARAM Calculated -- Total Partitions: " + parts.size())
    if (parts.size() > 0) {
      this.slices = sContext.parallelize(parts.asScala.toSeq, parts.size);
	  abstractLogger.info("Spark parallelize created : " + slices.getNumPartitions + " slices!");
    }
  }

  def getParts(pieces: Int): util.Collection[T]

  protected def finish() = {
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
