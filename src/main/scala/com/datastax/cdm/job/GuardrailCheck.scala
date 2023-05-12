package com.datastax.cdm.job

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.SparkConf
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

object GuardrailCheck extends AbstractJob {

  val logger = LoggerFactory.getLogger(this.getClass.getName)
  logger.info("Started Migration App")

  guardrailCheck(originConnection, targetConnection, sc)

  exitSpark

  private def guardrailCheck(sourceConnection: CassandraConnector, destinationConnection: CassandraConnector, config: SparkConf) = {
    val partitions = SplitPartitions.getRandomSubPartitions(numSplits, minPartition, maxPartition, Integer.parseInt(coveragePercent))
    logger.info("PARAM Calculated -- Total Partitions: " + partitions.size())
    val parts = sContext.parallelize(partitions.toSeq, partitions.size);
    logger.info("Spark parallelize created : " + parts.count() + " parts!");

    parts.foreach(part => {
      sourceConnection.withSessionDo(sourceSession =>
        destinationConnection.withSessionDo(destinationSession =>
          GuardrailCheckJobSession.getInstance(sourceSession, destinationSession, config)
            .guardrailCheck(part.getMin, part.getMax)))
    })

    GuardrailCheckJobSession.getInstance(null, null, sc).printCounts(true);
  }

}

