package com.datastax.cdm.job

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.SparkConf
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

// http://www.russellspitzer.com/2016/02/16/Multiple-Clusters-SparkSql-Cassandra/

object Migrate extends AbstractJob {

  val logger = LoggerFactory.getLogger(this.getClass.getName)
  logger.info("Started Migration App")

  migrateTable(originConnection, targetConnection, sc)

  exitSpark

  private def migrateTable(sourceConnection: CassandraConnector, destinationConnection: CassandraConnector, config: SparkConf) = {
    val partitions = SplitPartitions.getRandomSubPartitions(numSplits, minPartition, maxPartition, Integer.parseInt(coveragePercent))
    logger.info("PARAM Calculated -- Total Partitions: " + partitions.size())
    val parts = sContext.parallelize(partitions.toSeq, partitions.size);
    logger.info("Spark parallelize created : " + parts.count() + " parts!");

    parts.foreach(part => {
      sourceConnection.withSessionDo(sourceSession =>
        destinationConnection.withSessionDo(destinationSession =>
          CopyJobSession.getInstance(sourceSession, destinationSession, config)
            .getDataAndInsert(part.getMin, part.getMax)))
    })

    CopyJobSession.getInstance(null, null, sc).printCounts(true);
  }

}



