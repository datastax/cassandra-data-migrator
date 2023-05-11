package com.datastax.cdm.job

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.SparkConf
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

object DiffDataFailedPartitionsFromFile extends AbstractJob {

  val logger = LoggerFactory.getLogger(this.getClass.getName)
  logger.info("Started MigratePartitionsFromFile App")

  migrateTable(originConnection, targetConnection, sc)

  exitSpark

  private def migrateTable(sourceConnection: CassandraConnector, destinationConnection: CassandraConnector, config: SparkConf) = {
    val partitions = SplitPartitions.getFailedSubPartitionsFromFile(numSplits, tokenRangeFile)
    logger.info("PARAM Calculated -- Total Partitions: " + partitions.size())
    val parts = sContext.parallelize(partitions.toSeq, partitions.size);
    logger.info("Spark parallelize created : " + parts.count() + " parts!");

    parts.foreach(part => {
      sourceConnection.withSessionDo(sourceSession =>
        destinationConnection.withSessionDo(destinationSession =>
          DiffJobSession.getInstance(sourceSession, destinationSession, config)
            .getDataAndDiff(part.getMin, part.getMax)))
    })

    DiffJobSession.getInstance(null, null, config).printCounts(true);
  }

}
