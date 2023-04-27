package datastax.cdm.job

import com.datastax.spark.connector.cql.CassandraConnector
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

object DiffDataFailedPartitionsFromFile extends AbstractJob {

  val logger = LoggerFactory.getLogger(this.getClass.getName)
  logger.info("Started MigratePartitionsFromFile App")

  migrateTable(originConnection, targetConnection)

  exitSpark

  private def migrateTable(sourceConnection: CassandraConnector, destinationConnection: CassandraConnector) = {
    val partitions = SplitPartitions.getFailedSubPartitionsFromFile(numSplits, tokenRangeFile)
    logger.info("PARAM Calculated -- Total Partitions: " + partitions.size())
    val parts = sContext.parallelize(partitions.toSeq, partitions.size);
    logger.info("Spark parallelize created : " + parts.count() + " parts!");

    parts.foreach(part => {
      sourceConnection.withSessionDo(sourceSession =>
        destinationConnection.withSessionDo(destinationSession =>
          DiffJobSession.getInstance(sourceSession, destinationSession, sc)
            .getDataAndDiff(part.getMin, part.getMax)))
    })

    DiffJobSession.getInstance(null, null, sc).printCounts(true);
  }

}