package datastax.astra.migrate

import com.datastax.spark.connector.cql.CassandraConnector
import org.slf4j.LoggerFactory

import java.math.BigInteger
import scala.collection.JavaConversions._
import java.lang.Long

object MigratePartitionsFromFile extends AbstractJob {

  val logger = LoggerFactory.getLogger(this.getClass.getName)
  logger.info("Started MigratePartitionsFromFile App")

  migrateTable(sourceConnection, destinationConnection)

  exitSpark

  private def migrateTable(sourceConnection: CassandraConnector, destinationConnection: CassandraConnector) = {
    val partitions = SplitPartitions.getSubPartitionsFromFile(splitSize)
    logger.info("PARAM Calculated -- Total Partitions: " + partitions.size())
    val parts = sc.parallelize(partitions.toSeq, partitions.size);
    logger.info("Spark parallelize created : " + parts.count() + " parts!");

    parts.foreach(part => {
      sourceConnection.withSessionDo(sourceSession =>
        destinationConnection.withSessionDo(destinationSession =>
          CopyJobSession.getInstance(sourceSession, destinationSession, sc.getConf)
            .getDataAndInsert(part.getMin, part.getMax)))
    })

  }

}



