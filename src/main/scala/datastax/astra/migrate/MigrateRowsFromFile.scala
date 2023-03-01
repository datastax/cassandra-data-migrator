package datastax.astra.migrate

import com.datastax.spark.connector.cql.CassandraConnector
import org.slf4j.LoggerFactory

object MigrateRowsFromFile extends AbstractJob {

  val logger = LoggerFactory.getLogger(this.getClass.getName)
  logger.info("Started MigrateRowsFromFile App")

  migrateTable(sourceConnection, destinationConnection)

  exitSpark

  private def migrateTable(sourceConnection: CassandraConnector, destinationConnection: CassandraConnector) = {
    val listOfPKRows = SplitPartitions.getRowPartsFromFile(numSplits)
    logger.info("PARAM Calculated -- Number of PKRows: " + listOfPKRows.size())

    sourceConnection.withSessionDo(sourceSession =>
      destinationConnection.withSessionDo(destinationSession =>
        CopyPKJobSession.getInstance(sourceSession, destinationSession, sc)
          .getRowAndInsert(listOfPKRows)))
  }

}
