package datastax.cdm.job

import com.datastax.spark.connector.cql.CassandraConnector
import org.slf4j.LoggerFactory

object DiffDataFailedRowsFromFile extends AbstractJob {

  val logger = LoggerFactory.getLogger(this.getClass.getName)
  logger.info("Started MigrateRowsFromFile App")

  migrateTable(originConnection, targetConnection)

  exitSpark

  private def migrateTable(sourceConnection: CassandraConnector, destinationConnection: CassandraConnector) = {
    val listOfPKRows = SplitPartitions.getFailedRowPartsFromFile(numSplits, rowFailureFileSizeLimit, failedRowsFile)
    logger.info("PARAM Calculated -- Number of PKRows: " + listOfPKRows.size())
    /*
    sourceConnection.withSessionDo(sourceSession =>
      destinationConnection.withSessionDo(destinationSession =>
        CopyPKJobSession.getInstance(sourceSession, destinationSession, sc)
          .getRowAndDiff(listOfPKRows)))
    */
  }

}
