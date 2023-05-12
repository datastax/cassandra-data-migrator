package datastax.astra.migrate

import com.datastax.spark.connector.cql.CassandraConnector
import org.slf4j.LoggerFactory

import org.apache.spark.SparkConf
import scala.collection.JavaConversions._

object DiffPartitionsFromFile extends AbstractJob {

  val logger = LoggerFactory.getLogger(this.getClass.getName)
  logger.info("Started Data Validation App based on the partitions from partitions.csv file")

  diffTable(sourceConnection, destinationConnection, sc)

  exitSpark

  private def diffTable(sourceConnection: CassandraConnector, destinationConnection: CassandraConnector, config: SparkConf) = {
    val partitions = SplitPartitions.getSubPartitionsFromFile(numSplits)
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
