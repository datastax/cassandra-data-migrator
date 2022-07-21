package datastax.astra.migrate

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.log4j.Logger

import java.lang.Long
import java.math.BigInteger
import scala.collection.JavaConversions._

object DiffData extends AbstractJob {

  val logger = Logger.getLogger(this.getClass.getName)
  logger.info("Started Data Validation App")

  diffTable(sourceConnection, destinationConnection, minPartition, maxPartition)

  exitSpark

  private def diffTable(sourceConnection: CassandraConnector, destinationConnection: CassandraConnector, minPartition:BigInteger, maxPartition:BigInteger) = {
    val partitions = SplitPartitions.getRandomSubPartitions(BigInteger.valueOf(Long.parseLong(splitSize)), minPartition, maxPartition)
    val parts = sc.parallelize(partitions.toSeq,partitions.size);

    logger.info("Spark parallelize created : " + parts.count() + " parts!");

    parts.foreach(part => {
      sourceConnection.withSessionDo(sourceSession =>
        destinationConnection.withSessionDo(destinationSession =>
          DiffJobSession.getInstance(sourceSession, destinationSession, sc.getConf)
            .getDataAndDiff(part.getMin, part.getMax)))
    })

    DiffJobSession.getInstance(null, null, sc.getConf).printCounts("Job Final");
  }

}
