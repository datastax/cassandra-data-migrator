package datastax.astra.migrate

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.log4j.Logger

import java.lang.Long
import java.math.BigInteger
import scala.collection.JavaConversions._

// http://www.russellspitzer.com/2016/02/16/Multiple-Clusters-SparkSql-Cassandra/

object Migrate extends AbstractJob {

  val logger = Logger.getLogger(this.getClass.getName)
  logger.info("Started Migration App")

  migrateTable(sourceConnection, destinationConnection, minPartition, maxPartition)

  exitSpark

  private def migrateTable(sourceConnection: CassandraConnector, destinationConnection: CassandraConnector, minPartition:BigInteger, maxPartition:BigInteger) = {
    val partitions = SplitPartitions.getRandomSubPartitions(BigInteger.valueOf(Long.parseLong(splitSize)), minPartition, maxPartition)
    val parts = sc.parallelize(partitions.toSeq,partitions.size);

    logger.info("Spark parallelize created : " + parts.count() + " parts!");

    parts.foreach(part => {
      sourceConnection.withSessionDo(sourceSession =>
        destinationConnection.withSessionDo(destinationSession =>
          CopyJobSession.getInstance(sourceSession,destinationSession, sc.getConf)
            .getDataAndInsert(part.getMin, part.getMax)))
    })

  }

}



