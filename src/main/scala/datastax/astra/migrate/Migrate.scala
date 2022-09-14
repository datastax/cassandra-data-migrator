package datastax.astra.migrate

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.log4j.Logger

import scala.collection.JavaConversions._

// http://www.russellspitzer.com/2016/02/16/Multiple-Clusters-SparkSql-Cassandra/

object Migrate extends AbstractJob {

  val logger = Logger.getLogger(this.getClass.getName)
  logger.info("Started Migration App")

  migrateTable(sourceConnection, destinationConnection)

  exitSpark

  private def migrateTable(sourceConnection: CassandraConnector, destinationConnection: CassandraConnector) = {
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



