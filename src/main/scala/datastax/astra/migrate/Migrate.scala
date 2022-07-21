package datastax.astra.migrate

import com.datastax.oss.driver.api.core.{CqlIdentifier, CqlSession}
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.hive._
import org.apache.spark.sql.cassandra._

import scala.collection.JavaConversions._
import java.lang.Long
import java.math.BigInteger
import collection.JavaConversions._



// http://www.russellspitzer.com/2016/02/16/Multiple-Clusters-SparkSql-Cassandra/

object Migrate extends App {
  val logger = Logger.getLogger(this.getClass.getName)

  val spark = SparkSession.builder
    .appName("Datastax Data Migration")
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext


  val sourceUsername = sc.getConf.get("spark.migrate.source.username")
  val sourcePassword = sc.getConf.get("spark.migrate.source.password")
  val sourceHost = sc.getConf.get("spark.migrate.source.host")


  val astraUsername = sc.getConf.get("spark.migrate.astra.username")
  val astraPassword = sc.getConf.get("spark.migrate.astra.password")

  val astraScbPath = sc.getConf.get("spark.migrate.astra.scb")

  val minPartition = new BigInteger(sc.getConf.get("spark.migrate.source.minPartition"))
  val maxPartition = new BigInteger(sc.getConf.get("spark.migrate.source.maxPartition"))

  val splitSize = sc.getConf.get("spark.migrate.splitSize","10000")

  val sourceReadConsistencyLevel = sc.getConf.get("spark.cassandra.source.read.consistency.level","LOCAL_QUORUM")
  val astraReadConsistencyLevel = sc.getConf.get("spark.cassandra.astra.read.consistency.level","LOCAL_QUORUM")


  logger.info("Started Migration App")

  val isBeta = sc.getConf.get("spark.migrate.beta","false")

  var sourceConnection = CassandraConnector(
    sc.getConf
      .set("spark.cassandra.connection.host", sourceHost)
      .set("spark.cassandra.auth.username", sourceUsername)
      .set("spark.cassandra.auth.password", sourcePassword)
      .set("spark.cassandra.input.consistency.level", sourceReadConsistencyLevel))


  if("true".equals(isBeta)){
    sourceConnection = CassandraConnector(
      sc.getConf
        .set("spark.cassandra.connection.config.cloud.path", astraScbPath)
        .set("spark.cassandra.auth.username", astraUsername)
        .set("spark.cassandra.auth.password", astraPassword)
        .set("spark.cassandra.input.consistency.level", sourceReadConsistencyLevel)
    )

  }


  val astraConnection = CassandraConnector(sc.getConf
    .set("spark.cassandra.connection.config.cloud.path", astraScbPath)
    .set("spark.cassandra.auth.username", astraUsername)
    .set("spark.cassandra.auth.password", astraPassword)
    .set("spark.cassandra.input.consistency.level", astraReadConsistencyLevel))




  migrateTable(sourceConnection,astraConnection, minPartition, maxPartition)

  exitSpark

  private def migrateTable(sourceConnection: CassandraConnector, astraConnection: CassandraConnector, minPartition:BigInteger, maxPartition:BigInteger) = {

    val partitions = SplitPartitions.getRandomSubPartitions(BigInteger.valueOf(Long.parseLong(splitSize)), minPartition, maxPartition)
    val parts = sc.parallelize(partitions.toSeq,partitions.size);
    logger.info("Spark parallelize created : " + parts.count() + " parts!");
    parts.foreach(part => {
      sourceConnection.withSessionDo(sourceSession => astraConnection.withSessionDo(astraSession=>   CopyJobSession.getInstance(sourceSession,astraSession, sc.getConf).getDataAndInsert(part.getMin, part.getMax)))
    })
  }

  private def exitSpark = {
    spark.stop()
    sys.exit(0)
  }

}



