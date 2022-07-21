package datastax.astra.migrate

import com.datastax.oss.driver.api.core.{CqlIdentifier, CqlSession}
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import datastax.astra.migrate.Migrate.{astraPassword, astraReadConsistencyLevel, astraScbPath, astraUsername, sc, sourceHost, sourcePassword, sourceReadConsistencyLevel, sourceUsername}
import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.hive._
import org.apache.spark.sql.cassandra._

import scala.collection.JavaConversions._
import java.lang.Long
import java.math.BigInteger
import collection.JavaConversions._
import java.math.BigInteger

object DiffData extends App {

  val logger = Logger.getLogger(this.getClass.getName)

  val spark = SparkSession.builder
    .appName("Datastax Data Validation")
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext

  val sourceUsername = sc.getConf.get("spark.migrate.source.username")
  val sourcePassword = sc.getConf.get("spark.migrate.source.password")
  val sourceHost = sc.getConf.get("spark.migrate.source.host")
  val sourceReadConsistencyLevel = sc.getConf.get("spark.cassandra.source.read.consistency.level","LOCAL_QUORUM")

  val destinationHost =  sc.getConf.get("spark.migrate.destination.host", "")
  val destintationUsername = sc.getConf.get("spark.migrate.destination.username", "")
  val destinationPassword = sc.getConf.get("spark.migrate.destination.password", "")
  val destinationReadConsistencyLevel = sc.getConf.get("spark.migrate.destination.read.consistency.level", "LOCAL_QUORUM")

  val astraScbPath = sc.getConf.get("spark.migrate.astra.scb")
  val astraUsername = sc.getConf.get("spark.migrate.astra.username")
  val astraPassword = sc.getConf.get("spark.migrate.astra.password")
  val astraReadConsistencyLevel = sc.getConf.get("spark.cassandra.astra.read.consistency.level","LOCAL_QUORUM")

  val minPartition = new BigInteger(sc.getConf.get("spark.migrate.source.minPartition"))
  val maxPartition = new BigInteger(sc.getConf.get("spark.migrate.source.maxPartition"))

  val splitSize = sc.getConf.get("spark.migrate.splitSize","10000")


  logger.info("Started Data Validation App")

  val isBeta = sc.getConf.get("spark.migrate.beta","false")
  val isCassandraToCassandra = sc.getConf.get("spark.migrate.ctoc", "false")

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
  var destinationConnection = CassandraConnector(sc.getConf
    .set("spark.cassandra.connection.config.cloud.path", astraScbPath)
    .set("spark.cassandra.auth.username", astraUsername)
    .set("spark.cassandra.auth.password", astraPassword)
    .set("spark.cassandra.input.consistency.level", astraReadConsistencyLevel))

  if ("true".equals(isCassandraToCassandra)) {
    destinationConnection = CassandraConnector(
      sc.getConf
        .set("spark.cassandra.connection.host", destinationHost)
        .set("spark.cassandra.auth.username", destintationUsername)
        .set("spark.cassandra.auth.password", destinationPassword)
        .set("spark.cassandra.input.consistency.level", destinationReadConsistencyLevel))
  }

  diffTable(sourceConnection,destinationConnection, minPartition, maxPartition)

  exitSpark

  private def diffTable(sourceConnection: CassandraConnector, astraConnection: CassandraConnector, minPartition:BigInteger, maxPartition:BigInteger) = {
    val partitions = SplitPartitions.getRandomSubPartitions(BigInteger.valueOf(Long.parseLong(splitSize)), minPartition, maxPartition)
    val parts = sc.parallelize(partitions.toSeq,partitions.size);

    logger.info("Spark parallelize created : " + parts.count() + " parts!");
    parts.foreach(part => {
      sourceConnection.withSessionDo(sourceSession => 
        astraConnection.withSessionDo(astraSession => 
          DiffJobSession.getInstance(sourceSession,astraSession, sc.getConf)
            .getDataAndDiff(part.getMin, part.getMax)))
    })

    DiffJobSession.getInstance(null, null, sc.getConf).printCounts("Job Final");
  }

  private def exitSpark = {
    spark.stop()
    sys.exit(0)
  }

}
