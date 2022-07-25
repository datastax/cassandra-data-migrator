package datastax.astra.migrate

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.SparkSession

import java.lang.Long
import java.math.BigInteger
import scala.collection.JavaConversions._

object DiffMetaData extends App {

  val spark = SparkSession.builder
    .appName("Datastax Data Migration")
    .getOrCreate()

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

  println("Started Difference App")


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




  diffTable(sourceConnection,astraConnection, minPartition, maxPartition)

  exitSpark

  private def diffTable(sourceConnection: CassandraConnector, astraConnection: CassandraConnector, minPartition:BigInteger, maxPartition:BigInteger) = {

    val partitions = SplitPartitions.getRandomSubPartitions(BigInteger.valueOf(Long.parseLong(splitSize)), minPartition, maxPartition)
    val parts = sc.parallelize(partitions.toSeq,partitions.size);
    parts.foreach(part => {
      sourceConnection.withSessionDo(sourceSession => astraConnection.withSessionDo(astraSession=>DiffMetaJobSession.getInstance(sourceSession,astraSession, sc.getConf).getDataDiffAndCorrect(part.getMin, part.getMax)))
    })

    println(parts.collect.tail)


  }

  private def exitSpark = {
    spark.stop()
    sys.exit(0)
  }

}
