package datastax.astra.migrate

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import java.math.BigInteger

class AbstractJob extends App {

  val abstractLogger = Logger.getLogger(this.getClass.getName)
  val spark = SparkSession.builder
    .appName("Datastax Data Validation")
    .getOrCreate()

  val sc = spark.sparkContext

  val sourceIsAstra = sc.getConf.get("spark.migrate.source.isAstra", "false")
  val sourceScbPath = sc.getConf.get("spark.migrate.source.scb", "")
  val sourceHost = sc.getConf.get("spark.migrate.source.host", "")
  val sourceUsername = sc.getConf.get("spark.migrate.source.username")
  val sourcePassword = sc.getConf.get("spark.migrate.source.password")
  val sourceReadConsistencyLevel = sc.getConf.get("spark.migrate.source.read.consistency.level","LOCAL_QUORUM")

  val destinationIsAstra = sc.getConf.get("spark.migrate.destination.isAstra", "true")
  val destinationScbPath =  sc.getConf.get("spark.migrate.destination.scb", "")
  val destinationHost =  sc.getConf.get("spark.migrate.destination.host", "")
  val destinationUsername = sc.getConf.get("spark.migrate.destination.username")
  val destinationPassword = sc.getConf.get("spark.migrate.destination.password")
  val destinationReadConsistencyLevel = sc.getConf.get("spark.migrate.destination.read.consistency.level", "LOCAL_QUORUM")

  val minPartition = new BigInteger(sc.getConf.get("spark.migrate.source.minPartition"))
  val maxPartition = new BigInteger(sc.getConf.get("spark.migrate.source.maxPartition"))

  val splitSize = sc.getConf.get("spark.migrate.splitSize","10000")

  var sourceConnection = CassandraConnector(sc.getConf
      .set("spark.cassandra.connection.host", sourceHost)
      .set("spark.cassandra.auth.username", sourceUsername)
      .set("spark.cassandra.auth.password", sourcePassword)
      .set("spark.cassandra.input.consistency.level", sourceReadConsistencyLevel))
  if ("true".equals(sourceIsAstra)) {
    sourceConnection = CassandraConnector(sc.getConf
        .set("spark.cassandra.connection.config.cloud.path", sourceScbPath)
        .set("spark.cassandra.auth.username", sourceUsername)
        .set("spark.cassandra.auth.password", sourcePassword)
        .set("spark.cassandra.input.consistency.level", sourceReadConsistencyLevel))
    abstractLogger.info("Connected to Astra source!");
  } else {
    abstractLogger.info("Connected to Cassandra (or DSE) source!");
  }

  var destinationConnection = CassandraConnector(sc.getConf
    .set("spark.cassandra.connection.host", destinationHost)
    .set("spark.cassandra.auth.username", destinationUsername)
    .set("spark.cassandra.auth.password", destinationPassword)
    .set("spark.cassandra.input.consistency.level", destinationReadConsistencyLevel))
  if ("true".equals(destinationIsAstra)) {
    destinationConnection = CassandraConnector(
      sc.getConf
        .set("spark.cassandra.connection.config.cloud.path", destinationScbPath)
        .set("spark.cassandra.auth.username", destinationUsername)
        .set("spark.cassandra.auth.password", destinationPassword)
        .set("spark.cassandra.input.consistency.level", destinationReadConsistencyLevel))
    abstractLogger.info("Connected to Astra destination!");
  } else {
    abstractLogger.info("Connected to Cassandra (or DSE) destination!");
  }

  protected def exitSpark = {
    spark.stop()
    sys.exit(0)
  }

}
