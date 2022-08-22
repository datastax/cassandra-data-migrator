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
  val sourceReadConsistencyLevel = sc.getConf.get("spark.migrate.source.read.consistency.level", "LOCAL_QUORUM")
  val sourceTrustStorePath = sc.getConf.get("spark.migrate.source.trustStore.path", "")
  val sourceTrustStorePassword = sc.getConf.get("spark.migrate.source.trustStore.password", "")
  val sourceTrustStoreType = sc.getConf.get("spark.migrate.source.trustStore.type", "JKS")
  val sourceKeyStorePath = sc.getConf.get("spark.migrate.source.keyStore.path", "")
  val sourceKeyStorePassword = sc.getConf.get("spark.migrate.source.keyStore.password", "")

  val destinationIsAstra = sc.getConf.get("spark.migrate.destination.isAstra", "true")
  val destinationScbPath = sc.getConf.get("spark.migrate.destination.scb", "")
  val destinationHost = sc.getConf.get("spark.migrate.destination.host", "")
  val destinationUsername = sc.getConf.get("spark.migrate.destination.username")
  val destinationPassword = sc.getConf.get("spark.migrate.destination.password")
  val destinationReadConsistencyLevel = sc.getConf.get("spark.migrate.destination.read.consistency.level", "LOCAL_QUORUM")
  val destinationTrustStorePath = sc.getConf.get("spark.migrate.destination.trustStore.path", "")
  val destinationTrustStorePassword = sc.getConf.get("spark.migrate.destination.trustStore.password", "")
  val destinationTrustStoreType = sc.getConf.get("spark.migrate.destination.trustStore.type", "JKS")
  val destinationKeyStorePath = sc.getConf.get("spark.migrate.destination.keyStore.path", "")
  val destinationKeyStorePassword = sc.getConf.get("spark.migrate.destination.keyStore.password", "")

  val minPartition = new BigInteger(sc.getConf.get("spark.migrate.source.minPartition"))
  val maxPartition = new BigInteger(sc.getConf.get("spark.migrate.source.maxPartition"))

  val splitSize = sc.getConf.get("spark.migrate.splitSize", "10000")

  var sourceConnection = getConnection(true, sourceIsAstra, sourceScbPath, sourceHost, sourceUsername, sourcePassword, sourceReadConsistencyLevel,
    sourceTrustStorePath, sourceTrustStorePassword, sourceTrustStoreType, sourceKeyStorePath, sourceKeyStorePassword);

  var destinationConnection = getConnection(false, destinationIsAstra, destinationScbPath, destinationHost, destinationUsername, destinationPassword, destinationReadConsistencyLevel,
    destinationTrustStorePath, destinationTrustStorePassword, destinationTrustStoreType, destinationKeyStorePath, destinationKeyStorePassword);

  protected def exitSpark() = {
    spark.stop()
    sys.exit(0)
  }

  private def getConnection(isSource: Boolean, isAstra: String, scbPath: String, host: String, username: String, password: String, readConsistencyLevel: String,
                            trustStorePath: String, trustStorePassword: String, trustStoreType: String,
                            keyStorePath: String, keyStorePassword: String): CassandraConnector = {
    var connType: String = "Source"
    if (!isSource) {
      connType = "Destination"
    }

    if ("true".equals(isAstra)) {
      abstractLogger.info(connType + ": Connected to Astra!");

      return CassandraConnector(sc.getConf
        .set("spark.cassandra.auth.username", username)
        .set("spark.cassandra.auth.password", password)
        .set("spark.cassandra.input.consistency.level", readConsistencyLevel)
        .set("spark.cassandra.connection.config.cloud.path", scbPath))
    } else if (null != trustStorePath && !trustStorePath.trim.isEmpty) {
      abstractLogger.info(connType + ": Connected to Cassandra (or DSE) with SSL!");

      return CassandraConnector(sc.getConf
        .set("spark.cassandra.auth.username", username)
        .set("spark.cassandra.auth.password", password)
        .set("spark.cassandra.input.consistency.level", readConsistencyLevel)
        .set("spark.cassandra.connection.host", host)
        .set("spark.cassandra.connection.ssl.enabled", "true")
        .set("spark.cassandra.connection.ssl.trustStore.password", trustStorePassword) //"cassandra")
        .set("spark.cassandra.connection.ssl.trustStore.path", trustStorePath) //"/Users/pravin.bhat/rootca/dse-truststore.jks")
        .set("spark.cassandra.connection.ssl.keyStore.password", keyStorePassword) //"cassandra")
        .set("spark.cassandra.connection.ssl.keyStore.path", keyStorePath) //"/Users/pravin.bhat/dse/keystores/local.jks")
        .set("spark.cassandra.connection.ssl.trustStore.type", trustStoreType) // "JKS")
        .set("spark.cassandra.connection.ssl.clientAuth.enabled", "true")
      )
    } else {
      abstractLogger.info(connType + ": Connected to Cassandra (or DSE)!");

      return CassandraConnector(sc.getConf.set("spark.cassandra.auth.username", username)
        .set("spark.cassandra.auth.password", password)
        .set("spark.cassandra.input.consistency.level", readConsistencyLevel)
        .set("spark.cassandra.connection.host", host))
    }

  }

}
