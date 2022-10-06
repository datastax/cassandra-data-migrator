package datastax.astra.migrate

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.math.BigInteger
import java.lang.Long

class AbstractJob extends App {

  val abstractLogger = LoggerFactory.getLogger(this.getClass.getName)
  val spark = SparkSession.builder
    .appName("Datastax Data Validation")
    .getOrCreate()

  val sc = spark.sparkContext

  val sourceIsAstra = sc.getConf.get("spark.source.isAstra", "false")
  val sourceScbPath = sc.getConf.get("spark.source.scb", "")
  val sourceHost = sc.getConf.get("spark.source.host", "")
  val sourceUsername = sc.getConf.get("spark.source.username", "")
  val sourcePassword = sc.getConf.get("spark.source.password", "")
  val sourceReadConsistencyLevel = sc.getConf.get("spark.source.read.consistency.level", "LOCAL_QUORUM")
  val sourceTrustStorePath = sc.getConf.get("spark.source.trustStore.path", "")
  val sourceTrustStorePassword = sc.getConf.get("spark.source.trustStore.password", "")
  val sourceTrustStoreType = sc.getConf.get("spark.source.trustStore.type", "JKS")
  val sourceKeyStorePath = sc.getConf.get("spark.source.keyStore.path", "")
  val sourceKeyStorePassword = sc.getConf.get("spark.source.keyStore.password", "")
  val sourceEnabledAlgorithms = sc.getConf.get("spark.source.enabledAlgorithms", "")

  val destinationIsAstra = sc.getConf.get("spark.destination.isAstra", "true")
  val destinationScbPath = sc.getConf.get("spark.destination.scb", "")
  val destinationHost = sc.getConf.get("spark.destination.host", "")
  val destinationUsername = sc.getConf.get("spark.destination.username")
  val destinationPassword = sc.getConf.get("spark.destination.password")
  val destinationReadConsistencyLevel = sc.getConf.get("spark.destination.read.consistency.level", "LOCAL_QUORUM")
  val destinationTrustStorePath = sc.getConf.get("spark.destination.trustStore.path", "")
  val destinationTrustStorePassword = sc.getConf.get("spark.destination.trustStore.password", "")
  val destinationTrustStoreType = sc.getConf.get("spark.destination.trustStore.type", "JKS")
  val destinationKeyStorePath = sc.getConf.get("spark.destination.keyStore.path", "")
  val destinationKeyStorePassword = sc.getConf.get("spark.destination.keyStore.password", "")
  val destinationEnabledAlgorithms = sc.getConf.get("spark.destination.enabledAlgorithms", "")

  val minPartition = new BigInteger(sc.getConf.get("spark.source.minPartition","-9223372036854775808"))
  val maxPartition = new BigInteger(sc.getConf.get("spark.source.maxPartition","9223372036854775807"))
  val coveragePercent = sc.getConf.get("spark.coveragePercent", "100")
  val splitSize = sc.getConf.get("spark.splitSize", "10000")
  val partitions = SplitPartitions.getRandomSubPartitions(BigInteger.valueOf(Long.parseLong(splitSize)), minPartition, maxPartition,Integer.parseInt(coveragePercent))

  abstractLogger.info("PARAM -- Min Partition: " + minPartition)
  abstractLogger.info("PARAM -- Max Partition: " + maxPartition)
  abstractLogger.info("PARAM -- Split Size: " + coveragePercent)
  abstractLogger.info("PARAM -- Coverage Percent: " + coveragePercent)
  abstractLogger.info("PARAM Calculated -- Total Partitions: " + partitions.size())

  var sourceConnection = getConnection(true, sourceIsAstra, sourceScbPath, sourceHost, sourceUsername, sourcePassword, sourceReadConsistencyLevel,
    sourceTrustStorePath, sourceTrustStorePassword, sourceTrustStoreType, sourceKeyStorePath, sourceKeyStorePassword, sourceEnabledAlgorithms);

  var destinationConnection = getConnection(false, destinationIsAstra, destinationScbPath, destinationHost, destinationUsername, destinationPassword, destinationReadConsistencyLevel,
    destinationTrustStorePath, destinationTrustStorePassword, destinationTrustStoreType, destinationKeyStorePath, destinationKeyStorePassword, destinationEnabledAlgorithms);

  protected def exitSpark() = {
    spark.stop()
    sys.exit(0)
  }

  private def getConnection(isSource: Boolean, isAstra: String, scbPath: String, host: String, username: String, password: String, readConsistencyLevel: String,
                            trustStorePath: String, trustStorePassword: String, trustStoreType: String,
                            keyStorePath: String, keyStorePassword: String, enabledAlgorithms: String): CassandraConnector = {
    var connType: String = "Source"
    if (!isSource) {
      connType = "Destination"
    }

    if ("true".equals(isAstra)) {
      abstractLogger.info(connType + ": Connected to Astra using SCB: " + scbPath);

      return CassandraConnector(sc.getConf
        .set("spark.cassandra.auth.username", username)
        .set("spark.cassandra.auth.password", password)
        .set("spark.cassandra.input.consistency.level", readConsistencyLevel)
        .set("spark.cassandra.connection.config.cloud.path", scbPath))
    } else if (null != trustStorePath && !trustStorePath.trim.isEmpty) {
      abstractLogger.info(connType + ": Connected to Cassandra (or DSE) with SSL host: " + host);

      // Use defaults when not provided
      var enabledAlgorithmsVar = enabledAlgorithms
      if (enabledAlgorithms == null || enabledAlgorithms.trim.isEmpty) {
        enabledAlgorithmsVar = "TLS_RSA_WITH_AES_128_CBC_SHA, TLS_RSA_WITH_AES_256_CBC_SHA"
      }

      return CassandraConnector(sc.getConf
        .set("spark.cassandra.auth.username", username)
        .set("spark.cassandra.auth.password", password)
        .set("spark.cassandra.input.consistency.level", readConsistencyLevel)
        .set("spark.cassandra.connection.host", host)
        .set("spark.cassandra.connection.ssl.enabled", "true")
        .set("spark.cassandra.connection.ssl.enabledAlgorithms", enabledAlgorithmsVar)
        .set("spark.cassandra.connection.ssl.trustStore.password", trustStorePassword)
        .set("spark.cassandra.connection.ssl.trustStore.path", trustStorePath)
        .set("spark.cassandra.connection.ssl.keyStore.password", keyStorePassword)
        .set("spark.cassandra.connection.ssl.keyStore.path", keyStorePath)
        .set("spark.cassandra.connection.ssl.trustStore.type", trustStoreType)
        .set("spark.cassandra.connection.ssl.clientAuth.enabled", "true")
      )
    } else {
      abstractLogger.info(connType + ": Connected to Cassandra (or DSE) host: " + host);

      return CassandraConnector(sc.getConf.set("spark.cassandra.auth.username", username)
        .set("spark.cassandra.auth.password", password)
        .set("spark.cassandra.input.consistency.level", readConsistencyLevel)
        .set("spark.cassandra.connection.host", host))
    }

  }

}
