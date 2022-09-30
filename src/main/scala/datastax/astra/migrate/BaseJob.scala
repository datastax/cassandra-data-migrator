package datastax.astra.migrate

import java.math.BigInteger
import java.lang.Long
import org.slf4j.LoggerFactory
import org.apache.spark.sql.SparkSession

class BaseJob extends App {
  val abstractLogger = LoggerFactory.getLogger(this.getClass.getName)
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
  val sourceEnabledAlgorithms = sc.getConf.get("spark.migrate.source.enabledAlgorithms", "")

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
  val destinationEnabledAlgorithms = sc.getConf.get("spark.migrate.destination.enabledAlgorithms", "")

  val minPartition = new BigInteger(sc.getConf.get("spark.migrate.source.minPartition"))
  val maxPartition = new BigInteger(sc.getConf.get("spark.migrate.source.maxPartition"))
  val coveragePercent = sc.getConf.get("spark.migrate.coveragePercent", "100")
  val splitSize = sc.getConf.get("spark.migrate.splitSize", "10000")
  val partitions = SplitPartitions.getRandomSubPartitions(BigInteger.valueOf(Long.parseLong(splitSize)), minPartition, maxPartition,Integer.parseInt(coveragePercent))


  protected def exitSpark() = {
    spark.stop()
    sys.exit(0)
  }

}
