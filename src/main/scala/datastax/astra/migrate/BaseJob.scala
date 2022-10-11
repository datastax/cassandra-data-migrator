package datastax.astra.migrate

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.math.BigInteger
import java.lang.Long

class BaseJob extends App {

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


  val minPartition = new BigInteger(sc.getConf.get("spark.source.minPartition","-9223372036854775808"))
  val maxPartition = new BigInteger(sc.getConf.get("spark.source.maxPartition","9223372036854775807"))
  val coveragePercent = sc.getConf.get("spark.coveragePercent", "100")
  val splitSize = sc.getConf.get("spark.splitSize", "10000")
  val partitions = SplitPartitions.getRandomSubPartitions(BigInteger.valueOf(Long.parseLong(splitSize)), minPartition, maxPartition, Integer.parseInt(coveragePercent))

  protected def exitSpark() = {
    spark.stop()
    sys.exit(0)
  }

}
