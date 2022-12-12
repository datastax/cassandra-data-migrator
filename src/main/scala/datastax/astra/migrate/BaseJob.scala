package datastax.astra.migrate

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.math.BigInteger

class BaseJob extends App {

  val abstractLogger = LoggerFactory.getLogger(this.getClass.getName)
  val spark = SparkSession.builder
    .appName("Cassandra Data Migrator")
    .getOrCreate()
  abstractLogger.info("################################################################################################")
  abstractLogger.info("############################## Cassandra Data Migrator - Starting ##############################")
  abstractLogger.info("################################################################################################")

  val sContext = spark.sparkContext
  val sc = sContext.getConf

  val consistencyLevel = Util.getSparkPropOr(sc, "spark.read.consistency.level", "LOCAL_QUORUM")

  val sourceScbPath = Util.getSparkPropOrEmpty(sc, "spark.origin.scb")
  val sourceHost = Util.getSparkPropOrEmpty(sc, "spark.origin.host")
  val sourceUsername = Util.getSparkPropOrEmpty(sc, "spark.origin.username")
  val sourcePassword = Util.getSparkPropOrEmpty(sc, "spark.origin.password")
  val sourceTrustStorePath = Util.getSparkPropOrEmpty(sc, "spark.origin.trustStore.path")
  val sourceTrustStorePassword = Util.getSparkPropOrEmpty(sc, "spark.origin.trustStore.password")
  val sourceTrustStoreType = Util.getSparkPropOr(sc, "spark.origin.trustStore.type", "JKS")
  val sourceKeyStorePath = Util.getSparkPropOrEmpty(sc, "spark.origin.keyStore.path")
  val sourceKeyStorePassword = Util.getSparkPropOrEmpty(sc, "spark.origin.keyStore.password")
  val sourceEnabledAlgorithms = Util.getSparkPropOrEmpty(sc, "spark.origin.enabledAlgorithms")

  val destinationScbPath = Util.getSparkPropOrEmpty(sc, "spark.target.scb")
  val destinationHost = Util.getSparkPropOrEmpty(sc, "spark.target.host")
  val destinationUsername = Util.getSparkProp(sc, "spark.target.username")
  val destinationPassword = Util.getSparkProp(sc, "spark.target.password")
  val destinationTrustStorePath = Util.getSparkPropOrEmpty(sc, "spark.target.trustStore.path")
  val destinationTrustStorePassword = Util.getSparkPropOrEmpty(sc, "spark.target.trustStore.password")
  val destinationTrustStoreType = Util.getSparkPropOr(sc, "spark.target.trustStore.type", "JKS")
  val destinationKeyStorePath = Util.getSparkPropOrEmpty(sc, "spark.target.keyStore.path")
  val destinationKeyStorePassword = Util.getSparkPropOrEmpty(sc, "spark.target.keyStore.password")
  val destinationEnabledAlgorithms = Util.getSparkPropOrEmpty(sc, "spark.target.enabledAlgorithms")

  val minPartition = new BigInteger(Util.getSparkPropOr(sc, "spark.origin.minPartition", "-9223372036854775808"))
  val maxPartition = new BigInteger(Util.getSparkPropOr(sc, "spark.origin.maxPartition", "9223372036854775807"))
  val coveragePercent = Util.getSparkPropOr(sc, "spark.coveragePercent", "100")
  val splitSize = Integer.parseInt(Util.getSparkPropOr(sc, "spark.splitSize", "10000"))

  protected def exitSpark() = {
    spark.stop()
    abstractLogger.info("################################################################################################")
    abstractLogger.info("############################## Cassandra Data Migrator - Stopped ###############################")
    abstractLogger.info("################################################################################################")
    sys.exit(0)
  }

}
