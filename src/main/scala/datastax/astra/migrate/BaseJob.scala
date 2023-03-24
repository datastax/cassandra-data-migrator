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

  val consistencyLevel = Util.getSparkPropOr(sc, "spark.consistency.read", "LOCAL_QUORUM")

  val originScbPath = Util.getSparkPropOrEmpty(sc, "spark.origin.scb")
  val originHost = Util.getSparkPropOrEmpty(sc, "spark.origin.host")
  val originPort = Util.getSparkPropOr(sc, "spark.origin.port", "9042")
  val originUsername = Util.getSparkPropOrEmpty(sc, "spark.origin.username")
  val originPassword = Util.getSparkPropOrEmpty(sc, "spark.origin.password")
  val originSSLEnabled = Util.getSparkPropOr(sc, "spark.origin.ssl.enabled", "false")
  val originTrustStorePath = Util.getSparkPropOrEmpty(sc, "spark.origin.trustStore.path")
  val originTrustStorePassword = Util.getSparkPropOrEmpty(sc, "spark.origin.trustStore.password")
  val originTrustStoreType = Util.getSparkPropOr(sc, "spark.origin.trustStore.type", "JKS")
  val originKeyStorePath = Util.getSparkPropOrEmpty(sc, "spark.origin.keyStore.path")
  val originKeyStorePassword = Util.getSparkPropOrEmpty(sc, "spark.origin.keyStore.password")
  val originEnabledAlgorithms = Util.getSparkPropOrEmpty(sc, "spark.origin.enabledAlgorithms")

  val targetScbPath = Util.getSparkPropOrEmpty(sc, "spark.target.scb")
  val targetHost = Util.getSparkPropOrEmpty(sc, "spark.target.host")
  val targetPort = Util.getSparkPropOr(sc, "spark.target.port", "9042")
  val targetUsername = Util.getSparkProp(sc, "spark.target.username")
  val targetPassword = Util.getSparkProp(sc, "spark.target.password")
  val targetSSLEnabled = Util.getSparkPropOr(sc, "spark.target.ssl.enabled", "false")
  val targetTrustStorePath = Util.getSparkPropOrEmpty(sc, "spark.target.trustStore.path")
  val targetTrustStorePassword = Util.getSparkPropOrEmpty(sc, "spark.target.trustStore.password")
  val targetTrustStoreType = Util.getSparkPropOr(sc, "spark.target.trustStore.type", "JKS")
  val targetKeyStorePath = Util.getSparkPropOrEmpty(sc, "spark.target.keyStore.path")
  val targetKeyStorePassword = Util.getSparkPropOrEmpty(sc, "spark.target.keyStore.password")
  val targetEnabledAlgorithms = Util.getSparkPropOrEmpty(sc, "spark.target.enabledAlgorithms")

  val minPartition = new BigInteger(Util.getSparkPropOr(sc, "spark.origin.minPartition", "-9223372036854775808"))
  val maxPartition = new BigInteger(Util.getSparkPropOr(sc, "spark.origin.maxPartition", "9223372036854775807"))
  val coveragePercent = Util.getSparkPropOr(sc, "spark.coveragePercent", "100")
  val splitSizeBackwardCompatibility = Util.getSparkPropOr(sc, "spark.splitSize", "10000")
  val numSplits = Integer.parseInt(Util.getSparkPropOr(sc, "spark.numSplits", splitSizeBackwardCompatibility))

  protected def exitSpark() = {
    spark.stop()
    abstractLogger.info("################################################################################################")
    abstractLogger.info("############################## Cassandra Data Migrator - Stopped ###############################")
    abstractLogger.info("################################################################################################")
    sys.exit(0)
  }

}
