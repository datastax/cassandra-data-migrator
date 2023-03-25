package datastax.astra.migrate

import datastax.astra.migrate.properties.KnownProperties
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

  val consistencyLevel = Util.getSparkPropOr(sc, KnownProperties.READ_CL, "LOCAL_QUORUM")

  val originScbPath = Util.getSparkPropOrEmpty(sc, KnownProperties.ORIGIN_CONNECT_SCB)
  val originHost = Util.getSparkPropOrEmpty(sc, KnownProperties.ORIGIN_CONNECT_HOST)
  val originPort = Util.getSparkPropOr(sc, KnownProperties.ORIGIN_CONNECT_PORT, "9042")
  val originUsername = Util.getSparkPropOrEmpty(sc, KnownProperties.ORIGIN_CONNECT_USERNAME)
  val originPassword = Util.getSparkPropOrEmpty(sc, KnownProperties.ORIGIN_CONNECT_PASSWORD)
  val originSSLEnabled = Util.getSparkPropOr(sc, KnownProperties.ORIGIN_TLS_ENABLED, "false")
  val originTrustStorePath = Util.getSparkPropOrEmpty(sc, KnownProperties.ORIGIN_TLS_TRUSTSTORE_PATH)
  val originTrustStorePassword = Util.getSparkPropOrEmpty(sc, KnownProperties.ORIGIN_TLS_TRUSTSTORE_PASSWORD)
  val originTrustStoreType = Util.getSparkPropOr(sc, KnownProperties.ORIGIN_TLS_TRUSTSTORE_TYPE, "JKS")
  val originKeyStorePath = Util.getSparkPropOrEmpty(sc, KnownProperties.ORIGIN_TLS_KEYSTORE_PATH)
  val originKeyStorePassword = Util.getSparkPropOrEmpty(sc, KnownProperties.ORIGIN_TLS_KEYSTORE_PASSWORD)
  val originEnabledAlgorithms = Util.getSparkPropOrEmpty(sc, KnownProperties.ORIGIN_TLS_ALGORITHMS)

  val targetScbPath = Util.getSparkPropOrEmpty(sc, KnownProperties.TARGET_CONNECT_SCB)
  val targetHost = Util.getSparkPropOrEmpty(sc, KnownProperties.TARGET_CONNECT_HOST)
  val targetPort = Util.getSparkPropOr(sc, KnownProperties.TARGET_CONNECT_PORT, "9042")
  val targetUsername = Util.getSparkProp(sc, KnownProperties.TARGET_CONNECT_USERNAME)
  val targetPassword = Util.getSparkProp(sc, KnownProperties.TARGET_CONNECT_PASSWORD)
  val targetSSLEnabled = Util.getSparkPropOr(sc, KnownProperties.TARGET_TLS_ENABLED, "false")
  val targetTrustStorePath = Util.getSparkPropOrEmpty(sc, KnownProperties.TARGET_TLS_TRUSTSTORE_PATH)
  val targetTrustStorePassword = Util.getSparkPropOrEmpty(sc, KnownProperties.TARGET_TLS_TRUSTSTORE_PASSWORD)
  val targetTrustStoreType = Util.getSparkPropOr(sc, KnownProperties.TARGET_TLS_TRUSTSTORE_TYPE, "JKS")
  val targetKeyStorePath = Util.getSparkPropOrEmpty(sc, KnownProperties.TARGET_TLS_KEYSTORE_PATH)
  val targetKeyStorePassword = Util.getSparkPropOrEmpty(sc, KnownProperties.TARGET_TLS_KEYSTORE_PASSWORD)
  val targetEnabledAlgorithms = Util.getSparkPropOrEmpty(sc, KnownProperties.TARGET_TLS_ALGORITHMS)

  val minPartition = new BigInteger(Util.getSparkPropOr(sc, KnownProperties.PARTITION_MIN, "-9223372036854775808"))
  val maxPartition = new BigInteger(Util.getSparkPropOr(sc, KnownProperties.PARTITION_MAX, "9223372036854775807"))
  val coveragePercent = Util.getSparkPropOr(sc, KnownProperties.ORIGIN_COVERAGE_PERCENT, "100")
  val splitSizeBackwardCompatibility = Util.getSparkPropOr(sc, KnownProperties.DEPRECATED_SPARK_SPLIT_SIZE, "10000")
  val numSplits = Integer.parseInt(Util.getSparkPropOr(sc, KnownProperties.SPARK_NUM_SPLITS, splitSizeBackwardCompatibility))

  protected def exitSpark() = {
    spark.stop()
    abstractLogger.info("################################################################################################")
    abstractLogger.info("############################## Cassandra Data Migrator - Stopped ###############################")
    abstractLogger.info("################################################################################################")
    sys.exit(0)
  }

}
