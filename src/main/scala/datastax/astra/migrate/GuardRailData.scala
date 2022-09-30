package datastax.astra.migrate

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.log4j.Logger

import scala.collection.JavaConversions._

// http://www.russellspitzer.com/2016/02/16/Multiple-Clusters-SparkSql-Cassandra/

object GuardRailData  extends  BaseJob {

  val logger = Logger.getLogger(this.getClass.getName)
  logger.info("Started Migration App")
  var sourceConnection = getConnection(true, sourceIsAstra, sourceScbPath, sourceHost, sourceUsername, sourcePassword, sourceReadConsistencyLevel,
    sourceTrustStorePath, sourceTrustStorePassword, sourceTrustStoreType, sourceKeyStorePath, sourceKeyStorePassword, sourceEnabledAlgorithms);
  analyzeSourceTable(sourceConnection)
  exitSpark


  private def getConnection(isSource: Boolean, isAstra: String, scbPath: String, host: String, username: String, password: String, readConsistencyLevel: String,
                            trustStorePath: String, trustStorePassword: String, trustStoreType: String,
                            keyStorePath: String, keyStorePassword: String, enabledAlgorithms: String): CassandraConnector = {
    var connType: String = "Source"

    if ("true".equals(isAstra)) {
      abstractLogger.info(connType + ": Connected to Astra!");

      return CassandraConnector(sc.getConf
        .set("spark.cassandra.auth.username", username)
        .set("spark.cassandra.auth.password", password)
        .set("spark.cassandra.input.consistency.level", readConsistencyLevel)
        .set("spark.cassandra.connection.config.cloud.path", scbPath))
    } else if (null != trustStorePath && !trustStorePath.trim.isEmpty) {
      abstractLogger.info(connType + ": Connected to Cassandra (or DSE) with SSL!");

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
      abstractLogger.info(connType + ": Connected to Cassandra (or DSE)!");

      return CassandraConnector(sc.getConf.set("spark.cassandra.auth.username", username)
        .set("spark.cassandra.auth.password", password)
        .set("spark.cassandra.input.consistency.level", readConsistencyLevel)
        .set("spark.cassandra.connection.host", host))
    }

  }

  private def analyzeSourceTable(sourceConnection: CassandraConnector) = {
    val parts = sc.parallelize(partitions.toSeq,partitions.size);
    logger.info("Spark parallelize created : " + parts.count() + " parts!");

    parts.foreach(part => {
      sourceConnection.withSessionDo(sourceSession =>
        GuardRailJobSession.getInstance(sourceSession, sc.getConf)
          .getData(part.getMin, part.getMax))
    })

  }


}



