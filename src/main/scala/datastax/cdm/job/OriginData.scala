package datastax.cdm.job

/**
 * see deprecation note in OriginCountJobSession.java
 */
@Deprecated
object OriginData extends BaseJob {

//  val logger = LoggerFactory.getLogger(this.getClass.getName)
//  logger.info("Started Migration App")
//  var originConnection = getConnection(true, originScbPath, originHost, originUsername, originPassword,
//    originTrustStorePath, originTrustStorePassword, originTrustStoreType, originKeyStorePath, originKeyStorePassword, originEnabledAlgorithms);
//  analyzeSourceTable(originConnection)
//  exitSpark
//
//
//  private def getConnection(isSource: Boolean, scbPath: String, host: String, username: String, password: String,
//                            trustStorePath: String, trustStorePassword: String, trustStoreType: String,
//                            keyStorePath: String, keyStorePassword: String, enabledAlgorithms: String): CassandraConnector = {
//    var connType: String = "Source"
//
//    if (scbPath.nonEmpty) {
//      abstractLogger.info(connType + ": Connected to Astra!");
//
//      return CassandraConnector(sc
//        .set("spark.cassandra.auth.username", username)
//        .set("spark.cassandra.auth.password", password)
//        .set("spark.cassandra.input.consistency.level", consistencyLevel)
//        .set("spark.cassandra.connection.config.cloud.path", scbPath))
//    } else if (trustStorePath.nonEmpty) {
//      abstractLogger.info(connType + ": Connected to Cassandra (or DSE) with SSL!");
//
//      // Use defaults when not provided
//      var enabledAlgorithmsVar = enabledAlgorithms
//      if (enabledAlgorithms == null || enabledAlgorithms.trim.isEmpty) {
//        enabledAlgorithmsVar = "TLS_RSA_WITH_AES_128_CBC_SHA, TLS_RSA_WITH_AES_256_CBC_SHA"
//      }
//
//      return CassandraConnector(sc
//        .set("spark.cassandra.auth.username", username)
//        .set("spark.cassandra.auth.password", password)
//        .set("spark.cassandra.input.consistency.level", consistencyLevel)
//        .set("spark.cassandra.connection.host", host)
//        .set("spark.cassandra.connection.ssl.enabled", "true")
//        .set("spark.cassandra.connection.ssl.enabledAlgorithms", enabledAlgorithmsVar)
//        .set("spark.cassandra.connection.ssl.trustStore.password", trustStorePassword)
//        .set("spark.cassandra.connection.ssl.trustStore.path", trustStorePath)
//        .set("spark.cassandra.connection.ssl.keyStore.password", keyStorePassword)
//        .set("spark.cassandra.connection.ssl.keyStore.path", keyStorePath)
//        .set("spark.cassandra.connection.ssl.trustStore.type", trustStoreType)
//        .set("spark.cassandra.connection.ssl.clientAuth.enabled", "true")
//      )
//    } else {
//      abstractLogger.info(connType + ": Connected to Cassandra (or DSE)!");
//
//      return CassandraConnector(sc.set("spark.cassandra.auth.username", username)
//        .set("spark.cassandra.auth.password", password)
//        .set("spark.cassandra.input.consistency.level", consistencyLevel)
//        .set("spark.cassandra.connection.host", host))
//    }
//
//  }
//
//  private def analyzeSourceTable(originConnection: CassandraConnector) = {
//    val partitions = SplitPartitions.getRandomSubPartitions(numSplits, minPartition, maxPartition, Integer.parseInt(coveragePercent))
//    logger.info("PARAM Calculated -- Total Partitions: " + partitions.size())
//    val parts = sContext.parallelize(partitions.toSeq, partitions.size);
//    logger.info("Spark parallelize created : " + parts.count() + " parts!");
//
//    parts.foreach(part => {
//      originConnection.withSessionDo(sourceSession =>
//        OriginCountJobSession.getInstance(sourceSession, sc)
//          .getData(part.getMin, part.getMax))
//    })
//
//  }
}

