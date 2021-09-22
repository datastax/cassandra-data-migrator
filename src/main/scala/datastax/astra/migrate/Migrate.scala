package datastax.astra.migrate

import com.datastax.oss.driver.api.core.{CqlIdentifier, CqlSession}
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import datastax.astra.migrate.{CassUtil, CopyJobSession, SplitPartitions}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.hive._
import org.apache.spark.sql.cassandra._

import scala.collection.JavaConversions._
import java.lang.Long
import java.math.BigInteger
import collection.JavaConversions._


// http://www.russellspitzer.com/2016/02/16/Multiple-Clusters-SparkSql-Cassandra/

object Migrate extends App {

  val sourceUsername = ""
  val sourcePassword = ""

  val destUsername = ""
  val destPassword = ""

  val srcScbPath = "file:///home/cj/secure-connect-xrpl-reporting-full-history.zip"
  val destScbPath = "file:///home/cj/secure-connect-xrpl-reporting-full-history-eu.zip"


  println("Started Migration App")

  val spark = SparkSession.builder
    .appName("Datastax Scala example")
    //.enableHiveSupport()
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext

  val minPartition = new BigInteger(sc.getConf.get("spark.migrate.source.minPartition"))
  val maxPartition = new BigInteger(sc.getConf.get("spark.migrate.source.maxPartition"))


  val destConnection = CassandraConnector(sc.getConf
    .set("spark.cassandra.connection.config.cloud.path", destScbPath)
    .set("spark.cassandra.auth.username", destUsername)
    .set("spark.cassandra.auth.password", destPassword))

  val sourceConnection = CassandraConnector(
    sc.getConf
      .set("spark.cassandra.connection.config.cloud.path", srcScbPath)
      .set("spark.cassandra.auth.username", sourceUsername)
      .set("spark.cassandra.auth.password", sourcePassword))



  migrateTable(sourceConnection,destConnection, minPartition, maxPartition)
  println("Called migrateTable")

  exitSpark

  private def migrateTable(sourceConnection: CassandraConnector, destConnection: CassandraConnector, minPartition:BigInteger, maxPartition:BigInteger) = {
    println(minPartition)
    println(maxPartition)

    val partitions = SplitPartitions.getSubPartitions(BigInteger.valueOf(Long.parseLong("100000")), minPartition, maxPartition)


    val parts = sc.parallelize(partitions.toSeq,partitions.size);

    parts.foreach(part => {
        sourceConnection.withSessionDo(sourceSession => destConnection.withSessionDo(destSession =>   CopyJobSession.getInstance(sourceSession,destSession).getDataAndInsert(part.getMin, part.getMax)))


    })

    println(parts.collect.tail)






  }


  private def exitSpark = {
    spark.stop()
    sys.exit(0)
  }





}



//val df = spark.read.format("org.apache.spark.sql.cassandra").options(Map("keyspace" -> "test","table" -> "tbl1")).load
// val ds = df.filter(df(token("key1,key2") > "1000000"))


//.option("ttl."+ttlColumn, ttlColumnAlias)



