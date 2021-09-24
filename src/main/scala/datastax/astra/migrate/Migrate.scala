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

  val sourceUsername = "awSImgpvKitycEFFzRfhWDvC"
  val sourcePassword = "a0zDnbivYdiGvqjKwO4ZcN6d0U4j15kfnXz.tLt6JyqMwgrcG6+pEkBEXdbnJWGlk-TfEGzNyLtlbFQ3yZ-D8ZjlQOUrc+6ASDw6_x3PSiaMmg17Aw8ouZKsRADdrDBq"
  val sourceHost = "localhost"

  val astraUsername = "awSImgpvKitycEFFzRfhWDvC"
  val astraPassword = "a0zDnbivYdiGvqjKwO4ZcN6d0U4j15kfnXz.tLt6JyqMwgrcG6+pEkBEXdbnJWGlk-TfEGzNyLtlbFQ3yZ-D8ZjlQOUrc+6ASDw6_x3PSiaMmg17Aw8ouZKsRADdrDBq"

  val srcScbPath = "file:///Users/ankitpatel/Documents/Clients/Astra/clusters/secure-connect-enterprise-azure.zip"
  val astraScbPath = "file:///Users/ankitpatel/Documents/Clients/Astra/clusters/secure-connect-enterprise.zip"


  println("Started Migration App")

  val spark = SparkSession.builder
    .appName("Datastax Scala example")
    //.enableHiveSupport()
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext

val minPartition = new BigInteger(sc.getConf.get("spark.migrate.source.minPartition"))
val maxPartition = new BigInteger(sc.getConf.get("spark.migrate.source.maxPartition"))

//  val minPartition = SplitPartitions.MIN_PARTITION
//  val maxPartition = SplitPartitions.MAX_PARTITION


  val astraConnection = CassandraConnector(sc.getConf
    .set("spark.cassandra.connection.config.cloud.path", srcScbPath)
    .set("spark.cassandra.auth.username", astraUsername)
    .set("spark.cassandra.auth.password", astraPassword))

  val sourceConnection = CassandraConnector(
    sc.getConf
      .set("spark.cassandra.connection.config.cloud.path", astraScbPath)
//      .set("spark.cassandra.connection.host", sourceHost)
      .set("spark.cassandra.auth.username", sourceUsername)
      .set("spark.cassandra.auth.password", sourcePassword))


  migrateTable(sourceConnection,astraConnection, minPartition, maxPartition)

  exitSpark

  private def migrateTable(sourceConnection: CassandraConnector, astraConnection: CassandraConnector, minPartition:BigInteger, maxPartition:BigInteger) = {

    val partitions = SplitPartitions.getSubPartitions(BigInteger.valueOf(Long.parseLong("100")), minPartition, maxPartition)
    val parts = sc.parallelize(partitions.toSeq,partitions.size);
    parts.foreach(part => {
      sourceConnection.withSessionDo(sourceSession => astraConnection.withSessionDo(astraSession=>   CopyJobSession.getInstance(sourceSession,astraSession).getDataAndInsert(part.getMin, part.getMax)))
    })

    println(parts.collect.tail)

    partitions.foreach(partition => {
      val subPartitions =
        SplitPartitions.getSubPartitions(BigInteger.valueOf(Long.parseLong("1000")), BigInteger.valueOf(partition.getMin()), BigInteger.valueOf(partition.getMax()));


        parts.foreach(part => {
          sourceConnection.withSessionDo(sourceSession => astraConnection.withSessionDo(astraSession=>   CopyJobSession.getInstance(sourceSession,astraSession).getDataAndInsert(part.getMin, part.getMax)))
      })

      println(parts.collect.tail)

    })





  }


  private def exitSpark = {
    spark.stop()
    sys.exit(0)
  }





}



//val df = spark.read.format("org.apache.spark.sql.cassandra").options(Map("keyspace" -> "test","table" -> "tbl1")).load
// val ds = df.filter(df(token("key1,key2") > "1000000"))


//.option("ttl."+ttlColumn, ttlColumnAlias)



