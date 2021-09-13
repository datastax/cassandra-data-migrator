package datastax.astra.migrate

import com.datastax.oss.driver.api.core.CqlIdentifier
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
    .master("local")
    .appName("Datastax Scala example")
    //.enableHiveSupport()
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext

  val sourceKeyspaceName = sc.getConf.get("spark.migrate.source.keyspace")
  val sourceTableName = sc.getConf.get("spark.migrate.source.tableName")

  val minPartition = new BigInteger(sc.getConf.get("spark.migrate.source.minPartition"))
  val maxPartition = new BigInteger(sc.getConf.get("spark.migrate.source.minPartition"))


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


  val astraKeyspaceName = getAstraKeyspaceIfOnlyOne

  migrateTable(sourceConnection,astraConnection, minPartition, maxPartition)

  exitSpark

  private def migrateTable(sourceConnection: CassandraConnector, astraConnection: CassandraConnector, minPartition:BigInteger, maxPartition:BigInteger) = {

    val partitions = SplitPartitions.getSubPartitions(BigInteger.valueOf(Long.parseLong("10")), minPartition, maxPartition)


    partitions.foreach(partition => {
      val subPartitions =
        SplitPartitions.getSubPartitions(BigInteger.valueOf(Long.parseLong("1000")), BigInteger.valueOf(partition.getMin()), BigInteger.valueOf(partition.getMax()));
      val parts = sc.parallelize(subPartitions.toSeq,subPartitions.size);

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

  private def getAstraKeyspaceIfOnlyOne = {
    astraConnection.withSessionDo { session =>
      val keyspaces = session.getMetadata().getKeyspaces()
      val userKeyspaces = keyspaces.filterKeys(x => !x.toString.startsWith("system"))

      val keyspaceNames = userKeyspaces.map(keyspaceTuple => keyspaceTuple._2.getName())

      if (keyspaceNames.size > 1) {
        null
      } else {
        keyspaceNames.head.toString
      }

    }
  }




}



//val df = spark.read.format("org.apache.spark.sql.cassandra").options(Map("keyspace" -> "test","table" -> "tbl1")).load
// val ds = df.filter(df(token("key1,key2") > "1000000"))


//.option("ttl."+ttlColumn, ttlColumnAlias)



