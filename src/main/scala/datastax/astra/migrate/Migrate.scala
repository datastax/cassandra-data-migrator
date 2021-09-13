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

  val sourceUsername = "cassandra"
  val sourcePassword = "cassandra"
  val sourceHost = "localhost"

  val astraUsername = "datastax"
  val astraPassword = "datastax"

  val scbPath = "file:///home/tato/Downloads/secure-connect-free.zip"

  val spark = SparkSession.builder
    .master("local")
    .appName("Datastax Scala example")
    //.enableHiveSupport()
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext

  val sourceKeyspaceName = sc.getConf.get("spark.migrate.source.keyspace")
  val sourceTableName = sc.getConf.get("spark.migrate.source.tableName")
  val ttlColumnName = sc.getConf.get("spark.migrate.source.ttlColumnName")
  val ttlColumnAlias = sc.getConf.get("spark.migrate.source.ttlColumnAlias")

  val minPartition = new BigInteger(sc.getConf.get("spark.migrate.source.minPartition"))
  val maxPartition = new BigInteger(sc.getConf.get("spark.migrate.source.minPartition"))


  val astraConnection = CassandraConnector(sc.getConf
    .set("spark.cassandra.connection.config.cloud.path", scbPath)
    .set("spark.cassandra.auth.username", astraUsername)
    .set("spark.cassandra.auth.password", astraPassword))

  val sourceConnection = CassandraConnector(
    sc.getConf
      .set("spark.cassandra.connection.host", sourceHost)
      .set("spark.cassandra.auth.username", sourceUsername)
      .set("spark.cassandra.auth.password", sourcePassword))


  val astraKeyspaceName = getAstraKeyspaceIfOnlyOne

  migrateTable(sourceConnection,astraConnection,sourceTableName, sourceKeyspaceName,ttlColumnName,ttlColumnAlias, minPartition, maxPartition)

  exitSpark

  private def migrateTable(sourceConnection: CassandraConnector, astraConnection: CassandraConnector,tableName:String, keyspaceName:String, ttlColumn:String, ttlColumnAlias:String, minPartition:BigInteger, maxPartition:BigInteger) = {

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


    val df = spark
      .read

      .format("org.apache.spark.sql.cassandra")
      .option("ttl."+ttlColumn, ttlColumnAlias)
      .options(Map(
        "keyspace" -> keyspaceName,
        "table" -> tableName,
        "spark.cassandra.connection.host" -> sourceHost,
        "spark.cassandra.auth.password" -> sourcePassword,
        "spark.cassandra.auth.username" -> sourceUsername
      ))
      .load

    val ds = df.filter(df("token(col1)") > "1000000")
    //filter(df(ttlColumnAlias) > 1000)

    ds.show()

    /*
    df.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map(
        "keyspace" -> astraKeyspaceName,
        "table" -> tableName,
        "spark.cassandra.connection.config.cloud.path" -> scbPath,
        "spark.cassandra.auth.password" -> astraPassword,
        "spark.cassandra.auth.username" -> astraUsername
      ))
      .save

     */
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



