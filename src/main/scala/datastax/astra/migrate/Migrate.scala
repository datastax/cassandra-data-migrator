package com.datastax.astra

import com.datastax.oss.driver.api.core.CqlIdentifier
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import datastax.astra.migrate.CassUtil
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.hive._
import org.apache.spark.sql.cassandra._

import collection.JavaConversions._

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

  val (tableAndKeyspaceNames, createTableStatements) = tablesAndStatementsFromSource

  createAstraTables

  tableAndKeyspaceNames.foreach(tableAndKeyspaceName => {
    val nameArray = tableAndKeyspaceName.split("\\.")
    val sourceKeyspaceName = nameArray(0)
    val tableName = nameArray(1)
    migrateTable(tableName, sourceKeyspaceName)
  })

  exitSpark

  private def migrateTable(tableName:String, keyspaceName:String) = {
    val df = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map(
        "keyspace" -> keyspaceName,
        "table" -> tableName,
        "spark.cassandra.connection.host" -> sourceHost,
        "spark.cassandra.auth.password" -> sourcePassword,
        "spark.cassandra.auth.username" -> sourceUsername
      ))
      .load

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

  private def tablesAndStatementsFromSource : (Iterable[String], Iterable[String])  = {
    sourceConnection.withSessionDo { session =>
      val keyspaces = session.getMetadata().getKeyspaces()
      val userKeyspaces = keyspaces.filterKeys(x => !x.toString.startsWith("system"))

      val statementStrings: Iterable[String] = userKeyspaces.map((keyspaceTuple) => {
        val keyspaceName = if (astraKeyspaceName == null) {
          keyspaceTuple._1.toString
        } else {
          astraKeyspaceName
        }

        val tables = mapAsScalaMap(keyspaceTuple._2.getTables)
        tables.map(tableTuple => CassUtil.metadataToStatement(tableTuple._2, keyspaceName))
      }).toList.flatten

      val tableAndKeyspaceNames: Iterable[String] = userKeyspaces.flatMap((keyspaceTuple) => {
        val keyspaceName = keyspaceTuple._1.toString
        val tables = mapAsScalaMap(keyspaceTuple._2.getTables)
        val result = tables.map(tableTuple => (keyspaceName + "." + tableTuple._2.getName().toString))
        result
      })

      (tableAndKeyspaceNames, statementStrings)
    }
  }

  private def createAstraTables = {
    astraConnection.withSessionDo { session =>

      createTableStatements.map(createStatement => {
        val rs = session.execute(createStatement.toString)

        if (!rs.wasApplied()){
          print("Something went wrong creating statements")
          print("rs: " +rs.all())

          exitSpark
        }
      })

    }
  }



}