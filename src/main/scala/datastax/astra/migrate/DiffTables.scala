package datastax.astra.migrate

import au.com.bytecode.opencsv.CSVWriter
import com.datastax.oss.driver.api.core.CqlIdentifier
import com.datastax.oss.driver.api.core.`type`.codec.TypeCodec
import com.datastax.oss.driver.api.core.`type`.{DataTypes, ListType, MapType, SetType}
import com.datastax.oss.driver.api.core.metadata.schema.{ColumnMetadata, TableMetadata}
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.{CassandraConnector, CassandraConnectorConf}
import com.datastax.spark.connector.types.UserDefinedType
import org.apache.spark.sql.SparkSession
import org.json4s.JsonAST.JString
import org.json4s.jackson.Serialization
import org.json4s.{CustomSerializer, NoTypeHints}

import java.io.{BufferedWriter, FileWriter, OutputStreamWriter}
import java.math.BigInteger
import java.nio.file.{Files, Path, Paths}
import java.time.Instant
import java.util.function.Supplier
import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.sys.exit

/**
 * Diff Tables
 * usage: datastax.astra.migrate.DiffTables (-w [timestamp]) keyspace
 *
 * -w: WriteTime timestamp filter. Only process the records whose
 * largest write timestamp is greater than the value given
 */
object DiffTables extends App {

  @tailrec
  def nextArg(map: Map[String, String], list: List[String]): Map[String, String] = {
    list match {
      case Nil => map
      case "-w" :: value :: tail =>
        nextArg(map ++ Map("writeTimeFilter" -> value), tail)
      case "-t" :: value :: tail =>
        nextArg(map ++ Map("tables" -> value), tail)
      case "-o" :: value :: tail =>
        nextArg(map ++ Map("output" -> value), tail)
      case string :: Nil =>
        nextArg(map ++ Map("keyspace" -> string), list.tail)
      case unknown :: _ =>
        println("Unknown option " + unknown)
        exit(1)
    }
  }

  val options = nextArg(Map(), args.toList)

  if (!options.containsKey("keyspace")) {
    println(
      s"""
         |usage: spark-submit --class datastax.astra.migrate.DiffTables migrate-0.10.jar (-w <timestamp>) keyspace
         |
         |-w: WriteTime timestamp filter. Only process the records whose
         |    largest write timestamp is greater than the value given
         |-t: Comma separated list of tables to be included for data validation.
         |    If not give, include all tables.
         |-o: Output directory to write results. If not specified, "results/" in the current directory will be created and used.
         |""".stripMargin)
    exit(1)
  }

  val keyspace = options("keyspace")
  val includeTables = options.get("tables").map(_.split(",").map(_.trim))
  val writeTimeFilter = options.get("writeTimeFilter").map(_.toLong).orElse(Some(0L)).get
  if (writeTimeFilter > 0) {
    println(s"""Filtering with writetime greater than $writeTimeFilter""")
  }

  // Create output directory with the current timestamp
  val outputDir = options.get("output").orElse(Some("results")).get

  val spark = SparkSession.builder
    .appName("Datastax Data Validation")
    .getOrCreate()

  val sc = spark.sparkContext
  val sourceConf = sc.getConf.getAllWithPrefix("spark.migrate.source.").map({
    case (key, value) => ("spark.cassandra." + key, value)
  }).toMap

  val targetConf = sc.getConf.getAllWithPrefix("spark.migrate.target.").map({
    case (key, value) => ("spark.cassandra." + key, value)
  }).toMap

  val originConnector = CassandraConnector(CassandraConnectorConf.fromConnectionParams(sourceConf))

  val tables: Map[CqlIdentifier, TableMetadata] = {
    val allTables = originConnector.withSessionDo(session => {
      val ks = session.getMetadata.getKeyspace(keyspace).orElseThrow(new Supplier[Throwable] {
        override def get(): Throwable = new NoSuchElementException("Keyspace " + keyspace + " not found")
      })
      ks.getTables
    })
    includeTables match {
      case Some(tableNames) => allTables.filter(entry => tableNames.contains(entry._1.asInternal())).toMap
      case None => allTables.toMap
    }
  }

  val tasks = tables.values.toList.map(new DiffTask(spark, _, outputDir, originConnector, targetConf, writeTimeFilter))

  tasks.foreach(_.run())
}

/**
 * Column-level difference
 */
case class ColumnDifference(name: String, source: Any, target: Any)

/**
 * Result of the comparison between origin and target record
 */
sealed trait Result

/**
 * Origin and target record matched
 */
case object Match extends Result

/**
 * Target record is missing
 *
 * @param primaryKey Primary key values of the missing record
 */
case class Missing(primaryKey: Seq[Any]) extends Result

/**
 * Mismatch between origin and target records
 *
 * @param primaryKey Primary key values of the mismatched record
 */
case class Mismatch(primaryKey: Seq[Any], columns: Seq[ColumnDifference]) extends Result

/**
 * Summary of the comparison
 *
 * @param processedCount number of processed records
 * @param matchCount     number of matched records
 * @param missingCount   number of missing records in target
 * @param mismatchCount  number of mismatched records
 * @param missing        Missing information
 * @param mismatched     Mismatch information
 */
case class Summary(processedCount: Long,
                   matchCount: Long,
                   missingCount: Long,
                   mismatchCount: Long,
                   missing: Seq[Missing],
                   mismatched: Seq[Mismatch]) extends Result

class BigIntegerSerializer extends CustomSerializer[BigInteger](format => ( {
  case JString(s) => new BigInteger(s)
}, {
  case i: BigInteger => JString(i.toString)
}))

class DiffTask(spark: SparkSession,
               tableMetadata: TableMetadata,
               outputDirPath: String,
               source: CassandraConnector,
               targetConf: Map[String, String],
               writeTimeFilter: Long) extends Serializable {

  val outputFilePrefix = Seq(tableMetadata.getKeyspace, tableMetadata.getName, Instant.now().toEpochMilli).mkString("-")

  val targetSelectStatement: String = createTargetSelectStatement()

  val primaryKeyColumns: Seq[String] = tableMetadata.getPrimaryKey.map(pk => pk.getName.asInternal())

  def run(): Unit = {
    // Load from source
    val rdd = {
      implicit val c: CassandraConnector = source
      spark.sparkContext.cassandraTable(tableMetadata.getKeyspace.toString, tableMetadata.getName.toString)
        .select(columnsToSelect(writeTime = true, ttl = true): _*)
    }

    val filtered = if (writeTimeFilter > 0) {
      rdd.filter(row => {
        val writeTimeCols = row.metaData.columnNames.filter(_.toLowerCase.startsWith("writetime("))
        writeTimeCols.map(row.getLong).max > writeTimeFilter
      })
    } else {
      rdd
    }

    val summary = filtered.mapPartitions(partition => {
      val targetConnector = CassandraConnector(CassandraConnectorConf.fromConnectionParams(targetConf))
      val prepared = targetConnector.withSessionDo(session => {
        session.prepare(targetSelectStatement)
      })

      partition.map(row => {
        targetConnector.withSessionDo(session => {
          val builder = prepared.boundStatementBuilder()
          // Bind primary keys
          var primaryKeys = Seq[AnyRef]()
          tableMetadata.getPrimaryKey.foreach(pk => {
            val dataType = tableMetadata.getColumns.get(pk.getName).getType
            val data = row.get[AnyRef](pk.getName.asInternal())
            primaryKeys = primaryKeys :+ data
            val codec: TypeCodec[AnyRef] = builder.codecRegistry().codecFor(dataType)
            builder.set(pk.getName.asInternal(), data, codec)
          })
          val result = session.execute(builder.build())
          if (result.isEmpty) {
            Missing(primaryKeys)
          } else {
            val columns = result.getColumnDefinitions.map(c => c.getName.asInternal()).toIndexedSeq
            val targetRow = CassandraRow.fromJavaDriverRow(result.one(),
              CassandraRowMetadata.fromResultSet(columns, result, session))
            compareRow(primaryKeys, row, targetRow)
          }
        })
      })
    }).fold(Summary(0, 0, 0, 0, Seq(), Seq())) {
      case (s: Summary, Match) => Summary(s.processedCount + 1,
        s.matchCount + 1, s.missingCount,
        s.mismatchCount, s.missing, s.mismatched)
      case (s: Summary, m: Missing) => Summary(s.processedCount + 1,
        s.matchCount, s.missingCount + 1,
        s.mismatchCount, s.missing :+ m, s.mismatched)
      case (s: Summary, mismatch: Mismatch) => Summary(s.processedCount + 1,
        s.matchCount, s.missingCount,
        s.mismatchCount + 1, s.missing, s.mismatched :+ mismatch)
      case (s: Summary, r: Summary) => Summary(s.processedCount + r.processedCount,
        s.matchCount + r.matchCount, s.missingCount + r.missingCount,
        s.mismatchCount + r.mismatchCount, s.missing ++ r.missing, s.mismatched ++ r.mismatched)
    }

    // Final result
    val outputDir = Paths.get(outputDirPath)
    if (!Files.exists(outputDir)) {
      Files.createDirectories(outputDir)
    }
    outputResult(summary)
  }

  def outputResult[R <: Result](result: R): Unit = {
    result match {
      case Summary(processedCount, matchCount, missingCount, mismatchCount, missing, mismatched) =>
        val writer = Files.newBufferedWriter(Paths.get(outputDirPath, outputFilePrefix + "-summary.log"))
        try {
          writer.append(s"""Processed records: $processedCount""")
          writer.newLine()
          writer.write(s"""  Matched records: $matchCount""")
          writer.newLine()
          writer.write(s"""  Missing records: $missingCount""")
          writer.newLine()
          writer.write(s""" Mismatch records: $mismatchCount""")
          writer.newLine()
        } finally {
          writer.close()
        }
        outMissing(missing)
        outMismatch(mismatched)
    }
  }

  def outMissing(missing: Seq[Missing]): Unit = {
    if (missing.isEmpty) return

    val writer = new CSVWriter(Files.newBufferedWriter(Paths.get(outputDirPath, outputFilePrefix + "-missing.csv")))
    try {
      writer.writeNext(primaryKeyColumns.toArray)
      missing.foreach {
        case Missing(primaryKey) => writer.writeNext(primaryKey.map(_.toString).toArray)
      }
    } finally {
      writer.close()
    }
  }

  def outMismatch(mismatches: Seq[Mismatch]): Unit = {
    if (mismatches.isEmpty) return

    implicit val fmt = Serialization.formats(NoTypeHints) + new BigIntegerSerializer//org.json4s.DefaultFormats + new BigIntegerSerializer

    val writer = new CSVWriter(Files.newBufferedWriter(Paths.get(outputDirPath, outputFilePrefix + "-mismatch.csv")))
    try {
      writer.writeNext((primaryKeyColumns :+ "diff").toArray)
      mismatches.foreach {
        case Mismatch(primaryKey, columns) =>
          val diff = Serialization.write(columns)
          writer.writeNext((primaryKey :+ diff).map(_.toString).toArray)
      }
    } finally {
      writer.close()
    }
  }

  /**
   * Compares rows that have the same primary keys
   *
   * @param primaryKeys primary keys
   * @param source Row from the source cluster
   * @param target Row from the target cluster queried by the primary key
   * @return Result matching result
   */
  def compareRow(primaryKeys: Seq[AnyRef], source: CassandraRow, target: CassandraRow): Result = {
    val columns = tableMetadata.getColumns.values.filterNot(tableMetadata.getPrimaryKey.contains)
    val differences = columns.map(c => {
      val columnName = c.getName.asInternal()
      val sourceColumn = source.get[Option[AnyRef]](columnName)
      val targetColumn = target.get[Option[AnyRef]](columnName)

      (sourceColumn, targetColumn) match {
        case (Some(sourceValue: Array[Byte]), Some(targetValue: Array[Byte])) =>
          if (sourceValue.sameElements(targetValue)) {
            None
          } else {
            Some(ColumnDifference(columnName, sourceValue, targetValue))
          }
        case (Some(sourceValue: UDTValue), Some(targetValue: UDTValue)) =>
          // TODO this does not work for blob type inside UDT
          if (sourceValue.toMap == targetValue.toMap) {
            None
          } else {
            Some(ColumnDifference(columnName, sourceValue, targetValue))
          }
        case (Some(sourceValue), Some(targetValue)) =>
            if (sourceValue.equals(targetValue)) {
              None // no difference
            } else {
              Some(ColumnDifference(columnName, sourceValue, targetValue))
            }
        case (Some(sourceValue), None) => Some(ColumnDifference(columnName, sourceValue, null))
        case (None, Some(targetValue)) => Some(ColumnDifference(columnName, null, targetValue))
        case (None, None) => None // no difference
      }
    }).filter(_.isDefined)
    if (differences.isEmpty) {
      Match
    } else {
      Mismatch(primaryKeys, differences.flatten.toSeq)
    }
  }

  def columnsToSelect(writeTime: Boolean, ttl: Boolean): Seq[ColumnRef] = {
    val isCounterTable = tableMetadata.getColumns.values().exists(_.getType == DataTypes.COUNTER)
    val primaryKeys = tableMetadata.getPrimaryKey.map(column => column.getName).toSet
    tableMetadata.getColumns.flatMap {
      case (name, meta) =>
        val columnName = name.asCql(true)
        var cols: Seq[ColumnRef] = Seq(ColumnName(columnName))
        if (!isCounterTable && !primaryKeys.contains(name) && checkWritetimeTtlSupported(meta)) {
          if (writeTime) {
            cols = cols :+ WriteTime(columnName)
          }
          if (ttl) {
            cols = cols :+ TTL(columnName)
          }
        }
        cols
    }.toSeq
  }

  def createTargetSelectStatement(): String = {
    val select = new mutable.StringBuilder("SELECT ")
    select ++= columnsToSelect(writeTime = false, ttl = false).mkString(", ")
    select ++= " FROM "
    select ++= tableMetadata.getKeyspace.asCql(true) + "." + tableMetadata.getName.asCql(true)
    select ++= " WHERE "
    val criteria = tableMetadata.getPrimaryKey.map(column => column.getName.asCql(true) + " = ?").mkString(" AND ")
    select ++= criteria
    select.toString()
  }

  /**
   * WriteTime and TTL for Collection types and non frozen UDTs are not supported
   */
  def checkWritetimeTtlSupported(col: ColumnMetadata): Boolean = {
    col.getType match {
      case _: ListType | _: SetType | _: MapType => false
      case t: UserDefinedType => t.isFrozen
      case _ => true
    }
  }
}
