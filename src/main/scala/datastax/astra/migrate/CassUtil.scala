package datastax.astra.migrate

import com.datastax.oss.driver.api.core.CqlIdentifier
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata
import com.datastax.spark.connector.CassandraRowMetadata

import collection.JavaConversions._

object CassUtil {

  def quoteWrap(string: String) = {
    "\"" + string + "\""
  }

  def metadataToStatement(metadata: TableMetadata, keyspaceName: String): String =
  {

    val tableName = quoteWrap(metadata.getName.toString)


    val columns = metadata.getColumns
    val columnsString = mapAsScalaMap(columns).map(x => {
      (quoteWrap(x._2.getName.toString) + " " + x._2.getType.asCql(true,true))
    }).mkString(",")

    val parititionKeys = metadata.getPartitionKey
    val partitionKeyString = parititionKeys.map(x => quoteWrap(x.getName.toString)).mkString(",")

    val clusteringColumns= metadata.getClusteringColumns
    val clusteringColumnString = clusteringColumns.map(x => quoteWrap(x._1.getName.toString)).mkString(",")

    val primaryKey = if (clusteringColumns.isEmpty){
      "PRIMARY KEY ((" + partitionKeyString + "))";
    }else {
      "PRIMARY KEY ((" + partitionKeyString + "), " + clusteringColumnString + ")";
    }

    //TODO: add support for indexes
    return "CREATE TABLE IF NOT EXISTS " + keyspaceName + "." + tableName + " (" + columnsString +", "+ primaryKey + ");"

  }
}