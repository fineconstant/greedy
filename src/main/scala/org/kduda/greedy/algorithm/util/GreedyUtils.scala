package org.kduda.greedy.algorithm.util

import org.apache.spark.sql.Row

object GreedyUtils {

  /**
    * Filters rows' columns so they only contain values passed in columns parameter.
    *
    * @param rows Data to be filtered.
    * @param columns Columns and their values in format Tuple2(column, value)
    * @return Filtered rows.
    */
  def filterByColumns(rows: Array[Row], columns: List[(String, String)]): Array[Row] = {
    var result = rows
    columns.foreach(condition => {
      result = result.filter(row => row.getAs[String](condition._1) == condition._2)
    })
    result
  }

  /**
    * Counts the number of partitions created per passed column.
    *
    * @param dtFilteredRows Data as rows.
    * @param columnName Name of the column that should data be partitioned by.
    * @return Number of partitions (groups) per column.
    */
  def countDistinctValuesIn(dtFilteredRows: Array[Row], columnName: String): Int = {
    dtFilteredRows
    .groupBy(row => row.getAs[String](columnName))
    .keys
    .size
  }
}
