package org.kduda.greedy.algorithm

import org.apache.spark.sql.DataFrame
import org.kduda.greedy.spark.generic.SparkAware

import scala.collection.mutable.ArrayBuffer

object DecisionTableFactory extends SparkAware {

  /**
    * Produces k decision tables where k is number of attributes in information system.
    * The last attribute of each decision table is a decision attribute, preceding attributes are conditional attributes.
    *
    * @param is Information system to process.
    * @return ArrayBuffer containing DataFrames, each of them is a decision table.
    */
  def extractDecisionTables(is: DataFrame): Array[DataFrame] = {
    is.cache().createOrReplaceTempView("is")
    val isColumns = is.columns

    val dtColumns = isColumns.map(col => {
      var result = new ArrayBuffer[String]
      result.insertAll(0, isColumns)
      result -= col
      result += col
      result
    })

    val stringDtColumns = dtColumns.map(dt => dt.mkString(","))

    var dts = new ArrayBuffer[DataFrame]()
    for (cols <- stringDtColumns) dts += sql.sql(s"SELECT $cols FROM is")

    dts.toArray
  }
}
