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
    * @return ArrayBuffer containing cashed DataFrames, each of them is a decision table.
    */
  def extractDecisionTables(is: DataFrame): Array[DataFrame] = {
    is.cache().createOrReplaceTempView("is")
    val isColumns = is.columns

    val dtsColumns = isColumns.map(col => {
      var dtColumns = ArrayBuffer.empty[String]
      dtColumns.insertAll(0, isColumns)
      dtColumns -= col
      dtColumns += col
      dtColumns
    })

    val stringDtsColumns = dtsColumns.map(dt => dt.mkString(","))

    val dts = stringDtsColumns.map(columns => {
      sql.sql(s"SELECT $columns FROM is").cache()
    })

    is.unpersist()

    dts
  }

  /**
    * Maps decision tables by their decision attribute as a key.
    *
    * @param dts Decision tables (DataFrames) to be mapped.
    * @return Map[decision attribute -> decision table].
    */
  def createMapOf(dts: Array[DataFrame]): Map[String, DataFrame] = {
    dts.map { case (dt) => (dt.columns.last, dt) }.toMap
  }

  /**
    * Removes all of the inconsistencies from given decision tables.
    *
    * @param dts A Map of decision tables to be filtered.
    * @return Mapped decision table (DataFrame) without inconsistencies.
    */
  def removeInconsistencies(dts: Map[String, DataFrame]): Map[String, DataFrame] = {
    dts.map {
      case (key, value) =>
        val columns = value.columns.dropRight(1)
        (key, value.dropDuplicates(columns).cache())
    }
  }

  /**
    * Removes all of the inconsistencies from given decision tables.
    *
    * @param dts An Array of decision tables to be filtered.
    * @return Array of decison tables (DataFrames) without inconsistencies.
    */
  def removeInconsistencies(dts: Array[DataFrame]): Array[DataFrame] = {
    dts.map(dt => {
      val columns = dt.columns.dropRight(1)
      dt.dropDuplicates(columns).cache()
    })
  }
}
