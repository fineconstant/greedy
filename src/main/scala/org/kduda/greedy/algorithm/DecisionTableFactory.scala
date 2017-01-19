package org.kduda.greedy.algorithm

import org.apache.spark.sql.DataFrame
import org.kduda.greedy.spark.generic.SparkAware

object DecisionTableFactory extends SparkAware {

  //  TODO: update scaladoc
  /**
    * Produces k decision tables where k is number of attributes in information system.
    *
    * @param is Information system to process.
    */
  def extractDecisionTables(is: DataFrame): Unit = {
    is.cache().createOrReplaceTempView("is")

    //    sql.read.

    //    is.columns.foreach(col => sql.read("SELECT * FROM is") )

  }
}
