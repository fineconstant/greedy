package org.kduda.greedy.algorithm

import org.apache.spark.sql.DataFrame
import org.kduda.greedy.spark.generic.SparkAware

object HeuristicsM extends SparkAware {

  def calculateDecisionRules(dts: Array[DataFrame]): Unit = {
    val dt = dts(0)

    dt.show()
  }
}
