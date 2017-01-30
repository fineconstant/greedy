package org.kduda.greedy.algorithm

import org.apache.spark.sql.DataFrame
import org.kduda.greedy.spark.generic.SparkAware

object HeuristicsRM extends DecisionRulesCalculator with SparkAware {
  override def calculateDecisionRules(dts: Map[String, DataFrame]): Map[String, List[List[(String, String)]]] = {

    null
  }
}
