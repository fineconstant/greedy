package org.kduda.greedy.algorithm

import org.apache.spark.sql.{Dataset, Row}

object DecisionTableFactory {

  def extractDecisionTables(informationSystem: Dataset[Row]): Unit = {
    informationSystem.cache()

    informationSystem.columns.foreach(s => Console.println(s))
  }
}
