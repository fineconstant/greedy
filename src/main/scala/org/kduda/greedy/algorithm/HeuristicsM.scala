package org.kduda.greedy.algorithm


import org.apache.spark.sql.{DataFrame, Row}
import org.kduda.greedy.algorithm.util.GreedyUtils
import org.kduda.greedy.spark.generic.SparkAware

import scala.collection.mutable.ArrayBuffer

object HeuristicsM extends SparkAware {


  // TODO: define returning type
  def calculateDecisionRules(dts: Map[String, DataFrame]): Map[String, Option[DataFrame]] = {
    for ((key, dt) <- dts) {

    }
    // FIXME: remove dropRight
    dts.dropRight(2).map { case (key, dt) =>
      val result = ArrayBuffer.empty[List[(String, String)]]

      dt.cache()
      dt.show() // FIXME: remove

      val allCols = dt.columns
      val conditionCols = allCols.dropRight(1)

      val decisionCol = allCols.last
      // if degenerated or empty return Optional.empty
      if (dt.select(decisionCol).distinct().count() <= 1)
        return Map(decisionCol -> Option.empty[DataFrame])
      // else continue calculations
      else {

        val dtRows = dt.collect()

        // calculating for each row here
        for (dtRow <- dtRows) {
          Console.println(s"\nrow: $dtRow")
          // FIXME: remove
          val decision = dtRow.getAs[String](decisionCol)

          // potential columns with their M calculated, format: (M, column, value)
          val candidates = ArrayBuffer.empty[(Long, String, String)]
          // calculating M
          for (col <- conditionCols) {
            // N(T)
            val NT = dtRows.filter(row => row.getAs[String](col) == dtRow.getAs[String](col))
            val NTCount = NT.length
            // N(T, a)
            val NTA = NT.filter(row => row.getAs[String](decisionCol) == decision)
            val NTACount = NTA.length

            // M = N(T), - N(T, a)
            val M = NTCount - NTACount
            candidates += Tuple3(M, col, dtRow.getAs[String](col))
          }
          // order by the descending of value of M and get first item - it is the chosen column

          val chosenCol = candidates.sortWith(_._1 < _._1).head

          // row result () <- () ^ ... () ^ ()
          // head - decision
          // tail - condition
          val decisionRule = List((decisionCol, dtRow.getAs[String](decisionCol)), (chosenCol._2, chosenCol._3))
          result += decisionRule


          // create separable subtable for created decision rule
          val conditions = decisionRule.tail
          val dtFilteredRows = GreedyUtils.filterByColumns(dtRows, conditions)

          // check the number of distinct decision is separable subtable
          // if greater than 1 then add another condition col
          var distinctDecisions = GreedyUtils.countDistinctValuesIn(dtFilteredRows, decisionCol)

          // FIXME: remove
          Console.println("filtered rows")
          for (row <- dtFilteredRows) {
            Console.println(row)
          }
          println(s"distinctDecisions = $distinctDecisions")


          do {
            // calculations
            distinctDecisions += 1
          }
          while (distinctDecisions > 1)


          /** TODO: make it work in loop until subtable is empty or decision is a common decision for table
            * (all decisions identical identical in  subtable)
            */

        }

        //result.foreach(Console.println(_))


        // return result as map of Optional value
        (key, Option.apply(dt))
      }
    }
  }
}