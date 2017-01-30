package org.kduda.greedy.algorithm

import org.apache.spark.sql.DataFrame
import org.kduda.greedy.algorithm.util.GreedyUtils
import org.kduda.greedy.spark.generic.SparkAware

import scala.collection.mutable.ArrayBuffer

object HeuristicsM extends SparkAware {

  def calculateDecisionRules(dts: Map[String, DataFrame]): Map[String, List[List[(String, String)]]] = {
    dts.map {
      case (key, dt) =>
        val result: ArrayBuffer[List[(String, String)]] = ArrayBuffer.empty
        dt.cache()

        val ALL_COLUMNS = dt.columns
        val CONDITION_COLUMNS: ArrayBuffer[String] = ArrayBuffer(ALL_COLUMNS.dropRight(1): _*)
        val DECISION_COLUMN = key

        // if degenerated or empty return empty array
        if (dt.select(DECISION_COLUMN).distinct().count() <= 1)
          (key, List.empty[List[(String, String)]])
        else {

          // TODO: sprawdzic czy generowane z glownej tablicy z z podtablic tworzonych potem nizej
          var dtRows = dt.collect()

          // calculating decision rule for each row
          for (dtRow <- dtRows) {
            val decision = dtRow.getAs[String](DECISION_COLUMN)
            val conditionCols = CONDITION_COLUMNS.clone()

            var distinctDecisions = 0
            var decisionRule = ArrayBuffer((DECISION_COLUMN, dtRow.getAs[String](DECISION_COLUMN)))
            do {
              // potential columns with their M calculated, format: (M, column, value)
              val colsWithM = ArrayBuffer.empty[(Long, String, String)]

              // calculating M
              for (col <- conditionCols) {
                // N(T)
                val NT = dtRows.filter(row => row.getAs[String](col) == dtRow.getAs[String](col))
                val NTCount = NT.length
                // N(T, a)
                val NTA = NT.filter(row => row.getAs[String](DECISION_COLUMN) == decision)
                val NTACount = NTA.length

                // M = N(T), - N(T, a)
                val M = NTCount - NTACount
                colsWithM += Tuple3(M, col, dtRow.getAs[String](col))
              }
              // order by the descending of value of M and get first item - it is the chosen column
              val chosenCol = colsWithM.sortWith(_._1 < _._1).head

              // row result (support, value) () <- () ^ ... () ^ ()
              // head - support
              // head - decision
              // tail - condition
              decisionRule += Tuple2(chosenCol._2, chosenCol._3)

              // create separable subtable for created decision rule and check its decisions
              val conditions = decisionRule.tail
              val separableSubtable = GreedyUtils.filterByColumns(dtRows, conditions.toList)

              // prepare for next iteration
              distinctDecisions = GreedyUtils.countDistinctValuesIn(separableSubtable, DECISION_COLUMN)
              if (distinctDecisions > 1)
                conditionCols -= chosenCol._2

            }
            while (distinctDecisions > 1)

            val resultWithSupport = GreedyUtils.calculateSupport(dtRows, decisionRule)

            result += resultWithSupport
          }

          (key, result.toList)
        }
    }
  }
}