package org.kduda.greedy.algorithm


import org.apache.spark.sql.DataFrame
import org.kduda.greedy.spark.generic.SparkAware

import scala.collection.mutable.ArrayBuffer
import scala.collection.{SortedMap, mutable}

object HeuristicsM extends SparkAware {

  def calculateDecisionRules(dts: Map[String, DataFrame]): Map[String, Option[DataFrame]] = {
    for ((key, dt) <- dts) {

    }
    // FIXME: remove drop
    dts.dropRight(2).map { case (key, dt) =>
      dt.cache()
      dt.show() // FIXME: debug

      val allCols = dt.columns
      val conditionCols = allCols.dropRight(1)
      val decisionCol = allCols.last

      // if degenerated or empty return Optional.empty
      if (dt.select(decisionCol).distinct().count() <= 1)
        return Map(decisionCol -> Option.empty[DataFrame])
      // continue calculations
      else {

        val rows = dt.collect()

        // calculating for each row here
        for (currentRow <- rows) {
          val decision = currentRow.getAs[String](decisionCol)


          // potential columns with their M calculated
          val potential = ArrayBuffer.empty[(Int, String, String)]

          // calculating M
          for (col <- conditionCols) {
            val NT = rows.filter(row => row.getAs[String](col) == currentRow.getAs[String](col))
            val NTCount = NT.length
            val NTA = NT.filter(row => row.getAs[String](decisionCol) == decision)
            val NTACount = NTA.length
            val M = NTCount - NTACount

            potential += Tuple3(M, col, currentRow.getAs[String](col))
          }
          val sortedPotential = potential.sortWith(_._1 < _._1)


          // row result () ^ () ^ ... ^ () -> ()
          val x = Array(
            (sortedPotential.head._2, sortedPotential.head._3), (decisionCol, currentRow.getAs[String](decisionCol)))

          x.foreach(Console.println(_))
          Console.println()
          // TODO: make it work in loop until subtable is empty or more than one decision
        }


        // return result as map of Optional value
        (key, Option.apply(dt))
      }
    }
  }
}