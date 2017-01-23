package org.kduda.greedy.algorithm


import org.apache.spark.sql.DataFrame
import org.kduda.greedy.spark.generic.SparkAware

object HeuristicsM extends SparkAware {

  def calculateDecisionRules(dts: Map[String, DataFrame]): Map[String, Option[DataFrame]] = {
    for ((key, dt) <- dts) {

    }
    dts.map { case (key, dt) =>
      dt.cache()
      dt.show() //FIXME: debug

      val allCols = dt.columns
      val conditionCols = allCols.dropRight(1)
      val decisionCol = allCols.last


      // if degenerated or empty return Optional.empty
      if (dt.select(decisionCol).distinct().count() <= 1)
        return Map(decisionCol -> Option.empty[DataFrame])
      // continue calculations
      else {

        Console.println("calculating")

        //      dt.foreach(row => {
        //        val rowDecision = (decisionCol, row.getAs[String](decisionCol))
        //
        //        var x = mutable.HashMap.empty[String, Long]
        //        for (col <- conditionCols) {
        //          Console.println(dt.col(col))
        //
        //        }
        //      })


        // return result as map of Optional value
        (key, Option.apply(dt))
      }
    }
  }
}