package org.kduda.greedy.spark

import org.apache.spark.sql.SparkSession

object GreedySparkInstance {

  val sc = SparkSession.builder()
    .appName("greedy-spark")
    .master("local[4]")
    .getOrCreate()

  val sql = sc.sqlContext
}
