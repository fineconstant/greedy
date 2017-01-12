package org.kduda.greedy.spark

import org.apache.commons.configuration2.builder.fluent.Configurations
import org.apache.spark.sql.SparkSession

object GreedySparkInstance {
  private val configs = new Configurations()
  private val config = configs.properties("spark.properties")

  private val appName = config.getString("greedy.spark.appName")
  private val master = config.getString("greedy.spark.master")

  val sc = SparkSession.builder()
    .appName(appName)
    .master(master)
    .getOrCreate()

  val sql = sc.sqlContext
}
