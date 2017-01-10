package org.kduda.greedy.spark.reader

import org.apache.spark.sql.{Dataset, Row}
import org.kduda.greedy.spark.generic.SparkAware
import org.springframework.core.io.Resource

class CsvResourceSparkReader extends SparkReader with SparkAware {

  override def read(resource: Resource, options: java.util.Map[String, String]): Dataset[Row] = {
    if (options == null)
      sql.read.csv(resource.getURL.getPath)
    else
      sql.read.options(options).csv(resource.getURL.getPath)
  }
}
