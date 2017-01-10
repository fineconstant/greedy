package org.kduda.greedy.spark.reader

import javax.annotation.Nullable

import org.apache.spark.sql.{Dataset, Row}
import org.springframework.core.io.Resource

trait SparkReader {
  def read(resource: Resource, @Nullable options: java.util.Map[String, String]): Dataset[Row]
}
