package org.kduda.greedy.spark.reader.mongo

import org.apache.spark.sql.{Dataset, Row}

trait SparkMongoService {

  def readCsvByName(name: String): Dataset[Row]

  def readCsvById(id: String): Dataset[Row]
}
