package com.explore.apache.parquet

import com.explore.apache.beans
import com.explore.apache.utils.SparkJobHelper

object ParquetWriter extends App {

  private val _productAbsOutputPath = "<absoluteOutputDirectoryPath>"

  val sparkSession = SparkJobHelper.getLocalSparkSession("parquet-writer")

  import sparkSession.implicits._

  sparkSession
    .createDataset(constructDummyProducts)
    .coalesce(1)
    .write
    .option("header", "true")
    .parquet(_productAbsOutputPath)


  private def constructDummyProducts : List[beans.Product] = {
    (1 to 100)
      .map(integer => new beans.Product(integer, s"Product - ${integer}", s"Product - ${integer} description"))
      .toList
  }
}