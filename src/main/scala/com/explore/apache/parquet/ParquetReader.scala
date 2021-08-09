package com.explore.apache.parquet

import com.explore.apache.utils.SparkJobHelper

object ParquetReader extends App {

  private val _productAbsInputPath = "<absoluteInputDirectoryPath>"

  val sparkSession = SparkJobHelper.getLocalSparkSession("parquet-reader")

  sparkSession
    .read
    .parquet(_productAbsInputPath)
    .show(10)
}