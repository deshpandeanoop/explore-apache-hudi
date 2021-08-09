package com.explore.apache.hudi

import org.apache.spark.sql.SparkSession

trait HudiService[T] {

  def createTableFromCsv(inputCsvAbsPath : String, hudiTableBasePath : String, hudiTableName : String, sparkSession: SparkSession)

  def showSnapShotView(hudiTablePath : String, sparkSession: SparkSession)

  def upsertTable(upsertDataset : List[T], tableName: String, hudiBasePath : String, sparkSession: SparkSession)

  def deleteDataFromTable(tobeDeletedDataset : List[T], hudiBasePath : String, hudiTableName : String, sparkSession: SparkSession)

  def showIncrementalView(hudiBaseTablePath : String, sparkSession: SparkSession)

  def showPointInTimeView(hudiTablePath : String, sparkSession: SparkSession)
}